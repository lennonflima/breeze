import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import { runOutsideDbContext } from '../../db';
import { getRedis } from '../redis';

/**
 * The per-partner and global import lease (#8313 W4; spec 2026-10-19 §5.8),
 * built on the ZSET lease of services/accounting/accountingRateLimit.ts
 * (F22): each key holds one token per holding slice, scored by its expiry.
 *
 * | key                                       | cap                               |
 * | ticket-import:lease:partner:<partnerId>   | 1                                 |
 * | ticket-import:lease:global                | TICKET_IMPORT_GLOBAL_CONCURRENCY  |
 *
 * - Acquire is one Lua script over both keys: drop expired members, then add
 *   the token to both only if both are under their cap.
 * - Renew (a heartbeat every lease / 3) re-scores the token in both keys and
 *   extends both keys' TTL, whose own expiry would otherwise lapse one lease
 *   after acquire (the precedent sets it only there); a token missing from
 *   either key means the lease was lost.
 * - Release removes the token from both, in a `finally`.
 * - A crash frees the lease within one lease length.
 * - Fails closed: with no Redis nothing is acquired (BullMQ needs it anyway).
 *
 * The lease is for fairness, not correctness: two slices of one principal
 * writing at once are already safe (the unique indexes, the writer's locked
 * re-checks, one active run per source). A lost lease stops the slice at the
 * next ticket boundary.
 *
 * Scores use the client clock, as the precedent does (older self-hosted Redis
 * refuses TIME before a write in a script); skew between API instances only
 * shifts when an abandoned lease is reclaimed.
 */

export const TICKET_IMPORT_GLOBAL_LEASE_KEY = 'ticket-import:lease:global';

export function ticketImportPartnerLeaseKey(partnerId: string): string {
  return `ticket-import:lease:partner:${partnerId}`;
}

/** The per-partner cap: one slice of a partner at a time. */
export const TICKET_IMPORT_PARTNER_LEASE_CAP = 1;

// KEYS[1] partner set, KEYS[2] global set; ARGV: partner cap, global cap,
// lease ms, token, now ms. Returns 1 acquired, 0 refused.
export const LEASE_ACQUIRE_LUA = `
local now = tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) and redis.call('ZCARD', KEYS[2]) < tonumber(ARGV[2]) then
  local expiry = now + tonumber(ARGV[3])
  redis.call('ZADD', KEYS[1], expiry, ARGV[4])
  redis.call('ZADD', KEYS[2], expiry, ARGV[4])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return 1
end
return 0`;

// KEYS as above; ARGV: lease ms, token, now ms. Returns 1 renewed, 0 lost
// (the token is then removed from whichever key still held it).
export const LEASE_RENEW_LUA = `
local now = tonumber(ARGV[3])
local a = redis.call('ZSCORE', KEYS[1], ARGV[2])
local b = redis.call('ZSCORE', KEYS[2], ARGV[2])
if a and b and tonumber(a) > now and tonumber(b) > now then
  local expiry = now + tonumber(ARGV[1])
  redis.call('ZADD', KEYS[1], 'XX', expiry, ARGV[2])
  redis.call('ZADD', KEYS[2], 'XX', expiry, ARGV[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  redis.call('PEXPIRE', KEYS[2], ARGV[1])
  return 1
end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[2])
return 0`;

// KEYS as above; ARGV: token. Removing an absent token is a no-op.
export const LEASE_RELEASE_LUA = `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1`;

export type ImportLease = {
  readonly token: string;
  readonly partnerId: string;
  /** False once a renewal found the token gone (or Redis refused it): stop at the next ticket boundary. */
  held(): boolean;
  /** Stops the heartbeat and removes the token from both keys. Never throws. */
  release(): Promise<void>;
};

export type AcquireLeaseOptions = {
  partnerId: string;
  globalCap: number;
  leaseMs: number;
  /** Tests inject these; production uses the shared client and the wall clock. */
  redis?: Redis | null;
  now?: () => number;
  /** Heartbeat period; default leaseMs / 3. */
  renewEveryMs?: number;
};

function keysFor(partnerId: string): [string, string] {
  return [ticketImportPartnerLeaseKey(partnerId), TICKET_IMPORT_GLOBAL_LEASE_KEY];
}

/**
 * Takes the lease, or returns null when either cap is full or Redis is not
 * available. While held, a heartbeat renews it every `renewEveryMs`.
 */
export async function acquireImportLease(options: AcquireLeaseOptions): Promise<ImportLease | null> {
  const redis = options.redis === undefined ? getRedis() : options.redis;
  if (!redis) return null;
  const now = options.now ?? Date.now;
  const token = randomUUID();
  const [partnerKey, globalKey] = keysFor(options.partnerId);
  let acquired: unknown;
  try {
    acquired = await runOutsideDbContext(() => redis.eval(
      LEASE_ACQUIRE_LUA,
      2,
      partnerKey,
      globalKey,
      String(TICKET_IMPORT_PARTNER_LEASE_CAP),
      String(Math.max(1, options.globalCap)),
      String(options.leaseMs),
      token,
      String(now()),
    ));
  } catch (error) {
    console.error('[importLease] acquire failed; refusing the slice', error instanceof Error ? error.message : error);
    return null;
  }
  if (Number(acquired) !== 1) return null;

  let held = true;
  let renewing: Promise<void> | null = null;
  const renew = async () => {
    if (!held) return;
    try {
      const ok = await runOutsideDbContext(() => redis.eval(LEASE_RENEW_LUA, 2, partnerKey, globalKey, String(options.leaseMs), token, String(now())));
      if (Number(ok) !== 1) held = false;
    } catch (error) {
      // A renewal that cannot reach Redis cannot prove the lease is still
      // ours; the slice stops at its next ticket boundary.
      console.error('[importLease] renew failed; treating the lease as lost', error instanceof Error ? error.message : error);
      held = false;
    }
  };
  const timer = setInterval(() => {
    renewing = renew();
  }, Math.max(100, options.renewEveryMs ?? Math.floor(options.leaseMs / 3)));
  timer.unref?.();

  return {
    token,
    partnerId: options.partnerId,
    held: () => held,
    async release() {
      clearInterval(timer);
      held = false;
      if (renewing) await renewing.catch(() => undefined);
      try {
        await runOutsideDbContext(() => redis.eval(LEASE_RELEASE_LUA, 2, partnerKey, globalKey, token));
      } catch (error) {
        // The token expires on its own within one lease.
        console.error('[importLease] release failed; the lease expires on its own', error instanceof Error ? error.message : error);
      }
    },
  };
}

/** Test and ops helper: renews a token once, as the heartbeat does. */
export async function renewImportLeaseOnce(redis: Redis, partnerId: string, token: string, leaseMs: number, nowMs: number): Promise<boolean> {
  const [partnerKey, globalKey] = keysFor(partnerId);
  return Number(await redis.eval(LEASE_RENEW_LUA, 2, partnerKey, globalKey, String(leaseMs), token, String(nowMs))) === 1;
}
