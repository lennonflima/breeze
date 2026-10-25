import type { ImportedLink } from './moduleContract';

/**
 * A run's position (#8313 W4; spec 2026-10-19 §5.8, §5.9), stored in
 * `ticket_import_runs.cursor` and saved with every commit of a slice, in the
 * same transaction as the ledger rows it covers:
 *
 * - `scan.page` is the module's cursor of the page being processed, and
 *   `scan.index` how many of its rows are done. A crash mid-page re-reads the
 *   same page and skips the rows already committed; the ledger makes a row
 *   reprocessed after a source shift cost nothing.
 * - `links.queue` holds the links conversions asked for, for the links phase
 *   (only when an import-mode link writer exists, W2).
 * - `deletions.after` is the keyset position over the source's unseen
 *   imported items.
 * - `sourceFailures` counts consecutive source failures after retries; at
 *   SOURCE_FAILURE_LIMIT the run stops (`source_unavailable`).
 * - `ownersSeen` keeps `owner_unmatched` once per distinct owner per run.
 * - `clockSkewMs` is measured once per run, at its first slice's connect.
 */

export const SOURCE_FAILURE_LIMIT = 20;
/** Distinct unmatched owners remembered per run; past it an owner may be reported twice. */
const OWNERS_SEEN_MAX = 20_000;

export type QueuedLink = Omit<ImportedLink, 'at'> & { at: string; sourceVersion: string };

export type RunCursor = {
  scan: { page: unknown; index: number; done: boolean };
  links: { queue: QueuedLink[]; index: number };
  deletions: { after: string | null };
  sourceFailures: number;
  ownersSeen: string[];
  clockSkewMs: number | null | undefined;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function nonNegativeInt(value: unknown): number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : 0;
}

export function normalizeCursor(raw: unknown): RunCursor {
  const c = isRecord(raw) ? raw : {};
  const scan = isRecord(c.scan) ? c.scan : {};
  const links = isRecord(c.links) ? c.links : {};
  const deletions = isRecord(c.deletions) ? c.deletions : {};
  return {
    scan: { page: scan.page ?? null, index: nonNegativeInt(scan.index), done: scan.done === true },
    links: { queue: Array.isArray(links.queue) ? (links.queue as QueuedLink[]) : [], index: nonNegativeInt(links.index) },
    deletions: { after: typeof deletions.after === 'string' ? deletions.after : null },
    sourceFailures: nonNegativeInt(c.sourceFailures),
    ownersSeen: Array.isArray(c.ownersSeen) ? (c.ownersSeen as unknown[]).filter((v): v is string => typeof v === 'string') : [],
    clockSkewMs: typeof c.clockSkewMs === 'number' ? c.clockSkewMs : c.clockSkewMs === null ? null : undefined,
  };
}

/** Records a distinct unmatched owner; true when this run had not seen it yet. */
export function rememberOwner(cursor: RunCursor, sourceUserKey: string): boolean {
  if (cursor.ownersSeen.includes(sourceUserKey)) return false;
  if (cursor.ownersSeen.length < OWNERS_SEEN_MAX) cursor.ownersSeen.push(sourceUserKey);
  return true;
}
