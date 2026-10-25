/**
 * The `ticket-import` queue consumer (#8313 W4; spec
 * docs/superpowers/specs/api-platform/2026-10-19-ticket-import-modules-design.md
 * §5.8). One job per slice of a run.
 *
 * - The slice runs only under the per-partner + global lease
 *   (services/ticketImport/importLease.ts). A busy lease defers the job.
 * - A slice that worked its TICKET_IMPORT_SLICE_MS hands the run to seq + 1
 *   with a compare-and-set, and this worker enqueues that successor: the
 *   first job in the repo that continues itself (F24). A retried slice whose
 *   successor already took over exits, re-enqueueing the successor in case
 *   the first enqueue was lost (a plain add under the successor's id is a
 *   no-op when it exists).
 * - Deferrals spend no attempt (F22): a busy lease, a 429 from either window
 *   bucket, or a source Retry-After move the job to delayed with
 *   `moveToDelayed` + `DelayedError`, the mechanism of
 *   jobs/accountingJobDelay.ts, kept local so its log names this queue.
 * - Worker options are F24's long-job set. A lost lease or a shutdown makes
 *   the slice stop at its next ticket boundary.
 */
import { DelayedError, Worker, type Job } from 'bullmq';
import { withDbAccessContext } from '../db';
import { assertQueueJobName, parseQueueJobData } from '../services/bullmqValidation';
import { getBullMQConnection } from '../services/redis';
import { acquireImportLease, type ImportLease } from '../services/ticketImport/importLease';
import { ticketImportLimits } from '../services/ticketImport/importLimits';
import {
  closeTicketImportQueue,
  enqueueTicketImportSlice,
  TICKET_IMPORT_QUEUE,
  TICKET_IMPORT_SLICE_JOB,
} from '../services/ticketImport/importQueue';
import { runSlice, type SliceDeps, type SliceOutcome } from '../services/ticketImport/importRunner';
import { failRun, runContext } from '../services/ticketImport/runLedger';
import { ticketImportSliceJobDataSchema, type TicketImportSliceJobData } from './queueSchemas';
import { attachWorkerObservability } from './workerObservability';

const WORKER_NAME = 'ticketImportWorker';
/** A busy lease waits this long, plus jitter, before the slice tries again. */
const LEASE_RETRY_MS = 10_000;
const MAX_DEFER_MS = 24 * 60 * 60 * 1000;

let worker: Worker | null = null;
let stopping = false;

export type ProcessSliceDeps = {
  acquireLease?: typeof acquireImportLease;
  runSlice?: typeof runSlice;
  enqueue?: (data: TicketImportSliceJobData) => Promise<void>;
  sliceDeps?: SliceDeps;
};

export type ProcessSliceResult = SliceOutcome | { kind: 'lease_busy' };

/** moveToDelayed(now + delay) + DelayedError: the job waits and keeps its attempts. */
async function deferSlice(job: Job, token: string | undefined, delayMs: number, reason: string): Promise<never> {
  const delay = Math.min(Math.max(1_000, Number.isFinite(delayMs) ? delayMs : LEASE_RETRY_MS), MAX_DEFER_MS);
  if (!token) {
    // Without the lock token the job cannot be moved; a normal retry applies.
    throw new Error(`ticket import slice deferred (${reason}) without a lock token`);
  }
  await job.moveToDelayed(Date.now() + delay, token);
  console.warn(`[${WORKER_NAME}] slice deferred without spending an attempt`, { jobId: job.id, reason, delayMs: delay });
  throw new DelayedError();
}

export async function processTicketImportSlice(job: Job, token?: string, deps: ProcessSliceDeps = {}): Promise<ProcessSliceResult> {
  assertQueueJobName(TICKET_IMPORT_QUEUE, job, TICKET_IMPORT_SLICE_JOB);
  const data = parseQueueJobData(TICKET_IMPORT_QUEUE, job, ticketImportSliceJobDataSchema);
  const limits = ticketImportLimits();
  const enqueue = deps.enqueue ?? ((next: TicketImportSliceJobData) => enqueueTicketImportSlice(next));

  const lease: ImportLease | null = await (deps.acquireLease ?? acquireImportLease)({
    partnerId: data.partnerId,
    globalCap: limits.globalConcurrency,
    leaseMs: limits.leaseMs,
  });
  if (!lease) {
    await deferSlice(job, token, LEASE_RETRY_MS + Math.floor(Math.random() * 5_000), 'lease_busy');
    return { kind: 'lease_busy' };
  }

  let outcome: SliceOutcome;
  try {
    outcome = await (deps.runSlice ?? runSlice)(data, {
      ...deps.sliceDeps,
      shouldYield: () => stopping || !lease.held() || (deps.sliceDeps?.shouldYield?.() ?? false),
    });
  } catch (error) {
    // The last attempt on an unexpected error fails the run, so it does not
    // hold its source's one active run forever.
    const attempts = job.opts.attempts ?? 1;
    if (job.attemptsMade + 1 >= attempts) {
      await withDbAccessContext(runContext(data.partnerId), () => failRun(data.partnerId, data.runId, 'worker_error'))
        .catch((failError: unknown) => console.error(`[${WORKER_NAME}] could not fail run ${data.runId}`, failError));
    }
    throw error;
  } finally {
    await lease.release();
  }

  switch (outcome.kind) {
    case 'continue':
      await enqueue({ ...data, seq: data.seq + 1 });
      break;
    case 'superseded':
      // A retried slice whose successor took over: make sure the successor exists.
      if (outcome.active && outcome.sliceSeq === data.seq + 1) await enqueue({ ...data, seq: data.seq + 1 });
      break;
    case 'defer':
      await deferSlice(job, token, outcome.delayMs, outcome.reason);
      break;
    case 'finished':
      break;
  }
  return outcome;
}

export async function initializeTicketImportWorker(): Promise<void> {
  if (worker) return;
  stopping = false;
  worker = new Worker(
    TICKET_IMPORT_QUEUE,
    (job: Job, token?: string) => processTicketImportSlice(job, token),
    {
      connection: getBullMQConnection(),
      concurrency: ticketImportLimits().workerConcurrency,
      lockDuration: 300_000,
      stalledInterval: 60_000,
      maxStalledCount: 2,
    },
  );
  attachWorkerObservability(worker, WORKER_NAME);
}

export async function shutdownTicketImportWorker(): Promise<void> {
  // Running slices stop at their next ticket boundary and hand over.
  stopping = true;
  if (worker) {
    await worker.close();
    worker = null;
  }
  await closeTicketImportQueue();
}

export const __testOnly = { WORKER_NAME, setStopping: (value: boolean) => { stopping = value; } };
