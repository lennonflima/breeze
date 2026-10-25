import { afterEach, describe, expect, it, vi } from 'vitest';
import { MigrationWindowError } from '../migrationWindows';
import { TicketServiceError } from '../ticketService';
import type { ImportedTicket } from './importedTicket';
import type { ImportTicketResult } from './importWriter';
import { ticketImportLimits } from './importLimits';
import { normalizeCursor, rememberOwner } from './runCursor';
import {
  applyNotes,
  clampTicketTimes,
  classifyImportError,
  classifySourceError,
  importedTicketBytes,
  predictTicket,
  recordImportResult,
} from './runOutcomes';
import { emptyCounters, IssueBuffer, normalizeCounters } from './runReport';
import { SourceAuthError, SourceHttpError, SourceResponseTooLargeError, SourceUnavailableError } from './sourceFetch';

const NOW = new Date('2026-10-25T12:00:00.000Z');
const at = (msFromNow: number) => new Date(NOW.getTime() + msFromNow);

function ticket(overrides: Partial<ImportedTicket> = {}): ImportedTicket {
  return {
    sourceId: '1',
    orgId: '11111111-1111-4111-8111-111111111111',
    createdAt: at(-60_000),
    updatedAt: at(-30_000),
    subject: 'Subject',
    description: 'Body',
    priority: 'normal',
    source: 'manual',
    requester: { name: 'R' },
    finalStatus: 'open',
    comments: [{ sourceId: 'article:1', createdAt: at(-10_000), isPublic: true, author: { kind: 'staff', name: 'A' }, content: 'Hi' }],
    ...overrides,
  };
}

const result = (overrides: Partial<ImportTicketResult> = {}): ImportTicketResult => ({
  ticketId: 't',
  outcome: 'created',
  commentsWritten: 2,
  commentsSkipped: 1,
  fieldsUpdated: 0,
  statusSkipped: false,
  attachmentsWritten: 1,
  attachmentsSkipped: [],
  attachmentBytes: 300,
  requesterChanged: false,
  orgChanged: false,
  commentsChangedInSource: [],
  commentsMissingInSource: [],
  ...overrides,
});

afterEach(() => vi.unstubAllEnvs());

describe('clampTicketTimes (spec §5.2)', () => {
  it('leaves past times alone', () => {
    const t = ticket();
    expect(clampTicketTimes(t, NOW, 300_000)).toEqual({ kind: 'ok', ticket: t, clamped: false });
  });

  it('clamps a time up to the skew to now and says so', () => {
    const out = clampTicketTimes(ticket({ updatedAt: at(60_000), comments: [] }), NOW, 300_000);
    expect(out).toMatchObject({ kind: 'ok', clamped: true });
    expect(out.kind === 'ok' && out.ticket.updatedAt.getTime()).toBe(NOW.getTime());
  });

  it('fails a ticket whose time is beyond the skew', () => {
    expect(clampTicketTimes(ticket({ closedAt: at(301_000) }), NOW, 300_000)).toEqual({ kind: 'clock_skew' });
  });
});

describe('importedTicketBytes', () => {
  it('counts UTF-8 bytes of the content a ticket carries', () => {
    const base = importedTicketBytes(ticket({ comments: [] }));
    expect(importedTicketBytes(ticket({ comments: [], subject: 'Subject€' }))).toBe(base + 3);
  });
});

describe('applyNotes and recordImportResult (spec §5.10)', () => {
  it('reports an unmatched owner once per run, and counts the rest', () => {
    const counters = emptyCounters();
    const issues = new IssueBuffer(counters, 100);
    const cursor = normalizeCursor({});
    const target = { key: 'k', number: '#1' };
    applyNotes([
      { kind: 'owner_unmatched', sourceUserKey: '42' },
      { kind: 'owner_unmatched', sourceUserKey: '42' },
      { kind: 'requester_without_contact' },
      { kind: 'content_truncated', count: 2 },
      { kind: 'comments_system_skipped', count: 3 },
      { kind: 'attachments_alternative_ignored', count: 1 },
      { kind: 'custom_attribute', name: 'customer_tier' },
    ], counters, issues, cursor, target);
    expect(counters).toMatchObject({ ownersUnmatched: 1, requestersWithoutContact: 1, contentTruncated: 2, customAttributes: { customer_tier: 1 } });
    expect(counters.comments.systemSkipped).toBe(3);
    expect(counters.attachments.alternativesIgnored).toBe(1);
    expect(issues.take().map((i) => [i.kind, i.code, i.count])).toEqual([['owner_unmatched', '42', 1], ['content_truncated', null, 2]]);
  });

  it('turns a result into counters and lists what v1 only reports, made-internal comments flagged', () => {
    const counters = emptyCounters();
    const issues = new IssueBuffer(counters, 100);
    recordImportResult(result({
      outcome: 'resumed',
      statusSkipped: true,
      statusSkip: { statusOwner: { kind: 'user', principalId: null }, sourceStatus: 'closed', currentStatus: 'open' },
      attachmentsSkipped: [
        { sourceId: 'a', reason: 'too_large' },
        { sourceId: 'b', reason: 'too_large' },
        { sourceId: 'c', reason: 'present_elsewhere' },
      ],
      commentsChangedInSource: [
        { sourceId: 'x', contentChanged: false, visibility: 'made_internal' },
        { sourceId: 'y', contentChanged: true, visibility: null },
      ],
      commentsMissingInSource: ['z'],
      requesterChanged: true,
      orgChanged: true,
    }), counters, issues, { key: 'k', number: null });
    expect(counters.tickets.resumed).toBe(1);
    expect(counters).toMatchObject({ statusKept: 1, commentsChangedInSource: 2, commentsMissingInSource: 1, requestersChangedInSource: 1, orgsChangedInSource: 1 });
    expect(counters.attachments).toMatchObject({ written: 1, skipped: 2, bytes: 300 });
    expect(issues.take().map((i) => `${i.kind}:${i.code}:${i.count}`)).toEqual([
      'status_kept:closed:open:user:1',
      'attachments_skipped:too_large:2',
      'comments_changed:made_internal:2',
      'comments_missing:null:1',
      'requester_changed:null:1',
      'org_changed:null:1',
    ]);
  });

  it('keeps counters exact past the issue cap', () => {
    const counters = emptyCounters();
    const issues = new IssueBuffer(counters, 2);
    for (let i = 0; i < 5; i += 1) issues.add({ sourceTicketKey: `${i}`, sourceTicketNumber: null, kind: 'failed', code: 'x' });
    expect(issues.take()).toHaveLength(2);
    expect(counters).toMatchObject({ issuesWritten: 2, issuesDropped: 3 });
  });

  it('predicts a dry run from declared sizes', () => {
    const counters = emptyCounters();
    predictTicket(ticket({
      comments: [{
        sourceId: 'article:1', createdAt: at(-1), isPublic: true, author: { kind: 'staff', name: 'A' }, content: 'x',
        attachments: [
          { sourceId: 'f1', filename: 'a.pdf', declaredBytes: 10, inline: false },
          { sourceId: 'f2', filename: 'b.zip', declaredBytes: 11 * 1024 * 1024, inline: false },
          { sourceId: 'f3', filename: 'c.txt', declaredBytes: 0, inline: false },
          { sourceId: 'f4', filename: 'd.txt', inline: false },
        ],
      }],
    }), counters, true);
    expect(counters.predicted).toMatchObject({ resumes: 1, creates: 0, comments: 1, attachments: 2, attachmentBytes: 10, attachmentsSkipped: 2 });
  });
});

describe('classifyImportError (spec §5.8)', () => {
  const mwe = (code: ConstructorParameters<typeof MigrationWindowError>[0], status: 400 | 403 | 409 | 429 | 503, details?: Record<string, unknown>) =>
    new MigrationWindowError(code, 'x', status, details);

  it.each([
    [mwe('MIGRATION_WINDOW_NOT_OPEN', 409, { reason: 'closed' }), { kind: 'stop', reason: 'window_closed' }],
    [mwe('MIGRATION_WINDOW_RATE_LIMITED', 429, { kind: 'writes', retryAfterSeconds: 30 }), { kind: 'defer', delayMs: 30_000 }],
    [mwe('MIGRATION_WINDOW_RATE_LIMITED', 429, { kind: 'bytes', retryAfterSeconds: 5 }), { kind: 'defer', delayMs: 5_000 }],
    [mwe('MIGRATION_SOURCE_ID_IN_USE', 409, { reason: 'other_principal_ticket' }), { kind: 'failed', code: 'other_principal_ticket' }],
    [mwe('MIGRATION_SOURCE_ID_IN_USE', 409, { reason: 'deleted' }), { kind: 'failed', code: 'deleted' }],
    [mwe('TICKET_IMPORT_ATTACHMENT_UNAVAILABLE', 503), { kind: 'failed', code: 'attachment_unavailable' }],
    [mwe('MIGRATION_IMPORT_INVALID', 400), { kind: 'failed', code: 'MIGRATION_IMPORT_INVALID' }],
    [mwe('TICKET_IMPORT_OBJECT_STORAGE_REQUIRED', 409), { kind: 'stop', reason: 'object_storage_required' }],
    [mwe('MIGRATION_WINDOW_ORG_NOT_ALLOWED', 403), { kind: 'failed', code: 'org_outside_window' }],
    [new TicketServiceError('no', 400, 'ASSIGNEE_NOT_ELIGIBLE'), { kind: 'failed', code: 'ASSIGNEE_NOT_ELIGIBLE' }],
    [new Error('connection reset'), { kind: 'rethrow' }],
  ])('%s', (error, expected) => {
    expect(classifyImportError(error)).toEqual(expected);
  });

  it('classifies source failures', () => {
    expect(classifySourceError(new SourceAuthError())).toEqual({ kind: 'unauthorized' });
    expect(classifySourceError(new SourceHttpError(404))).toEqual({ kind: 'http', status: 404 });
    expect(classifySourceError(new SourceUnavailableError('x', 9000))).toEqual({ kind: 'unavailable', retryAfterMs: 9000 });
    expect(classifySourceError(new SourceResponseTooLargeError(1))).toEqual({ kind: 'too_large' });
    expect(classifySourceError(new TypeError('bug'))).toEqual({ kind: 'rethrow' });
  });
});

describe('the stored run state', () => {
  it('normalizes counters and cursors from an empty or partial row', () => {
    expect(normalizeCounters({})).toEqual(emptyCounters());
    expect(normalizeCounters({ tickets: { created: 3, bogus: 1 }, customAttributes: { a: 2, b: -1 }, linksWritten: 'x' }))
      .toMatchObject({ tickets: { created: 3, seen: 0 }, customAttributes: { a: 2, b: 0 }, linksWritten: 0 });
    expect(normalizeCursor(null)).toMatchObject({ scan: { page: null, index: 0, done: false }, sourceFailures: 0, clockSkewMs: undefined });
    const cursor = normalizeCursor({ scan: { page: 3, index: 2 }, clockSkewMs: null, ownersSeen: ['a', 7] });
    expect(cursor).toMatchObject({ scan: { page: 3, index: 2, done: false }, clockSkewMs: null, ownersSeen: ['a'] });
    expect(rememberOwner(cursor, 'a')).toBe(false);
    expect(rememberOwner(cursor, 'b')).toBe(true);
  });

  it('keeps the global lease cap below the worker concurrency', () => {
    vi.stubEnv('TICKET_IMPORT_WORKER_CONCURRENCY', '3');
    vi.stubEnv('TICKET_IMPORT_GLOBAL_CONCURRENCY', '5');
    expect(ticketImportLimits()).toMatchObject({ workerConcurrency: 3, globalConcurrency: 2 });
    vi.stubEnv('TICKET_IMPORT_WORKER_CONCURRENCY', '1');
    expect(ticketImportLimits().globalConcurrency).toBe(1);
    vi.stubEnv('TICKET_IMPORT_SLICE_MS', 'soon');
    vi.stubEnv('TICKET_IMPORT_LEASE_MS', '10');
    expect(ticketImportLimits()).toMatchObject({ sliceMs: 300_000, leaseMs: 5_000 });
  });
});
