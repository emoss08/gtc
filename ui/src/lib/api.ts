import { z } from 'zod';

// API responses are validated with zod so a contract drift between the Go
// server and the dashboard surfaces as a loud error, not silent NaNs.

export const sinkStatsSchema = z.object({
  name: z.string(),
  healthy: z.boolean(),
  breaker_state: z.number(),
  succeeded: z.number(),
  failed: z.number(),
  filtered: z.number(),
  retries: z.number(),
});

export const tableStatsSchema = z.object({
  table: z.string(),
  insert: z.number(),
  update: z.number(),
  delete: z.number(),
  read: z.number(),
  truncate: z.number(),
  total: z.number(),
});

export const backfillStatusSchema = z.object({
  schema: z.string(),
  table: z.string(),
  state: z.enum(['pending', 'running', 'done', 'skipped', 'failed']),
  rows_copied: z.number(),
  started_at: z.string().optional().nullable(),
  completed_at: z.string().optional().nullable(),
  error: z.string().optional(),
});

export const statsSchema = z.object({
  uptime_seconds: z.number(),
  ready: z.boolean(),
  streaming: z.boolean(),
  current_lsn: z.string(),
  wal_lag_bytes: z.number(),
  inflight: z.number(),
  events_total: z.number(),
  sinks: z.array(sinkStatsSchema),
  tables: z.array(tableStatsSchema),
  backfill: z.array(backfillStatusSchema),
  dlq: z.object({ enabled: z.boolean(), entries: z.number() }),
});

export const dlqEntrySchema = z.object({
  id: z.string(),
  sink: z.string(),
  schema: z.string(),
  table: z.string(),
  operation: z.string(),
  lsn: z.string(),
  error: z.string(),
  attempts: z.number(),
  first_failed_at: z.string().optional(),
  last_failed_at: z.string(),
});

export const dlqListSchema = z.object({
  total: z.number(),
  entries: z.array(dlqEntrySchema).nullable().transform((v) => v ?? []),
});

export type Stats = z.infer<typeof statsSchema>;
export type SinkStats = z.infer<typeof sinkStatsSchema>;
export type TableStats = z.infer<typeof tableStatsSchema>;
export type BackfillStatus = z.infer<typeof backfillStatusSchema>;
export type DlqEntry = z.infer<typeof dlqEntrySchema>;
export type DlqList = z.infer<typeof dlqListSchema>;

async function request(path: string, init?: RequestInit): Promise<unknown> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
  const body = (await res.json().catch(() => ({}))) as { error?: string };
  if (!res.ok) {
    throw new Error(body.error ?? `${res.status} ${res.statusText}`);
  }
  return body;
}

export const getStats = async (): Promise<Stats> => statsSchema.parse(await request('/api/stats'));

export const getDlq = async (limit = 100): Promise<DlqList> =>
  dlqListSchema.parse(await request(`/dlq?limit=${limit}`));

export const retryDlqEntry = (id: string) =>
  request('/dlq/retry', { method: 'POST', body: JSON.stringify({ id }) });

export const retryAllDlq = () =>
  request('/dlq/retry', { method: 'POST', body: JSON.stringify({ all: true }) }) as Promise<{
    retried?: number;
    succeeded?: number;
    failed?: number;
  }>;

export const discardDlqEntry = (id: string) =>
  request('/dlq/discard', { method: 'POST', body: JSON.stringify({ id }) });

export const triggerBackfill = (table?: string) =>
  request('/backfill', {
    method: 'POST',
    body: JSON.stringify(table ? { table } : { all: true }),
  });
