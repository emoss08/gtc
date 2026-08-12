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
  errors_by_type: z
    .record(z.string(), z.number())
    .nullable()
    .transform((v) => v ?? {}),
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
  version: z.string().default('dev'),
  slot_name: z.string().default(''),
  publication: z.string().default(''),
  ready: z.boolean(),
  streaming: z.boolean(),
  current_lsn: z.string(),
  wal_lag_bytes: z.number(),
  inflight: z.number(),
  events_total: z.number(),
  operations: z
    .record(z.string(), z.number())
    .nullable()
    .transform((v) => v ?? {}),
  sinks: z.array(sinkStatsSchema),
  tables: z.array(tableStatsSchema),
  backfill: z.array(backfillStatusSchema),
  dlq: z.object({
    enabled: z.boolean(),
    entries: z.number(),
    parked_total: z.number().default(0),
    retried_total: z.number().default(0),
  }),
});

export const historySchema = z.object({
  interval_seconds: z.number(),
  samples: z
    .array(
      z.object({
        t: z.number(),
        events_total: z.number(),
        wal_lag_bytes: z.number(),
        inflight: z.number(),
        dlq_entries: z.number(),
      }),
    )
    .nullable()
    .transform((v) => v ?? []),
});

export const columnDefSchema = z.object({
  name: z.string(),
  type: z.string(),
  type_oid: z.number(),
  part_of_key: z.boolean(),
});

export const schemaChangeSchema = z.object({
  schema: z.string(),
  table: z.string(),
  relation_id: z.number(),
  previous_schema: z.string().optional(),
  previous_table: z.string().optional(),
  added_columns: z.array(columnDefSchema).optional(),
  dropped_columns: z.array(columnDefSchema).optional(),
  changed_columns: z
    .array(z.object({ name: z.string(), from: columnDefSchema, to: columnDefSchema }))
    .optional(),
  key_columns: z.array(z.string()).optional(),
  previous_key_columns: z.array(z.string()).optional(),
  replica_identity: z.string(),
  previous_replica_identity: z.string().optional(),
  kinds: z.array(z.string()),
  lsn: z.string(),
  transaction_id: z.number(),
  timestamp: z.string(),
  breaking: z.boolean(),
  summary: z.string(),
});

export const schemaListSchema = z.object({
  total: z.number(),
  changes: z.array(schemaChangeSchema).nullable().transform((v) => v ?? []),
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
  // The full parked (already-transformed) event, for inspection.
  event: z.unknown().optional(),
});

export const dlqListSchema = z.object({
  total: z.number(),
  entries: z.array(dlqEntrySchema).nullable().transform((v) => v ?? []),
});

export type Stats = z.infer<typeof statsSchema>;
export type History = z.infer<typeof historySchema>;
export type HistorySample = History['samples'][number];
export type SinkStats = z.infer<typeof sinkStatsSchema>;
export type TableStats = z.infer<typeof tableStatsSchema>;
export type BackfillStatus = z.infer<typeof backfillStatusSchema>;
export type SchemaChange = z.infer<typeof schemaChangeSchema>;
export type ColumnDef = z.infer<typeof columnDefSchema>;
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

export const getHistory = async (): Promise<History> =>
  historySchema.parse(await request('/api/history'));

export const getSchemaChanges = async (): Promise<z.infer<typeof schemaListSchema>> =>
  schemaListSchema.parse(await request('/api/schema'));

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
