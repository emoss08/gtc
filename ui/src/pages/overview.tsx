import { useMemo } from 'react';
import { useTable, type ColumnDef } from '@tanstack/react-table';
import { Activity, ArrowDownUp, CircleCheck, CircleX, Gauge, Inbox, Layers } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { AreaSpark } from '@/components/spark';
import { DataTable } from '@/components/data-table';
import { StatCard } from '@/components/stat-card';
import { features } from '@/lib/table';
import { formatBytes, formatNumber, formatRate } from '@/lib/format';
import { useThroughput } from '@/lib/hooks';
import type { SinkStats, Stats } from '@/lib/api';

const BREAKER: Record<number, { label: string; variant: 'success' | 'warning' | 'destructive' }> = {
  0: { label: 'closed', variant: 'success' },
  1: { label: 'half-open', variant: 'warning' },
  2: { label: 'open', variant: 'destructive' },
};

const sinkColumns: Array<ColumnDef<typeof features, SinkStats>> = [
  {
    accessorKey: 'name',
    header: 'Sink',
    sortFn: 'text',
    cell: ({ row }) => (
      <span className="flex items-center gap-2 font-medium">
        <span
          className={
            row.original.healthy
              ? 'size-1.5 rounded-full bg-emerald-500'
              : 'size-1.5 rounded-full bg-red-500'
          }
        />
        {row.original.name}
      </span>
    ),
  },
  {
    accessorKey: 'breaker_state',
    header: 'Breaker',
    sortFn: 'basic',
    cell: ({ getValue }) => {
      const b = BREAKER[getValue<number>()] ?? BREAKER[0];
      return <Badge variant={b.variant}>{b.label}</Badge>;
    },
  },
  {
    accessorKey: 'succeeded',
    header: () => <span className="block text-right">Delivered</span>,
    sortFn: 'basic',
    cell: ({ getValue }) => (
      <span className="text-success block text-right tabular-nums">
        {formatNumber(getValue<number>())}
      </span>
    ),
  },
  {
    accessorKey: 'failed',
    header: () => <span className="block text-right">Failed</span>,
    sortFn: 'basic',
    cell: ({ getValue }) => {
      const v = getValue<number>();
      return (
        <span
          className={`block text-right tabular-nums ${v > 0 ? 'text-destructive' : 'text-muted-foreground'}`}
        >
          {formatNumber(v)}
        </span>
      );
    },
  },
  {
    accessorKey: 'retries',
    header: () => <span className="block text-right">Retries</span>,
    sortFn: 'basic',
    cell: ({ getValue }) => (
      <span className="text-muted-foreground block text-right tabular-nums">
        {formatNumber(getValue<number>())}
      </span>
    ),
  },
  {
    accessorKey: 'filtered',
    header: () => <span className="block text-right">Filtered</span>,
    sortFn: 'basic',
    cell: ({ getValue }) => (
      <span className="text-muted-foreground block text-right tabular-nums">
        {formatNumber(getValue<number>())}
      </span>
    ),
  },
];

export function OverviewPage({ stats }: { stats: Stats | undefined }) {
  const { rate, history } = useThroughput(stats);
  const sinks = useMemo(() => stats?.sinks ?? [], [stats]);

  const table = useTable({
    features,
    columns: sinkColumns,
    data: sinks,
  });

  const dlqCount = stats?.dlq.entries ?? 0;

  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Throughput"
          value={formatRate(rate)}
          sub={`${formatNumber(stats?.events_total)} total`}
          icon={Activity}
        >
          <AreaSpark
            points={history}
            width={96}
            height={46}
            className="mt-1 shrink-0 text-emerald-500 dark:text-emerald-400"
          />
        </StatCard>
        <StatCard
          label="WAL lag"
          value={formatBytes(stats?.wal_lag_bytes)}
          sub={stats?.streaming ? 'replication live' : 'replication down'}
          icon={Gauge}
        />
        <StatCard
          label="In flight"
          value={String(stats?.inflight ?? 0)}
          sub="events being processed"
          icon={Layers}
        />
        <StatCard
          label="Dead letters"
          value={String(dlqCount)}
          sub={
            stats?.dlq.enabled
              ? dlqCount > 0
                ? 'needs triage'
                : 'queue empty'
              : 'DLQ disabled'
          }
          icon={Inbox}
          tone={dlqCount > 0 ? 'danger' : 'default'}
        />
      </div>

      <Card className="gap-3">
        <CardHeader>
          <div>
            <CardTitle>Sinks</CardTitle>
            <CardDescription className="mt-1">
              Delivery state per destination — the circuit breaker opens while a sink is down.
            </CardDescription>
          </div>
          <Badge variant="outline" className="text-muted-foreground">
            <ArrowDownUp className="size-3" />
            {sinks.length} active
          </Badge>
        </CardHeader>
        <CardContent className="px-0">
          <DataTable
            table={table}
            empty={
              <div className="text-muted-foreground flex flex-col items-center gap-1 text-sm">
                <CircleX className="size-5" />
                No sinks configured — set REDIS_URL, REDIS_JSON_URL, or MEILISEARCH_URL.
              </div>
            }
          />
        </CardContent>
      </Card>

      {stats && (
        <div className="text-muted-foreground flex flex-wrap items-center gap-x-4 gap-y-1 px-1 text-xs">
          <span className="flex items-center gap-1">
            <CircleCheck className="size-3.5" />
            at-least-once delivery
          </span>
          <span>LSN {stats.current_lsn}</span>
          <span>refreshes every 2s</span>
        </div>
      )}
    </div>
  );
}
