import { useMemo } from 'react';
import { useTable, type ColumnDef } from '@tanstack/react-table';
import { Activity, ArrowDownUp, CircleX, Gauge, Inbox, Layers } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { LagChart, OperationDonut, OpLegend, ThroughputChart } from '@/components/charts';
import { DataTable } from '@/components/data-table';
import { StatCard } from '@/components/stat-card';
import { features } from '@/lib/table';
import { formatBytes, formatDuration, formatNumber, formatRate } from '@/lib/format';
import { useHistory } from '@/lib/hooks';
import type { SinkStats, Stats } from '@/lib/api';

const BREAKER: Record<number, { label: string; variant: 'success' | 'warning' | 'destructive' }> = {
  0: { label: 'closed', variant: 'success' },
  1: { label: 'half-open', variant: 'warning' },
  2: { label: 'open', variant: 'destructive' },
};

function errorSummary(errors: Record<string, number>): string {
  const parts = Object.entries(errors)
    .sort((a, b) => b[1] - a[1])
    .map(([type, n]) => `${type}: ${formatNumber(n)}`);
  return parts.length > 0 ? `errors — ${parts.join(', ')}` : '';
}

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
    cell: ({ row, getValue }) => {
      const v = getValue<number>();
      return (
        <span
          className={`block text-right tabular-nums ${v > 0 ? 'text-destructive' : 'text-muted-foreground'}`}
          title={errorSummary(row.original.errors_by_type)}
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

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3 text-xs">
      <span className="text-muted-foreground shrink-0">{label}</span>
      <span className="truncate text-right font-medium">{value}</span>
    </div>
  );
}

export function OverviewPage({ stats }: { stats: Stats | undefined }) {
  const { points } = useHistory();
  const sinks = useMemo(() => stats?.sinks ?? [], [stats]);
  const currentRate = points.length > 0 ? points[points.length - 1].rate : 0;

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
          value={formatRate(currentRate)}
          sub={`${formatNumber(stats?.events_total)} events total`}
          icon={Activity}
        />
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
              ? `${formatNumber(stats.dlq.parked_total)} parked · ${formatNumber(stats.dlq.retried_total)} retried all-time`
              : 'DLQ disabled'
          }
          icon={Inbox}
          tone={dlqCount > 0 ? 'danger' : 'default'}
        />
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card className="gap-2 xl:col-span-2">
          <CardHeader>
            <div>
              <CardTitle>Throughput</CardTitle>
              <CardDescription className="mt-1">
                Events per second · last 30 minutes
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="pr-2">
            <ThroughputChart points={points} />
          </CardContent>
        </Card>

        <Card className="gap-2">
          <CardHeader>
            <div>
              <CardTitle>Operation mix</CardTitle>
              <CardDescription className="mt-1">All events since start</CardDescription>
            </div>
          </CardHeader>
          <CardContent>
            <OperationDonut operations={stats?.operations ?? {}} />
            <div className="mt-2 flex justify-center">
              <OpLegend />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card className="gap-2 xl:col-span-2">
          <CardHeader>
            <div>
              <CardTitle>Replication lag</CardTitle>
              <CardDescription className="mt-1">
                WAL bytes between the server and the last confirmed position — sustained growth
                means a sink can't keep up
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent className="pr-2">
            <LagChart points={points} />
          </CardContent>
        </Card>

        <Card className="gap-2">
          <CardHeader>
            <CardTitle>Instance</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5">
            <InfoRow label="Version" value={stats?.version ?? '–'} />
            <InfoRow
              label="Status"
              value={
                <span className="flex items-center justify-end gap-1.5">
                  <Badge variant={stats?.streaming ? 'success' : 'destructive'}>
                    {stats?.streaming ? 'streaming' : 'disconnected'}
                  </Badge>
                  <Badge variant={stats?.ready ? 'success' : 'warning'}>
                    {stats?.ready ? 'ready' : 'not ready'}
                  </Badge>
                </span>
              }
            />
            <InfoRow label="Uptime" value={formatDuration(stats?.uptime_seconds)} />
            <InfoRow
              label="Current LSN"
              value={<span className="font-mono text-[11px]">{stats?.current_lsn ?? '–'}</span>}
            />
            <InfoRow
              label="Slot"
              value={<span className="font-mono text-[11px]">{stats?.slot_name || '–'}</span>}
            />
            <InfoRow
              label="Publication"
              value={<span className="font-mono text-[11px]">{stats?.publication || '–'}</span>}
            />
            <InfoRow
              label="Tables observed"
              value={String(stats?.tables.length ?? 0)}
            />
            <InfoRow
              label="Dead-letter queue"
              value={stats?.dlq.enabled ? `${dlqCount} parked` : 'disabled'}
            />
          </CardContent>
        </Card>
      </div>

      <Card className="gap-3">
        <CardHeader>
          <div>
            <CardTitle>Sinks</CardTitle>
            <CardDescription className="mt-1">
              Delivery state per destination — the circuit breaker opens while a sink is down. Hover
              a failure count for the error breakdown.
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
    </div>
  );
}
