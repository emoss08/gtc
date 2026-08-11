import { useMemo, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTable, type ColumnDef } from '@tanstack/react-table';
import { Inbox, RefreshCw, RotateCcw, Search, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { DataTable } from '@/components/data-table';
import { features } from '@/lib/table';
import { timeAgo } from '@/lib/format';
import { discardDlqEntry, retryAllDlq, retryDlqEntry, type DlqEntry } from '@/lib/api';
import { useDlq } from '@/lib/hooks';

const OP_VARIANT: Record<string, 'success' | 'secondary' | 'destructive' | 'outline'> = {
  INSERT: 'success',
  UPDATE: 'secondary',
  DELETE: 'destructive',
  READ: 'outline',
};

function RowActions({ entry }: { entry: DlqEntry }) {
  const queryClient = useQueryClient();
  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['dlq'] });
    void queryClient.invalidateQueries({ queryKey: ['stats'] });
  };

  const retry = useMutation({
    mutationFn: () => retryDlqEntry(entry.id),
    onSuccess: () => {
      toast.success(`Retried ${entry.id} successfully`);
      invalidate();
    },
    onError: (err: Error) => {
      toast.error(err.message);
      invalidate();
    },
  });

  const discard = useMutation({
    mutationFn: () => discardDlqEntry(entry.id),
    onSuccess: () => {
      toast.success(`Discarded ${entry.id}`);
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  return (
    <div className="flex justify-end gap-1.5">
      <Button
        size="xs"
        variant="outline"
        disabled={retry.isPending}
        onClick={() => retry.mutate()}
        title="Re-deliver this event to its sink"
      >
        {retry.isPending ? (
          <RefreshCw className="size-3 animate-spin" />
        ) : (
          <RotateCcw className="size-3" />
        )}
        Retry
      </Button>
      <Button
        size="xs"
        variant="ghost"
        className="text-destructive hover:text-destructive"
        disabled={discard.isPending}
        onClick={() => discard.mutate()}
        title="Drop this event permanently"
      >
        <Trash2 className="size-3" />
      </Button>
    </div>
  );
}

const columns: Array<ColumnDef<typeof features, DlqEntry>> = [
  {
    accessorKey: 'sink',
    header: 'Sink',
    sortFn: 'text',
    cell: ({ getValue }) => <Badge variant="outline">{getValue<string>()}</Badge>,
  },
  {
    id: 'target',
    accessorFn: (row) => `${row.schema}.${row.table}`,
    header: 'Table',
    sortFn: 'text',
    cell: ({ row, getValue }) => (
      <span className="flex items-center gap-2">
        <span className="font-medium">{getValue<string>()}</span>
        <Badge variant={OP_VARIANT[row.original.operation] ?? 'secondary'} className="text-[10px]">
          {row.original.operation}
        </Badge>
      </span>
    ),
  },
  {
    accessorKey: 'error',
    header: 'Error',
    enableSorting: false,
    cell: ({ getValue }) => (
      <span
        className="text-destructive block max-w-md truncate text-xs whitespace-normal sm:whitespace-nowrap"
        title={getValue<string>()}
      >
        {getValue<string>()}
      </span>
    ),
  },
  {
    accessorKey: 'attempts',
    header: () => <span className="block text-right">Attempts</span>,
    sortFn: 'basic',
    cell: ({ getValue }) => (
      <span className="block text-right tabular-nums">{getValue<number>()}</span>
    ),
  },
  {
    accessorKey: 'last_failed_at',
    header: 'Last failed',
    sortFn: 'datetime',
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{timeAgo(getValue<string>())}</span>
    ),
  },
  {
    id: 'actions',
    header: () => <span className="block text-right">Actions</span>,
    enableSorting: false,
    cell: ({ row }) => <RowActions entry={row.original} />,
  },
];

function RetryAllDialog({ count }: { count: number }) {
  const [open, setOpen] = useState(false);
  const queryClient = useQueryClient();

  const retryAll = useMutation({
    mutationFn: retryAllDlq,
    onSuccess: (result) => {
      toast.success(`Retried ${result.retried ?? 0}: ${result.succeeded ?? 0} succeeded, ${result.failed ?? 0} failed`);
      void queryClient.invalidateQueries({ queryKey: ['dlq'] });
      void queryClient.invalidateQueries({ queryKey: ['stats'] });
      setOpen(false);
    },
    onError: (err: Error) => toast.error(err.message),
  });

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" disabled={count === 0}>
          <RotateCcw className="size-4" />
          Retry all
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Retry all parked events?</DialogTitle>
          <DialogDescription>
            Re-delivers all {count} parked events to their sinks. Events that still fail stay in the
            queue with an updated error. If newer changes for the same rows have already flowed,
            retrying re-applies the older state — consider discarding stale entries and re-running a
            backfill instead.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={() => retryAll.mutate()} disabled={retryAll.isPending}>
            {retryAll.isPending && <RefreshCw className="size-4 animate-spin" />}
            Retry {count} events
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export function DlqPage({ dlqEnabled }: { dlqEnabled: boolean }) {
  const [filter, setFilter] = useState('');
  const { data } = useDlq(dlqEnabled);
  const entries = useMemo(() => data?.entries ?? [], [data]);

  const table = useTable({
    features,
    columns,
    data: entries,
    globalFilterFn: 'includesString',
    initialState: { sorting: [{ id: 'last_failed_at', desc: true }] },
    state: { globalFilter: filter },
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div className="relative max-w-xs flex-1">
          <Search className="text-muted-foreground absolute top-1/2 left-2.5 size-4 -translate-y-1/2" />
          <Input
            placeholder="Filter by sink, table, error…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="h-8 pl-8 text-sm"
          />
        </div>
        <RetryAllDialog count={entries.length} />
      </div>
      <Card className="py-0">
        <CardContent className="px-0">
          <DataTable
            table={table}
            empty={
              <div className="text-muted-foreground flex flex-col items-center gap-1 text-sm">
                <Inbox className="size-5" />
                {dlqEnabled
                  ? 'No parked events — the pipeline is healthy.'
                  : 'Dead-letter queue is disabled (set REDIS_URL and CDC_DLQ_ENABLED=true).'}
              </div>
            }
          />
        </CardContent>
      </Card>
    </div>
  );
}
