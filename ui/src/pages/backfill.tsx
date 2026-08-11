import { useMemo, useState } from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTable, type ColumnDef } from '@tanstack/react-table';
import { DatabaseBackup, RefreshCw } from 'lucide-react';
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
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { DataTable } from '@/components/data-table';
import { features } from '@/lib/table';
import { formatNumber, timeAgo } from '@/lib/format';
import { triggerBackfill, type BackfillStatus, type Stats } from '@/lib/api';

const STATE_BADGE: Record<
  BackfillStatus['state'],
  { label: string; variant: 'success' | 'warning' | 'destructive' | 'secondary' | 'default' }
> = {
  done: { label: 'done', variant: 'success' },
  running: { label: 'running', variant: 'default' },
  pending: { label: 'pending', variant: 'secondary' },
  skipped: { label: 'skipped', variant: 'warning' },
  failed: { label: 'failed', variant: 'destructive' },
};

const columns: Array<ColumnDef<typeof features, BackfillStatus>> = [
  {
    id: 'table',
    accessorFn: (row) => `${row.schema}.${row.table}`,
    header: 'Table',
    sortFn: 'text',
    cell: ({ getValue }) => <span className="font-medium">{getValue<string>()}</span>,
  },
  {
    accessorKey: 'state',
    header: 'State',
    sortFn: 'text',
    cell: ({ getValue }) => {
      const s = STATE_BADGE[getValue<BackfillStatus['state']>()];
      return (
        <Badge variant={s.variant}>
          {s.label === 'running' && <RefreshCw className="size-3 animate-spin" />}
          {s.label}
        </Badge>
      );
    },
  },
  {
    accessorKey: 'rows_copied',
    header: () => <span className="block text-right">Rows copied</span>,
    sortFn: 'basic',
    cell: ({ getValue }) => (
      <span className="block text-right tabular-nums">{formatNumber(getValue<number>())}</span>
    ),
  },
  {
    accessorKey: 'completed_at',
    header: 'Completed',
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-muted-foreground">
        {row.original.state === 'done' ? timeAgo(row.original.completed_at) : '–'}
      </span>
    ),
  },
  {
    accessorKey: 'error',
    header: 'Note',
    enableSorting: false,
    cell: ({ getValue }) => {
      const v = getValue<string | undefined>();
      return v ? <span className="text-destructive text-xs">{v}</span> : null;
    },
  },
];

// Either empty (sync everything) or a valid [schema.]table identifier.
const backfillSchema = z.object({
  table: z
    .string()
    .trim()
    .refine((v) => v === '' || /^([A-Za-z_][A-Za-z0-9_$]*\.)?[A-Za-z_][A-Za-z0-9_$]*$/.test(v), {
      message: 'Use schema.table (e.g. public.orders) or leave empty for all tables',
    }),
});

type BackfillForm = z.infer<typeof backfillSchema>;

function TriggerDialog() {
  const [open, setOpen] = useState(false);
  const queryClient = useQueryClient();

  const form = useForm<BackfillForm>({
    resolver: zodResolver(backfillSchema),
    defaultValues: { table: '' },
  });

  const mutation = useMutation({
    mutationFn: (values: BackfillForm) => triggerBackfill(values.table || undefined),
    onSuccess: (_, values) => {
      toast.success(
        values.table ? `Backfill enqueued for ${values.table}` : 'Backfill enqueued for all tables',
      );
      void queryClient.invalidateQueries({ queryKey: ['stats'] });
      setOpen(false);
      form.reset();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm">
          <DatabaseBackup className="size-4" />
          Sync tables
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Trigger a backfill</DialogTitle>
          <DialogDescription>
            Re-sync existing rows into the sinks. Runs concurrently with live streaming — no locks,
            no downtime.
          </DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form
            onSubmit={form.handleSubmit((values) => mutation.mutate(values))}
            className="space-y-4"
          >
            <FormField
              control={form.control}
              name="table"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Table</FormLabel>
                  <FormControl>
                    <Input placeholder="public.orders" autoComplete="off" {...field} />
                  </FormControl>
                  <FormDescription>Leave empty to backfill every published table.</FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setOpen(false)}>
                Cancel
              </Button>
              <Button type="submit" disabled={mutation.isPending}>
                {mutation.isPending && <RefreshCw className="size-4 animate-spin" />}
                Start backfill
              </Button>
            </DialogFooter>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}

export function BackfillPage({ stats }: { stats: Stats | undefined }) {
  const data = useMemo(() => stats?.backfill ?? [], [stats]);

  const table = useTable({
    features,
    columns,
    data,
    initialState: { sorting: [{ id: 'table', desc: false }] },
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-muted-foreground max-w-xl text-sm">
          Watermark-based chunked sync of existing data, interleaved with the live stream. Progress
          survives restarts.
        </p>
        <TriggerDialog />
      </div>
      <Card className="py-0">
        <CardContent className="px-0">
          <DataTable
            table={table}
            empty={
              <div className="text-muted-foreground flex flex-col items-center gap-1 text-sm">
                <DatabaseBackup className="size-5" />
                No backfills have run. New deployments backfill automatically when the replication
                slot is first created.
              </div>
            }
          />
        </CardContent>
      </Card>
    </div>
  );
}

export { TriggerDialog as BackfillTriggerDialog };
