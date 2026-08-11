import { useMemo, useState } from 'react';
import { useTable, type ColumnDef } from '@tanstack/react-table';
import { Search, Table2 } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { DataTable } from '@/components/data-table';
import { features } from '@/lib/table';
import { formatNumber } from '@/lib/format';
import type { Stats, TableStats } from '@/lib/api';

function num(v: number, tone: string) {
  return (
    <span className={`block text-right tabular-nums ${v > 0 ? tone : 'text-muted-foreground/50'}`}>
      {formatNumber(v)}
    </span>
  );
}

function makeColumns(grandTotal: number): Array<ColumnDef<typeof features, TableStats>> {
  return [
    {
      accessorKey: 'table',
      header: 'Table',
      sortFn: 'text',
      cell: ({ getValue }) => <span className="font-medium">{getValue<string>()}</span>,
    },
    {
      accessorKey: 'insert',
      header: () => <span className="block text-right">Inserts</span>,
      sortFn: 'basic',
      cell: ({ getValue }) => num(getValue<number>(), 'text-emerald-600 dark:text-emerald-400'),
    },
    {
      accessorKey: 'update',
      header: () => <span className="block text-right">Updates</span>,
      sortFn: 'basic',
      cell: ({ getValue }) => num(getValue<number>(), 'text-sky-600 dark:text-sky-400'),
    },
    {
      accessorKey: 'delete',
      header: () => <span className="block text-right">Deletes</span>,
      sortFn: 'basic',
      cell: ({ getValue }) => num(getValue<number>(), 'text-red-600 dark:text-red-400'),
    },
    {
      accessorKey: 'read',
      header: () => <span className="block text-right">Backfill reads</span>,
      sortFn: 'basic',
      cell: ({ getValue }) => num(getValue<number>(), 'text-violet-600 dark:text-violet-400'),
    },
    {
      accessorKey: 'total',
      header: () => <span className="block text-right">Total</span>,
      sortFn: 'basic',
      cell: ({ getValue }) => {
        const v = getValue<number>();
        const share = grandTotal > 0 ? (v / grandTotal) * 100 : 0;
        return (
          <div className="flex items-center justify-end gap-2">
            <span className="font-medium tabular-nums">{formatNumber(v)}</span>
            <span className="bg-muted relative h-1.5 w-16 overflow-hidden rounded-full">
              <span
                className="bg-primary/70 absolute inset-y-0 left-0 rounded-full"
                style={{ width: `${Math.max(2, share)}%` }}
              />
            </span>
          </div>
        );
      },
    },
  ];
}

export function TablesPage({ stats }: { stats: Stats | undefined }) {
  const [filter, setFilter] = useState('');
  const data = useMemo(() => stats?.tables ?? [], [stats]);
  const columns = useMemo(() => makeColumns(stats?.events_total ?? 0), [stats?.events_total]);

  const table = useTable({
    features,
    columns,
    data,
    globalFilterFn: 'includesString',
    initialState: { sorting: [{ id: 'total', desc: true }] },
    state: { globalFilter: filter },
  });

  return (
    <div className="space-y-4">
      <div className="relative max-w-xs">
        <Search className="text-muted-foreground absolute top-1/2 left-2.5 size-4 -translate-y-1/2" />
        <Input
          placeholder="Filter tables…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="h-8 pl-8 text-sm"
        />
      </div>
      <Card className="py-0">
        <CardContent className="px-0">
          <DataTable
            table={table}
            empty={
              <div className="text-muted-foreground flex flex-col items-center gap-1 text-sm">
                <Table2 className="size-5" />
                No change events observed yet.
              </div>
            }
          />
        </CardContent>
      </Card>
    </div>
  );
}
