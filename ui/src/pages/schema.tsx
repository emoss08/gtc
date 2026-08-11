import { useMemo, useState } from 'react';
import { useTable, type ColumnDef as TanstackColumnDef } from '@tanstack/react-table';
import { FileCode2, Search, TriangleAlert } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { DataTable } from '@/components/data-table';
import { features } from '@/lib/table';
import { timeAgo } from '@/lib/format';
import type { SchemaChange } from '@/lib/api';
import { useSchemaChanges } from '@/lib/hooks';

const KIND_LABEL: Record<string, string> = {
  column_added: 'column added',
  column_dropped: 'column dropped',
  column_type_changed: 'type changed',
  key_changed: 'key changed',
  replica_identity_changed: 'replica identity',
  table_renamed: 'renamed',
};

// Additive changes are safe; everything else can break existing consumers.
const SAFE_KINDS = new Set(['column_added']);

const columns: Array<TanstackColumnDef<typeof features, SchemaChange>> = [
  {
    id: 'target',
    accessorFn: (row) => `${row.schema}.${row.table}`,
    header: 'Table',
    sortFn: 'text',
    cell: ({ row, getValue }) => (
      <span className="flex items-center gap-2">
        <span className="font-medium">{getValue<string>()}</span>
        {row.original.breaking && (
          <TriangleAlert
            className="size-3.5 text-amber-500"
            aria-label="Potentially breaking for consumers"
          />
        )}
      </span>
    ),
  },
  {
    id: 'kinds',
    accessorFn: (row) => row.kinds.join(' '),
    header: 'Change',
    enableSorting: false,
    cell: ({ row }) => (
      <div className="flex flex-wrap gap-1">
        {row.original.kinds.map((kind) => (
          <Badge
            key={kind}
            variant={SAFE_KINDS.has(kind) ? 'success' : 'destructive'}
            className="text-[10px]"
          >
            {KIND_LABEL[kind] ?? kind}
          </Badge>
        ))}
      </div>
    ),
  },
  {
    accessorKey: 'summary',
    header: 'Details',
    enableSorting: false,
    cell: ({ getValue }) => (
      <span className="text-muted-foreground block max-w-md truncate text-xs" title={getValue<string>()}>
        {getValue<string>()}
      </span>
    ),
  },
  {
    accessorKey: 'lsn',
    header: 'LSN',
    enableSorting: false,
    cell: ({ getValue }) => (
      <span className="text-muted-foreground font-mono text-xs">{getValue<string>()}</span>
    ),
  },
  {
    accessorKey: 'timestamp',
    header: 'Detected',
    sortFn: 'datetime',
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{timeAgo(getValue<string>())}</span>
    ),
  },
];

export function SchemaPage() {
  const [filter, setFilter] = useState('');
  const { data } = useSchemaChanges();
  const changes = useMemo(() => data?.changes ?? [], [data]);
  const breaking = changes.filter((c) => c.breaking).length;

  const table = useTable({
    features,
    columns,
    data: changes,
    globalFilterFn: 'includesString',
    initialState: { sorting: [{ id: 'timestamp', desc: true }] },
    state: { globalFilter: filter },
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div className="relative max-w-xs flex-1">
          <Search className="text-muted-foreground absolute top-1/2 left-2.5 size-4 -translate-y-1/2" />
          <Input
            placeholder="Filter by table or change…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="h-8 pl-8 text-sm"
          />
        </div>
        {breaking > 0 && (
          <Badge variant="destructive" className="gap-1">
            <TriangleAlert className="size-3" />
            {breaking} potentially breaking
          </Badge>
        )}
      </div>
      <Card className="py-0">
        <CardContent className="px-0">
          <DataTable
            table={table}
            empty={
              <div className="text-muted-foreground flex flex-col items-center gap-1 text-sm">
                <FileCode2 className="size-5" />
                No schema changes observed since start.
              </div>
            }
          />
        </CardContent>
      </Card>
      <p className="text-muted-foreground px-1 text-xs">
        PostgreSQL does not stream DDL statements. GTC reports the effect of a change — columns
        added, dropped or retyped, replica identity, renames — the moment the table's next row
        change arrives, so a change to an idle table surfaces when its traffic resumes.
      </p>
    </div>
  );
}
