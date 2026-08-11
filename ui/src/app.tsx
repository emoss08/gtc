import { useEffect, useState } from 'react';
import { Shell, type Page } from '@/components/shell';
import { OverviewPage } from '@/pages/overview';
import { TablesPage } from '@/pages/tables';
import { SchemaPage } from '@/pages/schema';
import { BackfillPage } from '@/pages/backfill';
import { DlqPage } from '@/pages/dlq';
import { useStats } from '@/lib/hooks';

const PAGE_META: Record<Page, { title: string; description: string }> = {
  overview: { title: 'Overview', description: 'Pipeline health at a glance' },
  tables: { title: 'Table activity', description: 'Change events by source table' },
  schema: { title: 'Schema changes', description: 'DDL detected on published tables' },
  backfill: { title: 'Backfill', description: 'Sync existing data into the sinks' },
  dlq: { title: 'Dead letters', description: 'Poison events parked for triage' },
};

export function App() {
  const [page, setPage] = useState<Page>('overview');
  const { data: stats, isError } = useStats();
  const meta = PAGE_META[page];

  useEffect(() => {
    document.title = `${meta.title} · GTC`;
  }, [meta.title]);

  return (
    <Shell
      page={page}
      onNavigate={setPage}
      stats={stats}
      offline={isError}
      dlqCount={stats?.dlq.entries ?? 0}
      title={meta.title}
      description={meta.description}
    >
      {page === 'overview' && <OverviewPage stats={stats} />}
      {page === 'tables' && <TablesPage stats={stats} />}
      {page === 'schema' && <SchemaPage />}
      {page === 'backfill' && <BackfillPage stats={stats} />}
      {page === 'dlq' && <DlqPage dlqEnabled={stats?.dlq.enabled ?? false} />}
    </Shell>
  );
}
