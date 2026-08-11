import { useState } from 'react';
import { Shell, type Page } from '@/components/shell';
import { OverviewPage } from '@/pages/overview';
import { TablesPage } from '@/pages/tables';
import { BackfillPage } from '@/pages/backfill';
import { DlqPage } from '@/pages/dlq';
import { useStats } from '@/lib/hooks';

const PAGE_META: Record<Page, { title: string; description: string }> = {
  overview: { title: 'Overview', description: 'Pipeline health at a glance' },
  tables: { title: 'Table activity', description: 'Change events by source table' },
  backfill: { title: 'Backfill', description: 'Sync existing data into the sinks' },
  dlq: { title: 'Dead letters', description: 'Poison events parked for triage' },
};

export function App() {
  const [page, setPage] = useState<Page>('overview');
  const { data: stats } = useStats();
  const meta = PAGE_META[page];

  return (
    <Shell
      page={page}
      onNavigate={setPage}
      stats={stats}
      dlqCount={stats?.dlq.entries ?? 0}
      title={meta.title}
      description={meta.description}
    >
      {page === 'overview' && <OverviewPage stats={stats} />}
      {page === 'tables' && <TablesPage stats={stats} />}
      {page === 'backfill' && <BackfillPage stats={stats} />}
      {page === 'dlq' && <DlqPage dlqEnabled={stats?.dlq.enabled ?? false} />}
    </Shell>
  );
}
