import { type ReactNode } from 'react';
import {
  Activity,
  DatabaseBackup,
  Github,
  Inbox,
  LayoutDashboard,
  LineChart,
  Moon,
  Sun,
  Table2,
  Zap,
} from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { formatDuration } from '@/lib/format';
import type { Stats } from '@/lib/api';

export type Page = 'overview' | 'tables' | 'backfill' | 'dlq';

const NAV: { id: Page; label: string; icon: typeof LayoutDashboard }[] = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'tables', label: 'Tables', icon: Table2 },
  { id: 'backfill', label: 'Backfill', icon: DatabaseBackup },
  { id: 'dlq', label: 'Dead letters', icon: Inbox },
];

function toggleTheme() {
  const root = document.documentElement;
  const dark = root.classList.toggle('dark');
  localStorage.setItem('gtc-theme', dark ? 'dark' : 'light');
}

export function Shell({
  page,
  onNavigate,
  stats,
  offline = false,
  dlqCount,
  title,
  description,
  actions,
  children,
}: {
  page: Page;
  onNavigate: (page: Page) => void;
  stats: Stats | undefined;
  offline?: boolean;
  dlqCount: number;
  title: string;
  description: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="flex min-h-screen">
      {/* Sidebar: icon rail on small screens, full labels from lg up */}
      <aside className="bg-sidebar text-sidebar-foreground border-sidebar-border fixed inset-y-0 left-0 z-30 flex w-14 flex-col border-r lg:w-56">
        <div className="flex items-center justify-center gap-2.5 px-2 pt-5 pb-4 lg:justify-start lg:px-4">
          <div className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-gradient-to-br from-amber-400 to-orange-500 text-white shadow-sm">
            <Zap className="size-4.5" fill="currentColor" strokeWidth={0} />
          </div>
          <div className="hidden leading-tight lg:block">
            <div className="text-sm font-bold tracking-tight">GTC</div>
            <div className="text-sidebar-foreground/60 text-[10px] font-medium tracking-wide uppercase">
              Change Data Capture
            </div>
          </div>
        </div>

        <nav className="flex flex-1 flex-col gap-0.5 px-2 pt-2 lg:px-2.5">
          {NAV.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              type="button"
              title={label}
              onClick={() => onNavigate(id)}
              className={cn(
                'relative flex cursor-pointer items-center justify-center gap-2.5 rounded-md px-2.5 py-2 text-[13px] font-medium transition-colors lg:justify-start',
                page === id
                  ? 'bg-sidebar-accent text-sidebar-accent-foreground'
                  : 'text-sidebar-foreground/65 hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground',
              )}
            >
              <Icon className="size-4 shrink-0" strokeWidth={page === id ? 2.25 : 2} />
              <span className="hidden flex-1 text-left lg:block">{label}</span>
              {id === 'dlq' && dlqCount > 0 && (
                <>
                  <Badge
                    variant="destructive"
                    className="hidden h-5 min-w-5 px-1.5 tabular-nums lg:inline-flex"
                  >
                    {dlqCount}
                  </Badge>
                  <span className="absolute top-1.5 right-1.5 size-1.5 rounded-full bg-red-500 lg:hidden" />
                </>
              )}
            </button>
          ))}
        </nav>

        <div className="border-sidebar-border hidden space-y-3 border-t px-4 py-4 lg:block">
          <div className="text-sidebar-foreground/60 flex items-center justify-between text-xs">
            <span className="flex items-center gap-1.5">
              <span
                className={cn(
                  'relative flex size-2 rounded-full',
                  stats?.streaming ? 'bg-emerald-500' : 'bg-red-500',
                )}
              >
                {stats?.streaming && (
                  <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-500 opacity-60" />
                )}
              </span>
              {stats?.streaming ? 'Streaming' : 'Disconnected'}
            </span>
            <span className="tabular-nums">
              {stats ? `up ${formatDuration(stats.uptime_seconds)}` : ''}
            </span>
          </div>
          <div className="text-sidebar-foreground/60 flex items-center gap-1 text-xs">
            <a
              href="/metrics"
              target="_blank"
              rel="noreferrer"
              className="hover:bg-sidebar-accent hover:text-sidebar-accent-foreground flex items-center gap-1.5 rounded-md px-2 py-1"
            >
              <LineChart className="size-3.5" /> Metrics
            </a>
            <a
              href="https://github.com/emoss08/gtc"
              target="_blank"
              rel="noreferrer"
              className="hover:bg-sidebar-accent hover:text-sidebar-accent-foreground flex items-center gap-1.5 rounded-md px-2 py-1"
            >
              <Github className="size-3.5" /> GitHub
            </a>
            <button
              type="button"
              onClick={toggleTheme}
              className="hover:bg-sidebar-accent hover:text-sidebar-accent-foreground ml-auto cursor-pointer rounded-md p-1.5"
              aria-label="Toggle theme"
            >
              <Sun className="size-3.5 dark:hidden" />
              <Moon className="hidden size-3.5 dark:block" />
            </button>
          </div>
        </div>
      </aside>

      {/* Main */}
      <div className="ml-14 flex min-w-0 flex-1 flex-col lg:ml-56">
        {offline && (
          <div className="bg-destructive/90 sticky top-0 z-30 px-6 py-1.5 text-center text-xs font-medium text-white">
            Cannot reach GTC — the data below may be stale. Retrying…
          </div>
        )}
        <header className="bg-background/80 sticky top-0 z-20 border-b backdrop-blur">
          <div className="mx-auto flex h-14 w-full max-w-6xl items-center justify-between gap-4 px-6">
            <div className="min-w-0">
              <h1 className="truncate text-[15px] font-semibold tracking-tight">{title}</h1>
              <p className="text-muted-foreground truncate text-xs">{description}</p>
            </div>
            <div className="flex shrink-0 items-center gap-2">
              {stats && (
                <>
                  <Badge variant="outline" className="text-muted-foreground hidden font-mono text-[11px] sm:inline-flex">
                    <Activity className="size-3" />
                    {stats.current_lsn}
                  </Badge>
                  <Badge variant={stats.ready ? 'success' : 'warning'}>
                    {stats.ready ? 'Ready' : 'Not ready'}
                  </Badge>
                </>
              )}
              {actions}
            </div>
          </div>
        </header>
        <main className="mx-auto w-full max-w-6xl flex-1 px-6 py-6">{children}</main>
      </div>
    </div>
  );
}
