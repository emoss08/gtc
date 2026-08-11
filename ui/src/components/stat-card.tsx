import type { LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import { Card } from '@/components/ui/card';
import { cn } from '@/lib/utils';

export function StatCard({
  label,
  value,
  sub,
  icon: Icon,
  tone = 'default',
  children,
}: {
  label: string;
  value: string;
  sub?: string;
  icon: LucideIcon;
  tone?: 'default' | 'danger' | 'success';
  children?: ReactNode;
}) {
  return (
    <Card className="gap-0 py-4">
      <div className="flex items-start justify-between gap-3 px-4">
        <div className="min-w-0">
          <div className="text-muted-foreground flex items-center gap-1.5 text-xs font-medium">
            <Icon className="size-3.5" />
            {label}
          </div>
          <div
            className={cn(
              'mt-1.5 truncate text-2xl font-semibold tracking-tight tabular-nums',
              tone === 'danger' && 'text-destructive',
              tone === 'success' && 'text-success',
            )}
          >
            {value}
          </div>
          {sub && <div className="text-muted-foreground mt-0.5 truncate text-xs">{sub}</div>}
        </div>
        {children}
      </div>
    </Card>
  );
}
