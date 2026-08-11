import { useMemo } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { ResponsivePie } from '@nivo/pie';
import { ResponsiveBar } from '@nivo/bar';
import { formatBytes, formatNumber, formatRate } from '@/lib/format';
import { useIsDark, type RatePoint } from '@/lib/hooks';
import type { Stats, TableStats } from '@/lib/api';

export const OP_COLORS: Record<string, string> = {
  INSERT: '#10b981', // emerald-500
  UPDATE: '#0ea5e9', // sky-500
  DELETE: '#ef4444', // red-500
  READ: '#8b5cf6', // violet-500
  TRUNCATE: '#f59e0b', // amber-500
};

function usePalette() {
  const dark = useIsDark();
  return {
    dark,
    grid: dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)',
    axis: dark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)',
    tooltipBg: dark ? '#27272a' : '#ffffff',
    tooltipText: dark ? '#fafafa' : '#18181b',
    tooltipBorder: dark ? 'rgba(255,255,255,0.12)' : 'rgba(0,0,0,0.1)',
  };
}

function timeTick(t: number): string {
  const d = new Date(t);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function chartTooltipStyle(p: ReturnType<typeof usePalette>): React.CSSProperties {
  return {
    backgroundColor: p.tooltipBg,
    color: p.tooltipText,
    border: `1px solid ${p.tooltipBorder}`,
    borderRadius: 8,
    fontSize: 12,
    padding: '6px 10px',
    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
  };
}

// Recharts: throughput (events/s) over the server-side history window.
export function ThroughputChart({ points }: { points: RatePoint[] }) {
  const p = usePalette();
  return (
    <ResponsiveContainer width="100%" height={220}>
      <AreaChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: -12 }}>
        <defs>
          <linearGradient id="rateFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#10b981" stopOpacity={0.35} />
            <stop offset="100%" stopColor="#10b981" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid stroke={p.grid} vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={timeTick}
          stroke={p.axis}
          fontSize={11}
          tickLine={false}
          axisLine={false}
          minTickGap={48}
        />
        <YAxis
          stroke={p.axis}
          fontSize={11}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: number) => formatNumber(v)}
          width={48}
        />
        <Tooltip
          contentStyle={chartTooltipStyle(p)}
          labelFormatter={(t) => timeTick(Number(t))}
          formatter={(value) => [formatRate(Number(value)), 'events']}
        />
        <Area
          type="monotone"
          dataKey="rate"
          stroke="#10b981"
          strokeWidth={2}
          fill="url(#rateFill)"
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}

// Recharts: WAL lag (bytes) over time.
export function LagChart({ points }: { points: RatePoint[] }) {
  const p = usePalette();
  return (
    <ResponsiveContainer width="100%" height={140}>
      <AreaChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 8 }}>
        <defs>
          <linearGradient id="lagFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#f59e0b" stopOpacity={0.3} />
            <stop offset="100%" stopColor="#f59e0b" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid stroke={p.grid} vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={timeTick}
          stroke={p.axis}
          fontSize={11}
          tickLine={false}
          axisLine={false}
          minTickGap={48}
        />
        <YAxis
          stroke={p.axis}
          fontSize={11}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: number) => formatBytes(v)}
          width={64}
        />
        <Tooltip
          contentStyle={chartTooltipStyle(p)}
          labelFormatter={(t) => timeTick(Number(t))}
          formatter={(value) => [formatBytes(Number(value)), 'WAL lag']}
        />
        <Area
          type="monotone"
          dataKey="walLag"
          stroke="#f59e0b"
          strokeWidth={2}
          fill="url(#lagFill)"
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}

// Nivo: donut of the global operation mix.
export function OperationDonut({ operations }: { operations: Stats['operations'] }) {
  const p = usePalette();
  const data = useMemo(
    () =>
      Object.entries(operations)
        .filter(([, v]) => v > 0)
        .sort((a, b) => b[1] - a[1])
        .map(([id, value]) => ({ id, label: id, value })),
    [operations],
  );

  const total = data.reduce((acc, d) => acc + d.value, 0);

  if (data.length === 0) {
    return (
      <div className="text-muted-foreground flex h-[200px] items-center justify-center text-sm">
        No events yet.
      </div>
    );
  }

  return (
    <div className="relative h-[200px]">
      <ResponsivePie
        data={data}
        margin={{ top: 12, right: 12, bottom: 12, left: 12 }}
        innerRadius={0.72}
        padAngle={1.5}
        cornerRadius={3}
        colors={(d) => OP_COLORS[d.id as string] ?? '#71717a'}
        enableArcLinkLabels={false}
        enableArcLabels={false}
        borderWidth={0}
        isInteractive
        animate={false}
        tooltip={({ datum }) => (
          <div
            style={chartTooltipStyle(p)}
            className="flex items-center gap-2 whitespace-nowrap"
          >
            <span className="size-2 rounded-full" style={{ backgroundColor: datum.color }} />
            {datum.id}: {formatNumber(datum.value)} (
            {total > 0 ? ((datum.value / total) * 100).toFixed(1) : 0}%)
          </div>
        )}
      />
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
        <div className="text-xl font-semibold tabular-nums">{formatNumber(total)}</div>
        <div className="text-muted-foreground text-[11px]">events</div>
      </div>
    </div>
  );
}

// Nivo: horizontal stacked bars of the busiest tables by operation.
export function TableActivityBars({ tables }: { tables: TableStats[] }) {
  const p = usePalette();
  const data = useMemo(
    () =>
      tables
        .slice()
        .sort((a, b) => b.total - a.total)
        .slice(0, 8)
        .reverse()
        .map((t) => ({
          table: t.table,
          INSERT: t.insert,
          UPDATE: t.update,
          DELETE: t.delete,
          READ: t.read,
          TRUNCATE: t.truncate,
        })),
    [tables],
  );

  if (data.length === 0) {
    return (
      <div className="text-muted-foreground flex h-[220px] items-center justify-center text-sm">
        No events yet.
      </div>
    );
  }

  return (
    <div style={{ height: Math.max(180, data.length * 34 + 40) }}>
      <ResponsiveBar
        data={data}
        keys={['INSERT', 'UPDATE', 'DELETE', 'READ', 'TRUNCATE']}
        indexBy="table"
        layout="horizontal"
        margin={{ top: 0, right: 16, bottom: 28, left: 130 }}
        padding={0.35}
        colors={({ id }) => OP_COLORS[id as string] ?? '#71717a'}
        borderRadius={2}
        enableLabel={false}
        enableGridY={false}
        enableGridX
        animate={false}
        theme={{
          grid: { line: { stroke: p.grid } },
          axis: {
            ticks: { text: { fill: p.axis, fontSize: 11 } },
          },
        }}
        axisBottom={{ tickSize: 0, tickPadding: 8, format: (v) => formatNumber(Number(v)) }}
        axisLeft={{ tickSize: 0, tickPadding: 8 }}
        tooltip={({ id, value, indexValue, color }) => (
          <div style={chartTooltipStyle(p)} className="flex items-center gap-2 whitespace-nowrap">
            <span className="size-2 rounded-full" style={{ backgroundColor: color }} />
            {indexValue} · {id}: {formatNumber(value)}
          </div>
        )}
      />
    </div>
  );
}

export function OpLegend() {
  return (
    <div className="text-muted-foreground flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px]">
      {Object.entries(OP_COLORS).map(([op, color]) => (
        <span key={op} className="flex items-center gap-1.5">
          <span className="size-2 rounded-full" style={{ backgroundColor: color }} />
          {op}
        </span>
      ))}
    </div>
  );
}
