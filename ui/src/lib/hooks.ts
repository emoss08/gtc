import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import { getDlq, getHistory, getStats, type Stats } from '@/lib/api';

export const POLL_INTERVAL = 2000;

export function useStats() {
  return useQuery({
    queryKey: ['stats'],
    queryFn: getStats,
    refetchInterval: POLL_INTERVAL,
  });
}

export interface RatePoint {
  t: number;
  rate: number;
  walLag: number;
  inflight: number;
  dlq: number;
}

// Server-side history (5s samples, 30min window) turned into a rate series.
export function useHistory(): { points: RatePoint[]; isLoading: boolean } {
  const { data, isLoading } = useQuery({
    queryKey: ['history'],
    queryFn: getHistory,
    refetchInterval: 5000,
  });

  const points = useMemo(() => {
    const samples = data?.samples ?? [];
    const out: RatePoint[] = [];
    for (let i = 1; i < samples.length; i++) {
      const prev = samples[i - 1];
      const cur = samples[i];
      const dt = (cur.t - prev.t) / 1000;
      // Counter reset (process restart) shows as a negative delta; clamp.
      const rate = dt > 0 ? Math.max(0, (cur.events_total - prev.events_total) / dt) : 0;
      out.push({
        t: cur.t,
        rate,
        walLag: cur.wal_lag_bytes,
        inflight: cur.inflight,
        dlq: cur.dlq_entries,
      });
    }
    return out;
  }, [data]);

  return { points, isLoading };
}

// Charts need concrete colors per theme; watch the <html> class so they
// re-render when the toggle flips.
export function useIsDark(): boolean {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains('dark'));
  useEffect(() => {
    const observer = new MutationObserver(() => {
      setDark(document.documentElement.classList.contains('dark'));
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);
  return dark;
}

export function useDlq(enabled = true) {
  return useQuery({
    queryKey: ['dlq'],
    queryFn: () => getDlq(200),
    refetchInterval: POLL_INTERVAL,
    enabled,
  });
}

// Throughput is computed client-side from deltas of the cumulative event
// counter, keeping the server stateless. A rolling window feeds the sparkline.
export function useThroughput(stats: Stats | undefined): { rate: number; history: number[] } {
  const samples = useRef<{ t: number; total: number }[]>([]);
  const history = useRef<number[]>([]);
  const lastTotal = useRef<number | null>(null);

  if (stats) {
    const now = Date.now();
    const total = stats.events_total;
    if (lastTotal.current !== total || samples.current.length === 0) {
      // Counter reset (process restart) — start over.
      if (lastTotal.current !== null && total < lastTotal.current) {
        samples.current = [];
        history.current = [];
      }
      lastTotal.current = total;
      samples.current.push({ t: now, total });
      if (samples.current.length > 2) {
        const prev = samples.current[samples.current.length - 2];
        const dt = (now - prev.t) / 1000;
        if (dt > 0.2) {
          history.current.push(Math.max(0, (total - prev.total) / dt));
          if (history.current.length > 40) history.current.shift();
        }
      }
      if (samples.current.length > 50) samples.current.shift();
    }
  }

  const h = history.current;
  return { rate: h.length > 0 ? h[h.length - 1] : 0, history: [...h] };
}
