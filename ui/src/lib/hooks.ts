import { useQuery } from '@tanstack/react-query';
import { useRef } from 'react';
import { getDlq, getStats, type Stats } from '@/lib/api';

export const POLL_INTERVAL = 2000;

export function useStats() {
  return useQuery({
    queryKey: ['stats'],
    queryFn: getStats,
    refetchInterval: POLL_INTERVAL,
  });
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
