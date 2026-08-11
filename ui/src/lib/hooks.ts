import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';
import { getDlq, getHistory, getStats } from '@/lib/api';

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

