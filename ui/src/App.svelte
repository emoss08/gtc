<script>
  import { getStats, getDLQ, retryDLQ, retryAllDLQ, discardDLQ, triggerBackfill } from './lib/api.js';
  import { formatNumber, formatBytes, formatDuration, formatRate, timeAgo } from './lib/format.js';
  import Card from './lib/components/Card.svelte';
  import Badge from './lib/components/Badge.svelte';
  import Button from './lib/components/Button.svelte';
  import StatCard from './lib/components/StatCard.svelte';
  import Sparkline from './lib/components/Sparkline.svelte';

  const POLL_MS = 2000;

  let stats = $state(null);
  let dlq = $state({ total: 0, entries: [] });
  let error = $state('');
  let actionMessage = $state('');
  let busy = $state(false);
  let rateHistory = $state([]);
  let lastSample = null;

  let rate = $derived(rateHistory.length ? rateHistory[rateHistory.length - 1] : 0);
  let breakerLabel = (v) => (v >= 2 ? 'open' : v >= 1 ? 'half-open' : 'closed');

  async function poll() {
    try {
      const s = await getStats();
      if (lastSample) {
        const dt = (Date.now() - lastSample.at) / 1000;
        const delta = s.events_total - lastSample.total;
        if (dt > 0 && delta >= 0) {
          rateHistory = [...rateHistory.slice(-59), delta / dt];
        }
      }
      lastSample = { at: Date.now(), total: s.events_total };
      stats = s;
      error = '';

      if (s.dlq?.enabled) {
        dlq = await getDLQ(50);
      }
    } catch (e) {
      error = e.message;
    }
  }

  async function act(fn, okMessage) {
    busy = true;
    actionMessage = '';
    try {
      await fn();
      actionMessage = okMessage;
      await poll();
    } catch (e) {
      actionMessage = `Error: ${e.message}`;
    } finally {
      busy = false;
      setTimeout(() => (actionMessage = ''), 5000);
    }
  }

  function toggleTheme() {
    const dark = document.documentElement.classList.toggle('dark');
    localStorage.setItem('gtc-theme', dark ? 'dark' : 'light');
  }

  $effect(() => {
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => clearInterval(id);
  });

  const backfillVariant = {
    done: 'success',
    running: 'default',
    pending: 'secondary',
    failed: 'destructive',
    skipped: 'warning',
  };
</script>

<div class="mx-auto max-w-6xl px-4 py-6">
  <!-- Header -->
  <header class="mb-6 flex flex-wrap items-center justify-between gap-3">
    <div class="flex items-center gap-3">
      <div class="flex h-9 w-9 items-center justify-center rounded-lg bg-primary text-lg text-primary-foreground">
        ⚡
      </div>
      <div>
        <h1 class="text-lg font-semibold tracking-tight">GTC</h1>
        <p class="text-xs text-muted-foreground">PostgreSQL change data capture</p>
      </div>
    </div>
    <div class="flex items-center gap-2">
      {#if stats}
        {#if stats.streaming}
          <Badge variant="success">
            <span class="h-1.5 w-1.5 animate-pulse rounded-full bg-current"></span>
            Streaming
          </Badge>
        {:else}
          <Badge variant="destructive">● Disconnected</Badge>
        {/if}
        <Badge variant="outline">LSN {stats.current_lsn || '–'}</Badge>
        <Badge variant="outline">up {formatDuration(stats.uptime_seconds)}</Badge>
      {/if}
      <Button variant="ghost" onclick={toggleTheme} title="Toggle theme">◐</Button>
    </div>
  </header>

  {#if error}
    <div class="mb-4 rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-2 text-sm text-destructive">
      Cannot reach GTC: {error}
    </div>
  {/if}
  {#if actionMessage}
    <div class="mb-4 rounded-lg border border-border bg-muted px-4 py-2 text-sm">
      {actionMessage}
    </div>
  {/if}

  {#if stats}
    <!-- Stat cards -->
    <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <StatCard label="Throughput" value={formatRate(rate)} sub="{formatNumber(stats.events_total)} events total">
        <Sparkline points={rateHistory} />
      </StatCard>
      <StatCard
        label="WAL lag"
        value={formatBytes(stats.wal_lag_bytes)}
        sub={stats.ready ? 'ready' : 'not ready'}
      />
      <StatCard label="In flight" value={formatNumber(stats.inflight)} sub="events being processed" />
      <StatCard
        label="Dead letters"
        value={stats.dlq.enabled ? formatNumber(stats.dlq.entries) : 'off'}
        sub={stats.dlq.enabled ? 'parked events' : 'DLQ disabled'}
      />
    </div>

    <!-- Sinks -->
    <div class="mt-4 grid gap-4 lg:grid-cols-2">
      <Card title="Sinks">
        {#if stats.sinks.length === 0}
          <p class="px-4 py-6 text-sm text-muted-foreground">No sinks configured.</p>
        {:else}
          <div class="divide-y divide-border">
            {#each stats.sinks as s (s.name)}
              <div class="flex items-center justify-between gap-2 px-4 py-3">
                <div class="flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full {s.healthy ? 'bg-success' : 'bg-destructive'}"></span>
                  <span class="text-sm font-medium">{s.name}</span>
                  {#if s.breaker_state >= 1}
                    <Badge variant={s.breaker_state >= 2 ? 'destructive' : 'warning'}>
                      breaker {breakerLabel(s.breaker_state)}
                    </Badge>
                  {/if}
                </div>
                <div class="flex items-center gap-3 text-xs text-muted-foreground tabular-nums">
                  <span title="processed">✓ {formatNumber(s.succeeded)}</span>
                  {#if s.failed > 0}<span class="text-destructive" title="failed">✕ {formatNumber(s.failed)}</span>{/if}
                  {#if s.retries > 0}<span title="retries">↻ {formatNumber(s.retries)}</span>{/if}
                  {#if s.filtered > 0}<span title="filtered by transforms">⊘ {formatNumber(s.filtered)}</span>{/if}
                </div>
              </div>
            {/each}
          </div>
        {/if}
      </Card>

      <!-- Tables -->
      <Card title="Table activity">
        {#if stats.tables.length === 0}
          <p class="px-4 py-6 text-sm text-muted-foreground">No events received yet.</p>
        {:else}
          <div class="max-h-72 overflow-y-auto">
            <table class="w-full text-sm">
              <thead class="sticky top-0 bg-card text-left text-xs text-muted-foreground">
                <tr class="border-b border-border">
                  <th class="px-4 py-2 font-medium">Table</th>
                  <th class="px-2 py-2 text-right font-medium">INS</th>
                  <th class="px-2 py-2 text-right font-medium">UPD</th>
                  <th class="px-2 py-2 text-right font-medium">DEL</th>
                  <th class="px-2 py-2 text-right font-medium">READ</th>
                  <th class="px-4 py-2 text-right font-medium">Total</th>
                </tr>
              </thead>
              <tbody class="tabular-nums">
                {#each stats.tables as t (t.table)}
                  <tr class="border-b border-border last:border-0">
                    <td class="px-4 py-2 font-medium">{t.table}</td>
                    <td class="px-2 py-2 text-right text-muted-foreground">{formatNumber(t.insert)}</td>
                    <td class="px-2 py-2 text-right text-muted-foreground">{formatNumber(t.update)}</td>
                    <td class="px-2 py-2 text-right text-muted-foreground">{formatNumber(t.delete)}</td>
                    <td class="px-2 py-2 text-right text-muted-foreground">{formatNumber(t.read)}</td>
                    <td class="px-4 py-2 text-right font-medium">{formatNumber(t.total)}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      </Card>
    </div>

    <!-- Backfill + DLQ -->
    <div class="mt-4 grid gap-4 lg:grid-cols-2">
      <Card title="Backfill">
        {#snippet action()}
          <Button
            {busy}
            disabled={busy}
            onclick={() => act(() => triggerBackfill(), 'Backfill of all tables enqueued.')}
          >
            Sync all tables
          </Button>
        {/snippet}
        {#if stats.backfill.length === 0}
          <p class="px-4 py-6 text-sm text-muted-foreground">No backfills recorded.</p>
        {:else}
          <div class="max-h-72 divide-y divide-border overflow-y-auto">
            {#each stats.backfill as b (b.schema + '.' + b.table)}
              <div class="flex items-center justify-between gap-2 px-4 py-3">
                <div class="flex min-w-0 items-center gap-2">
                  <span class="truncate text-sm font-medium">{b.schema}.{b.table}</span>
                  <Badge variant={backfillVariant[b.state] ?? 'secondary'}>{b.state}</Badge>
                </div>
                <div class="flex items-center gap-3 text-xs text-muted-foreground tabular-nums">
                  <span>{formatNumber(b.rows_copied)} rows</span>
                  {#if b.error}<span class="max-w-40 truncate text-destructive" title={b.error}>{b.error}</span>{/if}
                </div>
              </div>
            {/each}
          </div>
        {/if}
      </Card>

      <Card title="Dead-letter queue">
        {#snippet action()}
          {#if dlq.total > 0}
            <Button
              disabled={busy}
              onclick={() => act(retryAllDLQ, 'Retried all dead-letter entries.')}
            >
              Retry all
            </Button>
          {/if}
        {/snippet}
        {#if !stats.dlq.enabled}
          <p class="px-4 py-6 text-sm text-muted-foreground">
            DLQ is disabled (requires REDIS_URL). Sink failures stall the pipeline.
          </p>
        {:else if dlq.entries.length === 0}
          <p class="px-4 py-6 text-sm text-muted-foreground">No parked events. 🎉</p>
        {:else}
          <div class="max-h-72 divide-y divide-border overflow-y-auto">
            {#each dlq.entries as e (e.id)}
              <div class="px-4 py-3">
                <div class="flex items-center justify-between gap-2">
                  <div class="flex min-w-0 items-center gap-2">
                    <Badge variant="outline">{e.sink}</Badge>
                    <span class="truncate text-sm font-medium">{e.schema}.{e.table}</span>
                    <Badge variant="secondary">{e.operation}</Badge>
                  </div>
                  <div class="flex shrink-0 items-center gap-1">
                    <Button
                      disabled={busy}
                      onclick={() => act(() => retryDLQ(e.id), `Retried ${e.id}.`)}
                    >
                      Retry
                    </Button>
                    <Button
                      variant="destructive"
                      disabled={busy}
                      onclick={() => act(() => discardDLQ(e.id), `Discarded ${e.id}.`)}
                    >
                      Discard
                    </Button>
                  </div>
                </div>
                <p class="mt-1 truncate text-xs text-destructive" title={e.error}>{e.error}</p>
                <p class="mt-0.5 text-xs text-muted-foreground tabular-nums">
                  {e.attempts} attempts · LSN {e.lsn} · last failed {timeAgo(e.last_failed_at)}
                </p>
              </div>
            {/each}
          </div>
        {/if}
      </Card>
    </div>

    <footer class="mt-6 flex items-center justify-between text-xs text-muted-foreground">
      <span>refreshes every {POLL_MS / 1000}s</span>
      <div class="flex gap-3">
        <a class="hover:text-foreground" href="/metrics" target="_blank" rel="noreferrer">metrics</a>
        <a class="hover:text-foreground" href="/readiness" target="_blank" rel="noreferrer">readiness</a>
        <a
          class="hover:text-foreground"
          href="https://github.com/emoss08/gtc"
          target="_blank"
          rel="noreferrer">github</a
        >
      </div>
    </footer>
  {:else if !error}
    <p class="py-16 text-center text-sm text-muted-foreground">Loading…</p>
  {/if}
</div>
