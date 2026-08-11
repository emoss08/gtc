<script>
  let { label, value, sub = '', children } = $props();
</script>

<div class="rounded-lg border border-border bg-card p-4 text-card-foreground shadow-sm">
  <div class="flex items-start justify-between gap-2">
    <div class="min-w-0">
      <p class="text-xs font-medium text-muted-foreground">{label}</p>
      <p class="mt-1 truncate text-2xl font-semibold tracking-tight tabular-nums">{value}</p>
      {#if sub}
        <p class="mt-0.5 truncate text-xs text-muted-foreground">{sub}</p>
      {/if}
    </div>
    {@render children?.()}
  </div>
</div>
