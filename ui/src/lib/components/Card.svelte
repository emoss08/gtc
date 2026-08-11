<script>
  let { title = '', action, children, class: className = '' } = $props();
</script>

<div class="rounded-lg border border-border bg-card text-card-foreground shadow-sm {className}">
  {#if title || action}
    <div class="flex items-center justify-between border-b border-border px-4 py-3">
      <h3 class="text-sm font-semibold tracking-tight">{title}</h3>
      {#if action}{@render action()}{/if}
    </div>
  {/if}
  {@render children?.()}
</div>
