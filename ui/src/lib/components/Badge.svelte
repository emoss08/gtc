<script>
  let { variant = 'secondary', children } = $props();

  const variants = {
    default: 'bg-primary text-primary-foreground',
    secondary: 'bg-secondary text-secondary-foreground',
    destructive: 'bg-destructive/15 text-destructive',
    success: 'bg-success/15 text-success',
    warning: 'bg-warning/20 text-warning',
    outline: 'border border-border text-foreground',
  };
</script>

<span
  class="inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-xs font-medium whitespace-nowrap {variants[
    variant
  ] ?? variants.secondary}"
>
  {@render children?.()}
</span>
