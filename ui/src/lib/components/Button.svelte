<script>
  let {
    variant = 'outline',
    size = 'sm',
    disabled = false,
    onclick,
    children,
    title = '',
  } = $props();

  const variants = {
    default: 'bg-primary text-primary-foreground hover:bg-primary/90',
    outline: 'border border-border bg-transparent hover:bg-accent hover:text-accent-foreground',
    destructive: 'text-destructive border border-destructive/30 hover:bg-destructive/10',
    ghost: 'hover:bg-accent hover:text-accent-foreground',
  };
  const sizes = {
    sm: 'h-7 px-2.5 text-xs',
    md: 'h-9 px-4 text-sm',
  };
</script>

<button
  type="button"
  {title}
  {disabled}
  {onclick}
  class="inline-flex items-center justify-center gap-1 rounded-md font-medium transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50 {variants[
    variant
  ] ?? variants.outline} {sizes[size] ?? sizes.sm}"
>
  {@render children?.()}
</button>
