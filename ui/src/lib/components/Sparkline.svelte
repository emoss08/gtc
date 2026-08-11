<script>
  let { points = [], width = 96, height = 36 } = $props();

  let path = $derived.by(() => {
    if (points.length < 2) return '';
    const max = Math.max(...points, 1e-9);
    const stepX = width / (points.length - 1);
    return points
      .map((p, i) => {
        const x = (i * stepX).toFixed(1);
        const y = (height - 2 - (p / max) * (height - 4)).toFixed(1);
        return `${i === 0 ? 'M' : 'L'}${x},${y}`;
      })
      .join(' ');
  });
</script>

{#if path}
  <svg {width} {height} viewBox="0 0 {width} {height}" class="shrink-0 text-muted-foreground/70">
    <path d={path} fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" />
  </svg>
{/if}
