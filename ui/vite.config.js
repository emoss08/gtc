import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

// In dev, proxy the gateway's API so `npm run dev` works against a locally
// running GTC on :8080.
const proxy = {};
for (const path of ['/api', '/dlq', '/backfill', '/health', '/readiness', '/metrics']) {
  proxy[path] = 'http://localhost:8080';
}

export default defineConfig({
  plugins: [svelte(), tailwindcss()],
  server: { proxy },
  build: { outDir: 'dist', emptyOutDir: true },
});
