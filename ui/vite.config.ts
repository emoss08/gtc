import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'node:path';

// In dev, proxy the gateway's API so `npm run dev` works against a locally
// running GTC on :8080.
const proxy: Record<string, string> = {};
for (const p of ['/api', '/dlq', '/backfill', '/health', '/readiness', '/metrics']) {
  proxy[p] = 'http://localhost:8080';
}

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': path.resolve(__dirname, './src') },
  },
  server: { proxy },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      output: {
        manualChunks: {
          charts: ['recharts', '@nivo/core', '@nivo/pie', '@nivo/bar'],
          data: ['@tanstack/react-table', '@tanstack/react-query', 'react-hook-form', 'zod'],
        },
      },
    },
  },
});
