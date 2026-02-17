import { defineConfig } from 'vite';
import path from 'path';
import { readFileSync } from 'fs';

const pkg = JSON.parse(readFileSync(path.resolve(__dirname, 'package.json'), 'utf-8'));

export default defineConfig({
  define: {
    __APP_VERSION__: JSON.stringify(pkg.version),
  },
  base: './',  // Use relative paths to work from any location
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      external: ['hls.js'],
      input: {
        main: path.resolve(__dirname, 'index.html'),
        setup: path.resolve(__dirname, 'setup.html'),
        sw: path.resolve(__dirname, 'public/sw-pwa.js'),
      },
      output: {
        entryFileNames: (chunkInfo) => {
          // Service Worker goes to root of dist (not assets/)
          if (chunkInfo.name === 'sw') {
            return 'sw-pwa.js';
          }
          return 'assets/[name]-[hash].js';
        },
      },
    },
  },
  // No more path aliases - using npm packages instead
  server: {
    port: 5174,
    proxy: {
      // Proxy XMDS requests to CMS during development
      '/xmds.php': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
});
