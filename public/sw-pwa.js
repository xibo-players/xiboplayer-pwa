/**
 * Standalone Service Worker for Xibo PWA Player
 * Thin entry point — all reusable logic lives in @xiboplayer/sw
 *
 * Architecture:
 * - @xiboplayer/sw: RequestHandler, MessageHandler
 * - @xiboplayer/cache: DownloadManager, LayoutTaskBuilder
 * - @xiboplayer/proxy: ContentStore (filesystem storage — runs server-side)
 * - This file: PWA-specific wiring (lifecycle events, Interactive Control)
 *
 * Media storage flow:
 *   CMS → proxy /file-proxy → ContentStore (filesystem) → proxy /store → SW → renderer
 *   The SW orchestrates downloads but never stores media — the proxy does.
 */

import { DownloadManager } from '@xiboplayer/cache/download-manager';
import { VERSION as CACHE_VERSION } from '@xiboplayer/cache';
import {
  RequestHandler,
  MessageHandler,
  calculateChunkConfig,
  SWLogger
} from '@xiboplayer/sw';
import { BASE } from '@xiboplayer/sw/utils';

// ── Configuration ──────────────────────────────────────────────────────────
const SW_VERSION = __BUILD_DATE__;

const log = new SWLogger('SW');

// ── Device-adaptive chunk config ───────────────────────────────────────────
const CHUNK_CONFIG = calculateChunkConfig(log);
const CHUNK_SIZE = CHUNK_CONFIG.chunkSize;
const CHUNK_STORAGE_THRESHOLD = CHUNK_CONFIG.threshold;
const CONCURRENT_DOWNLOADS = CHUNK_CONFIG.concurrency;

log.info('Loading modular Service Worker:', SW_VERSION);

// ── Initialize shared instances ────────────────────────────────────────────
const downloadManager = new DownloadManager({
  concurrency: CONCURRENT_DOWNLOADS,
  chunkSize: CHUNK_SIZE,
  chunksPerFile: 2
});

const requestHandler = new RequestHandler(downloadManager);

const messageHandler = new MessageHandler(downloadManager, {
  chunkSize: CHUNK_SIZE,
  chunkStorageThreshold: CHUNK_STORAGE_THRESHOLD
});

// ── PWA-specific: Interactive Control handler ──────────────────────────────

/**
 * Handle Interactive Control requests from widget iframes.
 * Forwards to main thread via MessageChannel and returns the response.
 * IC library in widgets uses XHR to /player/pwa/ic/{route}.
 */
async function handleInteractiveControl(event) {
  const url = new URL(event.request.url);
  const icPath = url.pathname.replace(BASE + '/ic', '');
  const method = event.request.method;

  log.info('Interactive Control request:', method, icPath);

  let body = null;
  if (method === 'POST' || method === 'PUT') {
    try {
      body = await event.request.text();
    } catch (_) {}
  }

  // Forward to main thread via MessageChannel
  const clients = await self.clients.matchAll({ type: 'window' });
  if (clients.length === 0) {
    return new Response(JSON.stringify({ error: 'No active player' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });
  }

  const client = clients[0];

  try {
    const response = await new Promise((resolve, reject) => {
      const channel = new MessageChannel();
      const timer = setTimeout(() => reject(new Error('IC timeout')), 5000);

      channel.port1.onmessage = (msg) => {
        clearTimeout(timer);
        resolve(msg.data);
      };

      client.postMessage({
        type: 'INTERACTIVE_CONTROL',
        method,
        path: icPath,
        search: url.search,
        body
      }, [channel.port2]);
    });

    return new Response(response.body || '', {
      status: response.status || 200,
      headers: {
        'Content-Type': response.contentType || 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });
  } catch (error) {
    log.error('IC handler error:', error);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });
  }
}

// ── Lifecycle: Install ─────────────────────────────────────────────────────
self.addEventListener('install', (event) => {
  log.info('Installing... Version:', SW_VERSION);
  event.waitUntil(
    (async () => {
      // Check if same version is already active — skip activation to preserve streams
      if (self.registration.active) {
        try {
          const versionCache = await caches.open('xibo-sw-version');
          const stored = await versionCache.match('version');
          if (stored) {
            const activeVersion = await stored.text();
            if (activeVersion === SW_VERSION) {
              log.info('Same version already active, skipping activation to preserve streams');
              return;
            }
            log.info('Version changed:', activeVersion, '→', SW_VERSION);
          }
        } catch (_) {}
      }
      log.info('New version, activating immediately');
      return self.skipWaiting();
    })()
  );
});

// ── Lifecycle: Activate ────────────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  log.info('Activating... Version:', SW_VERSION, '| @xiboplayer/cache:', CACHE_VERSION);
  event.waitUntil(
    // Clean up legacy Cache API caches (migration from pre-ContentStore)
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames
          .filter((name) => name.startsWith('xibo-') && name !== 'xibo-sw-version')
          .map((name) => {
            log.info('Deleting legacy cache:', name);
            return caches.delete(name);
          })
      );
    }).then(async () => {
      const versionCache = await caches.open('xibo-sw-version');
      await versionCache.put('version', new Response(SW_VERSION));
      log.info('Taking control of all clients immediately');
      return self.clients.claim();
    }).then(async () => {
      log.info('Notifying all clients that fetch handler is ready');
      const clients = await self.clients.matchAll();
      clients.forEach(client => {
        client.postMessage({ type: 'SW_READY' });
      });
    })
  );
});

// ── Fetch handler ──────────────────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  const shouldIntercept =
    url.pathname.startsWith(BASE + '/cache/') ||
    url.pathname.startsWith(BASE + '/ic/') ||
    url.pathname.startsWith('/player/') && (url.pathname.endsWith('.html') || url.pathname === '/player/') ||
    (url.pathname.includes('xmds.php') && url.searchParams.has('file') && event.request.method === 'GET');

  if (shouldIntercept) {
    if (url.pathname.startsWith(BASE + '/ic/')) {
      event.respondWith(handleInteractiveControl(event));
      return;
    }
    event.respondWith(requestHandler.handleRequest(event));
  }
});

// ── Message handler ────────────────────────────────────────────────────────
self.addEventListener('message', (event) => {
  event.waitUntil(
    messageHandler.handleMessage(event).then((result) => {
      event.ports[0]?.postMessage(result);
    })
  );
});

log.info('Modular Service Worker ready');
