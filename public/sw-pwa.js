/**
 * Standalone Service Worker for Xibo PWA Player
 * Thin entry point — all reusable logic lives in @xiboplayer/sw
 *
 * Architecture:
 * - @xiboplayer/sw: CacheManager, BlobCache, RequestHandler, MessageHandler
 * - @xiboplayer/cache: DownloadManager, LayoutTaskBuilder
 * - This file: PWA-specific wiring (lifecycle events, Interactive Control)
 */

import { DownloadManager } from '@xiboplayer/cache/download-manager';
import { VERSION as CACHE_VERSION } from '@xiboplayer/cache';
import {
  CacheManager,
  BlobCache,
  RequestHandler,
  MessageHandler,
  calculateChunkConfig,
  SWLogger
} from '@xiboplayer/sw';
import { BASE } from '@xiboplayer/sw/utils';

// ── Configuration ──────────────────────────────────────────────────────────
const SW_VERSION = __BUILD_DATE__;
const CACHE_NAME = 'xibo-media-v1';
const STATIC_CACHE = 'xibo-static-v1';

const STATIC_FILES = [
  BASE + '/',
  BASE + '/index.html',
  BASE + '/setup.html'
];

const log = new SWLogger('SW');

// ── Device-adaptive chunk config ───────────────────────────────────────────
const CHUNK_CONFIG = calculateChunkConfig(log);
const CHUNK_SIZE = CHUNK_CONFIG.chunkSize;
const CHUNK_STORAGE_THRESHOLD = CHUNK_CONFIG.threshold;
const BLOB_CACHE_SIZE_MB = CHUNK_CONFIG.blobCacheSize;
const CONCURRENT_DOWNLOADS = CHUNK_CONFIG.concurrency;

log.info('Loading modular Service Worker:', SW_VERSION);

// ── Initialize shared instances ────────────────────────────────────────────
const downloadManager = new DownloadManager({
  concurrency: CONCURRENT_DOWNLOADS,
  chunkSize: CHUNK_SIZE,
  chunksPerFile: 2
});

const cacheManager = new CacheManager({
  cacheName: CACHE_NAME,
  chunkSize: CHUNK_SIZE
});

const blobCache = new BlobCache(BLOB_CACHE_SIZE_MB);

const requestHandler = new RequestHandler(downloadManager, cacheManager, blobCache, {
  staticCache: STATIC_CACHE
});

const messageHandler = new MessageHandler(downloadManager, cacheManager, blobCache, {
  cacheName: CACHE_NAME,
  staticCache: STATIC_CACHE,
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
    Promise.all([
      cacheManager.init(),
      caches.open(STATIC_CACHE).then((cache) => {
        log.info('Caching static files');
        return cache.addAll(STATIC_FILES);
      })
    ]).then(async () => {
      log.info('Cache initialized');
      // Only skipWaiting if this is a genuinely new version.
      // Re-activating the same version kills in-flight fetch responses
      // (e.g. video streams), causing playback stalls.
      // But always activate if there's no active SW (fresh install or after wipe).
      if (self.registration.active) {
        try {
          const versionCache = await caches.open('xibo-sw-version');
          const stored = await versionCache.match('version');
          if (stored) {
            const activeVersion = await stored.text();
            if (activeVersion === SW_VERSION) {
              log.info('Same version already active, skipping activation to preserve streams');
              return; // Don't skipWaiting — let the existing SW keep serving
            }
            log.info('Version changed:', activeVersion, '→', SW_VERSION);
          }
        } catch (_) {
          // Can't read version cache — proceed with skipWaiting
        }
      }
      log.info('New version, activating immediately');
      return self.skipWaiting();
    })
  );
});

// ── Lifecycle: Activate ────────────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  log.info('Activating... Version:', SW_VERSION, '| @xiboplayer/cache:', CACHE_VERSION);
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames
          .filter((name) => name.startsWith('xibo-') && name !== CACHE_NAME && name !== STATIC_CACHE && name !== 'xibo-sw-version')
          .map((name) => {
            log.info('Deleting old cache:', name);
            return caches.delete(name);
          })
      );
    }).then(async () => {
      // Store current version so future installs can detect same-version reloads
      const versionCache = await caches.open('xibo-sw-version');
      await versionCache.put('version', new Response(SW_VERSION));
      log.info('Taking control of all clients immediately');
      return self.clients.claim();
    }).then(async () => {
      // Signal to all clients that SW is ready to handle fetch events
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

  // Only intercept specific requests, let everything else pass through
  const shouldIntercept =
    url.pathname.startsWith(BASE + '/cache/') ||  // Cache requests
    url.pathname.startsWith(BASE + '/ic/') ||  // Interactive Control requests from widgets
    url.pathname.startsWith('/player/') && (url.pathname.endsWith('.html') || url.pathname === '/player/') ||
    (url.pathname.includes('xmds.php') && url.searchParams.has('file') && event.request.method === 'GET');

  if (shouldIntercept) {
    // Interactive Control routes - forward to main thread
    if (url.pathname.startsWith(BASE + '/ic/')) {
      event.respondWith(handleInteractiveControl(event));
      return;
    }
    event.respondWith(requestHandler.handleRequest(event));
  }
  // Let POST requests and other requests pass through without interception
});

// ── Message handler ────────────────────────────────────────────────────────
self.addEventListener('message', (event) => {
  event.waitUntil(
    messageHandler.handleMessage(event).then((result) => {
      // Send response back to client
      event.ports[0]?.postMessage(result);
    })
  );
});

log.info('Modular Service Worker ready');
