/**
 * Standalone Service Worker for Xibo Players
 * Version: 2026-02-07 - Refactored to use shared DownloadManager module
 *
 * Architecture:
 * - DownloadManager: Imported from @tecman/xibo-player-core (shared module)
 * - CacheManager: Wraps Cache API with type-aware keys
 * - RequestHandler: Handles fetch events (serve from cache or wait for download)
 * - MessageHandler: Handles postMessage from client
 *
 * No HTTP 202 responses - always returns actual files or 404
 */

import { DownloadManager } from '../../../packages/cache/src/download-manager.js';
import {
  formatBytes,
  parseRangeHeader,
  createMediaHeaders,
  createErrorResponse,
  CacheKey,
  HTTP_STATUS,
  TIMEOUTS,
  getChunkBoundaries,
  getChunksForRange,
  extractRangeFromChunks
} from './sw-utils.js';

// Simple logger for Service Worker context
// Uses console but can be configured via self.swLogLevel
class SWLogger {
  constructor(name) {
    this.name = name;
    // Default level: INFO (can be changed via self.swLogLevel = 'DEBUG')
    this.level = self.swLogLevel || 'INFO';
  }

  debug(...args) {
    if (this.level === 'DEBUG') {
      console.log(`[${this.name}] DEBUG:`, ...args);
    }
  }

  info(...args) {
    if (this.level === 'DEBUG' || this.level === 'INFO') {
      console.log(`[${this.name}]`, ...args);
    }
  }

  warn(...args) {
    console.warn(`[${this.name}]`, ...args);
  }

  error(...args) {
    console.error(`[${this.name}]`, ...args);
  }
}

const log = new SWLogger('SW');

const SW_VERSION = '2026-02-11-chunk-race-fix';
const CACHE_NAME = 'xibo-media-v1';
const STATIC_CACHE = 'xibo-static-v1';

// Track in-progress chunk storage operations (cacheKey → Promise)
// Prevents serving chunked files before chunks are fully written to cache
const pendingChunkStorage = new Map();

// Dynamic chunk sizing based on available RAM
// These are BASE values - actual values calculated from device RAM
const BASE_CHUNK_SIZE = 50 * 1024 * 1024; // 50MB default
const BASE_CHUNK_STORAGE_THRESHOLD = 100 * 1024 * 1024; // Files > 100 MB
const BASE_BLOB_CACHE_SIZE_MB = 200; // In-memory blob cache limit

/**
 * Calculate optimal chunk size based on available device memory
 * @returns {Object} { chunkSize, threshold, blobCacheSize }
 */
function calculateChunkConfig() {
  // Try to detect device memory (Chrome only for now)
  const deviceMemoryGB = navigator.deviceMemory || null;

  // Fallback: estimate from user agent
  let estimatedRAM_GB = 4; // Default assumption

  if (deviceMemoryGB) {
    estimatedRAM_GB = deviceMemoryGB;
    log.info('Detected device memory:', deviceMemoryGB, 'GB');
  } else {
    // Parse user agent for hints
    const ua = navigator.userAgent.toLowerCase();
    if (ua.includes('raspberry pi') || ua.includes('armv6')) {
      estimatedRAM_GB = 0.5; // Pi Zero
      log.info('Detected Pi Zero (512 MB RAM estimated)');
    } else if (ua.includes('armv7')) {
      estimatedRAM_GB = 1; // Pi 3/4
      log.info('Detected ARM device (1 GB RAM estimated)');
    } else {
      log.info('Using default RAM estimate:', estimatedRAM_GB, 'GB');
    }
  }

  // Calculate chunk size: 1-5% of available RAM
  // Lower RAM = smaller chunks, higher RAM = larger chunks
  // Configure based on RAM - chunk size, cache, threshold, AND concurrency
  let chunkSize, blobCacheSize, threshold, concurrency;

  if (estimatedRAM_GB <= 0.5) {
    // Pi Zero (512 MB) - very conservative
    chunkSize = 10 * 1024 * 1024;
    blobCacheSize = 25;
    threshold = 25 * 1024 * 1024;
    concurrency = 1;  // Single download to avoid overwhelming CPU/RAM
    log.info('Low-memory config: 10 MB chunks, 25 MB cache, 1 concurrent download');
  } else if (estimatedRAM_GB <= 1) {
    // 1 GB RAM (Pi 3) - conservative
    chunkSize = 20 * 1024 * 1024;
    blobCacheSize = 50;
    threshold = 50 * 1024 * 1024;
    concurrency = 2;  // Limited concurrency
    log.info('1GB-RAM config: 20 MB chunks, 50 MB cache, 2 concurrent downloads');
  } else if (estimatedRAM_GB <= 2) {
    // 2 GB RAM - moderate
    chunkSize = 30 * 1024 * 1024;
    blobCacheSize = 100;
    threshold = 75 * 1024 * 1024;
    concurrency = 2;  // Moderate
    log.info('2GB-RAM config: 30 MB chunks, 100 MB cache, 2 concurrent downloads');
  } else if (estimatedRAM_GB <= 4) {
    // 4 GB RAM - default
    chunkSize = 50 * 1024 * 1024;
    blobCacheSize = 200;
    threshold = 100 * 1024 * 1024;
    concurrency = 4;  // Standard
    log.info('4GB-RAM config: 50 MB chunks, 200 MB cache, 4 concurrent downloads');
  } else {
    // 8+ GB RAM - generous
    chunkSize = 100 * 1024 * 1024;
    blobCacheSize = 500;
    threshold = 200 * 1024 * 1024;
    concurrency = 6;  // Higher for powerful systems
    log.info('High-RAM config: 100 MB chunks, 500 MB cache, 6 concurrent downloads');
  }

  return { chunkSize, blobCacheSize, threshold, concurrency };
}

// Calculate config based on device RAM
const CHUNK_CONFIG = calculateChunkConfig();
const CHUNK_SIZE = CHUNK_CONFIG.chunkSize;
const CHUNK_STORAGE_THRESHOLD = CHUNK_CONFIG.threshold;
const BLOB_CACHE_SIZE_MB = CHUNK_CONFIG.blobCacheSize;
const CONCURRENT_DOWNLOADS = CHUNK_CONFIG.concurrency;  // Adaptive concurrency
const CONCURRENT_CHUNKS = CHUNK_CONFIG.concurrency;  // Same as file concurrency

// Static files to cache on install
const STATIC_FILES = [
  '/player/',
  '/player/index.html',
  '/player/setup.html',
  '/player/manifest.json'
];

log.info('Loading modular Service Worker:', SW_VERSION);

// ============================================================================
// Class 1: CacheManager - Wraps Cache API
// ============================================================================

class CacheManager {
  constructor() {
    this.cache = null;
    this.log = new SWLogger('Cache');
  }

  async init() {
    this.cache = await caches.open(CACHE_NAME);
  }

  /**
   * Get cached file
   * Returns Response or null
   */
  async get(cacheKey) {
    if (!this.cache) await this.init();
    // Use ignoreVary and ignoreSearch for more lenient matching
    return await this.cache.match(cacheKey, {
      ignoreSearch: true,
      ignoreVary: true
    });
  }

  /**
   * Put file in cache
   */
  async put(cacheKey, blob, contentType) {
    if (!this.cache) await this.init();

    const response = new Response(blob, {
      headers: {
        'Content-Type': contentType,
        'Content-Length': blob.size,
        'Access-Control-Allow-Origin': '*',
        'Accept-Ranges': 'bytes'
      }
    });

    await this.cache.put(cacheKey, response);
  }

  /**
   * Delete file from cache
   */
  async delete(cacheKey) {
    if (!this.cache) await this.init();
    return await this.cache.delete(cacheKey);
  }

  /**
   * Clear all cached files
   */
  async clear() {
    if (!this.cache) await this.init();
    const keys = await this.cache.keys();
    await Promise.all(keys.map(key => this.cache.delete(key)));
    this.log.info('Cleared', keys.length, 'cached files');
  }

  /**
   * Check if file exists (supports both whole files and chunked storage)
   * Single source of truth for file existence checks
   * @param {string} cacheKey - Full cache key (e.g., /player/pwa/cache/media/6)
   * @returns {Promise<{exists: boolean, chunked: boolean, metadata: Object|null}>}
   */
  async fileExists(cacheKey) {
    if (!this.cache) await this.init();

    // Check for whole file first (backward compatibility)
    const wholeFile = await this.get(cacheKey);
    if (wholeFile) {
      return { exists: true, chunked: false, metadata: null };
    }

    // Check for chunked metadata
    const metadata = await this.getMetadata(cacheKey);
    if (metadata && metadata.chunked) {
      return { exists: true, chunked: true, metadata };
    }

    return { exists: false, chunked: false, metadata: null };
  }

  /**
   * Get file size (works for both whole files and chunks)
   * @param {string} cacheKey - Full cache key
   * @returns {Promise<number|null>} File size in bytes, or null if not found
   */
  async getFileSize(cacheKey) {
    const info = await this.fileExists(cacheKey);

    if (!info.exists) return null;

    if (info.chunked) {
      return info.metadata.totalSize;  // From chunked metadata
    }

    const response = await this.get(cacheKey);
    const contentLength = response?.headers.get('Content-Length');
    return contentLength ? parseInt(contentLength) : null;
  }

  /**
   * Store file as chunks for large files (low memory streaming)
   * @param {string} cacheKey - Base cache key (e.g., /player/pwa/cache/media/123)
   * @param {Blob} blob - File blob to store as chunks
   * @param {string} contentType - Content type
   */
  async putChunked(cacheKey, blob, contentType) {
    if (!this.cache) await this.init();

    const totalSize = blob.size;
    const numChunks = Math.ceil(totalSize / CHUNK_SIZE);

    this.log.info(`Storing as ${numChunks} chunks: ${cacheKey} (${formatBytes(totalSize)})`);

    // Store metadata
    const metadata = {
      totalSize,
      chunkSize: CHUNK_SIZE,
      numChunks,
      contentType,
      chunked: true,
      createdAt: Date.now()
    };

    const metadataResponse = new Response(JSON.stringify(metadata), {
      headers: { 'Content-Type': 'application/json' }
    });
    await this.cache.put(`${cacheKey}/metadata`, metadataResponse);

    // Store chunks
    for (let i = 0; i < numChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, totalSize);
      const chunkBlob = blob.slice(start, end);

      const chunkResponse = new Response(chunkBlob, {
        headers: {
          'Content-Type': contentType,
          'Content-Length': chunkBlob.size,
          'X-Chunk-Index': i,
          'X-Total-Chunks': numChunks
        }
      });

      await this.cache.put(`${cacheKey}/chunk-${i}`, chunkResponse);

      if ((i + 1) % 5 === 0 || i === numChunks - 1) {
        this.log.info(`Stored chunk ${i + 1}/${numChunks} (${formatBytes(chunkBlob.size)})`);
      }
    }

    this.log.info(`Chunked storage complete: ${cacheKey}`);
  }

  /**
   * Get metadata for chunked file
   * @param {string} cacheKey - Base cache key
   * @returns {Promise<Object|null>}
   */
  async getMetadata(cacheKey) {
    if (!this.cache) await this.init();

    const cached = await this.cache.match(`${cacheKey}/metadata`);
    if (!cached) return null;

    const text = await cached.text();
    return JSON.parse(text);
  }

  /**
   * Check if file is stored as chunks
   * @param {string} cacheKey - Base cache key
   * @returns {Promise<boolean>}
   */
  async isChunked(cacheKey) {
    const metadata = await this.getMetadata(cacheKey);
    return metadata?.chunked === true;
  }

  /**
   * Get specific chunk
   * @param {string} cacheKey - Base cache key
   * @param {number} chunkIndex - Chunk index
   * @returns {Promise<Response|null>}
   */
  async getChunk(cacheKey, chunkIndex) {
    if (!this.cache) await this.init();
    return await this.cache.match(`${cacheKey}/chunk-${chunkIndex}`);
  }
}

// ============================================================================
// Class 2: BlobCache - LRU cache for blob objects
// ============================================================================

/**
 * In-memory LRU cache for blob objects
 * Prevents re-materializing blobs from Cache API on every Range request
 */
class BlobCache {
  constructor(maxSizeMB = BLOB_CACHE_SIZE_MB) {
    this.cache = new Map(); // cacheKey → { blob, lastAccess, size }
    this.maxBytes = maxSizeMB * 1024 * 1024;
    this.currentBytes = 0;
    this.log = new SWLogger('BlobCache');
  }

  /**
   * Get blob from cache or load via loader function
   * @param {string} cacheKey - Cache key
   * @param {Function} loader - Async function that returns blob
   * @returns {Promise<Blob>}
   */
  async get(cacheKey, loader) {
    // Check memory cache first
    if (this.cache.has(cacheKey)) {
      const entry = this.cache.get(cacheKey);
      entry.lastAccess = Date.now();
      this.log.debug(`HIT: ${cacheKey} (${formatBytes(entry.size)})`);
      return entry.blob;
    }

    // Cache miss - load from Cache API
    this.log.debug(`MISS: ${cacheKey} - loading from Cache API`);
    const blob = await loader();

    // Evict LRU entries if over limit
    while (this.currentBytes + blob.size > this.maxBytes && this.cache.size > 0) {
      this.evictLRU();
    }

    // Cache if under limit
    if (this.currentBytes + blob.size <= this.maxBytes) {
      this.cache.set(cacheKey, {
        blob,
        lastAccess: Date.now(),
        size: blob.size
      });
      this.currentBytes += blob.size;
      const utilization = (this.currentBytes / this.maxBytes * 100).toFixed(1);
      this.log.debug(`CACHED: ${cacheKey} (${formatBytes(blob.size)}) - utilization: ${utilization}%`);
    } else {
      this.log.warn(`File too large to cache: ${cacheKey} (${formatBytes(blob.size)} > ${formatBytes(this.maxBytes)})`);
    }

    return blob;
  }

  /**
   * Evict least recently used entry
   */
  evictLRU() {
    let oldest = null;
    let oldestKey = null;

    for (const [key, entry] of this.cache) {
      if (!oldest || entry.lastAccess < oldest.lastAccess) {
        oldest = entry;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      this.currentBytes -= oldest.size;
      this.cache.delete(oldestKey);
      this.log.debug(`EVICTED LRU: ${oldestKey} (${formatBytes(oldest.size)})`);
    }
  }

  /**
   * Clear all cached blobs
   */
  clear() {
    this.cache.clear();
    this.currentBytes = 0;
    this.log.info('Cleared all cached blobs');
  }

  /**
   * Get cache statistics
   */
  getStats() {
    return {
      entries: this.cache.size,
      bytes: this.currentBytes,
      maxBytes: this.maxBytes,
      utilization: (this.currentBytes / this.maxBytes * 100).toFixed(1) + '%'
    };
  }
}

// ============================================================================
// Class 3: RequestHandler - Handles fetch events
// ============================================================================

class RequestHandler {
  constructor(downloadManager, cacheManager, blobCache) {
    this.downloadManager = downloadManager;
    this.cacheManager = cacheManager;
    this.blobCache = blobCache;
    this.pendingFetches = new Map(); // filename → Promise<Response> for deduplication
    this.log = log; // Use main SW logger
  }

  /**
   * Route file request to appropriate handler based on storage format
   * Single source of truth for format detection and handler selection
   *
   * @param {string} cacheKey - Cache key (e.g., /player/pwa/cache/media/6)
   * @param {string} method - HTTP method ('GET' or 'HEAD')
   * @param {string|null} rangeHeader - Range header value or null
   * @returns {Promise<{found: boolean, handler: string, data: Object}>}
   */
  async routeFileRequest(cacheKey, method, rangeHeader) {
    // Check file existence and format (centralized API)
    const fileInfo = await this.cacheManager.fileExists(cacheKey);

    if (!fileInfo.exists) {
      return { found: false, handler: null, data: null };
    }

    // Route based on storage format and request type
    if (fileInfo.chunked) {
      // Chunked storage routing
      const data = { metadata: fileInfo.metadata, cacheKey };

      if (method === 'HEAD') {
        return { found: true, handler: 'head-chunked', data };
      }
      if (rangeHeader) {
        return { found: true, handler: 'range-chunked', data: { ...data, rangeHeader } };
      }
      // GET without Range - serve full file from chunks
      return { found: true, handler: 'full-chunked', data };

    } else {
      // Whole file storage routing
      const cached = await this.cacheManager.get(cacheKey);
      const data = { cached, cacheKey };

      if (method === 'HEAD') {
        return { found: true, handler: 'head-whole', data };
      }
      if (rangeHeader) {
        return { found: true, handler: 'range-whole', data: { ...data, rangeHeader } };
      }
      // GET without Range - serve whole file
      return { found: true, handler: 'full-whole', data };
    }
  }

  /**
   * Handle fetch request
   * - Serve from cache if available
   * - Wait for download if in progress
   * - Return 404 if not cached and not downloading
   */
  async handleRequest(event) {
    const url = new URL(event.request.url);
    log.info('handleRequest called for:', url.href);
    log.info('pathname:', url.pathname);

    // Handle static files (player pages)
    if (url.pathname === '/player/' ||
        url.pathname === '/player/index.html' ||
        url.pathname === '/player/setup.html' ||
        url.pathname === '/player/manifest.json') {
      const cache = await caches.open(STATIC_CACHE);
      const cached = await cache.match(event.request);
      if (cached) {
        log.info('Serving static file from cache:', url.pathname);
        return cached;
      }
      // Fallback to network
      log.info('Fetching static file from network:', url.pathname);
      return fetch(event.request);
    }

    // Handle widget resources (bundle.min.js, fonts)
    // Uses pendingFetches for deduplication — concurrent requests share one fetch
    if (url.pathname.includes('xmds.php') &&
        (url.searchParams.get('fileType') === 'bundle' ||
         url.searchParams.get('fileType') === 'fontCss' ||
         url.searchParams.get('fileType') === 'font')) {
      const filename = url.searchParams.get('file');
      const cacheKey = `/player/pwa/cache/static/${filename}`;
      const cache = await caches.open(STATIC_CACHE);

      const cached = await cache.match(cacheKey);
      if (cached) {
        log.info('Serving widget resource from cache:', filename);
        return cached.clone();
      }

      // Check if another request is already fetching this resource
      if (this.pendingFetches.has(filename)) {
        log.info('Deduplicating widget resource fetch:', filename);
        const pending = await this.pendingFetches.get(filename);
        return pending.clone();
      }

      // Fetch from CMS with deduplication
      log.info('Fetching widget resource from CMS:', filename);
      const fetchPromise = (async () => {
        try {
          const response = await fetch(event.request);

          if (response.ok) {
            log.info('Caching widget resource:', filename, `(${response.headers.get('Content-Type')})`);
            const responseClone = response.clone();
            // AWAIT cache.put to prevent race condition
            await cache.put(cacheKey, responseClone);
            return response;
          } else {
            log.warn('Widget resource not available (', response.status, '):', filename, '- NOT caching');
            return response;
          }
        } catch (error) {
          log.error('Failed to fetch widget resource:', filename, error);
          return new Response('Failed to fetch widget resource', {
            status: 502,
            statusText: 'Bad Gateway',
            headers: { 'Content-Type': 'text/plain' }
          });
        }
      })();

      this.pendingFetches.set(filename, fetchPromise);
      try {
        const response = await fetchPromise;
        return response.clone();
      } finally {
        this.pendingFetches.delete(filename);
      }
    }

    // Handle XMDS media requests (XLR compatibility)
    if (url.pathname.includes('xmds.php') && url.searchParams.has('file')) {
      const filename = url.searchParams.get('file');
      const fileId = filename.split('.')[0];
      const fileType = url.searchParams.get('type');
      const cacheType = fileType === 'L' ? 'layout' : 'media';

      log.info('XMDS request:', filename, 'type:', fileType, '→ /player/pwa/cache/' + cacheType + '/' + fileId);

      const cacheKey = `/player/pwa/cache/${cacheType}/${fileId}`;
      const cached = await this.cacheManager.get(cacheKey);
      if (cached) {
        // Clone the response to avoid consuming the body
        return new Response(cached.clone().body, {
          headers: {
            'Content-Type': cached.headers.get('Content-Type') || 'video/mp4',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=31536000',
            'Accept-Ranges': 'bytes'
          }
        });
      }

      // Not cached - pass through to CMS
      log.info('XMDS file not cached, passing through:', filename);
      return fetch(event.request);
    }

    // Handle static widget resources (rewritten URLs from widget HTML)
    // These are absolute CMS URLs rewritten to /player/pwa/cache/static/<filename>
    if (url.pathname.startsWith('/player/pwa/cache/static/')) {
      const filename = url.pathname.split('/').pop();
      log.info('Static resource request:', filename);

      // Try xibo-static-v1 first
      const staticCache = await caches.open(STATIC_CACHE);
      const staticCached = await staticCache.match(`/player/pwa/cache/static/${filename}`);
      if (staticCached) {
        log.info('Serving static resource from static cache:', filename);
        return staticCached.clone();
      }

      // Try xibo-media-v1 at the static path (dual-cached from download manager)
      const mediaCached = await this.cacheManager.get(url.pathname);
      if (mediaCached) {
        log.info('Serving static resource from media cache:', filename);
        return new Response(mediaCached.clone().body, {
          headers: {
            'Content-Type': mediaCached.headers.get('Content-Type') || 'application/octet-stream',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=31536000'
          }
        });
      }

      // Not cached yet — return 404 (SW widget-resource fetch will cache it on first CMS hit)
      log.warn('Static resource not cached:', filename);
      return new Response('Resource not cached', { status: 404 });
    }

    // Only handle /player/pwa/cache/* requests below
    if (!url.pathname.startsWith('/player/pwa/cache/')) {
      log.info('NOT a cache request, returning null:', url.pathname);
      return null; // Let browser handle
    }

    log.info('IS a cache request, proceeding...', url.pathname);

    // Handle widget HTML requests
    if (url.pathname.startsWith('/player/pwa/cache/widget/')) {
      log.info('Widget HTML request:', url.pathname);
      const cached = await this.cacheManager.get(url.pathname);
      if (cached) {
        return new Response(cached.clone().body, {
          headers: {
            'Content-Type': 'text/html; charset=utf-8',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=31536000'
          }
        });
      }
      return new Response('<!DOCTYPE html><html><body>Widget not found</body></html>', {
        status: 404,
        headers: { 'Content-Type': 'text/html' }
      });
    }

    // Extract cache key: already in correct format /player/pwa/cache/media/123
    const cacheKey = url.pathname;
    const method = event.request.method;
    const rangeHeader = event.request.headers.get('Range');

    log.debug('Request URL:', url.pathname);
    log.debug('Cache key:', cacheKey);
    if (rangeHeader) {
      log.info(method, cacheKey, `Range: ${rangeHeader}`);
    } else {
      log.info(method, cacheKey);
    }

    // Use routing helper to determine how to serve this file
    const route = await this.routeFileRequest(cacheKey, method, rangeHeader);

    // If file exists, dispatch to appropriate handler
    if (route.found) {
      switch (route.handler) {
        case 'head-whole':
          return this.handleHeadWhole(route.data.cached?.headers.get('Content-Length'));

        case 'head-chunked':
          return this.handleHeadChunked(route.data.metadata);

        case 'range-whole':
          return this.handleRangeRequest(route.data.cached, route.data.rangeHeader, route.data.cacheKey);

        case 'range-chunked':
          return this.handleChunkedRangeRequest(route.data.cacheKey, route.data.rangeHeader, route.data.metadata);

        case 'full-whole':
          return this.handleFullWhole(route.data.cached, route.data.cacheKey);

        case 'full-chunked':
          return this.handleFullChunked(route.data.cacheKey, route.data.metadata);

        default:
          log.error('Unknown handler:', route.handler);
          return new Response('Internal error: unknown handler', { status: 500 });
      }
    }

    // File not found - check if download in progress
    const parts = cacheKey.split('/');
    const type = parts[2]; // 'media' or 'layout'
    const id = parts[3];

    let task = null;
    for (const [downloadUrl, activeTask] of this.downloadManager.queue.active.entries()) {
      if (activeTask.fileInfo.type === type && activeTask.fileInfo.id === id) {
        task = activeTask;
        break;
      }
    }

    if (task) {
      log.info('Download in progress, waiting:', cacheKey);

      try {
        await task.wait();

        // After download, re-route to serve the file
        const retryRoute = await this.routeFileRequest(cacheKey, method, rangeHeader);
        if (retryRoute.found) {
          log.info('Download complete, serving via', retryRoute.handler);

          switch (retryRoute.handler) {
            case 'full-whole':
              return this.handleFullWhole(retryRoute.data.cached, retryRoute.data.cacheKey);
            case 'full-chunked':
              return this.handleFullChunked(retryRoute.data.cacheKey, retryRoute.data.metadata);
            default:
              // For Range/HEAD after download, fall through to normal routing
              return this.handleRequest(event);  // Recursive call with fresh state
          }
        }
      } catch (error) {
        log.error('Download failed:', cacheKey, error);
        return new Response('Download failed: ' + error.message, { status: 500 });
      }
    }

    // Not cached and not downloading - return 404
    log.info('Not found:', cacheKey);
    return new Response('Not found', { status: 404 });
  }

  /**
   * Handle HEAD request for whole file
   */
  handleHeadWhole(size) {
    log.info('HEAD response: File exists (whole file)');
    return new Response(null, {
      status: 200,
      headers: {
        'Content-Length': size ? size.toString() : '',
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }

  /**
   * Handle HEAD request for chunked file
   */
  handleHeadChunked(metadata) {
    log.info('HEAD response: File exists (chunked)');
    return new Response(null, {
      status: 200,
      headers: {
        'Content-Length': metadata.totalSize.toString(),
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }

  /**
   * Handle full GET request for whole file (no Range)
   */
  handleFullWhole(cached, cacheKey) {
    const contentLength = cached.headers.get('Content-Length');
    const fileSize = contentLength ? formatBytes(parseInt(contentLength)) : 'unknown size';
    log.info('Serving from cache:', cacheKey, `(${fileSize})`);

    return new Response(cached.body, {
      headers: {
        'Content-Type': cached.headers.get('Content-Type') || 'application/octet-stream',
        'Content-Length': contentLength || '',
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=31536000'
      }
    });
  }

  /**
   * Handle full GET request for chunked file (no Range) - serve entire file as chunks
   */
  async handleFullChunked(cacheKey, metadata) {
    log.info('Chunked file GET without Range:', cacheKey, `- serving full file from ${metadata.numChunks} chunks`);

    // Serve entire file using synthetic range
    const syntheticRange = `bytes=0-${metadata.totalSize - 1}`;
    return this.handleChunkedRangeRequest(cacheKey, syntheticRange, metadata);
  }

  /**
   * Handle Range request for video seeking with blob caching
   * @param {Response} cachedResponse - Cached response from Cache API
   * @param {string} rangeHeader - Range header value (e.g., "bytes=0-1000")
   * @param {string} cacheKey - Cache key for blob cache lookup
   */
  async handleRangeRequest(cachedResponse, rangeHeader, cacheKey) {
    // Use blob cache to avoid re-materializing on every seek
    const blob = await this.blobCache.get(cacheKey, async () => {
      const cachedClone = cachedResponse.clone();
      return await cachedClone.blob();
    });

    const fileSize = blob.size;

    // Parse Range header using utility
    const { start, end } = parseRangeHeader(rangeHeader, fileSize);

    // Extract requested range (blob.slice is lazy - no copy!)
    const rangeBlob = blob.slice(start, end + 1);

    this.log.debug(`Range: bytes ${start}-${end}/${fileSize} (${formatBytes(rangeBlob.size)} of ${formatBytes(fileSize)})`);

    return new Response(rangeBlob, {
      status: 206,
      statusText: 'Partial Content',
      headers: {
        'Content-Type': cachedResponse.headers.get('Content-Type') || 'video/mp4',
        'Content-Length': rangeBlob.size.toString(),
        'Content-Range': `bytes ${start}-${end}/${fileSize}`,
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }

  /**
   * Handle Range request for chunked files (load only required chunks)
   * @param {string} cacheKey - Base cache key
   * @param {string} rangeHeader - Range header
   * @param {Object} metadata - Chunk metadata
   */
  async handleChunkedRangeRequest(cacheKey, rangeHeader, metadata) {
    const { totalSize, chunkSize, numChunks, contentType } = metadata;

    // Parse Range header using utility
    const { start, end: parsedEnd } = parseRangeHeader(rangeHeader, totalSize);

    // Cap open-ended ranges (e.g., "bytes=0-") to a single chunk for progressive streaming.
    // The browser only needs a small probe initially — it will make follow-up range requests
    // for the moov atom and actual streaming data. This prevents assembling 271MB into one
    // response blob and enables instant playback even while chunks are still being stored.
    let end = parsedEnd;
    const rangeStr = rangeHeader.replace(/bytes=/, '');
    const isOpenEnded = rangeStr.indexOf('-') === rangeStr.length - 1;
    if (isOpenEnded) {
      const startChunkIdx = Math.floor(start / chunkSize);
      const cappedEnd = Math.min((startChunkIdx + 1) * chunkSize - 1, totalSize - 1);
      if (cappedEnd < end) {
        end = cappedEnd;
        log.info(`Progressive streaming: capping bytes=${start}- to chunk ${startChunkIdx} (bytes ${start}-${end}/${totalSize})`);
      }
    }

    // Calculate which chunks contain the requested range using utility
    const { startChunk, endChunk, count: chunksNeeded } = getChunksForRange(start, end, chunkSize);

    this.log.debug(`Chunked range: bytes ${start}-${end}/${totalSize} (chunks ${startChunk}-${endChunk}, ${chunksNeeded} chunks)`);

    // Load required chunks (with blob caching!)
    // Chunks may still be writing in background — retry if not yet available
    const chunkBlobs = [];
    for (let i = startChunk; i <= endChunk; i++) {
      const chunkKey = `${cacheKey}/chunk-${i}`;

      const chunkBlob = await this.blobCache.get(chunkKey, async () => {
        let chunkResponse = null;
        for (let retry = 0; retry < 20; retry++) {
          chunkResponse = await this.cacheManager.getChunk(cacheKey, i);
          if (chunkResponse) break;
          // Chunk not yet stored — background putChunked() still running
          log.info(`Chunk ${i}/${numChunks} not yet available for ${cacheKey}, waiting... (attempt ${retry + 1})`);
          await new Promise(resolve => setTimeout(resolve, 250));
        }
        if (!chunkResponse) {
          throw new Error(`Chunk ${i} not found for ${cacheKey} after retries`);
        }
        return await chunkResponse.blob();
      });

      chunkBlobs.push(chunkBlob);
    }

    // Calculate slice offsets within the chunks
    const firstChunkStart = start % chunkSize;
    const lastChunkEnd = end % chunkSize;

    // Extract the exact range from the chunks
    let rangeData;

    if (chunksNeeded === 1) {
      // Range within single chunk
      rangeData = chunkBlobs[0].slice(firstChunkStart, firstChunkStart + (end - start + 1));
    } else {
      // Range spans multiple chunks - concatenate
      const parts = [];

      // First chunk (partial)
      parts.push(chunkBlobs[0].slice(firstChunkStart));

      // Middle chunks (complete)
      for (let i = 1; i < chunksNeeded - 1; i++) {
        parts.push(chunkBlobs[i]);
      }

      // Last chunk (partial)
      if (chunksNeeded > 1) {
        parts.push(chunkBlobs[chunksNeeded - 1].slice(0, lastChunkEnd + 1));
      }

      rangeData = new Blob(parts, { type: contentType });
    }

    this.log.debug(`Serving chunked range: ${formatBytes(rangeData.size)} from ${chunksNeeded} chunk(s)`);

    return new Response(rangeData, {
      status: 206,
      statusText: 'Partial Content',
      headers: {
        'Content-Type': contentType,
        'Content-Length': rangeData.size.toString(),
        'Content-Range': `bytes ${start}-${end}/${totalSize}`,
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }
}

// ============================================================================
// Class 3: MessageHandler - Handles postMessage from client
// ============================================================================

class MessageHandler {
  constructor(downloadManager, cacheManager) {
    this.downloadManager = downloadManager;
    this.cacheManager = cacheManager;
    this.log = new SWLogger('SW Message');
  }

  /**
   * Handle message from client
   */
  async handleMessage(event) {
    const { type, data } = event.data;

    this.log.info('Received:', type);

    switch (type) {
      case 'PING':
        // Client is checking if SW is ready - broadcast SW_READY to caller
        this.log.info('PING received, broadcasting SW_READY');
        // Send SW_READY back to the client that sent PING
        const clients = await self.clients.matchAll();
        clients.forEach(client => {
          client.postMessage({ type: 'SW_READY' });
        });
        return { success: true };

      case 'DOWNLOAD_FILES':
        return await this.handleDownloadFiles(data.files);

      case 'CLEAR_CACHE':
        return await this.handleClearCache();

      case 'GET_DOWNLOAD_PROGRESS':
        return await this.handleGetProgress();

      default:
        this.log.warn('Unknown message type:', type);
        return { success: false, error: 'Unknown message type' };
    }
  }

  /**
   * Handle DOWNLOAD_FILES message
   * Enqueue all files for download and wait for them to START
   */
  async handleDownloadFiles(files) {
    this.log.info('Enqueueing', files.length, 'files for download');

    let enqueuedCount = 0;
    const enqueuedTasks = [];

    for (const file of files) {
      // Skip files with no path
      if (!file.path || file.path === 'null' || file.path === 'undefined') {
        this.log.debug('Skipping file with no path:', file.id);
        continue;
      }

      const cacheKey = `/player/pwa/cache/${file.type}/${file.id}`;

      // Check if already cached (supports both whole files and chunked storage)
      const fileInfo = await this.cacheManager.fileExists(cacheKey);
      if (fileInfo.exists) {
        this.log.debug('File already cached:', cacheKey, fileInfo.chunked ? '(chunked)' : '(whole file)');

        // Ensure widget resources (.js, .css, fonts) are in the static cache
        // This handles files cached before the dual-cache deploy
        await this.ensureStaticCacheEntry(file);

        continue;
      }

      // Check if already downloading (prevent duplicates during collection cycles)
      const activeTask = this.downloadManager.getTask(file.path);
      if (activeTask) {
        this.log.debug('File already downloading:', cacheKey, '- skipping duplicate');
        continue;
      }

      // Enqueue for download - NOTE: DownloadTask now handles caching internally
      // We need to wrap it to cache after download completes
      const task = this.downloadManager.enqueue(file);
      enqueuedTasks.push(this.cacheFileAfterDownload(task, file));
      enqueuedCount++;
    }

    this.log.info('Enqueued', enqueuedCount, 'files, waiting for downloads to start...');

    // Wait for processQueue() to actually start downloads
    await new Promise(resolve => setTimeout(resolve, 100));

    // Verify downloads started
    const activeCount = this.downloadManager.queue.running;
    const queuedCount = this.downloadManager.queue.queue.length;
    this.log.info('Downloads started:', activeCount, 'active,', queuedCount, 'queued');

    return {
      success: true,
      enqueuedCount,
      activeCount,
      queuedCount
    };
  }

  /**
   * Cache file after download completes
   * This wraps the DownloadTask to add caching behavior
   */
  async cacheFileAfterDownload(task, fileInfo) {
    try {
      const blob = await task.wait();

      // Cache the downloaded file
      const cacheKey = `/player/pwa/cache/${fileInfo.type}/${fileInfo.id}`;
      const contentType = fileInfo.type === 'layout' ? 'text/xml' :
                          fileInfo.type === 'widget' ? 'text/html' :
                          'application/octet-stream';

      // Use chunked storage for large files (low memory streaming)
      if (blob.size > CHUNK_STORAGE_THRESHOLD) {
        log.info('Large file detected, using chunked storage:', cacheKey, `(${(blob.size / 1024 / 1024).toFixed(1)} MB)`);

        // Store metadata first, then notify immediately!
        // This allows layout to switch while chunks are being stored
        const numChunks = Math.ceil(blob.size / CHUNK_SIZE);
        const metadata = {
          totalSize: blob.size,
          chunkSize: CHUNK_SIZE,
          numChunks,
          contentType,
          chunked: true,
          createdAt: Date.now()
        };

        // Store metadata first
        const metadataResponse = new Response(JSON.stringify(metadata), {
          headers: { 'Content-Type': 'application/json' }
        });
        const cache = await caches.open(CACHE_NAME);
        await cache.put(`${cacheKey}/metadata`, metadataResponse);

        // Notify clients immediately - file is now "ready" for streaming!
        const clients = await self.clients.matchAll();
        clients.forEach(client => {
          client.postMessage({
            type: 'FILE_CACHED',
            fileId: fileInfo.id,
            fileType: fileInfo.type,
            size: blob.size
          });
        });
        log.info('File ready for streaming, storing chunks in background:', cacheKey);

        // Store chunks in background but track for race-condition safety
        // handleChunkedRangeRequest() will await this if chunks aren't ready yet
        const storagePromise = this.cacheManager.putChunked(cacheKey, blob, contentType)
          .then(() => pendingChunkStorage.delete(cacheKey))
          .catch(err => {
            this.log.error('Background chunk storage failed:', cacheKey, err);
            pendingChunkStorage.delete(cacheKey);
          });
        pendingChunkStorage.set(cacheKey, storagePromise);

      } else {
        await this.cacheManager.put(cacheKey, blob, contentType);
        log.info('Cached after download:', cacheKey, `(${blob.size} bytes)`);

        // Notify all clients that file is cached
        const clients = await self.clients.matchAll();
        clients.forEach(client => {
          client.postMessage({
            type: 'FILE_CACHED',
            fileId: fileInfo.id,
            fileType: fileInfo.type,
            size: blob.size
          });
        });
      }

      // Also cache widget resources (.js, .css, fonts) for static serving
      // This allows rewritten URLs (/player/pwa/cache/static/bundle.min.js) to resolve instantly
      const filename = fileInfo.path ? (() => {
        try { return new URL(fileInfo.path).searchParams.get('file'); } catch { return null; }
      })() : null;

      if (filename && (filename.endsWith('.js') || filename.endsWith('.css') ||
          /\.(otf|ttf|woff2?|eot|svg)$/i.test(filename))) {
        const staticCache = await caches.open(STATIC_CACHE);
        const staticKey = `/player/pwa/cache/static/${filename}`;

        // Determine correct content type for the resource
        const ext = filename.split('.').pop().toLowerCase();
        const staticContentType = {
          'js': 'application/javascript',
          'css': 'text/css',
          'otf': 'font/otf',
          'ttf': 'font/ttf',
          'woff': 'font/woff',
          'woff2': 'font/woff2',
          'eot': 'application/vnd.ms-fontobject',
          'svg': 'image/svg+xml'
        }[ext] || 'application/octet-stream';

        await Promise.all([
          staticCache.put(staticKey, new Response(blob.slice(0, blob.size, blob.type), {
            headers: { 'Content-Type': staticContentType }
          })),
          this.cacheManager.put(staticKey, blob.slice(0, blob.size, blob.type), staticContentType)
        ]);

        log.info('Also cached as static resource:', filename, `(${staticContentType})`);
      }

      return blob;
    } catch (error) {
      this.log.error('Failed to cache after download:', fileInfo.id, error);
      throw error;
    }
  }

  /**
   * Ensure widget resource files have static cache entries
   * Handles files that were cached before the dual-cache deploy
   */
  async ensureStaticCacheEntry(fileInfo) {
    const filename = fileInfo.path ? (() => {
      try { return new URL(fileInfo.path).searchParams.get('file'); } catch { return null; }
    })() : null;

    if (!filename || !(filename.endsWith('.js') || filename.endsWith('.css') ||
        /\.(otf|ttf|woff2?|eot|svg)$/i.test(filename))) {
      return; // Not a widget resource
    }

    const staticCache = await caches.open(STATIC_CACHE);
    const staticKey = `/player/pwa/cache/static/${filename}`;

    // Check if already in static cache
    const existing = await staticCache.match(staticKey);
    if (existing) return; // Already populated

    // Read from media cache and copy to static cache
    const cacheKey = `/player/pwa/cache/${fileInfo.type}/${fileInfo.id}`;
    const cached = await this.cacheManager.get(cacheKey);
    if (!cached) return;

    const blob = await cached.blob();
    const ext = filename.split('.').pop().toLowerCase();
    const staticContentType = {
      'js': 'application/javascript',
      'css': 'text/css',
      'otf': 'font/otf',
      'ttf': 'font/ttf',
      'woff': 'font/woff',
      'woff2': 'font/woff2',
      'eot': 'application/vnd.ms-fontobject',
      'svg': 'image/svg+xml'
    }[ext] || 'application/octet-stream';

    const staticPathKey = `/player/pwa/cache/static/${filename}`;

    await Promise.all([
      staticCache.put(staticKey, new Response(blob.slice(0, blob.size, blob.type), {
        headers: { 'Content-Type': staticContentType }
      })),
      this.cacheManager.put(staticPathKey, blob.slice(0, blob.size, blob.type), staticContentType)
    ]);

    log.info('Backfilled static cache for:', filename, `(${staticContentType}, ${blob.size} bytes)`);
  }

  /**
   * Handle CLEAR_CACHE message
   */
  async handleClearCache() {
    this.log.info('Clearing cache');
    await this.cacheManager.clear();
    return { success: true };
  }

  /**
   * Handle GET_DOWNLOAD_PROGRESS message
   */
  async handleGetProgress() {
    const progress = this.downloadManager.getProgress();
    return { success: true, progress };
  }
}

// ============================================================================
// Initialize Service Worker
// ============================================================================

// Create shared DownloadManager instance
const downloadManager = new DownloadManager({
  concurrency: CONCURRENT_DOWNLOADS,
  chunkSize: CHUNK_SIZE,
  chunksPerFile: CONCURRENT_CHUNKS
});

const cacheManager = new CacheManager();
const blobCache = new BlobCache(BLOB_CACHE_SIZE_MB);
const requestHandler = new RequestHandler(downloadManager, cacheManager, blobCache);
const messageHandler = new MessageHandler(downloadManager, cacheManager);

// Initialize cache on install
self.addEventListener('install', (event) => {
  log.info('Installing... Version:', SW_VERSION);
  event.waitUntil(
    Promise.all([
      cacheManager.init(),
      caches.open(STATIC_CACHE).then((cache) => {
        log.info('Caching static files');
        return cache.addAll(STATIC_FILES);
      })
    ]).then(() => {
      log.info('Cache initialized');
      // Force immediate activation
      return self.skipWaiting();
    })
  );
});

// Activate and claim clients
self.addEventListener('activate', (event) => {
  log.info('Activating... Version:', SW_VERSION);
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames
          .filter((name) => name.startsWith('xibo-') && name !== CACHE_NAME && name !== STATIC_CACHE)
          .map((name) => {
            log.info('Deleting old cache:', name);
            return caches.delete(name);
          })
      );
    }).then(() => {
      log.info('Taking control of all clients immediately');
      // Force immediate control of all pages
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

// Handle fetch events
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Only intercept specific requests, let everything else pass through
  const shouldIntercept =
    url.pathname.startsWith('/player/pwa/cache/') ||  // Cache requests (NEW PATH!)
    url.pathname.startsWith('/player/') && (url.pathname.endsWith('.html') || url.pathname === '/player/') ||
    (url.pathname.includes('xmds.php') && url.searchParams.has('file') && event.request.method === 'GET');

  if (shouldIntercept) {
    event.respondWith(requestHandler.handleRequest(event));
  }
  // Let POST requests and other requests pass through without interception
});

// Handle messages from client
self.addEventListener('message', (event) => {
  event.waitUntil(
    messageHandler.handleMessage(event).then((result) => {
      // Send response back to client
      event.ports[0]?.postMessage(result);
    })
  );
});

log.info('Modular Service Worker ready');
