/**
 * Standalone Service Worker for Xibo Players
 * Version: 2026-02-07 - Refactored to use shared DownloadManager module
 *
 * Architecture:
 * - DownloadManager: Imported from @xiboplayer/cache
 * - CacheManager: Wraps Cache API with type-aware keys
 * - RequestHandler: Handles fetch events (serve from cache or wait for download)
 * - MessageHandler: Handles postMessage from client
 *
 * No HTTP 202 responses - always returns actual files or 404
 */

import { DownloadManager, LayoutTaskBuilder, BARRIER } from '@xiboplayer/cache/download-manager';
import { VERSION as CACHE_VERSION } from '@xiboplayer/cache';
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
  extractRangeFromChunks,
  BASE
} from './sw-utils.js';

/** Content-type map for static widget resources (JS, CSS, fonts, SVG) */
const STATIC_CONTENT_TYPES = {
  'js': 'application/javascript',
  'css': 'text/css',
  'otf': 'font/otf',
  'ttf': 'font/ttf',
  'woff': 'font/woff',
  'woff2': 'font/woff2',
  'eot': 'application/vnd.ms-fontobject',
  'svg': 'image/svg+xml'
};

/** Extract media IDs from XLF XML content */
function extractMediaIdsFromXlf(xlfText, log) {
  const ids = new Set();
  // fileId is the CMS media library ID (id is just the widget sequence number)
  const fileIdMatches = xlfText.matchAll(/<media[^>]+fileId="(\d+)"/g);
  for (const m of fileIdMatches) ids.add(m[1]);
  // background attribute on <layout> is also a media file ID
  const bgMatches = xlfText.matchAll(/<layout[^>]+background="(\d+)"/g);
  for (const m of bgMatches) ids.add(m[1]);
  if (log) log.debug(`extractMediaIdsFromXlf: found ${ids.size} IDs: ${[...ids].join(', ')} (XLF ${xlfText.length} bytes)`);
  return ids;
}

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

const SW_VERSION = '2026-02-13-090851';
const CACHE_NAME = 'xibo-media-v1';
const STATIC_CACHE = 'xibo-static-v1';

// Track in-progress chunk storage operations (cacheKey → Promise)
// Prevents serving chunked files before chunks are fully written to cache
const pendingChunkStorage = new Map();

// In-memory metadata cache: cacheKey → metadata object
// Eliminates Cache API lookups for chunk metadata on every Range request
const metadataCache = new Map();

// Pending chunk blob loads: chunkKey → Promise<Blob>
// Coalesces concurrent reads for the same chunk into a single Cache API operation
const pendingChunkLoads = new Map();

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
// Flat queue: concurrency directly controls total HTTP connections.
// Chromium limits 6 connections per host — the flat queue ensures every download
// unit (full small file or single chunk) competes equally for connection slots,
// avoiding the N×M explosion of the old two-layer approach.
const CONCURRENT_DOWNLOADS = CHUNK_CONFIG.concurrency;

// Static files to cache on install
const STATIC_FILES = [
  BASE + '/',
  BASE + '/index.html',
  BASE + '/setup.html'
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
   * Delete file from cache (whole file, or all chunks + metadata)
   */
  async delete(cacheKey) {
    if (!this.cache) await this.init();

    // Clear in-memory metadata cache
    const meta = metadataCache.get(cacheKey);
    metadataCache.delete(cacheKey);

    // If chunked, delete all chunks + metadata
    if (meta) {
      const promises = [this.cache.delete(`${cacheKey}/metadata`)];
      for (let i = 0; i < meta.numChunks; i++) {
        promises.push(this.cache.delete(`${cacheKey}/chunk-${i}`));
      }
      await Promise.all(promises);
      return true;
    }

    return await this.cache.delete(cacheKey);
  }

  /**
   * Clear all cached files
   */
  async clear() {
    if (!this.cache) await this.init();
    const keys = await this.cache.keys();
    await Promise.all(keys.map(key => this.cache.delete(key)));
    metadataCache.clear();
    this.log.info('Cleared', keys.length, 'cached files');
  }

  /**
   * Check if file exists (supports both whole files and chunked storage)
   * Single source of truth for file existence checks.
   * Uses in-memory metadataCache to avoid Cache API lookups on hot paths.
   * @param {string} cacheKey - Full cache key (e.g., /player/pwa/cache/media/6)
   * @returns {Promise<{exists: boolean, chunked: boolean, metadata: Object|null}>}
   */
  async fileExists(cacheKey) {
    if (!this.cache) await this.init();

    // Fast path: check in-memory metadata cache first (no async I/O)
    const cachedMeta = metadataCache.get(cacheKey);
    if (cachedMeta) {
      return { exists: true, chunked: true, metadata: cachedMeta };
    }

    // Check for whole file
    const wholeFile = await this.get(cacheKey);
    if (wholeFile) {
      return { exists: true, chunked: false, metadata: null };
    }

    // Check for chunked metadata (Cache API fallback)
    const metadata = await this.getMetadata(cacheKey);
    if (metadata && metadata.chunked) {
      // Populate in-memory cache for future requests
      metadataCache.set(cacheKey, metadata);
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

    // Store metadata (complete: false until all chunks are written)
    const metadata = {
      totalSize,
      chunkSize: CHUNK_SIZE,
      numChunks,
      contentType,
      chunked: true,
      complete: false,
      createdAt: Date.now()
    };

    const metadataResponse = new Response(JSON.stringify(metadata), {
      headers: { 'Content-Type': 'application/json' }
    });
    await this.cache.put(`${cacheKey}/metadata`, metadataResponse);
    // Populate in-memory cache
    metadataCache.set(cacheKey, metadata);

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

    // All chunks stored — mark metadata complete
    metadata.complete = true;
    await this.cache.put(`${cacheKey}/metadata`, new Response(
      JSON.stringify(metadata),
      { headers: { 'Content-Type': 'application/json' } }
    ));
    metadataCache.set(cacheKey, metadata);

    this.log.info(`Chunked storage complete: ${cacheKey}`);
  }

  /**
   * Get metadata for chunked file.
   * Checks in-memory cache first to avoid Cache API I/O on hot paths.
   * @param {string} cacheKey - Base cache key
   * @returns {Promise<Object|null>}
   */
  async getMetadata(cacheKey) {
    // Fast path: in-memory cache
    const cached = metadataCache.get(cacheKey);
    if (cached) return cached;

    if (!this.cache) await this.init();

    const response = await this.cache.match(`${cacheKey}/metadata`);
    if (!response) return null;

    const text = await response.text();
    const metadata = JSON.parse(text);

    // Populate in-memory cache
    metadataCache.set(cacheKey, metadata);
    return metadata;
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
   * Check if a key exists in memory cache (no Cache API fallback)
   * @param {string} cacheKey - Cache key
   * @returns {boolean}
   */
  has(cacheKey) {
    return this.cache.has(cacheKey);
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
      this.log.debug(`Skipping memory cache (too large): ${cacheKey} (${formatBytes(blob.size)} > ${formatBytes(this.maxBytes)})`);
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
    if (url.pathname === BASE + '/' ||
        url.pathname === BASE + '/index.html' ||
        url.pathname === BASE + '/setup.html') {
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
      const cacheKey = `${BASE}/cache/static/${filename}`;
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

      log.info('XMDS request:', filename, 'type:', fileType, '→', BASE + '/cache/' + cacheType + '/' + fileId);

      const cacheKey = `${BASE}/cache/${cacheType}/${fileId}`;
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
    if (url.pathname.startsWith(BASE + '/cache/static/')) {
      const filename = url.pathname.split('/').pop();
      log.info('Static resource request:', filename);

      // Try xibo-static-v1 first
      const staticCache = await caches.open(STATIC_CACHE);
      const staticCached = await staticCache.match(`${BASE}/cache/static/${filename}`);
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
    if (!url.pathname.startsWith(BASE + '/cache/')) {
      log.info('NOT a cache request, returning null:', url.pathname);
      return null; // Let browser handle
    }

    log.info('IS a cache request, proceeding...', url.pathname);

    // Handle widget HTML requests
    if (url.pathname.startsWith(BASE + '/cache/widget/')) {
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
          return this.handleHeadChunked(route.data.metadata, route.data.cacheKey);

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
   * Handle HEAD request for chunked file.
   * Only reports 200 if chunk 0 is actually in cache (not just metadata).
   * Metadata-only means the progressive download has started but no data
   * is servable yet — the client should treat this as "not ready".
   */
  async handleHeadChunked(metadata, cacheKey) {
    const chunk0 = await this.cacheManager.getChunk(cacheKey, 0);
    if (!chunk0) {
      log.info('HEAD response: Chunked file not yet playable (chunk 0 missing):', cacheKey);
      return new Response(null, { status: 404 });
    }
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

    // Cap open-ended ranges (e.g., "bytes=0-") to a single chunk for progressive streaming,
    // but ONLY if some chunks are still missing. When all chunks from the start position
    // to the end of file are already cached, serve the full range to avoid sequential
    // chunk-by-chunk round-trips that can cause video stalls.
    let end = parsedEnd;
    const rangeStr = rangeHeader.replace(/bytes=/, '');
    const isOpenEnded = rangeStr.indexOf('-') === rangeStr.length - 1;
    if (isOpenEnded) {
      const startChunkIdx = Math.floor(start / chunkSize);
      const lastChunkIdx = numChunks - 1;

      // Check if all remaining chunks are cached (quick BlobCache check first, then Cache API)
      let allCached = true;
      for (let i = startChunkIdx; i <= lastChunkIdx; i++) {
        const chunkKey = `${cacheKey}/chunk-${i}`;
        if (this.blobCache.has(chunkKey)) continue;
        // Not in BlobCache — check Cache API
        const resp = await this.cacheManager.getChunk(cacheKey, i);
        if (!resp) {
          allCached = false;
          break;
        }
      }

      if (!allCached) {
        const cappedEnd = Math.min((startChunkIdx + 1) * chunkSize - 1, totalSize - 1);
        if (cappedEnd < end) {
          end = cappedEnd;
          log.info(`Progressive streaming: capping bytes=${start}- to chunk ${startChunkIdx} (bytes ${start}-${end}/${totalSize})`);
        }
      } else {
        log.info(`All chunks cached from ${startChunkIdx} to ${lastChunkIdx}, serving full range (bytes ${start}-${end}/${totalSize})`);
      }
    }

    // Calculate which chunks contain the requested range using utility
    const { startChunk, endChunk, count: chunksNeeded } = getChunksForRange(start, end, chunkSize);

    this.log.debug(`Chunked range: bytes ${start}-${end}/${totalSize} (chunks ${startChunk}-${endChunk}, ${chunksNeeded} chunks)`);

    // Load a single chunk, with coalescing + blob caching.
    // Returns the blob immediately if cached, or polls until available.
    const loadChunk = (i) => {
      const chunkKey = `${cacheKey}/chunk-${i}`;

      return this.blobCache.get(chunkKey, () => {
        // Coalesce: reuse in-flight Cache API read if another request is
        // already loading this exact chunk
        if (pendingChunkLoads.has(chunkKey)) {
          return pendingChunkLoads.get(chunkKey);
        }

        const loadPromise = (async () => {
          let chunkResponse = await this.cacheManager.getChunk(cacheKey, i);
          if (chunkResponse) return await chunkResponse.blob();

          // Chunk not yet stored — progressive download still running.
          // Signal emergency priority: video is stalled waiting for this chunk.
          // Moves it to front of queue with exclusive bandwidth.
          log.info(`Chunk ${i}/${numChunks} not yet available for ${cacheKey}, signalling urgent...`);
          {
            const keyParts = cacheKey.split('/');
            const urgentFileId = keyParts[keyParts.length - 1];
            const urgentFileType = keyParts[keyParts.length - 2];
            downloadManager.queue.urgentChunk(urgentFileType, urgentFileId, i);
          }

          // Poll with increasing backoff: 60 × 1s = 60s max wait.
          for (let retry = 0; retry < 60; retry++) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            chunkResponse = await this.cacheManager.getChunk(cacheKey, i);
            if (chunkResponse) {
              log.info(`Chunk ${i}/${numChunks} arrived for ${cacheKey} after ${retry + 1}s`);
              return await chunkResponse.blob();
            }
          }
          throw new Error(`Chunk ${i} not available for ${cacheKey} after 60s`);
        })();

        pendingChunkLoads.set(chunkKey, loadPromise);
        loadPromise.finally(() => pendingChunkLoads.delete(chunkKey));
        return loadPromise;
      });
    };

    // Fast path: try to load all chunks immediately (no waiting).
    // If all are cached, serve the blob response synchronously.
    const immediateBlobs = [];
    let allImmediate = true;
    for (let i = startChunk; i <= endChunk; i++) {
      const chunkResponse = await this.cacheManager.getChunk(cacheKey, i);
      if (chunkResponse) {
        const chunkKey = `${cacheKey}/chunk-${i}`;
        const blob = await this.blobCache.get(chunkKey, async () => await chunkResponse.blob());
        immediateBlobs.push(blob);
      } else {
        allImmediate = false;
        break;
      }
    }

    if (allImmediate && immediateBlobs.length === chunksNeeded) {
      // All chunks available — serve immediately (common path for completed downloads)
      const rangeData = extractRangeFromChunks(immediateBlobs, start, end, chunkSize);
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

    // Slow path: some chunks still downloading. Return a 206 with a
    // ReadableStream body so Chrome sees a "slow" response (buffering
    // spinner) instead of an error. The stream pushes data as chunks arrive.
    log.info(`Streaming response for ${cacheKey} bytes ${start}-${end} (waiting for chunks)`);
    const cacheManager = this.cacheManager;
    const blobCache = this.blobCache;
    const rangeSize = end - start + 1;

    const stream = new ReadableStream({
      async start(controller) {
        try {
          // Load all required chunks (with waiting for missing ones)
          const chunkBlobs = [];
          for (let i = startChunk; i <= endChunk; i++) {
            const blob = await loadChunk(i);
            chunkBlobs.push(blob);
          }

          // Extract the exact byte range and push to stream
          const rangeData = extractRangeFromChunks(chunkBlobs, start, end, chunkSize);
          const buffer = await rangeData.arrayBuffer();
          controller.enqueue(new Uint8Array(buffer));
          controller.close();
        } catch (err) {
          log.error(`Stream error for ${cacheKey}: ${err.message}`);
          controller.error(err);
        }
      }
    });

    return new Response(stream, {
      status: 206,
      statusText: 'Partial Content',
      headers: {
        'Content-Type': contentType,
        'Content-Length': rangeSize.toString(),
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

    // Log progress polls at debug (high-frequency), everything else at info
    if (type === 'GET_DOWNLOAD_PROGRESS') {
      this.log.debug('Received:', type);
    } else {
      this.log.info('Received:', type);
    }

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
        return await this.handleDownloadFiles(data);

      case 'PRIORITIZE_DOWNLOAD':
        return this.handlePrioritizeDownload(data.fileType, data.fileId);

      case 'CLEAR_CACHE':
        return await this.handleClearCache();

      case 'GET_DOWNLOAD_PROGRESS':
        return await this.handleGetProgress();

      case 'DELETE_FILES':
        return await this.handleDeleteFiles(data.files);

      case 'PREWARM_VIDEO_CHUNKS':
        return await this.handlePrewarmVideoChunks(data.mediaIds);

      case 'PRIORITIZE_LAYOUT_FILES':
        this.downloadManager.prioritizeLayoutFiles(data.mediaIds);
        return { success: true };

      case 'URGENT_CHUNK':
        return this.handleUrgentChunk(data.fileType, data.fileId, data.chunkIndex);

      default:
        this.log.warn('Unknown message type:', type);
        return { success: false, error: 'Unknown message type' };
    }
  }

  /**
   * Handle DELETE_FILES message - purge obsolete files from cache
   */
  async handleDeleteFiles(files) {
    if (!files || !Array.isArray(files)) {
      return { success: false, error: 'No files provided' };
    }

    let deleted = 0;
    for (const file of files) {
      const cacheKey = `${BASE}/cache/${file.type}/${file.id}`;
      const wasDeleted = await this.cacheManager.delete(cacheKey);
      if (wasDeleted) {
        this.log.info('Purged:', cacheKey);
        deleted++;
      } else {
        this.log.debug('Not cached (skip purge):', cacheKey);
      }
    }

    this.log.info(`Purge complete: ${deleted}/${files.length} files deleted`);
    return { success: true, deleted, total: files.length };
  }

  /**
   * Handle PREWARM_VIDEO_CHUNKS - pre-load first and last chunks into BlobCache
   * for faster video startup (avoids IndexedDB reads on initial Range requests)
   */
  async handlePrewarmVideoChunks(mediaIds) {
    if (!mediaIds || !Array.isArray(mediaIds) || mediaIds.length === 0) {
      return { success: false, error: 'No mediaIds provided' };
    }

    let warmed = 0;
    for (const mediaId of mediaIds) {
      const cacheKey = `${BASE}/cache/media/${mediaId}`;
      const metadata = await this.cacheManager.getMetadata(cacheKey);

      if (metadata?.chunked) {
        // Chunked file: pre-warm first chunk (ftyp/mdat) and last chunk (moov atom)
        const lastChunk = metadata.numChunks - 1;
        const chunksToWarm = [0];
        if (lastChunk > 0) chunksToWarm.push(lastChunk);

        for (const idx of chunksToWarm) {
          const chunkKey = `${cacheKey}/chunk-${idx}`;
          // Load into BlobCache (no-op if already cached)
          await blobCache.get(chunkKey, async () => {
            const resp = await this.cacheManager.getChunk(cacheKey, idx);
            if (!resp) return new Blob(); // shouldn't happen for cached media
            return await resp.blob();
          });
        }
        this.log.info(`Pre-warmed ${chunksToWarm.length} chunks for media ${mediaId} (${metadata.numChunks} total)`);
        warmed++;
      } else {
        // Whole file: pre-warm entire blob
        const cached = await this.cacheManager.get(cacheKey);
        if (cached) {
          await blobCache.get(cacheKey, async () => await cached.clone().blob());
          this.log.info(`Pre-warmed whole file for media ${mediaId}`);
          warmed++;
        }
      }
    }

    return { success: true, warmed, total: mediaIds.length };
  }

  /**
   * Handle PRIORITIZE_DOWNLOAD - move file to front of download queue
   */
  handlePrioritizeDownload(fileType, fileId) {
    this.log.info('Prioritize request:', `${fileType}/${fileId}`);
    const found = this.downloadManager.queue.prioritize(fileType, fileId);
    // Trigger queue processing in case there's capacity
    this.downloadManager.queue.processQueue();
    return { success: true, found };
  }

  /**
   * Handle URGENT_CHUNK — emergency priority for a stalled streaming chunk.
   * External path (main thread can signal via postMessage).
   */
  handleUrgentChunk(fileType, fileId, chunkIndex) {
    this.log.info('Urgent chunk request:', `${fileType}/${fileId}`, 'chunk', chunkIndex);
    const acted = this.downloadManager.queue.urgentChunk(fileType, fileId, chunkIndex);
    return { success: true, acted };
  }

  /**
   * Handle DOWNLOAD_FILES with XLF-driven media resolution.
   *
   * Accepts { layoutOrder: number[], files: Array } from PlayerCore.
   * Builds lookup maps from the flat CMS file list, fetches/parses XLFs to
   * discover which media each layout needs, then enqueues per-layout chunks
   * with barriers in playback order.
   *
   * @param {{ layoutOrder: number[], files: Array }} payload
   */
  async handleDownloadFiles({ layoutOrder, files }) {
    const dm = this.downloadManager;
    const queue = dm.queue;
    let enqueuedCount = 0;
    const enqueuedTasks = [];

    // Build lookup maps from flat CMS file list
    const xlfFiles = new Map();     // layoutId → file entry (for XLF download URL)
    const resources = [];            // fonts, bundle.min.js etc.
    const mediaFiles = new Map();    // mediaId (string) → file entry
    for (const f of files) {
      if (f.type === 'layout') {
        xlfFiles.set(parseInt(f.id), f);
      } else if (f.type === 'resource' || f.code === 'fonts.css'
          || (f.path && (f.path.includes('bundle.min') || f.path.includes('fonts')))) {
        resources.push(f);
      } else {
        mediaFiles.set(String(f.id), f);
      }
    }

    this.log.info(`Download: ${layoutOrder.length} layouts, ${mediaFiles.size} media, ${resources.length} resources`);

    // ── Step 1: Fetch + cache + parse all XLFs directly (parallel) ──
    const layoutMediaMap = new Map(); // layoutId → Set<mediaId>
    const xlfPromises = [];
    for (const layoutId of layoutOrder) {
      const xlfFile = xlfFiles.get(layoutId);
      if (!xlfFile?.path) continue;

      xlfPromises.push((async () => {
        const cacheKey = `${BASE}/cache/layout/${layoutId}`;
        const existing = await this.cacheManager.get(cacheKey);
        let xlfText;
        if (existing) {
          xlfText = await existing.clone().text();
        } else {
          const resp = await fetch(xlfFile.path);
          if (!resp.ok) { this.log.warn(`XLF fetch failed: ${layoutId} (${resp.status})`); return; }
          const blob = await resp.blob();
          await this.cacheManager.put(cacheKey, blob, 'text/xml');
          this.log.info(`Fetched + cached XLF ${layoutId} (${blob.size} bytes)`);
          // Notify clients so pending layouts can clear
          const clients = await self.clients.matchAll();
          clients.forEach(c => c.postMessage({ type: 'FILE_CACHED', fileId: String(layoutId), fileType: 'layout', size: blob.size }));
          xlfText = await blob.text();
        }
        layoutMediaMap.set(layoutId, extractMediaIdsFromXlf(xlfText, this.log));
      })());
    }
    // Also fetch XLFs NOT in layoutOrder (non-scheduled layouts, e.g. default)
    for (const [layoutId, xlfFile] of xlfFiles) {
      if (layoutOrder.includes(layoutId)) continue;
      xlfPromises.push((async () => {
        const cacheKey = `${BASE}/cache/layout/${layoutId}`;
        const existing = await this.cacheManager.get(cacheKey);
        if (!existing && xlfFile.path) {
          const resp = await fetch(xlfFile.path);
          if (resp.ok) {
            const blob = await resp.blob();
            await this.cacheManager.put(cacheKey, blob, 'text/xml');
            this.log.info(`Fetched + cached XLF ${layoutId} (non-scheduled, ${blob.size} bytes)`);
            const clients = await self.clients.matchAll();
            clients.forEach(c => c.postMessage({ type: 'FILE_CACHED', fileId: String(layoutId), fileType: 'layout', size: blob.size }));
            const xlfText = await blob.text();
            layoutMediaMap.set(layoutId, extractMediaIdsFromXlf(xlfText, this.log));
          }
        } else if (existing) {
          const xlfText = await existing.clone().text();
          layoutMediaMap.set(layoutId, extractMediaIdsFromXlf(xlfText, this.log));
        }
      })());
    }
    await Promise.allSettled(xlfPromises);
    this.log.info(`Parsed ${layoutMediaMap.size} XLFs`);

    // ── Step 2: Enqueue resources ──
    const resourceBuilder = new LayoutTaskBuilder(queue);
    for (const file of resources) {
      const enqueued = await this._enqueueFile(dm, resourceBuilder, file, enqueuedTasks);
      if (enqueued) enqueuedCount++;
    }
    const resourceTasks = await resourceBuilder.build();
    if (resourceTasks.length > 0) {
      resourceTasks.push(BARRIER);
      queue.enqueueOrderedTasks(resourceTasks);
    }

    // ── Step 3: For each layout in play order, get media from XLF + enqueue ──
    const claimed = new Set(); // Track media IDs already claimed by a layout

    // Process scheduled layouts first (in play order), then non-scheduled
    const allLayoutIds = [...layoutOrder, ...[...layoutMediaMap.keys()].filter(id => !layoutOrder.includes(id))];

    for (const layoutId of allLayoutIds) {
      const xlfMediaIds = layoutMediaMap.get(layoutId);
      if (!xlfMediaIds) continue;

      const matched = [];
      for (const id of xlfMediaIds) {
        if (claimed.has(id)) continue; // Already claimed by earlier layout
        const file = mediaFiles.get(id);
        if (file) {
          matched.push(file);
          claimed.add(id);
        }
      }
      if (matched.length === 0) continue;

      this.log.info(`Layout ${layoutId}: ${matched.length} media`);
      matched.sort((a, b) => (a.size || 0) - (b.size || 0));
      const builder = new LayoutTaskBuilder(queue);
      for (const file of matched) {
        const enqueued = await this._enqueueFile(dm, builder, file, enqueuedTasks);
        if (enqueued) enqueuedCount++;
      }
      const orderedTasks = await builder.build();
      if (orderedTasks.length > 0) {
        orderedTasks.push(BARRIER);
        queue.enqueueOrderedTasks(orderedTasks);
      }
    }

    // Warn about unclaimed media (in CMS file list but not referenced by any XLF)
    const unclaimed = [...mediaFiles.keys()].filter(id => !claimed.has(id));
    if (unclaimed.length > 0) {
      this.log.warn(`${unclaimed.length} media not in any XLF: ${unclaimed.join(', ')}`);
    }

    const activeCount = queue.running;
    const queuedCount = queue.queue.length;
    this.log.info('Downloads active:', activeCount, ', queued:', queuedCount);
    return { success: true, enqueuedCount, activeCount, queuedCount };
  }

  /**
   * Enqueue a single file for download (shared by phase 1 and phase 2).
   * Handles cache checks, dedup, and incomplete chunked resume.
   * @returns {boolean} true if file was enqueued (new download)
   */
  async _enqueueFile(dm, builder, file, enqueuedTasks) {
    // Skip files with no path
    if (!file.path || file.path === 'null' || file.path === 'undefined') {
      this.log.debug('Skipping file with no path:', file.id);
      return false;
    }

    const cacheKey = `${BASE}/cache/${file.type}/${file.id}`;

    // Check if already cached (supports both whole files and chunked storage)
    const fileInfo = await this.cacheManager.fileExists(cacheKey);
    if (fileInfo.exists) {
      // For chunked files, verify download actually completed
      if (fileInfo.chunked && fileInfo.metadata && !fileInfo.metadata.complete) {
        const { numChunks } = fileInfo.metadata;
        const skipChunks = new Set();
        for (let j = 0; j < numChunks; j++) {
          const chunk = await this.cacheManager.getChunk(cacheKey, j);
          if (chunk) skipChunks.add(j);
        }

        if (skipChunks.size === numChunks) {
          this.log.info('All chunks present but metadata incomplete, marking complete:', cacheKey);
          fileInfo.metadata.complete = true;
          const cache = await caches.open(CACHE_NAME);
          await cache.put(`${cacheKey}/metadata`, new Response(
            JSON.stringify(fileInfo.metadata),
            { headers: { 'Content-Type': 'application/json' } }
          ));
          metadataCache.set(cacheKey, fileInfo.metadata);
          return false;
        }

        this.log.info(`Incomplete chunked download: ${skipChunks.size}/${numChunks} chunks cached, resuming:`, cacheKey);
        file.skipChunks = skipChunks;
      } else {
        this.log.debug('File already cached:', cacheKey, fileInfo.chunked ? '(chunked)' : '(whole file)');
        await this.ensureStaticCacheEntry(file);
        return false;
      }
    }

    // Check if already downloading
    const stableKey = `${file.type}/${file.id}`;
    const activeTask = dm.getTask(stableKey);
    if (activeTask) {
      this.log.debug('File already downloading:', stableKey, '- skipping duplicate');
      return false;
    }

    const fileDownload = builder.addFile(file);
    // Only set up caching callback for NEW files (not deduped)
    if (fileDownload.state === 'pending') {
      const cachePromise = this.cacheFileAfterDownload(fileDownload, file);
      enqueuedTasks.push(cachePromise);
      return true;
    }
    return false;
  }

  /**
   * Cache file after download completes.
   * For large files (> CHUNK_STORAGE_THRESHOLD): uses PROGRESSIVE caching —
   *   each chunk is stored to cache as soon as it downloads from the CMS,
   *   metadata is written after the HEAD request, and the client is notified
   *   after the first chunk so video playback can start immediately.
   * For small files: traditional whole-file caching.
   */
  async cacheFileAfterDownload(task, fileInfo) {
    try {
      const cacheKey = `${BASE}/cache/${fileInfo.type}/${fileInfo.id}`;
      const contentType = fileInfo.type === 'layout' ? 'text/xml' :
                          fileInfo.type === 'widget' ? 'text/html' :
                          'application/octet-stream';

      // Large files: progressive chunk caching (stream while downloading)
      const fileSize = parseInt(fileInfo.size) || 0;
      if (fileSize > CHUNK_STORAGE_THRESHOLD) {
        return await this._progressiveCacheFile(task, fileInfo, cacheKey, contentType, fileSize);
      }

      // Small files: wait for full download, then cache whole file
      const blob = await task.wait();

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

      // Also cache widget resources (.js, .css, fonts) for static serving
      this._cacheStaticResource(fileInfo, blob);

      // Now safe to remove from active — file is in cache, won't be re-enqueued
      this.downloadManager.queue.removeCompleted(`${fileInfo.type}/${fileInfo.id}`);

      return blob;
    } catch (error) {
      this.log.error('Failed to cache after download:', fileInfo.id, error);
      this.downloadManager.queue.removeCompleted(`${fileInfo.type}/${fileInfo.id}`);
      throw error;
    }
  }

  /**
   * Progressive chunk caching: store each chunk to cache as it downloads.
   * Video can start playing after first chunk + metadata are stored.
   */
  async _progressiveCacheFile(task, fileInfo, cacheKey, contentType, fileSize) {
    const cache = await caches.open(CACHE_NAME);
    let metadataStored = false;
    let chunksStored = 0;
    let clientNotified = false;

    // Compute expected chunk count from declared file size
    const expectedChunks = Math.ceil(fileSize / CHUNK_SIZE);
    log.info(`Progressive download: ${cacheKey} (${formatBytes(fileSize)}, ~${expectedChunks} chunks)`);

    // Store metadata NOW based on declared file size so Range requests can
    // start working as soon as the first chunk lands in cache
    const metadata = {
      totalSize: fileSize,
      chunkSize: CHUNK_SIZE,
      numChunks: expectedChunks,
      contentType,
      chunked: true,
      complete: false,
      createdAt: Date.now()
    };

    await cache.put(`${cacheKey}/metadata`, new Response(
      JSON.stringify(metadata),
      { headers: { 'Content-Type': 'application/json' } }
    ));
    // Also populate in-memory cache so Range requests skip Cache API lookup
    metadataCache.set(cacheKey, metadata);
    metadataStored = true;
    log.info('Metadata stored, ready for progressive streaming:', cacheKey);

    // Hook into DownloadTask's chunk-by-chunk download.
    // Each chunk gets stored to Cache API the moment it arrives from the CMS,
    // so handleChunkedRangeRequest() can serve it immediately.
    task.onChunkDownloaded = async (chunkIndex, chunkBlob, totalChunks) => {
      // Store chunk to cache immediately
      const chunkResponse = new Response(chunkBlob, {
        headers: {
          'Content-Type': contentType,
          'Content-Length': chunkBlob.size,
          'X-Chunk-Index': chunkIndex,
          'X-Total-Chunks': totalChunks
        }
      });
      await cache.put(`${cacheKey}/chunk-${chunkIndex}`, chunkResponse);
      chunksStored++;

      if (chunksStored % 2 === 0 || chunksStored === totalChunks) {
        log.info(`Progressive: chunk ${chunksStored}/${totalChunks} cached for ${fileInfo.id}`);
      }

      // Notify client when key chunks arrive for early playback:
      // - chunk 0: ftyp/mdat header (first bytes of file)
      // - last chunk: moov atom (MP4 structure, needed by browser before playback)
      // Download manager sends these two first (out-of-order priority).
      if (!clientNotified && (chunkIndex === 0 || chunkIndex === totalChunks - 1)) {
        // Only notify once both chunk 0 AND last chunk are stored
        const hasChunk0 = chunkIndex === 0 || await this.cacheManager.getChunk(cacheKey, 0);
        const hasLastChunk = chunkIndex === totalChunks - 1 || await this.cacheManager.getChunk(cacheKey, totalChunks - 1);

        if (hasChunk0 && hasLastChunk) {
          clientNotified = true;
          const clients = await self.clients.matchAll();
          clients.forEach(client => {
            client.postMessage({
              type: 'FILE_CACHED',
              fileId: fileInfo.id,
              fileType: fileInfo.type,
              size: fileSize,
              progressive: true,
              chunksReady: chunksStored,
              totalChunks
            });
          });
          log.info('Chunk 0 + last chunk cached — client notified, early playback ready:', cacheKey);
        }
      }

      // Update metadata with actual chunk count if it differs (edge case)
      if (totalChunks !== expectedChunks && !metadataStored) {
        metadata.numChunks = totalChunks;
        await cache.put(`${cacheKey}/metadata`, new Response(
          JSON.stringify(metadata),
          { headers: { 'Content-Type': 'application/json' } }
        ));
      }
    };

    // Wait for DownloadTask to finish (all chunks downloaded + callbacks fired).
    // When onChunkDownloaded was used, task.wait() returns an empty Blob
    // (data is already stored to cache chunk by chunk).
    // When downloadFull was used instead (actual size < 100MB), returns the full Blob.
    const downloadedBlob = await task.wait();

    // If the callback never fired (actual file smaller than DownloadTask's chunk
    // threshold of 100MB), use the already-downloaded blob instead of re-fetching.
    if (chunksStored === 0) {
      log.warn('Progressive callback never fired, falling back to putChunked:', cacheKey);

      if (downloadedBlob.size > 0) {
        // Full blob available from downloadFull path — cache it
        await this.cacheManager.putChunked(cacheKey, downloadedBlob, contentType);
      } else {
        // Truly empty — should never happen, but cache whole file as safety net
        await this.cacheManager.put(cacheKey, downloadedBlob, contentType);
      }

      // Notify client
      const clients = await self.clients.matchAll();
      clients.forEach(client => {
        client.postMessage({
          type: 'FILE_CACHED',
          fileId: fileInfo.id,
          fileType: fileInfo.type,
          size: downloadedBlob.size || fileSize
        });
      });
      this.downloadManager.queue.removeCompleted(`${fileInfo.type}/${fileInfo.id}`);
      return downloadedBlob;
    }

    // URL expired mid-download: some chunks cached, but not all.
    // Don't mark complete — next collection cycle resumes with fresh URLs.
    if (task._urlExpired) {
      log.warn(`URL expired mid-download, partial cache: ${cacheKey} (${chunksStored}/${expectedChunks} chunks stored)`);
      this.downloadManager.queue.removeCompleted(`${fileInfo.type}/${fileInfo.id}`);
      return new Blob([], { type: contentType });
    }

    log.info(`Progressive download complete: ${cacheKey} (${chunksStored} chunks stored)`);

    // Mark metadata as complete — this is the commit point.
    // Until this flag is set, the file is considered incomplete and will be
    // resumed (not re-downloaded) on the next collection cycle.
    metadata.complete = true;
    await cache.put(`${cacheKey}/metadata`, new Response(
      JSON.stringify(metadata),
      { headers: { 'Content-Type': 'application/json' } }
    ));
    metadataCache.set(cacheKey, metadata);

    // Notify client with final complete state
    const clients = await self.clients.matchAll();
    clients.forEach(client => {
      client.postMessage({
        type: 'FILE_CACHED',
        fileId: fileInfo.id,
        fileType: fileInfo.type,
        size: fileSize,
        complete: true
      });
    });

    // Remove from pending storage tracker (all chunks are already stored)
    pendingChunkStorage.delete(cacheKey);

    // Now safe to remove from active — all chunks are in cache
    this.downloadManager.queue.removeCompleted(`${fileInfo.type}/${fileInfo.id}`);

    return new Blob([], { type: contentType }); // Data is in cache, not in memory
  }

  /**
   * Cache widget static resources (.js, .css, fonts) alongside the media cache
   */
  _cacheStaticResource(fileInfo, blob) {
    const filename = fileInfo.path ? (() => {
      try { return new URL(fileInfo.path).searchParams.get('file'); } catch { return null; }
    })() : null;

    if (filename && (filename.endsWith('.js') || filename.endsWith('.css') ||
        /\.(otf|ttf|woff2?|eot|svg)$/i.test(filename))) {

      // Fire-and-forget — don't block the main cache flow
      (async () => {
        try {
          const staticCache = await caches.open(STATIC_CACHE);
          const staticKey = `${BASE}/cache/static/${filename}`;

          const ext = filename.split('.').pop().toLowerCase();
          const staticContentType = STATIC_CONTENT_TYPES[ext] || 'application/octet-stream';

          await Promise.all([
            staticCache.put(staticKey, new Response(blob.slice(0, blob.size, blob.type), {
              headers: { 'Content-Type': staticContentType }
            })),
            this.cacheManager.put(staticKey, blob.slice(0, blob.size, blob.type), staticContentType)
          ]);

          log.info('Also cached as static resource:', filename, `(${staticContentType})`);
        } catch (e) {
          log.warn('Failed to cache static resource:', filename, e);
        }
      })();
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
    const staticKey = `${BASE}/cache/static/${filename}`;

    // Check if already in static cache
    const existing = await staticCache.match(staticKey);
    if (existing) return; // Already populated

    // Read from media cache and copy to static cache
    const cacheKey = `${BASE}/cache/${fileInfo.type}/${fileInfo.id}`;
    const cached = await this.cacheManager.get(cacheKey);
    if (!cached) return;

    const blob = await cached.blob();
    const ext = filename.split('.').pop().toLowerCase();
    const staticContentType = STATIC_CONTENT_TYPES[ext] || 'application/octet-stream';

    const staticPathKey = `${BASE}/cache/static/${filename}`;

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
  chunksPerFile: 2 // Max parallel chunks per file — prevents one large file from hogging all connections
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
    ]).then(async () => {
      log.info('Cache initialized');
      // Only skipWaiting if this is a genuinely new version.
      // Re-activating the same version kills in-flight fetch responses
      // (e.g. video streams), causing playback stalls.
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
      log.info('New version, activating immediately');
      return self.skipWaiting();
    })
  );
});

// Activate and claim clients
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

// Handle fetch events
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
