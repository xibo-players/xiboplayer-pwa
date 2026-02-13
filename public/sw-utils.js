/**
 * Service Worker Utility Functions
 * Shared helpers to eliminate code duplication and improve maintainability
 */

/**
 * Dynamic base path derived from the Service Worker's registration scope.
 * Allows the same build to serve /player/pwa/, /player/pwa-xmds/, /player/pwa-xlr/.
 */
export const BASE = (() => {
  if (typeof self !== 'undefined' && self.registration?.scope) {
    return new URL(self.registration.scope).pathname.replace(/\/$/, '');
  }
  return '/player/pwa'; // fallback
})();

/**
 * Format byte size to human-readable string
 * @param {number} bytes - Size in bytes
 * @param {number} decimals - Decimal places (default: 1)
 * @returns {string} Formatted size (e.g., "1.5 MB", "512.0 KB")
 */
export function formatBytes(bytes, decimals = 1) {
  if (bytes === 0) return '0 Bytes';
  if (bytes < 1024) return `${bytes} Bytes`;

  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(decimals)} KB`;

  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(decimals)} MB`;

  const gb = mb / 1024;
  return `${gb.toFixed(decimals)} GB`;
}

/**
 * Parse HTTP Range header
 * @param {string} rangeHeader - Range header value (e.g., "bytes=0-1000")
 * @param {number} totalSize - Total file size
 * @returns {{ start: number, end: number }} Byte range
 */
export function parseRangeHeader(rangeHeader, totalSize) {
  const parts = rangeHeader.replace(/bytes=/, '').split('-');
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : totalSize - 1;

  return { start, end };
}

/**
 * Create standardized response headers for media files
 * @param {Object} options - Header options
 * @param {string} options.contentType - Content-Type header
 * @param {number|string} options.contentLength - Content-Length header
 * @param {boolean} options.includeCache - Include Cache-Control header
 * @param {string} options.contentRange - Content-Range for 206 responses
 * @returns {Object} Response headers
 */
export function createMediaHeaders({
  contentType = 'application/octet-stream',
  contentLength = null,
  includeCache = false,
  contentRange = null
}) {
  const headers = {
    'Content-Type': contentType,
    'Accept-Ranges': 'bytes',
    'Access-Control-Allow-Origin': '*'
  };

  if (contentLength !== null) {
    headers['Content-Length'] = String(contentLength);
  }

  if (includeCache) {
    headers['Cache-Control'] = 'public, max-age=31536000';
  }

  if (contentRange) {
    headers['Content-Range'] = contentRange;
  }

  return headers;
}

/**
 * Create error response with consistent format
 * @param {string} message - Error message
 * @param {number} status - HTTP status code
 * @returns {Response}
 */
export function createErrorResponse(message, status = 500) {
  const statusTexts = {
    404: 'Not Found',
    500: 'Internal Server Error',
    502: 'Bad Gateway',
    503: 'Service Unavailable'
  };

  return new Response(message, {
    status,
    statusText: statusTexts[status] || 'Error',
    headers: { 'Content-Type': 'text/plain' }
  });
}

/**
 * Cache key builder
 */
export const CacheKey = {
  media: (fileId) => `${BASE}/cache/media/${fileId}`,
  layout: (layoutId) => `${BASE}/cache/layout/${layoutId}`,
  widget: (layoutId, regionId, widgetId) => `${BASE}/cache/widget/${layoutId}/${regionId}/${widgetId}`,
  chunk: (baseKey, chunkIndex) => `${baseKey}/chunk-${chunkIndex}`,
  metadata: (baseKey) => `${baseKey}/metadata`,

  // Normalize key from URL pathname
  fromPathname: (pathname) => pathname.startsWith(BASE) ? pathname : `${BASE}${pathname}`
};

/**
 * HTTP Status Codes
 */
export const HTTP_STATUS = {
  OK: 200,
  PARTIAL_CONTENT: 206,
  NOT_FOUND: 404,
  INTERNAL_ERROR: 500,
  BAD_GATEWAY: 502,
  SERVICE_UNAVAILABLE: 503
};

/**
 * Timeouts (milliseconds)
 */
export const TIMEOUTS = {
  SW_CLAIM_WAIT: 100,        // Wait for SW to claim page
  SW_FETCH_READY: 200,       // Wait for SW fetch handler
  SW_READY_MAX: 10000,       // Max wait for SW ready
  DOWNLOAD_CHECK: 1000       // Download progress check interval
};

/**
 * Calculate chunk boundaries
 * @param {number} chunkIndex - Chunk index
 * @param {number} chunkSize - Size per chunk
 * @param {number} totalSize - Total file size
 * @returns {{ start: number, end: number, size: number }}
 */
export function getChunkBoundaries(chunkIndex, chunkSize, totalSize) {
  const start = chunkIndex * chunkSize;
  const end = Math.min(start + chunkSize, totalSize);
  return { start, end, size: end - start };
}

/**
 * Calculate which chunks contain a byte range
 * @param {number} rangeStart - Range start byte
 * @param {number} rangeEnd - Range end byte
 * @param {number} chunkSize - Size per chunk
 * @returns {{ startChunk: number, endChunk: number, count: number }}
 */
export function getChunksForRange(rangeStart, rangeEnd, chunkSize) {
  const startChunk = Math.floor(rangeStart / chunkSize);
  const endChunk = Math.floor(rangeEnd / chunkSize);
  return {
    startChunk,
    endChunk,
    count: endChunk - startChunk + 1
  };
}

/**
 * Extract byte range from chunk blobs
 * @param {Blob[]} chunkBlobs - Array of chunk blobs
 * @param {number} rangeStart - Desired start byte (absolute position in file)
 * @param {number} rangeEnd - Desired end byte (absolute position in file)
 * @param {number} chunkSize - Size per chunk
 * @param {string} contentType - Content type for result blob
 * @returns {Blob} Extracted range
 */
export function extractRangeFromChunks(chunkBlobs, rangeStart, rangeEnd, chunkSize, contentType) {
  if (chunkBlobs.length === 1) {
    // Single chunk - simple slice
    const offset = rangeStart % chunkSize;
    const length = rangeEnd - rangeStart + 1;
    return chunkBlobs[0].slice(offset, offset + length);
  }

  // Multiple chunks - concatenate parts
  const parts = [];
  const firstChunkOffset = rangeStart % chunkSize;
  const lastChunkEnd = rangeEnd % chunkSize;

  // First chunk (partial from offset to end)
  parts.push(chunkBlobs[0].slice(firstChunkOffset));

  // Middle chunks (complete - use as-is)
  for (let i = 1; i < chunkBlobs.length - 1; i++) {
    parts.push(chunkBlobs[i]);
  }

  // Last chunk (partial from start to lastChunkEnd)
  if (chunkBlobs.length > 1) {
    parts.push(chunkBlobs[chunkBlobs.length - 1].slice(0, lastChunkEnd + 1));
  }

  return new Blob(parts, { type: contentType });
}
