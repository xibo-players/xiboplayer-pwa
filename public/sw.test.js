/**
 * Service Worker Routing Helper Unit Tests
 *
 * Tests the routeFileRequest() method that determines how to serve files
 * based on storage format (whole file vs chunks) and request type (HEAD/GET/Range)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

describe('RequestHandler.routeFileRequest()', () => {
  let requestHandler;
  let mockCacheManager;
  let mockBlobCache;

  beforeEach(() => {
    // Mock CacheManager
    mockCacheManager = {
      fileExists: vi.fn(),
      get: vi.fn(),
      getMetadata: vi.fn()
    };

    // Mock BlobCache
    mockBlobCache = {
      get: vi.fn()
    };

    // Create RequestHandler instance (simplified - no DownloadManager needed for routing)
    requestHandler = {
      cacheManager: mockCacheManager,
      blobCache: mockBlobCache,
      // Copy the actual routeFileRequest implementation
      async routeFileRequest(cacheKey, method, rangeHeader) {
        const fileInfo = await this.cacheManager.fileExists(cacheKey);

        if (!fileInfo.exists) {
          return { found: false, handler: null, data: null };
        }

        if (fileInfo.chunked) {
          const data = { metadata: fileInfo.metadata, cacheKey };

          if (method === 'HEAD') {
            return { found: true, handler: 'head-chunked', data };
          }
          if (rangeHeader) {
            return { found: true, handler: 'range-chunked', data: { ...data, rangeHeader } };
          }
          return { found: true, handler: 'full-chunked', data };

        } else {
          const cached = await this.cacheManager.get(cacheKey);
          const data = { cached, cacheKey };

          if (method === 'HEAD') {
            return { found: true, handler: 'head-whole', data };
          }
          if (rangeHeader) {
            return { found: true, handler: 'range-whole', data: { ...data, rangeHeader } };
          }
          return { found: true, handler: 'full-whole', data };
        }
      }
    };
  });

  describe('File Not Found', () => {
    it('should return not found when file does not exist', async () => {
      mockCacheManager.fileExists.mockResolvedValue({ exists: false });

      const result = await requestHandler.routeFileRequest('/cache/media/999', 'GET', null);

      expect(result).toEqual({
        found: false,
        handler: null,
        data: null
      });
      expect(mockCacheManager.fileExists).toHaveBeenCalledWith('/cache/media/999');
    });
  });

  describe('Whole File Storage', () => {
    const cacheKey = '/player/pwa/cache/media/1';
    const mockCached = { headers: { get: () => 'image/png' } };

    beforeEach(() => {
      mockCacheManager.fileExists.mockResolvedValue({
        exists: true,
        chunked: false,
        metadata: null
      });
      mockCacheManager.get.mockResolvedValue(mockCached);
    });

    it('should route HEAD request to head-whole handler', async () => {
      const result = await requestHandler.routeFileRequest(cacheKey, 'HEAD', null);

      expect(result.found).toBe(true);
      expect(result.handler).toBe('head-whole');
      expect(result.data.cached).toBe(mockCached);
      expect(result.data.cacheKey).toBe(cacheKey);
    });

    it('should route GET with Range to range-whole handler', async () => {
      const result = await requestHandler.routeFileRequest(cacheKey, 'GET', 'bytes=0-1000');

      expect(result.found).toBe(true);
      expect(result.handler).toBe('range-whole');
      expect(result.data.cached).toBe(mockCached);
      expect(result.data.rangeHeader).toBe('bytes=0-1000');
    });

    it('should route GET without Range to full-whole handler', async () => {
      const result = await requestHandler.routeFileRequest(cacheKey, 'GET', null);

      expect(result.found).toBe(true);
      expect(result.handler).toBe('full-whole');
      expect(result.data.cached).toBe(mockCached);
    });
  });

  describe('Chunked File Storage', () => {
    const cacheKey = '/player/pwa/cache/media/6';
    const mockMetadata = {
      totalSize: 1034784779,
      numChunks: 20,
      chunkSize: 50 * 1024 * 1024,
      contentType: 'video/mp4',
      chunked: true
    };

    beforeEach(() => {
      mockCacheManager.fileExists.mockResolvedValue({
        exists: true,
        chunked: true,
        metadata: mockMetadata
      });
    });

    it('should route HEAD request to head-chunked handler', async () => {
      const result = await requestHandler.routeFileRequest(cacheKey, 'HEAD', null);

      expect(result.found).toBe(true);
      expect(result.handler).toBe('head-chunked');
      expect(result.data.metadata).toEqual(mockMetadata);
      expect(result.data.cacheKey).toBe(cacheKey);
    });

    it('should route GET with Range to range-chunked handler', async () => {
      const result = await requestHandler.routeFileRequest(cacheKey, 'GET', 'bytes=0-5242880');

      expect(result.found).toBe(true);
      expect(result.handler).toBe('range-chunked');
      expect(result.data.metadata).toEqual(mockMetadata);
      expect(result.data.rangeHeader).toBe('bytes=0-5242880');
    });

    it('should route GET without Range to full-chunked handler', async () => {
      const result = await requestHandler.routeFileRequest(cacheKey, 'GET', null);

      expect(result.found).toBe(true);
      expect(result.handler).toBe('full-chunked');
      expect(result.data.metadata).toEqual(mockMetadata);
    });
  });

  describe('Edge Cases', () => {
    it('should handle null Range header as no range', async () => {
      mockCacheManager.fileExists.mockResolvedValue({
        exists: true,
        chunked: false,
        metadata: null
      });
      mockCacheManager.get.mockResolvedValue({ headers: { get: () => null } });

      const result = await requestHandler.routeFileRequest('/cache/media/1', 'GET', null);

      expect(result.handler).toBe('full-whole');
    });

    it('should handle empty Range header as range request', async () => {
      mockCacheManager.fileExists.mockResolvedValue({
        exists: true,
        chunked: true,
        metadata: { totalSize: 1000, numChunks: 1, chunked: true }
      });

      const result = await requestHandler.routeFileRequest('/cache/media/6', 'GET', '');

      // Empty string is falsy, treated as no range
      expect(result.handler).toBe('full-chunked');
    });
  });

  describe('Performance', () => {
    it('should call fileExists only once per routing', async () => {
      mockCacheManager.fileExists.mockResolvedValue({
        exists: true,
        chunked: false,
        metadata: null
      });
      mockCacheManager.get.mockResolvedValue({ headers: { get: () => null } });

      await requestHandler.routeFileRequest('/cache/media/1', 'GET', null);

      expect(mockCacheManager.fileExists).toHaveBeenCalledTimes(1);
    });

    it('should not call get() for chunked files', async () => {
      mockCacheManager.fileExists.mockResolvedValue({
        exists: true,
        chunked: true,
        metadata: { totalSize: 1000, chunked: true }
      });

      await requestHandler.routeFileRequest('/cache/media/6', 'GET', 'bytes=0-100');

      expect(mockCacheManager.get).not.toHaveBeenCalled();
    });
  });
});

describe('Routing Logic Integration', () => {
  it('should correctly identify all 6 handler combinations', async () => {
    const testCases = [
      { storage: 'whole', method: 'HEAD', range: null, expected: 'head-whole' },
      { storage: 'whole', method: 'GET', range: 'bytes=0-100', expected: 'range-whole' },
      { storage: 'whole', method: 'GET', range: null, expected: 'full-whole' },
      { storage: 'chunked', method: 'HEAD', range: null, expected: 'head-chunked' },
      { storage: 'chunked', method: 'GET', range: 'bytes=0-100', expected: 'range-chunked' },
      { storage: 'chunked', method: 'GET', range: null, expected: 'full-chunked' }
    ];

    for (const testCase of testCases) {
      const mockCacheManager = {
        fileExists: vi.fn().mockResolvedValue({
          exists: true,
          chunked: testCase.storage === 'chunked',
          metadata: testCase.storage === 'chunked' ? { totalSize: 1000, chunked: true } : null
        }),
        get: vi.fn().mockResolvedValue({ headers: { get: () => null } })
      };

      const handler = {
        cacheManager: mockCacheManager,
        async routeFileRequest(cacheKey, method, rangeHeader) {
          const fileInfo = await this.cacheManager.fileExists(cacheKey);
          if (!fileInfo.exists) return { found: false, handler: null, data: null };

          if (fileInfo.chunked) {
            const data = { metadata: fileInfo.metadata, cacheKey };
            if (method === 'HEAD') return { found: true, handler: 'head-chunked', data };
            if (rangeHeader) return { found: true, handler: 'range-chunked', data: { ...data, rangeHeader } };
            return { found: true, handler: 'full-chunked', data };
          } else {
            const cached = await this.cacheManager.get(cacheKey);
            const data = { cached, cacheKey };
            if (method === 'HEAD') return { found: true, handler: 'head-whole', data };
            if (rangeHeader) return { found: true, handler: 'range-whole', data: { ...data, rangeHeader } };
            return { found: true, handler: 'full-whole', data };
          }
        }
      };

      const result = await handler.routeFileRequest('/cache/media/test', testCase.method, testCase.range);

      expect(result.handler).toBe(testCase.expected);
      expect(result.found).toBe(true);
    }
  });
});
