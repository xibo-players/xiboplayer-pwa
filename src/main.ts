/**
 * PWA Player with RendererLite
 *
 * Lightweight PWA player using modular PlayerCore orchestration.
 * Platform layer handles UI, DOM manipulation, and platform-specific features.
 */

// @ts-ignore - JavaScript module
import { RendererLite } from '@xiboplayer/renderer';
// @ts-ignore - JavaScript module
import { CacheProxy } from '@xiboplayer/cache';
// @ts-ignore - JavaScript module
import { PlayerCore } from '@xiboplayer/core';
// @ts-ignore - JavaScript module
import { createLogger, isDebug } from '@xiboplayer/utils';
import { DownloadOverlay, getDefaultOverlayConfig } from './download-overlay.js';

const log = createLogger('PWA');

// Import core modules (will be loaded at runtime)
let cacheManager: any;
let scheduleManager: any;
let config: any;
let XmdsClient: any;
let XmrWrapper: any;
let cacheProxy: CacheProxy;
let StatsCollector: any;
let formatStats: any;
let DisplaySettings: any;

class PwaPlayer {
  private renderer!: RendererLite;
  private core!: PlayerCore;
  private xmds!: any;
  private downloadOverlay: DownloadOverlay | null = null;
  private statsCollector: any = null;
  private displaySettings: any = null;
  private currentScheduleId: number = -1; // Track scheduleId for stats
  private preparingLayoutId: number | null = null; // Guard against concurrent prepareAndRenderLayout calls

  async init() {
    log.info('Initializing player with RendererLite + PlayerCore...');

    // Load core modules
    await this.loadCoreModules();

    // Register Service Worker for offline-first kiosk mode
    if ('serviceWorker' in navigator) {
      try {
        const registration = await navigator.serviceWorker.register(`/player/pwa/sw.js?v=${Date.now()}`, {
          scope: '/player/pwa/',  // Scope matches SW location
          type: 'module', // Enable ES6 module imports
          updateViaCache: 'none'  // Don't cache sw.js - always fetch fresh
        });
        log.info('Service Worker registered for offline mode:', registration.scope);

        // Request persistent storage (kiosk requirement)
        if (navigator.storage && navigator.storage.persist) {
          const persistent = await navigator.storage.persist();
          if (persistent) {
            log.info('Persistent storage granted - cache won\'t be evicted');
          } else {
            log.warn('Persistent storage denied - cache may be evicted');
          }
        }
      } catch (error) {
        log.warn('Service Worker registration failed:', error);
      }
    }

    // Initialize cache
    log.info('Initializing cache...');
    await cacheManager.init();

    // Initialize CacheProxy (Service Worker only - waits for SW to be ready)
    log.info('Initializing CacheProxy...');
    cacheProxy = new CacheProxy();
    await cacheProxy.init();  // Waits for Service Worker to be ready and controlling
    log.info('CacheProxy ready - using Service Worker backend');

    // Create renderer
    const container = document.getElementById('player-container');
    if (!container) {
      throw new Error('No #player-container found');
    }

    this.renderer = new RendererLite(
      {
        cmsUrl: config.cmsAddress,
        hardwareKey: config.hardwareKey
      },
      container,
      {
        // Provide media URL resolver - uses streaming via Service Worker
        getMediaUrl: async (fileId: number) => {
          log.debug(`getMediaUrl called for media ${fileId}`);

          // Check if file exists in cache (no blob creation - streaming!)
          const exists = await cacheProxy.hasFile('media', String(fileId));

          if (!exists) {
            log.warn(`Media ${fileId} not in cache`);
            return '';
          }

          // Return direct URL - Service Worker streams via Range requests
          // This eliminates blob creation delay and reduces memory usage!
          const streamingUrl = `/player/pwa/cache/media/${fileId}`;
          log.debug(`Using streaming URL for media ${fileId}: ${streamingUrl}`);
          return streamingUrl;
        },

        // Provide widget HTML resolver
        getWidgetHtml: async (widget: any) => {
          const cacheKey = `/player/pwa/cache/widget/${widget.layoutId}/${widget.regionId}/${widget.id}`;
          log.debug(`Looking for widget HTML at: ${cacheKey}`, widget);

          try {
            const cache = await caches.open('xibo-media-v1');
            const response = await cache.match(cacheKey);

            if (response) {
              log.debug(`Widget HTML cached at ${cacheKey}, using cache URL for iframe`);
              // Return cache URL + fallback HTML for hard reload recovery
              // On Ctrl+Shift+R, iframe.src navigation bypasses SW → 404
              // Renderer detects this and falls back to widget.raw (original CMS URLs)
              return { url: cacheKey, fallback: widget.raw || '' };
            } else {
              log.warn(`No cached HTML found at ${cacheKey}`);
            }
          } catch (error) {
            log.error(`Failed to get cached widget HTML for ${widget.id}:`, error);
          }

          // Fallback to widget.raw (XLF template)
          log.warn(`Using widget.raw fallback for ${widget.id}`);
          return widget.raw || '';
        }
      }
    );

    // Create PlayerCore
    this.core = new PlayerCore({
      config,
      xmds: this.xmds,
      cache: cacheProxy,
      schedule: scheduleManager,
      renderer: this.renderer,
      xmrWrapper: XmrWrapper,
      statsCollector: this.statsCollector,
      displaySettings: this.displaySettings
    });

    // Setup platform-specific event handlers
    this.setupCoreEventHandlers();
    this.setupRendererEventHandlers();
    this.setupServiceWorkerEventHandlers();

    // Setup UI
    this.setupUI();

    // Initialize download progress overlay (configurable debug feature)
    const overlayConfig = getDefaultOverlayConfig();
    if (overlayConfig.enabled) {
      this.downloadOverlay = new DownloadOverlay(overlayConfig, cacheProxy);
      log.info('Download overlay enabled (hover bottom-right corner)');
    }

    // Start collection cycle
    await this.core.collect();

    log.info('Player initialized successfully');
  }

  /**
   * Load core modules
   */
  private async loadCoreModules() {
    try {
      // @ts-ignore - JavaScript modules
      const cacheModule = await import('@xiboplayer/cache');
      // @ts-ignore
      const xmdsModule = await import('@xiboplayer/xmds');
      // @ts-ignore
      const scheduleModule = await import('@xiboplayer/schedule');
      // @ts-ignore
      const configModule = await import('@xiboplayer/utils');
      // @ts-ignore
      const xmrModule = await import('@xiboplayer/xmr');
      // @ts-ignore
      const statsModule = await import('@xiboplayer/stats');
      // @ts-ignore
      const displaySettingsModule = await import('@xiboplayer/settings');

      cacheManager = cacheModule.cacheManager;
      scheduleManager = scheduleModule.scheduleManager;
      config = configModule.config;
      XmdsClient = xmdsModule.XmdsClient;
      XmrWrapper = xmrModule.XmrWrapper;
      StatsCollector = statsModule.StatsCollector;
      formatStats = statsModule.formatStats;
      DisplaySettings = displaySettingsModule.DisplaySettings;

      this.xmds = new XmdsClient(config);

      // Initialize stats collector
      this.statsCollector = new StatsCollector();
      await this.statsCollector.init();
      log.info('Stats collector initialized');

      // Initialize display settings manager
      this.displaySettings = new DisplaySettings();
      log.info('Display settings manager initialized');

      log.info('Core modules loaded');
    } catch (error) {
      log.error('Failed to load core modules:', error);
      throw error;
    }
  }

  /**
   * Setup PlayerCore event handlers (Platform-specific UI updates)
   */
  private setupCoreEventHandlers() {
    // Collection events
    this.core.on('collection-start', () => {
      this.updateStatus('Collecting data from CMS...');
    });

    this.core.on('register-complete', (regResult: any) => {
      const displayName = this.displaySettings?.getDisplayName() || regResult.displayName || config.hardwareKey;
      this.updateStatus(`Registered: ${displayName}`);

      // Update page title with display name
      if (this.displaySettings) {
        document.title = `Xibo Player - ${this.displaySettings.getDisplayName()}`;
      }
    });

    this.core.on('files-received', (files: any[]) => {
      this.updateStatus(`Downloading ${files.length} files...`);
    });

    this.core.on('download-request', async (files: any[]) => {
      // Platform handles the actual download via CacheProxy
      try {
        await cacheProxy.requestDownload(files);
        log.info('Download request complete');
      } catch (error) {
        log.error('Download request failed:', error);
        this.updateStatus('Download failed: ' + error, 'error');
      }
    });

    this.core.on('schedule-received', (schedule: any) => {
      this.updateStatus('Processing schedule...');

      // Extract scheduleId for stats tracking
      // Check layouts or campaigns for scheduleId
      if (schedule.layouts && schedule.layouts.length > 0) {
        this.currentScheduleId = parseInt(schedule.layouts[0].scheduleid) || -1;
      } else if (schedule.campaigns && schedule.campaigns.length > 0) {
        this.currentScheduleId = parseInt(schedule.campaigns[0].scheduleid) || -1;
      }

      log.debug('Current scheduleId for stats:', this.currentScheduleId);
    });

    this.core.on('layout-prepare-request', async (layoutId: number) => {
      await this.prepareAndRenderLayout(layoutId);
    });

    this.core.on('layout-already-playing', () => {
      // Layout already playing, no action needed
    });

    this.core.on('no-layouts-scheduled', () => {
      this.updateStatus('No layouts scheduled');
    });

    this.core.on('collection-complete', () => {
      this.updateStatus('Collection complete');
    });

    this.core.on('collection-error', (error: any) => {
      this.updateStatus(`Collection error: ${error}`, 'error');
    });

    this.core.on('xmr-connected', (url: string) => {
      log.info('XMR connected:', url);
    });

    // React to CMS log level changes — toggle download overlay at runtime
    this.core.on('log-level-changed', () => {
      const debugNow = isDebug();
      log.info(`Log level changed, debug=${debugNow}`);

      if (debugNow && !this.downloadOverlay) {
        this.downloadOverlay = new DownloadOverlay(getDefaultOverlayConfig(), cacheProxy);
        log.info('Download overlay enabled (log level → DEBUG)');
      } else if (!debugNow && this.downloadOverlay) {
        this.downloadOverlay.destroy();
        this.downloadOverlay = null;
        log.info('Download overlay disabled (log level above DEBUG)');
      }
    });

    // Display settings events
    if (this.displaySettings) {
      this.displaySettings.on('interval-changed', (newInterval: number) => {
        log.info(`Collection interval changed to ${newInterval}s`);
      });

      this.displaySettings.on('settings-applied', (_settings: any, changes: string[]) => {
        if (changes.length > 0) {
          log.info('Settings updated from CMS:', changes.join(', '));
        }
      });
    }

    // Stats submission
    this.core.on('submit-stats-request', async () => {
      await this.submitStats();
    });

    // Listen for media downloads completing
    window.addEventListener('media-cached', async (event: any) => {
      const mediaId = event.detail?.id;
      log.debug(`Media ${mediaId} download completed`);

      // Notify core that media is ready
      this.core.notifyMediaReady(mediaId);
    });

    // Handle check-pending-layout events
    // Re-run prepareAndRenderLayout which checks XLF + actual media IDs correctly
    // (avoids the bug where setPendingLayout(id,[id]) treated layoutId as mediaId)
    this.core.on('check-pending-layout', async (layoutId: number) => {
      await this.prepareAndRenderLayout(layoutId);
    });
  }

  /**
   * Setup Service Worker event handlers (bridges SW messages to PlayerCore)
   */
  private setupServiceWorkerEventHandlers() {
    if (!navigator.serviceWorker) return;

    navigator.serviceWorker.addEventListener('message', (event: any) => {
      const { type, fileId, fileType } = event.data;

      if (type === 'FILE_CACHED') {
        log.debug(`Service Worker cached ${fileType}/${fileId}`);

        // Notify PlayerCore that file is ready
        // Pass fileType so PlayerCore can distinguish layout files from media files
        if (fileType === 'media' || fileType === 'layout') {
          this.core.notifyMediaReady(parseInt(fileId), fileType);
        }
      }
    });
  }

  /**
   * Setup renderer event handlers
   */
  private setupRendererEventHandlers() {
    this.renderer.on('layoutStart', (layoutId: number, _layout: any) => {
      log.info('Layout started:', layoutId);
      this.updateStatus(`Playing layout ${layoutId}`);
      this.core.setCurrentLayout(layoutId);

      // Track stats: start layout
      if (this.statsCollector) {
        this.statsCollector.startLayout(layoutId, this.currentScheduleId).catch((err: any) => {
          log.error('Failed to start layout stat:', err);
        });
      }
    });

    this.renderer.on('layoutEnd', (layoutId: number) => {
      log.info('Layout ended:', layoutId);

      // Record play at END so maxPlaysPerHour doesn't interrupt the current play.
      // Previously recorded at layoutStart, which caused periodic collections to
      // filter the layout mid-playback (e.g., 200s video cut at 168s).
      scheduleManager?.recordPlay(layoutId.toString());

      // Track stats: end layout
      if (this.statsCollector) {
        this.statsCollector.endLayout(layoutId, this.currentScheduleId).catch((err: any) => {
          log.error('Failed to end layout stat:', err);
        });
      }

      // Report to CMS
      this.core.notifyLayoutStatus(layoutId);

      // Clear current layout to allow replay
      this.core.clearCurrentLayout();

      // If a new layout is already pending download, don't re-collect
      // (avoids redundant XMDS calls and duplicate download requests)
      const pending = this.core.getPendingLayouts();
      if (pending.length > 0) {
        log.info(`Layout ${pending[0]} pending download, skipping collection`);
        return;
      }

      // Trigger schedule check to replay the layout (or switch to new one)
      log.info('Layout cycle completed, checking schedule...');
      this.core.collect().catch((error: any) => {
        log.error('Failed to check schedule:', error);
      });
    });

    this.renderer.on('widgetStart', (data: any) => {
      const { widgetId, layoutId, mediaId } = data;
      log.debug('Widget started:', data.type, widgetId, 'media:', mediaId);

      // Track stats: start widget/media
      if (this.statsCollector && mediaId) {
        this.statsCollector.startWidget(mediaId, layoutId, this.currentScheduleId).catch((err: any) => {
          log.error('Failed to start widget stat:', err);
        });
      }
    });

    this.renderer.on('widgetEnd', (data: any) => {
      const { widgetId, layoutId, mediaId } = data;
      log.debug('Widget ended:', data.type, widgetId, 'media:', mediaId);

      // Track stats: end widget/media
      if (this.statsCollector && mediaId) {
        this.statsCollector.endWidget(mediaId, layoutId, this.currentScheduleId).catch((err: any) => {
          log.error('Failed to end widget stat:', err);
        });
      }
    });

    this.renderer.on('error', (error: any) => {
      log.error('Renderer error:', error);
      this.updateStatus(`Error: ${error.type}`, 'error');
    });
  }

  /**
   * Prepare and render layout (Platform-specific logic)
   */
  private async prepareAndRenderLayout(layoutId: number) {
    // Guard: skip if already playing this layout (another event already rendered it)
    if (this.core.getCurrentLayoutId() === layoutId) {
      log.debug(`Layout ${layoutId} already playing, skipping duplicate prepare`);
      return;
    }

    // Guard: prevent concurrent preparations of the same layout
    // (e.g., two check-pending-layout events firing close together)
    if (this.preparingLayoutId === layoutId) {
      log.debug(`Layout ${layoutId} preparation already in progress, skipping`);
      return;
    }

    this.preparingLayoutId = layoutId;
    try {
      // Get XLF from cache
      const xlfBlob = await cacheManager.getCachedFile('layout', layoutId);
      if (!xlfBlob) {
        log.info('Layout not in cache yet, marking as pending:', layoutId);
        // Mark layout as pending so when it downloads, we'll retry
        // Use layoutId as required file (will trigger on layout file cached)
        this.core.setPendingLayout(layoutId, [layoutId]);
        this.updateStatus(`Downloading layout ${layoutId}...`);
        return;
      }

      const xlfXml = await xlfBlob.text();

      // Check if all required media is cached
      const requiredMedia = await this.getRequiredMediaIds(xlfXml);
      const allMediaCached = await this.checkAllMediaCached(requiredMedia);

      if (!allMediaCached) {
        log.info(`Waiting for media to finish downloading for layout ${layoutId}`);
        this.updateStatus(`Preparing layout ${layoutId}...`);
        this.core.setPendingLayout(layoutId, requiredMedia);
        return; // Keep playing current layout until media is ready
      }

      // Pre-fetch common widget dependencies (bundle.min.js, fonts.css)
      await this.prefetchWidgetDependencies();

      // Fetch widget HTML for all widgets in the layout
      await this.fetchWidgetHtml(xlfXml, layoutId);

      // Render layout
      await this.renderer.renderLayout(xlfXml, layoutId);
      this.updateStatus(`Playing layout ${layoutId}`);

    } catch (error) {
      log.error('Failed to prepare layout:', layoutId, error);
      this.updateStatus(`Failed to load layout ${layoutId}`, 'error');
    } finally {
      this.preparingLayoutId = null;
    }
  }

  /**
   * Get all required media file IDs from layout XLF
   */
  private async getRequiredMediaIds(xlfXml: string): Promise<number[]> {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xlfXml, 'text/xml');
    const mediaIds: number[] = [];

    // Find all media elements with fileId
    const mediaElements = doc.querySelectorAll('media[fileId]');
    mediaElements.forEach(el => {
      const fileId = el.getAttribute('fileId');
      if (fileId) {
        mediaIds.push(parseInt(fileId, 10));
      }
    });

    return mediaIds;
  }

  /**
   * Check if all required media files are cached and ready
   */
  private async checkAllMediaCached(mediaIds: number[]): Promise<boolean> {
    for (const mediaId of mediaIds) {
      try {
        // Use CacheProxy API - it delegates to SW's CacheManager.fileExists()
        const exists = await cacheProxy.hasFile('media', String(mediaId));

        if (!exists) {
          log.debug(`Media ${mediaId} not yet cached`);
          return false;
        }

        // File exists (either whole file or chunks) - now validate it
        // Check for whole-file storage first
        const response = await cacheManager.getCachedResponse('media', mediaId);

        if (!response) {
          // Must be chunked storage - get metadata for display
          const cache = await caches.open('xibo-media-v1');
          const metadataResponse = await cache.match(`/player/pwa/cache/media/${mediaId}/metadata`);

          if (metadataResponse) {
            const metadataText = await metadataResponse.text();
            const metadata = JSON.parse(metadataText);
            const sizeMB = (metadata.totalSize / 1024 / 1024).toFixed(1);
            log.debug(`Media ${mediaId} cached as chunks (${metadata.numChunks} x ${(metadata.chunkSize / 1024 / 1024).toFixed(0)} MB = ${sizeMB} MB total)`);
            continue;
          }
        }

        // Validate cached file (detect corrupted entries)
        const contentType = response.headers.get('Content-Type') || '';
        const blob = await response.blob();

        // Check for bad cache
        if (contentType === 'text/plain' || blob.size < 100) {
          log.warn(`Media ${mediaId} corrupted (${contentType}, ${blob.size} bytes) - will re-download`);

          // Delete bad cache entry
          const cache = await caches.open('xibo-media-v1');
          const cacheKey = `/player/pwa/cache/media/${mediaId}`;
          await cache.delete(cacheKey);

          return false;
        }

        // Format size appropriately (KB for small files, MB for large)
        const sizeKB = blob.size / 1024;
        const sizeMB = sizeKB / 1024;
        const sizeStr = sizeMB >= 1 ? `${sizeMB.toFixed(1)} MB` : `${sizeKB.toFixed(1)} KB`;
        log.debug(`Media ${mediaId} cached and valid (${sizeStr})`);

      } catch (error) {
        log.warn(`Unable to verify media ${mediaId}, assuming cached (offline mode)`);
      }
    }
    return true;
  }

  /**
   * Pre-fetch common widget dependencies (bundle.min.js, fonts.css)
   * These are downloaded by the SW via signed URLs from RequiredFiles.
   * Just check the SW's static cache — don't construct manual URLs.
   */
  private async prefetchWidgetDependencies() {
    const filenames = ['bundle.min.js', 'fonts.css'];

    const cache = await caches.open('xibo-static-v1');
    for (const filename of filenames) {
      const cached = await cache.match(`/player/pwa/cache/static/${filename}`);
      if (cached) {
        log.debug(`Widget dependency ${filename} already cached by SW`);
      } else {
        log.debug(`Widget dependency ${filename} not yet cached (will be fetched by SW on first use)`);
      }
    }
  }

  /**
   * Fetch widget HTML for all widgets in layout (parallel)
   */
  private async fetchWidgetHtml(xlfXml: string, layoutId: number) {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xlfXml, 'text/xml');

    const widgetTypes = ['clock', 'calendar', 'weather', 'currencies', 'stocks',
                        'twitter', 'global', 'embedded', 'text', 'ticker'];

    const fetchPromises: Promise<void>[] = [];

    for (const regionEl of doc.querySelectorAll('region')) {
      const regionId = regionEl.getAttribute('id');

      for (const mediaEl of regionEl.querySelectorAll('media')) {
        const type = mediaEl.getAttribute('type');
        const widgetId = mediaEl.getAttribute('id');

        if (widgetTypes.some(w => type?.includes(w))) {
          const cacheKey = `/cache/widget/${layoutId}/${regionId}/${widgetId}`;

          fetchPromises.push(
            (async () => {
              try {
                const cache = await caches.open('xibo-media-v1');
                const cachedResponse = await cache.match(cacheKey);

                let html: string;
                if (cachedResponse) {
                  html = await cachedResponse.text();
                  log.debug(`Using cached widget HTML for ${type} ${widgetId}`);
                } else {
                  html = await this.xmds.getResource(layoutId, regionId, widgetId);
                  await cacheManager.cacheWidgetHtml(layoutId, regionId, widgetId, html);
                  log.debug(`Retrieved widget HTML for ${type} ${widgetId}`);
                }

                // Update raw content in XLF
                const rawEl = mediaEl.querySelector('raw');
                if (rawEl) {
                  rawEl.textContent = html;
                } else {
                  const newRaw = doc.createElement('raw');
                  newRaw.textContent = html;
                  mediaEl.appendChild(newRaw);
                }
              } catch (error) {
                log.warn(`Failed to get widget HTML for ${type} ${widgetId}:`, error);
              }
            })()
          );
        }
      }
    }

    if (fetchPromises.length > 0) {
      log.info(`Fetching ${fetchPromises.length} widget HTML resources in parallel...`);
      await Promise.all(fetchPromises);
      log.debug('All widget HTML fetched');
    }
  }

  /**
   * Setup UI
   */
  private setupUI() {
    const container = document.getElementById('player-container');
    if (!container) {
      log.warn('No #player-container found');
    }

    this.updateConfigDisplay();
  }

  /**
   * Update config display
   */
  private updateConfigDisplay() {
    const configEl = document.getElementById('config-info');
    if (configEl) {
      configEl.textContent = `CMS: ${config.cmsAddress} | Display: ${config.displayName || config.hardwareKey} | Mode: Lite+Core`;
    }
  }

  /**
   * Submit proof of play stats to CMS
   */
  private async submitStats() {
    if (!this.statsCollector) {
      log.warn('Stats collector not initialized');
      return;
    }

    try {
      // Get stats ready for submission (up to 50 at a time)
      const stats = await this.statsCollector.getStatsForSubmission(50);

      if (stats.length === 0) {
        log.debug('No stats to submit');
        return;
      }

      log.info(`Submitting ${stats.length} proof of play stats...`);

      // Format stats as XML
      const statsXml = formatStats(stats);

      // Submit to CMS via XMDS
      const success = await this.xmds.submitStats(statsXml);

      if (success) {
        log.info('Stats submitted successfully');
        // Clear submitted stats from database
        await this.statsCollector.clearSubmittedStats(stats);
        log.debug(`Cleared ${stats.length} submitted stats from database`);
      } else {
        log.warn('Stats submission failed (CMS returned false)');
      }
    } catch (error) {
      log.error('Failed to submit stats:', error);
    }
  }

  /**
   * Update status message (Platform-specific UI)
   */
  private updateStatus(message: string, type: 'info' | 'error' = 'info') {
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = message;
      statusEl.className = `status status-${type}`;
    }
    if (type === 'error') {
      log.error('Status:', message);
    } else {
      log.info('Status:', message);
    }
  }

  /**
   * Cleanup
   */
  cleanup() {
    this.core.cleanup();
    this.renderer.cleanup();

    // Cleanup download overlay if active
    if (this.downloadOverlay) {
      this.downloadOverlay.destroy();
    }
  }
}

// Initialize player
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    const player = new PwaPlayer();
    player.init().catch(error => {
      log.error('Failed to initialize:', error);
    });

    window.addEventListener('beforeunload', () => {
      player.cleanup();
    });
  });
} else {
  const player = new PwaPlayer();
  player.init().catch(error => {
    log.error('Failed to initialize:', error);
  });

  window.addEventListener('beforeunload', () => {
    player.cleanup();
  });
}
