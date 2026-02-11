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
import { createLogger, isDebug, registerLogSink } from '@xiboplayer/utils';
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
let LogReporter: any;
let formatLogs: any;
let DisplaySettings: any;

class PwaPlayer {
  private renderer!: RendererLite;
  private core!: PlayerCore;
  private xmds!: any;
  private downloadOverlay: DownloadOverlay | null = null;
  private statsCollector: any = null;
  private logReporter: any = null;
  private displaySettings: any = null;
  private currentScheduleId: number = -1; // Track scheduleId for stats
  private preparingLayoutId: number | null = null; // Guard against concurrent prepareAndRenderLayout calls
  private _screenshotInterval: any = null;
  private _wakeLock: any = null; // Screen Wake Lock sentinel

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
    this.setupInteractiveControl();

    // Setup UI
    this.setupUI();

    // Online/offline event listeners for seamless offline mode
    window.addEventListener('online', () => {
      console.log('[PWA] Browser reports online — triggering immediate collection');
      this.updateStatus('Back online, syncing...');
      this.removeOfflineIndicator();
      this.core.collectNow().catch((error: any) => {
        log.error('Failed to collect after coming online:', error);
      });
    });
    window.addEventListener('offline', () => {
      console.warn('[PWA] Browser reports offline — continuing playback with cached data');
      this.updateStatus('Offline mode — using cached content');
      this.showOfflineIndicator();
    });

    // Initialize download progress overlay (configurable debug feature)
    const overlayConfig = getDefaultOverlayConfig();
    if (overlayConfig.enabled) {
      this.downloadOverlay = new DownloadOverlay(overlayConfig, cacheProxy);
      log.info('Download overlay enabled (hover bottom-right corner)');
    }

    // Request Screen Wake Lock to prevent display sleep
    await this.requestWakeLock();

    // Re-acquire wake lock when tab becomes visible again
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') {
        this.requestWakeLock();
      }
    });

    // Start collection cycle
    await this.core.collect();

    log.info('Player initialized successfully');
  }

  /**
   * Request Screen Wake Lock to prevent display from sleeping
   * Re-acquired on visibility change (browser releases it when tab is hidden)
   */
  private async requestWakeLock() {
    if (!('wakeLock' in navigator)) {
      log.debug('Wake Lock API not supported');
      return;
    }

    try {
      this._wakeLock = await (navigator as any).wakeLock.request('screen');
      log.info('Screen Wake Lock acquired — display will stay on');

      this._wakeLock.addEventListener('release', () => {
        log.debug('Screen Wake Lock released');
        this._wakeLock = null;
      });
    } catch (error: any) {
      log.warn('Wake Lock request failed:', error?.message);
    }
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
      LogReporter = statsModule.LogReporter;
      formatLogs = statsModule.formatLogs;
      DisplaySettings = displaySettingsModule.DisplaySettings;

      this.xmds = new XmdsClient(config);

      // Initialize stats collector
      this.statsCollector = new StatsCollector();
      await this.statsCollector.init();
      log.info('Stats collector initialized');

      // Initialize log reporter for CMS log submission
      this.logReporter = new LogReporter();
      await this.logReporter.init();
      log.info('Log reporter initialized');

      // Bridge logger output to LogReporter for CMS submission
      registerLogSink(({ level, name, args }: { level: string; name: string; args: any[] }) => {
        if (!this.logReporter) return;
        const message = args.map((a: any) => typeof a === 'string' ? a : JSON.stringify(a)).join(' ');
        this.logReporter.log(level, `[${name}] ${message}`, 'PLAYER').catch(() => {});
      });

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

    this.core.on('offline-mode', (isOffline: boolean) => {
      if (isOffline) {
        this.updateStatus('Offline mode — using cached content');
        this.showOfflineIndicator();
      } else {
        this.updateStatus('Back online');
        this.removeOfflineIndicator();
      }
    });

    this.core.on('purge-request', async (purgeFiles: any[]) => {
      try {
        const result = await cacheProxy.deleteFiles(purgeFiles);
        log.info(`Purge complete: ${result.deleted}/${result.total} files deleted`);
      } catch (error) {
        log.warn('Purge failed:', error);
      }
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

      // Report fault to CMS (triggers dashboard alert)
      this.logReporter?.reportFault(
        'COLLECTION_FAILED',
        `Collection cycle failed: ${error?.message || error}`
      );
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

    // Overlay layout push from XMR
    this.core.on('overlay-layout-request', async (layoutId: number) => {
      log.info('Overlay layout requested:', layoutId);
      // Re-use existing overlay rendering (schedule-driven overlays already work)
      // Just need to prepare and render the overlay layout
      await this.prepareAndRenderLayout(layoutId);
    });

    // Revert to schedule (undo XMR layout override)
    this.core.on('revert-to-schedule', () => {
      log.info('Reverting to scheduled content');
      this.updateStatus('Reverting to schedule...');
    });

    // Purge all cache
    this.core.on('purge-all-request', async () => {
      log.info('Purging all cached content...');
      this.updateStatus('Purging cache...');
      try {
        // Delete all caches
        const cacheNames = await caches.keys();
        await Promise.all(cacheNames.map(name => caches.delete(name)));
        log.info(`Purged ${cacheNames.length} caches`);

        // Re-initialize cache after purge
        await cacheManager.init();
      } catch (error) {
        log.error('Cache purge failed:', error);
      }
    });

    // Command execution result
    this.core.on('command-result', (result: any) => {
      log.info('Command result:', result);
      if (!result.success) {
        this.logReporter?.reportFault(
          'COMMAND_FAILED',
          `Command ${result.code} failed: ${result.reason || 'unknown'}`
        );
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
        // Start periodic screenshots once we have settings (only first time)
        if (!this._screenshotInterval) {
          this.startScreenshotInterval();
        }
      });
    }

    // Stats submission
    this.core.on('submit-stats-request', async () => {
      await this.submitStats();
    });

    // Log submission to CMS
    this.core.on('submit-logs-request', async () => {
      await this.submitLogs();
    });

    // Screenshot capture (triggered by XMR or periodic interval)
    this.core.on('screenshot-request', async () => {
      await this.captureAndSubmitScreenshot();
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
   * Setup Interactive Control handler (receives messages from SW for widget IC requests)
   * IC library in widget iframes makes XHR to /player/pwa/ic/*, SW forwards here.
   */
  private setupInteractiveControl() {
    navigator.serviceWorker?.addEventListener('message', (event: any) => {
      if (event.data?.type !== 'INTERACTIVE_CONTROL') return;

      const { method, path, search, body } = event.data;
      const port = event.ports?.[0];
      if (!port) return;

      const response = this.handleInteractiveControl(method, path, search, body);
      port.postMessage(response);
    });
  }

  /**
   * Handle an Interactive Control request from a widget
   */
  private handleInteractiveControl(method: string, path: string, search: string, body: string | null): any {
    log.debug('IC request:', method, path, search);

    switch (path) {
      case '/info':
        return {
          status: 200,
          body: JSON.stringify({
            hardwareKey: config.hardwareKey,
            displayName: config.displayName,
            playerType: 'pwa',
            currentLayoutId: this.core.getCurrentLayoutId()
          })
        };

      case '/trigger': {
        let data: any = {};
        try { data = body ? JSON.parse(body) : {}; } catch (_) {}
        // Forward to renderer for layout-level actions (widget navigation)
        this.renderer.emit('interactiveTrigger', {
          targetId: data.id,
          triggerCode: data.trigger
        });
        // Forward to core for schedule-level actions (layout navigation)
        if (data.trigger) {
          this.core.handleTrigger(data.trigger);
        }
        return { status: 200, body: 'OK' };
      }

      case '/duration/expire': {
        let data: any = {};
        try { data = body ? JSON.parse(body) : {}; } catch (_) {}
        log.info('IC: Widget duration expire requested for', data.id);
        this.renderer.emit('widgetExpire', { widgetId: data.id });
        return { status: 200, body: 'OK' };
      }

      case '/duration/extend': {
        let data: any = {};
        try { data = body ? JSON.parse(body) : {}; } catch (_) {}
        log.info('IC: Widget duration extend by', data.duration, 'for', data.id);
        this.renderer.emit('widgetExtendDuration', {
          widgetId: data.id,
          duration: parseInt(data.duration)
        });
        return { status: 200, body: 'OK' };
      }

      case '/duration/set': {
        let data: any = {};
        try { data = body ? JSON.parse(body) : {}; } catch (_) {}
        log.info('IC: Widget duration set to', data.duration, 'for', data.id);
        this.renderer.emit('widgetSetDuration', {
          widgetId: data.id,
          duration: parseInt(data.duration)
        });
        return { status: 200, body: 'OK' };
      }

      case '/fault': {
        let data: any = {};
        try { data = body ? JSON.parse(body) : {}; } catch (_) {}
        this.logReporter?.reportFault(
          data.code || 'WIDGET_FAULT',
          data.reason || 'Widget reported fault'
        );
        return { status: 200, body: 'OK' };
      }

      case '/realtime': {
        const params = new URLSearchParams(search);
        const dataKey = params.get('dataKey');
        log.debug('IC: Realtime data request for key:', dataKey);

        if (!dataKey) {
          return { status: 400, body: JSON.stringify({ error: 'Missing dataKey parameter' }) };
        }

        const dcManager = this.core.getDataConnectorManager();
        const connectorData = dcManager.getData(dataKey);

        if (connectorData === null) {
          return { status: 404, body: JSON.stringify({ error: `No data available for key: ${dataKey}` }) };
        }

        // Return data as JSON (stringify if it's an object, pass through if already a string)
        const responseBody = typeof connectorData === 'string' ? connectorData : JSON.stringify(connectorData);
        return { status: 200, body: responseBody };
      }

      default:
        return { status: 404, body: JSON.stringify({ error: 'Unknown IC route' }) };
    }
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

      // Clear current layout to allow replay/advance
      this.core.clearCurrentLayout();

      // If a new layout is already pending download, don't advance
      // (avoids redundant XMDS calls and duplicate download requests)
      const pending = this.core.getPendingLayouts();
      if (pending.length > 0) {
        log.info(`Layout ${pending[0]} pending download, skipping advance`);
        return;
      }

      // Advance to the next layout in the schedule (round-robin cycling)
      // This avoids a full collect() cycle — just picks the next layout and renders it.
      // Periodic collect() cycles still run on the collection interval to sync with CMS.
      log.info('Layout cycle completed, advancing to next layout...');
      this.core.advanceToNextLayout();
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

      // Report fault to CMS (triggers dashboard alert)
      this.logReporter?.reportFault(
        error.type || 'RENDERER_ERROR',
        `Renderer error: ${error.message || error.type} (layout ${error.layoutId || 'unknown'})`
      );
    });

    // Handle interactive actions from touch/click and keyboard triggers
    this.renderer.on('action-trigger', (data: any) => {
      const { actionType, triggerCode, layoutCode, targetId, commandCode } = data;
      log.info('Action trigger:', actionType, data);

      switch (actionType) {
        case 'navLayout':
        case 'navigateToLayout':
          if (triggerCode) {
            this.core.handleTrigger(triggerCode);
          } else if (layoutCode) {
            this.core.changeLayout(layoutCode);
          }
          break;

        case 'navWidget':
        case 'navigateToWidget':
          if (triggerCode) {
            this.core.handleTrigger(triggerCode);
          } else if (targetId) {
            this.renderer.navigateToWidget(targetId);
          }
          break;

        case 'command':
          if (commandCode) {
            this.core.executeCommand(commandCode);
          }
          break;

        default:
          log.warn('Unknown action type:', actionType);
      }
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
        // Tell SW to prioritize this layout's media over other downloads.
        // This moves pending media to the front of the queue and avoids
        // competing for bandwidth with media needed by other layouts.
        for (const mediaId of requiredMedia) {
          cacheProxy.prioritizeDownload('media', String(mediaId));
        }

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

    } catch (error: any) {
      log.error('Failed to prepare layout:', layoutId, error);
      this.updateStatus(`Failed to load layout ${layoutId}`, 'error');

      // Report fault to CMS (triggers dashboard alert)
      this.logReporter?.reportFault(
        'LAYOUT_LOAD_FAILED',
        `Failed to prepare layout ${layoutId}: ${error?.message || error}`
      );
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

    // Include background image file ID from layout element
    const layoutEl = doc.querySelector('layout');
    const bgFileId = layoutEl?.getAttribute('background');
    if (bgFileId) {
      const parsed = parseInt(bgFileId, 10);
      if (!isNaN(parsed) && !mediaIds.includes(parsed)) {
        mediaIds.push(parsed);
      }
    }

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
      // Use aggregation level from CMS settings if available
      const aggregationLevel = this.displaySettings?.getSetting('aggregationLevel') || 'Individual';
      const stats = aggregationLevel === 'Aggregate'
        ? await this.statsCollector.getAggregatedStatsForSubmission(50)
        : await this.statsCollector.getStatsForSubmission(50);

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
   * Submit player logs to CMS for remote debugging
   */
  private async submitLogs() {
    if (!this.logReporter) return;

    try {
      const logs = await this.logReporter.getLogsForSubmission(100);

      if (logs.length === 0) {
        log.debug('No logs to submit');
        return;
      }

      log.info(`Submitting ${logs.length} logs to CMS...`);

      const logXml = formatLogs(logs);
      const success = await this.xmds.submitLog(logXml);

      if (success) {
        log.info('Logs submitted successfully');
        await this.logReporter.clearSubmittedLogs(logs);
      } else {
        log.warn('Log submission failed (CMS returned false)');
      }
    } catch (error) {
      log.error('Failed to submit logs:', error);
    }
  }

  /**
   * Capture screenshot using html2canvas and submit to CMS
   */
  private async captureAndSubmitScreenshot() {
    try {
      const html2canvas = (await import('html2canvas')).default;
      const container = document.getElementById('player-container');
      if (!container) {
        log.warn('No player container for screenshot');
        return;
      }

      const scale = 0.5;
      const cw = container.clientWidth;
      const ch = container.clientHeight;

      // Create master canvas at scaled resolution
      const master = document.createElement('canvas');
      master.width = Math.round(cw * scale);
      master.height = Math.round(ch * scale);
      const ctx = master.getContext('2d')!;

      // Fill with layout background colour
      ctx.fillStyle = container.style.backgroundColor || '#000';
      ctx.fillRect(0, 0, master.width, master.height);

      // Draw background image if set
      const bgUrl = container.style.backgroundImage?.match(/url\("?(.+?)"?\)/)?.[1];
      if (bgUrl) {
        try {
          const img = new Image();
          img.crossOrigin = 'anonymous';
          await new Promise<void>((resolve, reject) => {
            img.onload = () => resolve();
            img.onerror = reject;
            img.src = bgUrl;
          });
          ctx.drawImage(img, 0, 0, master.width, master.height);
        } catch (_) { /* background image failed, continue */ }
      }

      // Capture each region's iframe content and composite onto master canvas
      const regionEls = container.querySelectorAll('.renderer-lite-region');
      for (const regionEl of regionEls) {
        const rect = (regionEl as HTMLElement).getBoundingClientRect();
        const containerRect = container.getBoundingClientRect();
        const x = (rect.left - containerRect.left) * scale;
        const y = (rect.top - containerRect.top) * scale;
        const w = rect.width * scale;
        const h = rect.height * scale;

        // Find visible widget (iframe or media element) in this region
        const iframes = regionEl.querySelectorAll('iframe');
        for (const iframe of iframes) {
          if ((iframe as HTMLElement).style.visibility === 'hidden') continue;
          try {
            // Same-origin iframes (SW-cached widget HTML) can be captured
            const iframeDoc = (iframe as HTMLIFrameElement).contentDocument;
            if (iframeDoc?.body) {
              const iframeCanvas = await html2canvas(iframeDoc.body, {
                scale, useCORS: true, allowTaint: true, logging: false,
                width: rect.width, height: rect.height
              });
              ctx.drawImage(iframeCanvas, x, y, w, h);
            }
          } catch (_) {
            // Cross-origin iframe — can't capture, leave as background
          }
        }

        // Capture media elements (images, videos) directly
        const images = regionEl.querySelectorAll('img');
        for (const img of images) {
          if ((img as HTMLElement).style.visibility === 'hidden') continue;
          try {
            ctx.drawImage(img as HTMLImageElement, x, y, w, h);
          } catch (_) { /* tainted canvas, skip */ }
        }

        const videos = regionEl.querySelectorAll('video');
        for (const video of videos) {
          if ((video as HTMLElement).style.visibility === 'hidden') continue;
          try {
            ctx.drawImage(video as HTMLVideoElement, x, y, w, h);
          } catch (_) { /* tainted canvas, skip */ }
        }
      }

      const base64 = master.toDataURL('image/jpeg', 0.7).split(',')[1];
      const success = await this.xmds.submitScreenShot(base64);
      if (success) {
        log.info('Screenshot submitted successfully');
      } else {
        log.warn('Screenshot submission failed');
      }
    } catch (error) {
      log.error('Failed to capture screenshot:', error);
    }
  }

  /**
   * Start periodic screenshot submission
   */
  private startScreenshotInterval() {
    const intervalSecs = this.displaySettings?.getSetting('screenshotInterval') || 0;
    if (!intervalSecs || intervalSecs <= 0) return;

    const intervalMs = intervalSecs * 1000;
    log.info(`Starting periodic screenshots every ${intervalSecs}s`);
    this._screenshotInterval = setInterval(() => {
      this.captureAndSubmitScreenshot();
    }, intervalMs);
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

  private showOfflineIndicator() {
    if (document.getElementById('offline-indicator')) return;
    const el = document.createElement('div');
    el.id = 'offline-indicator';
    el.textContent = 'OFFLINE';
    el.style.cssText = 'position:fixed;top:8px;right:8px;background:rgba(255,60,60,0.85);color:#fff;padding:4px 12px;border-radius:4px;font-size:12px;font-weight:bold;z-index:99999;pointer-events:none;';
    document.body.appendChild(el);
  }

  private removeOfflineIndicator() {
    document.getElementById('offline-indicator')?.remove();
  }

  /**
   * Cleanup
   */
  cleanup() {
    this.core.cleanup();
    this.renderer.cleanup();

    if (this._screenshotInterval) {
      clearInterval(this._screenshotInterval);
      this._screenshotInterval = null;
    }

    if (this._wakeLock) {
      this._wakeLock.release();
      this._wakeLock = null;
    }

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
