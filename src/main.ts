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
import { TimelineOverlay, isTimelineVisible } from './timeline-overlay.js';

declare const __APP_VERSION__: string;
declare const __BUILD_DATE__: string;

const log = createLogger('PWA');

// Dynamic base path — same build serves /player/pwa/, /player/pwa-xmds/, /player/pwa-xlr/
const PLAYER_BASE = new URL('./', window.location.href).pathname.replace(/\/$/, '');

// Import core modules (will be loaded at runtime)
let cacheWidgetHtml: any;
let scheduleManager: any;
let config: any;
let RestClient: any;
let XmdsClient: any;
let XmrWrapper: any;
let cacheProxy: CacheProxy;
let StatsCollector: any;
let formatStats: any;
let LogReporter: any;
let formatLogs: any;
let DisplaySettings: any;
let SyncManager: any;

// SDK package versions (populated in loadCoreModules)
const sdkVersions: Record<string, string> = {};

class PwaPlayer {
  private renderer!: RendererLite;
  private core!: PlayerCore;
  private xmds!: any;
  private downloadOverlay: DownloadOverlay | null = null;
  private timelineOverlay: TimelineOverlay | null = null;
  private statsCollector: any = null;
  private logReporter: any = null;
  private displaySettings: any = null;
  private currentScheduleId: number = -1; // Track scheduleId for stats
  private scheduledLayoutIds: Set<number> = new Set(); // Layout IDs from current schedule
  private preparingLayoutId: number | null = null; // Guard against concurrent prepareAndRenderLayout calls
  private _screenshotInterval: any = null;
  private _screenshotMethod: 'electron' | 'native' | 'html2canvas' | null = null;
  private _screenshotInFlight = false; // Concurrency guard — one capture at a time
  private _html2canvasMod: any = null; // Pre-loaded module
  private _wakeLock: any = null; // Screen Wake Lock sentinel
  private syncManager: any = null; // Multi-display sync coordinator
  private _currentLayoutEnableStat: boolean = true; // enableStat from current layout XLF
  private _probeTimer: any = null; // Debounce timer for duration probing
  private _pendingFollowerStats: any[] | null = null; // In-flight stats delegated to lead
  private _pendingFollowerLogs: any[] | null = null; // In-flight logs delegated to lead

  async init() {
    log.info('Initializing player with RendererLite + PlayerCore...');

    // Load core modules
    await this.loadCoreModules();

    // Register Service Worker for offline-first kiosk mode
    if ('serviceWorker' in navigator) {
      try {
        const registration = await navigator.serviceWorker.register(`${PLAYER_BASE}/sw-pwa.js?v=${Date.now()}`, {
          scope: `${PLAYER_BASE}/`,
          type: 'module',
          updateViaCache: 'none'
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
          const streamingUrl = `${PLAYER_BASE}/cache/media/${fileId}`;
          log.debug(`Using streaming URL for media ${fileId}: ${streamingUrl}`);
          return streamingUrl;
        },

        // Provide widget HTML resolver
        getWidgetHtml: async (widget: any) => {
          const cacheKey = `${PLAYER_BASE}/cache/widget/${widget.layoutId}/${widget.regionId}/${widget.id}`;
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
    this.setupRemoteControls();

    // Set display location from CMS settings when registration completes
    this.core.on('register-complete', (regResult: any) => {
      const lat = parseFloat(regResult?.settings?.latitude);
      const lng = parseFloat(regResult?.settings?.longitude);
      if (lat && lng && !isNaN(lat) && !isNaN(lng)) {
        log.info(`Display location from CMS: ${lat.toFixed(4)}, ${lng.toFixed(4)}`);
        if (scheduleManager?.setLocation) {
          scheduleManager.setLocation(lat, lng);
        }
      } else if (this.core.requestGeoLocation) {
        // No CMS coordinates — try browser Geolocation API as fallback
        log.info('No CMS coordinates, requesting browser geolocation...');
        this.core.requestGeoLocation();
      }
    });

    // Setup UI
    this.updateConfigDisplay();

    // Online/offline event listeners for seamless offline mode
    window.addEventListener('online', () => {
      log.info('Browser reports online — triggering immediate collection');
      this.updateStatus('Back online, syncing...');
      this.removeOfflineIndicator();
      this.core.collectNow().catch((error: any) => {
        log.error('Failed to collect after coming online:', error);
      });
    });
    window.addEventListener('offline', () => {
      log.warn('Browser reports offline — continuing playback with cached data');
      this.updateStatus('Offline mode — using cached content');
      this.showOfflineIndicator();
    });

    // Initialize download progress overlay (configurable debug feature)
    const overlayConfig = getDefaultOverlayConfig();
    if (overlayConfig.enabled) {
      this.downloadOverlay = new DownloadOverlay(overlayConfig);
      log.info('Download overlay enabled (hover bottom-right corner)');
    }

    // Timeline overlay — created on first T key press (or if previously visible)
    if (isTimelineVisible()) {
      this.timelineOverlay = new TimelineOverlay(true, (layoutId) => this.skipToLayout(layoutId));
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
      // @ts-ignore
      const coreModule = await import('@xiboplayer/core');
      // @ts-ignore
      const rendererModule = await import('@xiboplayer/renderer');
      // @ts-ignore
      const syncModule = await import('@xiboplayer/sync');

      cacheWidgetHtml = cacheModule.cacheWidgetHtml;
      SyncManager = syncModule.SyncManager;
      scheduleManager = scheduleModule.scheduleManager;
      config = configModule.config;
      RestClient = xmdsModule.RestClient;
      XmdsClient = xmdsModule.XmdsClient;
      XmrWrapper = xmrModule.XmrWrapper;
      StatsCollector = statsModule.StatsCollector;
      formatStats = statsModule.formatStats;
      LogReporter = statsModule.LogReporter;
      formatLogs = statsModule.formatLogs;
      DisplaySettings = displaySettingsModule.DisplaySettings;

      // Capture SDK package versions
      sdkVersions.core = coreModule.VERSION || '?';
      sdkVersions.cache = cacheModule.VERSION || '?';
      sdkVersions.renderer = rendererModule.VERSION || '?';
      sdkVersions.schedule = scheduleModule.VERSION || '?';
      sdkVersions.xmds = xmdsModule.VERSION || '?';
      sdkVersions.xmr = xmrModule.VERSION || '?';
      sdkVersions.utils = configModule.VERSION || '?';
      sdkVersions.stats = statsModule.VERSION || '?';
      sdkVersions.settings = displaySettingsModule.VERSION || '?';

      // Get MAC address from Electron if available (for WOL support)
      if ((window as any).electronAPI?.getSystemInfo) {
        try {
          const sysInfo = await (window as any).electronAPI.getSystemInfo();
          if (sysInfo.macAddress) {
            config.macAddress = sysInfo.macAddress;
          }
        } catch (_) { /* pure PWA — no Electron API */ }
      }

      // Transport auto-detection:
      // - /player/pwa-xmds/ or ?transport=xmds → forced SOAP
      // - Otherwise → try REST, fall back to SOAP if unavailable
      const forceXmds = PLAYER_BASE.includes('pwa-xmds')
        || new URLSearchParams(window.location.search).get('transport') === 'xmds';

      if (forceXmds) {
        log.info('Using XMDS/SOAP transport (forced)');
        this.xmds = new XmdsClient(config);
      } else {
        // Try REST — registerDisplay() is always the first call anyway.
        // If the CMS lacks /pwa/ REST endpoints, fall back to SOAP.
        this.xmds = new RestClient(config);
        try {
          await this.xmds.registerDisplay();
          log.info('Using REST transport');
        } catch (e: any) {
          log.warn('REST unavailable, falling back to XMDS/SOAP:', e.message);
          this.xmds = new XmdsClient(config);
        }
      }

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

      // Log version and environment information for debugging
      const buildDate = typeof __BUILD_DATE__ !== 'undefined' ? __BUILD_DATE__ : '?';
      const appVersion = typeof __APP_VERSION__ !== 'undefined' ? __APP_VERSION__ : '?';
      log.info(`v${appVersion} built ${buildDate}`);
      const versionParts = Object.entries(sdkVersions).map(([k, v]) => `${k}=${v}`).join(' ');
      log.info(`SDK: ${versionParts}`);
      const isElectron = !!(window as any).electronAPI;
      const electronVersion = isElectron ? (navigator.userAgent.match(/Electron\/([\d.]+)/)?.[1] || '?') : null;
      const chromeVersion = navigator.userAgent.match(/Chrome\/([\d.]+)/)?.[1] || '?';
      const platform = isElectron ? `Electron ${electronVersion} / Chrome ${chromeVersion}` : `Chrome ${chromeVersion}`;
      log.info(`Env: PWA v${appVersion} | ${platform} | ${navigator.platform} | ${screen.width}x${screen.height}`);

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

    // Multi-display sync: create SyncManager when CMS provides sync config
    this.core.on('sync-config', (syncConfig: any) => {
      if (this.syncManager) {
        this.syncManager.stop();
      }
      this.syncManager = new SyncManager({
        displayId: config.hardwareKey,
        syncConfig,
        onLayoutChange: async (layoutId: string) => {
          // Follower: lead requested a layout change — load it but don't show yet
          log.info(`[Sync] Loading layout ${layoutId} (waiting for show signal)`);
          await this.prepareAndRenderLayout(parseInt(layoutId, 10));
          // Report ready to lead
          this.syncManager?.reportReady(layoutId);
        },
        onLayoutShow: (layoutId: string) => {
          // Lead/Follower: show the layout now (already rendered by prepareAndRenderLayout)
          log.info(`[Sync] Show signal for layout ${layoutId}`);
        },
        onVideoStart: (layoutId: string, regionId: string) => {
          // Resume paused video in the specified region
          log.info(`[Sync] Video start: layout ${layoutId} region ${regionId}`);
          this.renderer.resumeRegionMedia?.(regionId);
        },
        // Lead: follower delegated stats — submit on their behalf
        onStatsReport: async (followerId: string, statsXml: string, ack: () => void) => {
          log.info(`[Sync] Submitting stats for follower ${followerId}`);
          try {
            const success = await this.xmds.submitStats(statsXml, followerId);
            if (success) ack();
          } catch (err: any) {
            log.warn(`[Sync] Stats submission failed for follower ${followerId}:`, err);
          }
        },
        // Lead: follower delegated logs — submit on their behalf
        onLogsReport: async (followerId: string, logsXml: string, ack: () => void) => {
          log.info(`[Sync] Submitting logs for follower ${followerId}`);
          try {
            const success = await this.xmds.submitLog(logsXml, followerId);
            if (success) ack();
          } catch (err: any) {
            log.warn(`[Sync] Log submission failed for follower ${followerId}:`, err);
          }
        },
        // Follower: lead confirmed our stats were submitted
        onStatsAck: async (_displayId: string) => {
          log.info('[Sync] Lead confirmed stats submission');
          if (this._pendingFollowerStats && this.statsCollector) {
            await this.statsCollector.clearSubmittedStats(this._pendingFollowerStats);
            this._pendingFollowerStats = null;
          }
        },
        // Follower: lead confirmed our logs were submitted
        onLogsAck: async (_displayId: string) => {
          log.info('[Sync] Lead confirmed logs submission');
          if (this._pendingFollowerLogs && this.logReporter) {
            await this.logReporter.clearSubmittedLogs(this._pendingFollowerLogs);
            this._pendingFollowerLogs = null;
          }
        },
      });
      this.core.setSyncManager(this.syncManager);
      this.syncManager.start();
      log.info(`[Sync] SyncManager started as ${syncConfig.isLead ? 'LEAD' : 'FOLLOWER'}`);
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

    this.core.on('download-request', async (groupedFiles: any) => {
      // Platform handles the actual download via CacheProxy
      // Restart overlay polling while downloads are active
      this.downloadOverlay?.startUpdating();
      try {
        // groupedFiles is { layouts: [{ layoutId, mediaFiles }] }
        await cacheProxy.requestDownload(groupedFiles);
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

      // Selectively clear preloaded layouts not in the new schedule.
      // Keep warm entries whose layout ID is still scheduled — their DOM is still valid.
      // (The CMS schedule CRC changes every collection due to timestamps, even when
      // the actual layout list hasn't changed. Blindly clearing would destroy preloads.)
      if (this.renderer?.layoutPool) {
        const scheduledIds = new Set<number>();
        if (schedule.layouts) {
          for (const l of schedule.layouts) {
            const id = parseInt(String(l.file || l.id || l).replace('.xlf', ''), 10);
            if (id) scheduledIds.add(id);
          }
        }
        if (schedule.campaigns) {
          for (const c of schedule.campaigns) {
            if (c.layouts) {
              for (const l of c.layouts) {
                const id = parseInt(String(l.file || l.id || l).replace('.xlf', ''), 10);
                if (id) scheduledIds.add(id);
              }
            }
          }
        }
        const cleared = this.renderer.layoutPool.clearWarmNotIn(scheduledIds);
        if (cleared > 0) {
          log.info(`Cleared ${cleared} preloaded layout(s) no longer in schedule`);
        }
        this.scheduledLayoutIds = scheduledIds;
      }

      log.debug('Current scheduleId for stats:', this.currentScheduleId);
    });

    this.core.on('layout-prepare-request', async (layoutId: number) => {
      await this.prepareAndRenderLayout(layoutId);
    });

    this.core.on('no-layouts-scheduled', () => {
      this.updateStatus('No layouts scheduled');
    });

    this.core.on('collection-complete', () => {
      const layoutId = this.core.getCurrentLayoutId();
      if (layoutId) {
        this.updateStatus(`Playing layout ${layoutId}`);
      } else if (this.preparingLayoutId) {
        this.updateStatus(`Downloading layout ${this.preparingLayoutId}...`);
      }

      // Probe video durations for accurate timeline (metadata only, not full download)
      this.probeLayoutDurations().catch(err => {
        log.debug('Duration probe failed (non-blocking):', err);
      });
    });

    this.core.on('collection-error', (error: any) => {
      this.updateStatus(`Collection error: ${error}`, 'error');

      // Report fault to CMS (triggers dashboard alert)
      this.logReporter?.reportFault(
        'COLLECTION_FAILED',
        `Collection cycle failed: ${error?.message || error}`
      );
      this.submitFault('COLLECTION_FAILED', `Collection cycle failed: ${error?.message || error}`);
    });

    this.core.on('xmr-connected', (url: string) => {
      log.info('XMR connected:', url);
    });

    this.core.on('xmr-misconfigured', (info: { reason: string; url?: string; message: string }) => {
      log.warn(`XMR misconfigured (${info.reason}): ${info.message}`);
      log.warn(`XMR misconfigured: ${info.message}`);
    });

    // React to CMS log level changes — toggle download overlay at runtime
    this.core.on('log-level-changed', () => {
      const debugNow = isDebug();
      log.info(`Log level changed, debug=${debugNow}`);

      if (debugNow && !this.downloadOverlay) {
        this.downloadOverlay = new DownloadOverlay(getDefaultOverlayConfig());
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
        this.submitFault('COMMAND_FAILED', `Command ${result.code} failed: ${result.reason || 'unknown'}`);
      }
    });

    // Scheduled commands (#17) — execute commands whose scheduled time has arrived
    this.core.on('scheduled-command', (command: any) => {
      log.info(`Scheduled command: ${command.code}`);
      this.core.executeCommand(command.code);
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

    // Handle check-pending-layout events
    // Re-run prepareAndRenderLayout which checks XLF + actual media IDs correctly
    // (avoids the bug where setPendingLayout(id,[id]) treated layoutId as mediaId)
    this.core.on('check-pending-layout', async (layoutId: number) => {
      await this.prepareAndRenderLayout(layoutId);
    });

    // Timeline overlay — visualize upcoming schedule
    this.core.on('timeline-updated', (timeline: any[]) => {
      this.timelineOverlay?.update(timeline, this.core.getCurrentLayoutId());
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
   * Setup keyboard and presenter remote controls.
   * Handles arrow keys, page up/down, space for next/prev/pause,
   * and MediaSession API for multimedia keyboard keys.
   */
  private setupRemoteControls() {
    // Keep focus on main document so keyboard shortcuts work even with widget iframes.
    // Iframes steal focus — this pulls it back after a short delay so interactive
    // widgets still work momentarily but keyboard control returns to the player.
    window.addEventListener('blur', () => {
      setTimeout(() => window.focus(), 200);
    });

    // Forward keyboard events from widget iframes to the main document.
    // Iframes have their own document, so keydown on the parent never fires
    // when an iframe has focus. We observe new iframes and attach forwarders.
    const attachIframeKeyForwarder = (iframe: HTMLIFrameElement) => {
      const tryAttach = () => {
        try {
          const iframeDoc = iframe.contentDocument || iframe.contentWindow?.document;
          if (!iframeDoc) return;
          if ((iframe as any).__keyForwarderAttached) return;
          (iframe as any).__keyForwarderAttached = true;
          iframeDoc.addEventListener('keydown', (e: KeyboardEvent) => {
            // Re-dispatch on the main document so our handler fires
            const clone = new KeyboardEvent('keydown', {
              key: e.key, code: e.code, keyCode: e.keyCode,
              ctrlKey: e.ctrlKey, shiftKey: e.shiftKey, altKey: e.altKey, metaKey: e.metaKey,
              bubbles: true, cancelable: true,
            });
            if (document.dispatchEvent(clone)) return; // not prevented
            e.preventDefault();
          });
        } catch { /* cross-origin iframe, ignore */ }
      };
      iframe.addEventListener('load', tryAttach);
      tryAttach();
    };

    // Attach to existing and future iframes
    Array.from(document.querySelectorAll('iframe')).forEach(f => attachIframeKeyForwarder(f as HTMLIFrameElement));
    const iframeObserver = new MutationObserver((mutations) => {
      for (const m of mutations) {
        for (const node of m.addedNodes) {
          if (node instanceof HTMLIFrameElement) attachIframeKeyForwarder(node);
          if (node instanceof HTMLElement) {
            node.querySelectorAll('iframe').forEach(f => attachIframeKeyForwarder(f as HTMLIFrameElement));
          }
        }
      }
    });
    iframeObserver.observe(document.body, { childList: true, subtree: true });

    // Keyboard / presenter remote (clicker) controls
    document.addEventListener('keydown', (e: KeyboardEvent) => {
      switch (e.key) {
        case 't':
        case 'T':
          if (!this.timelineOverlay) {
            this.timelineOverlay = new TimelineOverlay(true, (layoutId) => this.skipToLayout(layoutId));
          }
          this.timelineOverlay.toggle();
          break;
        case 'd':
        case 'D':
          if (!this.downloadOverlay) {
            this.downloadOverlay = new DownloadOverlay({ enabled: true, autoHide: false });
          }
          this.downloadOverlay.toggle();
          break;
        case 'v':
        case 'V': {
          const videos = document.querySelectorAll('video');
          const show = videos.length > 0 && !videos[0].controls;
          videos.forEach(v => v.controls = show);
          break;
        }
        // Playback control: next/prev/pause
        case 'ArrowRight':
        case 'PageDown':
          log.info('[Remote] Next layout (keyboard)');
          this.core.advanceToNextLayout();
          e.preventDefault();
          break;
        case 'ArrowLeft':
        case 'PageUp':
          log.info('[Remote] Previous layout (keyboard)');
          this.core.advanceToPreviousLayout();
          e.preventDefault();
          break;
        case ' ':
          log.info('[Remote] Toggle pause (keyboard)');
          if (this.renderer.isPaused()) {
            this.renderer.resume();
          } else {
            this.renderer.pause();
          }
          e.preventDefault();
          break;
        case 'r':
        case 'R':
          if (this.core.isLayoutOverridden()) {
            log.info('[Remote] Revert to schedule (keyboard)');
            this.core.revertToSchedule();
          }
          break;
      }
    });

    // MediaSession API for multimedia keys (only fires when media is active)
    if ('mediaSession' in navigator) {
      navigator.mediaSession.setActionHandler('nexttrack', () => {
        log.info('[Remote] Next layout (MediaSession)');
        this.core.advanceToNextLayout();
      });
      navigator.mediaSession.setActionHandler('previoustrack', () => {
        log.info('[Remote] Previous layout (MediaSession)');
        this.core.advanceToPreviousLayout();
      });
      navigator.mediaSession.setActionHandler('pause', () => {
        log.info('[Remote] Pause (MediaSession)');
        this.renderer.pause();
      });
      navigator.mediaSession.setActionHandler('play', () => {
        log.info('[Remote] Resume (MediaSession)');
        this.renderer.resume();
      });
    }

    log.info('Remote controls initialized (keyboard + MediaSession)');
  }

  /**
   * Skip to a specific layout by ID (from timeline click or XMR command).
   * Uses changeLayout() which sets a layout override — press R to revert to schedule.
   */
  private skipToLayout(layoutId: number) {
    log.info(`Skipping to layout ${layoutId} (timeline click)`);
    this.core.changeLayout(layoutId);
  }

  private parseBody(body: string | null): any {
    try { return body ? JSON.parse(body) : {}; } catch (_) { return {}; }
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
        const data = this.parseBody(body);
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
        const data = this.parseBody(body);
        log.info('IC: Widget duration expire requested for', data.id);
        this.renderer.emit('widgetExpire', { widgetId: data.id });
        return { status: 200, body: 'OK' };
      }

      case '/duration/extend': {
        const data = this.parseBody(body);
        log.info('IC: Widget duration extend by', data.duration, 'for', data.id);
        this.renderer.emit('widgetExtendDuration', {
          widgetId: data.id,
          duration: parseInt(data.duration)
        });
        return { status: 200, body: 'OK' };
      }

      case '/duration/set': {
        const data = this.parseBody(body);
        log.info('IC: Widget duration set to', data.duration, 'for', data.id);
        this.renderer.emit('widgetSetDuration', {
          widgetId: data.id,
          duration: parseInt(data.duration)
        });
        return { status: 200, body: 'OK' };
      }

      case '/fault': {
        const data = this.parseBody(body);
        this.logReporter?.reportFault(
          data.code || 'WIDGET_FAULT',
          data.reason || 'Widget reported fault'
        );
        this.submitFault(data.code || 'WIDGET_FAULT', data.reason || 'Widget reported fault', {
          layoutId: data.layoutId,
          regionId: data.regionId,
          widgetId: data.widgetId
        });
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

        const responseBody = typeof connectorData === 'string' ? connectorData : JSON.stringify(connectorData);
        return { status: 200, body: responseBody };
      }

      case '/criteria': {
        // Return display properties/criteria that widgets can query
        // Used by widgets to adapt content based on display characteristics
        return {
          status: 200,
          body: JSON.stringify({
            displayId: config.displayId,
            hardwareKey: config.hardwareKey,
            displayName: config.displayName,
            width: window.innerWidth,
            height: window.innerHeight,
            latitude: config.latitude || null,
            longitude: config.longitude || null,
            playerType: 'pwa'
          })
        };
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

        // Debounced duration probe — run after downloads settle
        if (this._probeTimer) clearTimeout(this._probeTimer);
        this._probeTimer = setTimeout(() => {
          this._probeTimer = null;
          this.probeLayoutDurations().catch(() => {});
        }, 3000);
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

      // Store layout-level enableStat for use in layoutEnd
      this._currentLayoutEnableStat = _layout?.enableStat !== false;

      // Update timeline overlay highlight
      this.timelineOverlay?.update(null, layoutId);

      // Correct timeline duration if renderer discovered actual duration
      // (e.g., video loadedmetadata replaces the 60s estimate)
      if (_layout?.duration) {
        this.core.recordLayoutDuration(String(layoutId), _layout.duration);
      }

      // Track stats: start layout (only if enableStat is not disabled)
      if (this.statsCollector && this._currentLayoutEnableStat) {
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

      // Track stats: end layout (only if enableStat was not disabled)
      if (this.statsCollector && this._currentLayoutEnableStat) {
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

      // Track stats: start widget/media (only if enableStat is not disabled)
      if (this.statsCollector && mediaId && data.enableStat !== false) {
        this.statsCollector.startWidget(mediaId, layoutId, this.currentScheduleId).catch((err: any) => {
          log.error('Failed to start widget stat:', err);
        });
      }
    });

    this.renderer.on('widgetEnd', (data: any) => {
      const { widgetId, layoutId, mediaId } = data;
      log.debug('Widget ended:', data.type, widgetId, 'media:', mediaId);

      // Track stats: end widget/media (only if enableStat is not disabled)
      if (this.statsCollector && mediaId && data.enableStat !== false) {
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
      this.submitFault(error.type || 'RENDERER_ERROR', `Renderer error: ${error.message || error.type}`, {
        layoutId: error.layoutId,
        regionId: error.regionId,
        widgetId: error.widgetId
      });
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

        case 'previousWidget':
          this.renderer.previousWidget(data.source?.regionId);
          break;

        case 'nextWidget':
          this.renderer.nextWidget(data.source?.regionId);
          break;

        case 'command':
          if (commandCode) {
            this.core.executeCommand(commandCode);
          }
          break;

        default:
          log.warn('Unknown action type:', actionType);
      }

      // Record interaction event for proof of play (#19)
      if (this.statsCollector) {
        this.statsCollector.recordEvent('touch', this.core.getCurrentLayoutId(), data.targetId || null, this.currentScheduleId);
      }
    });

    // Widget duration webhooks (#16) — fire HTTP POST when widget duration expires
    this.renderer.on('widgetAction', (data: any) => {
      if (data.type === 'durationEnd' && data.url) {
        log.info(`Widget ${data.widgetId} duration ended, calling webhook: ${data.url}`);

        // Record webhook event for proof of play (#19)
        if (this.statsCollector) {
          this.statsCollector.recordEvent('webhook', data.layoutId, data.widgetId, this.currentScheduleId);
        }

        fetch(data.url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            widgetId: data.widgetId,
            layoutId: data.layoutId,
            regionId: data.regionId,
            event: 'durationEnd',
            timestamp: new Date().toISOString()
          })
        }).catch(err => log.warn('Webhook failed (non-critical):', err));
      }
    });

    // Correct timeline duration when video metadata reveals actual duration
    this.renderer.on('layoutDurationUpdated', (layoutId: number, duration: number) => {
      this.core.recordLayoutDuration(String(layoutId), duration);
    });

    // Handle next layout preload request from renderer
    // Fired at 75% of current layout duration to pre-build the next layout's DOM
    this.renderer.on('request-next-layout-preload', async () => {
      try {
        // Peek at the next layout without advancing the schedule index
        const next = this.core.peekNextLayout();
        if (!next) {
          log.debug('No next layout to preload (single layout schedule or same layout)');
          return;
        }

        const nextLayoutId = next.layoutId;

        // Skip if already preloaded
        if (this.renderer.layoutPool.has(nextLayoutId)) {
          log.debug(`Layout ${nextLayoutId} already in preload pool`);
          return;
        }

        log.info(`Preloading next layout ${nextLayoutId}...`);

        // Get XLF from cache
        const xlfBlob = await cacheProxy.getFile('layout', nextLayoutId);
        if (!xlfBlob) {
          log.debug(`Layout ${nextLayoutId} XLF not cached, skipping preload`);
          return;
        }

        const xlfXml = await xlfBlob.text();

        // Check if all required media is cached
        const { allMedia: requiredMedia, videoMedia: videoMediaIds } = this.getMediaIds(xlfXml);
        const allMediaCached = await this.checkAllMediaCached(requiredMedia, videoMediaIds);

        if (!allMediaCached) {
          log.debug(`Media not fully cached for layout ${nextLayoutId}, skipping preload`);
          return;
        }

        // Fetch widget HTML before preloading (same as prepareAndRenderLayout)
        await this.fetchWidgetHtml(xlfXml, nextLayoutId);

        // Pre-warm video chunks in SW BlobCache
        if (videoMediaIds.length > 0) {
          await cacheProxy.prewarmVideoChunks(videoMediaIds);
        }

        // Preload the layout into the renderer's pool
        const success = await this.renderer.preloadLayout(xlfXml, nextLayoutId);
        if (success) {
          log.info(`Layout ${nextLayoutId} preloaded successfully`);
        } else {
          log.warn(`Layout ${nextLayoutId} preload failed (will fall back to normal render)`);
        }
      } catch (error) {
        log.warn('Layout preload failed (non-blocking):', error);
        // Non-blocking: preload failure is graceful, normal render path will be used
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
      const xlfBlob = await cacheProxy.getFile('layout', layoutId);
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
      const { allMedia: requiredMedia, videoMedia: videoMediaIds } = this.getMediaIds(xlfXml);
      const allMediaCached = await this.checkAllMediaCached(requiredMedia, videoMediaIds);

      if (!allMediaCached) {
        // Reorder download queue: current layout's media first, hold others.
        // All files (including all chunks) must complete before other layouts start.
        cacheProxy.prioritizeLayoutFiles(requiredMedia.map(String));

        log.info(`Waiting for media to finish downloading for layout ${layoutId}`);
        this.updateStatus(`Preparing layout ${layoutId}...`);
        this.core.setPendingLayout(layoutId, requiredMedia);
        return; // Keep playing current layout until media is ready
      }

      // Fetch widget HTML for all widgets in the layout
      await this.fetchWidgetHtml(xlfXml, layoutId);

      // Pre-warm video chunks in SW BlobCache (first + last chunks for moov atom)
      if (videoMediaIds.length > 0) {
        log.info(`Pre-warming ${videoMediaIds.length} video file(s) for layout ${layoutId}`);
        await cacheProxy.prewarmVideoChunks(videoMediaIds);
      }

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
      this.submitFault('LAYOUT_LOAD_FAILED', `Failed to prepare layout ${layoutId}: ${error?.message || error}`, {
        layoutId
      });
    } finally {
      this.preparingLayoutId = null;
    }
  }

  /**
   * Get all required media file IDs and video-specific IDs from layout XLF.
   * Single parse to avoid double DOMParser overhead on the same XML.
   */
  private getMediaIds(xlfXml: string): { allMedia: number[]; videoMedia: number[] } {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xlfXml, 'text/xml');
    const allMedia: number[] = [];
    const videoMedia: number[] = [];

    doc.querySelectorAll('media[fileId]').forEach(el => {
      const fileId = el.getAttribute('fileId');
      if (fileId) {
        const id = parseInt(fileId, 10);
        allMedia.push(id);
        if (el.getAttribute('type') === 'video') {
          videoMedia.push(id);
        }
      }
    });

    // Include background image file ID from layout element
    const bgFileId = doc.querySelector('layout')?.getAttribute('background');
    if (bgFileId) {
      const parsed = parseInt(bgFileId, 10);
      if (!isNaN(parsed) && !allMedia.includes(parsed)) {
        allMedia.push(parsed);
      }
    }

    return { allMedia, videoMedia };
  }

  /**
   * Check if all required media files are cached and ready
   */
  private async checkAllMediaCached(mediaIds: number[], videoMediaIds: number[] = []): Promise<boolean> {
    for (const mediaId of mediaIds) {
      try {
        // Use CacheProxy API - it delegates to SW's CacheManager.fileExists()
        const exists = await cacheProxy.hasFile('media', String(mediaId));

        if (!exists) {
          log.debug(`Media ${mediaId} not yet cached`);
          return false;
        }

        // File exists (either whole file or chunks) - now validate it
        // Check for chunked storage via metadata
        const cache = await caches.open('xibo-media-v1');
        const metadataResponse = await cache.match(`${PLAYER_BASE}/cache/media/${mediaId}/metadata`);

        if (metadataResponse) {
          {
            const metadataText = await metadataResponse.text();
            const metadata = JSON.parse(metadataText);
            const sizeMB = (metadata.totalSize / 1024 / 1024).toFixed(1);
            const isVideo = videoMediaIds.includes(mediaId);

            if (isVideo) {
              // Video: early playback — need chunk 0 (ftyp header) + last chunk (moov atom).
              // Download manager prioritizes these two chunks first (out-of-order download).
              // SW's Range handler retries up to 60s per chunk for middle ones still downloading.
              const chunk0 = await cache.match(`${PLAYER_BASE}/cache/media/${mediaId}/chunk-0`);
              if (!chunk0) {
                log.debug(`Media ${mediaId} video: chunk 0 not yet available`);
                return false;
              }
              const lastIdx = metadata.numChunks - 1;
              if (lastIdx > 0) {
                const lastChunk = await cache.match(`${PLAYER_BASE}/cache/media/${mediaId}/chunk-${lastIdx}`);
                if (!lastChunk) {
                  log.debug(`Media ${mediaId} video: last chunk (${lastIdx}) not yet available`);
                  return false;
                }
              }
              log.info(`Media ${mediaId} video ready for early playback (chunk 0 + ${lastIdx} of ${metadata.numChunks}, ${sizeMB} MB total)`);
            } else {
              // Non-video: require all chunks (last chunk present = all downloaded)
              const lastChunkKey = `${PLAYER_BASE}/cache/media/${mediaId}/chunk-${metadata.numChunks - 1}`;
              const lastChunk = await cache.match(lastChunkKey);
              if (!lastChunk) {
                log.debug(`Media ${mediaId} chunked but still downloading (chunk ${metadata.numChunks - 1} missing)`);
                return false;
              }
              log.debug(`Media ${mediaId} cached as chunks (${metadata.numChunks} x ${(metadata.chunkSize / 1024 / 1024).toFixed(0)} MB = ${sizeMB} MB total)`);
            }
            continue;
          }
        }

        // Validate cached whole file (detect corrupted entries)
        const cacheKey = `${PLAYER_BASE}/cache/media/${mediaId}`;
        const response = await cache.match(cacheKey);
        if (!response) continue; // Shouldn't happen — hasFile was true

        const contentType = response.headers.get('Content-Type') || '';
        const blob = await response.blob();

        // Check for bad cache
        if (contentType === 'text/plain' || blob.size < 100) {
          log.warn(`Media ${mediaId} corrupted (${contentType}, ${blob.size} bytes) - will re-download`);
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
   * Fetch widget HTML for all widgets in layout (parallel)
   */
  private async fetchWidgetHtml(xlfXml: string, layoutId: number) {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xlfXml, 'text/xml');

    const fetchPromises: Promise<void>[] = [];

    for (const regionEl of doc.querySelectorAll('region')) {
      const regionId = regionEl.getAttribute('id');

      for (const mediaEl of regionEl.querySelectorAll('media')) {
        const type = mediaEl.getAttribute('type');
        const widgetId = mediaEl.getAttribute('id');
        const render = mediaEl.getAttribute('render');

        // XLF render="html" means CMS provides pre-rendered HTML via getResource.
        // render="native" means player handles the media directly (video, image, audio).
        if (render === 'html') {
          const cacheKey = `${PLAYER_BASE}/cache/widget/${layoutId}/${regionId}/${widgetId}`;

          fetchPromises.push(
            (async () => {
              try {
                const cache = await caches.open('xibo-media-v1');
                const cachedResponse = await cache.match(cacheKey);

                let html: string;
                if (cachedResponse) {
                  html = await cachedResponse.text();
                  log.debug(`Found cached widget HTML for ${type} ${widgetId}`);
                } else {
                  html = await this.xmds.getResource(layoutId, regionId, widgetId);
                  log.debug(`Retrieved widget HTML for ${type} ${widgetId} from CMS`);
                }
                // Always process: injects <base> tag, rewrites CMS signed URLs
                // to local /cache/static/ paths, and fetches static resources.
                // cacheWidgetHtml is idempotent — already-rewritten URLs won't re-match.
                await cacheWidgetHtml(layoutId, regionId, widgetId, html);
                const processed = await cache.match(cacheKey);
                if (processed) html = await processed.text();

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
   * Probe video durations for all scheduled layouts.
   * Uses preload="metadata" — only fetches headers (~50KB), not the full video.
   * Feeds discovered durations into PlayerCore for accurate timeline calculation.
   */
  private async probeLayoutDurations() {
    if (this.scheduledLayoutIds.size === 0) return;

    for (const layoutId of this.scheduledLayoutIds) {

      try {
        const xlfBlob = await cacheProxy.getFile('layout', layoutId);
        if (!xlfBlob) continue;

        const xlfXml = await xlfBlob.text();
        const { videoMedia } = this.getMediaIds(xlfXml);
        if (videoMedia.length === 0) continue;

        // Parse XLF to find video widgets with duration=0 (use media length)
        const parser = new DOMParser();
        const doc = parser.parseFromString(xlfXml, 'text/xml');

        // Probe actual video durations, keyed by fileId
        const videoDurations = new Map<string, number>();
        for (const mediaEl of doc.querySelectorAll('media[type="video"]')) {
          const useDuration = mediaEl.getAttribute('useDuration');
          if (useDuration === '1') continue; // Has explicit CMS duration, skip

          const fileId = mediaEl.getAttribute('fileId');
          if (!fileId) continue;

          const exists = await cacheProxy.hasFile('media', fileId);
          if (!exists) continue;

          // Probe metadata only — does NOT download the full video
          const duration = await this.probeVideoDuration(`${PLAYER_BASE}/cache/media/${fileId}`);
          if (duration > 0) {
            videoDurations.set(fileId, duration);
          }
        }

        if (videoDurations.size === 0) continue;

        // Calculate full layout duration (max region, summing all widgets per region)
        // Same logic as renderer's updateLayoutDuration() — accounts for non-video
        // widgets (PDFs, images, tickers) alongside probed video durations.
        let maxRegionDuration = 0;
        for (const regionEl of doc.querySelectorAll('region')) {
          if (regionEl.getAttribute('type') === 'drawer') continue;
          let regionDuration = 0;

          for (const mediaEl of regionEl.querySelectorAll('media')) {
            const dur = parseInt(mediaEl.getAttribute('duration') || '0', 10);
            const useDur = parseInt(mediaEl.getAttribute('useDuration') || '1', 10);
            const fileId = mediaEl.getAttribute('fileId') || '';
            const probedDur = videoDurations.get(fileId);

            if (probedDur !== undefined) {
              // Video with probed duration
              regionDuration += probedDur;
            } else if (dur > 0 && useDur !== 0) {
              // Non-video widget with explicit duration
              regionDuration += dur;
            }
            // Looping widgets (useDuration=0, no probe) contribute 0
          }

          maxRegionDuration = Math.max(maxRegionDuration, regionDuration);
        }

        if (maxRegionDuration > 0) {
          this.core.recordLayoutDuration(String(layoutId), maxRegionDuration);
        }
      } catch (err) {
        log.debug(`Duration probe failed for layout ${layoutId}:`, err);
      }
    }
  }

  /**
   * Probe a single video's duration using metadata only.
   * Creates a temporary <video preload="metadata"> element, reads duration, destroys it.
   */
  private probeVideoDuration(url: string): Promise<number> {
    return new Promise((resolve) => {
      const video = document.createElement('video');
      video.preload = 'metadata';
      video.muted = true;

      const cleanup = () => {
        video.removeAttribute('src');
        video.load(); // Release resources
      };

      video.addEventListener('loadedmetadata', () => {
        const dur = Math.floor(video.duration);
        cleanup();
        resolve(dur);
      }, { once: true });

      video.addEventListener('error', () => {
        cleanup();
        resolve(0);
      }, { once: true });

      // Safety timeout — don't block forever
      setTimeout(() => {
        cleanup();
        resolve(0);
      }, 5000);

      video.src = url;
    });
  }

  /**
   * Update config display
   */
  private updateConfigDisplay() {
    const configEl = document.getElementById('config-info');
    if (configEl) {
      const version = typeof __APP_VERSION__ !== 'undefined' ? __APP_VERSION__ : '?';
      const buildDate = typeof __BUILD_DATE__ !== 'undefined' ? __BUILD_DATE__.replace('T', ' ').replace(/\.\d+Z$/, '') : '';
      const versionStr = buildDate ? `v${version} (${buildDate})` : `v${version}`;
      configEl.textContent = `${versionStr} | CMS: ${config.cmsAddress} | Display: ${config.displayName || 'Unknown'} | HW: ${config.hardwareKey}`;
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

    // Guard: don't start a new delegation while one is in-flight
    if (this._pendingFollowerStats !== null) {
      log.debug('Stats delegation in-flight, skipping');
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

      // Format stats as XML
      const statsXml = formatStats(stats);

      // Follower with live lead: delegate stats via BroadcastChannel
      if (this.syncManager && !this.syncManager.isLead && this._syncLeadAlive()) {
        log.info(`[Sync] Delegating ${stats.length} stats to lead`);
        this._pendingFollowerStats = stats;
        this.syncManager.reportStats(statsXml);
        return;
      }

      // Lead, standalone, or lead-dead follower: submit directly
      if (this.syncManager && !this.syncManager.isLead) {
        log.warn('[Sync] Lead not alive, submitting stats directly');
      }

      log.info(`Submitting ${stats.length} proof of play stats...`);

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

    // Guard: don't start a new delegation while one is in-flight
    if (this._pendingFollowerLogs !== null) {
      log.debug('Logs delegation in-flight, skipping');
      return;
    }

    try {
      const logs = await this.logReporter.getLogsForSubmission();

      if (logs.length === 0) {
        log.debug('No logs to submit');
        return;
      }

      const logXml = formatLogs(logs);

      // Follower with live lead: delegate logs via BroadcastChannel
      if (this.syncManager && !this.syncManager.isLead && this._syncLeadAlive()) {
        log.info(`[Sync] Delegating ${logs.length} logs to lead`);
        this._pendingFollowerLogs = logs;
        this.syncManager.reportLogs(logXml);
        return;
      }

      // Lead, standalone, or lead-dead follower: submit directly
      if (this.syncManager && !this.syncManager.isLead) {
        log.warn('[Sync] Lead not alive, submitting logs directly');
      }

      log.info(`Submitting ${logs.length} logs to CMS...`);

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
   * Submit a fault report to CMS for the player_faults dashboard.
   * Runs alongside logReporter.reportFault() which feeds the log dashboard.
   */
  private submitFault(code: string, reason: string, details?: { layoutId?: number; regionId?: string; widgetId?: string }) {
    if (!this.xmds) return;

    const fault = JSON.stringify([{
      code,
      reason,
      date: new Date().toISOString().replace('T', ' ').substring(0, 19),
      ...details
    }]);

    this.xmds.reportFaults(fault).catch((err: any) => {
      log.debug('reportFaults failed (non-critical):', err);
    });
  }

  /**
   * Capture screenshot and submit to CMS.
   *
   * Strategy (best available, tried in order):
   *  0. Electron IPC — webContents.capturePage() via preload bridge.
   *     Pixel-perfect, captures video/WebGL/composited layers, zero DOM cost.
   *     Only available when running inside the Electron shell.
   *  1. getDisplayMedia() — native pixel capture, works on Chrome with
   *     --auto-select-desktop-capture-source flag (kiosk). Pixel-perfect,
   *     includes video, composited layers, everything the GPU renders.
   *  2. html2canvas — fallback for Firefox or Chrome without the flag.
   *     Re-renders the DOM to canvas; needs a video overlay workaround
   *     because html2canvas can't read <video> pixels.
   *
   * The first successful method is cached for subsequent calls.
   */
  private async captureAndSubmitScreenshot() {
    // Concurrency guard — skip if a capture is already in flight
    if (this._screenshotInFlight) {
      log.debug('Screenshot capture already in progress, skipping');
      return;
    }
    this._screenshotInFlight = true;

    try {
      let base64: string;

      // Electron path: use native webContents.capturePage() via IPC
      if (this._screenshotMethod === 'electron' ||
          (this._screenshotMethod === null && (window as any).electronAPI?.captureScreenshot)) {
        const electronResult = await (window as any).electronAPI.captureScreenshot();
        if (electronResult) {
          this._screenshotMethod = 'electron';
          base64 = electronResult;
        } else {
          // Electron capture returned null (window not yet painted).
          // Do NOT fall through to getDisplayMedia — it triggers a
          // permission dialog that blocks the whole UI.  Skip this
          // cycle; capturePage() will succeed on the next interval.
          log.debug('Electron screenshot not ready yet, will retry next interval');
          return;
        }
      } else {
        base64 = await this.captureWithBrowserMethods();
      }

      const success = await this.xmds.submitScreenShot(base64);
      if (success) {
        log.info(`Screenshot submitted (${this._screenshotMethod})`);
      } else {
        log.warn('Screenshot submission failed');
      }
    } catch (error) {
      log.error('Failed to capture screenshot:', error);
    } finally {
      this._screenshotInFlight = false;
    }
  }

  /**
   * Capture screenshot using browser-native methods (non-Electron path).
   * Uses html2canvas directly — getDisplayMedia() is not used because:
   * - On Wayland, it always shows an OS-level picker dialog (XDG Desktop Portal)
   * - Chrome's --auto-select-desktop-capture-source flag only works on X11
   * - html2canvas works without permissions and captures the layout faithfully
   */
  private async captureWithBrowserMethods(): Promise<string> {
    this._screenshotMethod = 'html2canvas';
    return this.captureHtml2Canvas();
  }

  /**
   * Capture screenshot by manually composing a canvas from visible elements.
   * - Images/video/canvas: drawn directly via ctx.drawImage() with object-fit emulation
   * - Iframes: content cloned into main document, rendered via html2canvas
   *   (html2canvas fails on cross-document elements, so we clone first)
   * - Background: read from #player-container computed style
   */
  private async captureHtml2Canvas(): Promise<string> {
    const canvas = document.createElement('canvas');
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    const ctx = canvas.getContext('2d')!;

    // Background: black (matches player default)
    ctx.fillStyle = '#000';
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    const container = document.getElementById('player-container');
    if (!container) {
      return canvas.toDataURL('image/jpeg', 0.8).split(',')[1];
    }

    // Draw container background (layout bgcolor + background image)
    const containerRect = container.getBoundingClientRect();
    const containerStyle = getComputedStyle(container);
    const bgColor = containerStyle.backgroundColor;
    if (bgColor && bgColor !== 'transparent' && bgColor !== 'rgba(0, 0, 0, 0)') {
      ctx.fillStyle = bgColor;
      ctx.fillRect(containerRect.left, containerRect.top, containerRect.width, containerRect.height);
    }
    // Background image (blob URL from layout XLF)
    const bgImage = containerStyle.backgroundImage;
    if (bgImage && bgImage !== 'none') {
      const urlMatch = bgImage.match(/url\(["']?(.*?)["']?\)/);
      if (urlMatch) {
        try {
          const bgImg = new Image();
          bgImg.crossOrigin = 'anonymous';
          await new Promise<void>((resolve) => {
            bgImg.onload = () => resolve();
            bgImg.onerror = () => resolve();
            setTimeout(() => resolve(), 2000);
            bgImg.src = urlMatch[1];
          });
          if (bgImg.naturalWidth) {
            ctx.drawImage(bgImg, containerRect.left, containerRect.top, containerRect.width, containerRect.height);
          }
        } catch (_) { /* skip failed background */ }
      }
    }

    // Ensure html2canvas is loaded (pre-loaded at init, fallback to lazy load)
    if (!this._html2canvasMod) {
      this._html2canvasMod = (await import('html2canvas')).default;
    }

    // Draw each visible widget element onto the canvas
    const elements = container.querySelectorAll('img, video, iframe, canvas');
    let drawn = 0;

    for (const el of elements) {
      const htmlEl = el as HTMLElement;
      if (htmlEl.style.visibility === 'hidden') continue;
      if (htmlEl.style.display === 'none') continue;
      const rect = el.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) continue;

      try {
        if (el instanceof HTMLImageElement) {
          if (!el.complete || !el.naturalWidth) continue;
          // Emulate object-fit: contain — draw at correct aspect ratio within bounding rect
          const fit = getComputedStyle(el).objectFit;
          if (fit === 'contain' && el.naturalWidth && el.naturalHeight) {
            const d = this.containedRect(el.naturalWidth, el.naturalHeight, rect);
            ctx.drawImage(el, d.x, d.y, d.w, d.h);
          } else {
            ctx.drawImage(el, rect.left, rect.top, rect.width, rect.height);
          }
          drawn++;
        } else if (el instanceof HTMLVideoElement) {
          if (el.readyState < 2) continue;
          // Emulate object-fit: contain — draw at correct aspect ratio within bounding rect
          const fit = getComputedStyle(el).objectFit;
          if (fit === 'contain' && el.videoWidth && el.videoHeight) {
            const d = this.containedRect(el.videoWidth, el.videoHeight, rect);
            ctx.drawImage(el, d.x, d.y, d.w, d.h);
          } else {
            ctx.drawImage(el, rect.left, rect.top, rect.width, rect.height);
          }
          drawn++;
        } else if (el instanceof HTMLCanvasElement) {
          ctx.drawImage(el, rect.left, rect.top, rect.width, rect.height);
          drawn++;
        } else if (el instanceof HTMLIFrameElement) {
          const iDoc = el.contentDocument;
          if (!iDoc?.body) continue;

          // html2canvas fails on cross-document elements (produces transparent canvas).
          // Clone the iframe's styles + content into the main document first,
          // then run html2canvas on the clone in the main document context.
          const captureDiv = document.createElement('div');
          captureDiv.style.cssText = `position:fixed;left:-9999px;top:0;width:${rect.width}px;height:${rect.height}px;overflow:hidden;`;

          // Clone stylesheets with absolute URLs (iframe base may differ)
          const linkPromises: Promise<void>[] = [];
          for (const styleEl of iDoc.querySelectorAll('style')) {
            captureDiv.appendChild(styleEl.cloneNode(true));
          }
          for (const linkEl of iDoc.querySelectorAll('link[rel="stylesheet"]')) {
            const newLink = document.createElement('link');
            newLink.rel = 'stylesheet';
            newLink.href = new URL(linkEl.getAttribute('href') || '', iDoc.baseURI).href;
            captureDiv.appendChild(newLink);
            // Wait for each stylesheet to load (or fail) instead of arbitrary delay
            linkPromises.push(new Promise<void>(resolve => {
              newLink.onload = () => resolve();
              newLink.onerror = () => resolve();
            }));
          }

          // Clone body content
          captureDiv.appendChild(iDoc.body.cloneNode(true));
          document.body.appendChild(captureDiv);

          // Collect natural dimensions from ORIGINAL iframe images (before html2canvas clones).
          // html2canvas doesn't support object-fit, so we fix sizing in onclone.
          const origImgs = iDoc.querySelectorAll('img');
          const imgNaturals = new Map<string, { nw: number; nh: number }>();
          origImgs.forEach((img, i) => {
            if (img.naturalWidth && img.naturalHeight) {
              imgNaturals.set(String(i), { nw: img.naturalWidth, nh: img.naturalHeight });
            }
          });

          // Wait for stylesheets to load (with 500ms safety timeout)
          if (linkPromises.length > 0) {
            await Promise.race([
              Promise.all(linkPromises),
              new Promise(r => setTimeout(r, 500)),
            ]);
          }

          const iframeCanvas = await this._html2canvasMod(captureDiv, {
            useCORS: true, allowTaint: true, logging: false,
            backgroundColor: null,
            width: rect.width, height: rect.height,
            onclone: (clonedDoc: Document) => {
              // Force visible — widget CSS animations reset to opacity:0 in cloned DOM
              const s = clonedDoc.createElement('style');
              s.textContent = '*, *::before, *::after { animation: none !important; transition: none !important; opacity: 1 !important; }';
              clonedDoc.head.appendChild(s);

              // Fix object-fit: contain — html2canvas stretches images, ignoring object-fit.
              // Replace with explicit sizing + centering so html2canvas draws correct proportions.
              const clonedImgs = clonedDoc.querySelectorAll('img');
              clonedImgs.forEach((cImg, i) => {
                const style = clonedDoc.defaultView?.getComputedStyle(cImg);
                if (!style || style.objectFit !== 'contain') return;
                const dims = imgNaturals.get(String(i));
                if (!dims) return;

                const cW = cImg.clientWidth || parseFloat(style.width) || 0;
                const cH = cImg.clientHeight || parseFloat(style.height) || 0;
                if (!cW || !cH) return;

                const srcAspect = dims.nw / dims.nh;
                const dstAspect = cW / cH;
                let drawW: number, drawH: number;
                if (srcAspect > dstAspect) {
                  drawW = cW;
                  drawH = cW / srcAspect;
                } else {
                  drawH = cH;
                  drawW = cH * srcAspect;
                }

                // Wrap in a flex container to center, remove object-fit
                const wrapper = clonedDoc.createElement('div');
                wrapper.style.cssText = `width:${cW}px;height:${cH}px;display:flex;align-items:center;justify-content:center;overflow:hidden;`;
                cImg.style.objectFit = 'fill';
                cImg.style.width = `${drawW}px`;
                cImg.style.height = `${drawH}px`;
                cImg.parentNode?.insertBefore(wrapper, cImg);
                wrapper.appendChild(cImg);
              });
            },
          });

          document.body.removeChild(captureDiv);
          ctx.drawImage(iframeCanvas, rect.left, rect.top, rect.width, rect.height);
          drawn++;
        }
      } catch (e: any) {
        log.warn('Screenshot: failed to draw element', el.tagName, e);
      }
    }

    log.debug(`Screenshot: composed ${drawn}/${elements.length} elements`);
    return canvas.toDataURL('image/jpeg', 0.8).split(',')[1];
  }

  /**
   * Calculate the destination rect for object-fit: contain.
   * Returns the centered rect that preserves the source aspect ratio
   * within the bounding rect (letterbox/pillarbox).
   */
  private containedRect(
    srcW: number, srcH: number, rect: DOMRect
  ): { x: number; y: number; w: number; h: number } {
    const srcAspect = srcW / srcH;
    const dstAspect = rect.width / rect.height;
    let w: number, h: number;
    if (srcAspect > dstAspect) {
      // Source is wider — fit to width, letterbox top/bottom
      w = rect.width;
      h = rect.width / srcAspect;
    } else {
      // Source is taller — fit to height, pillarbox left/right
      h = rect.height;
      w = rect.height * srcAspect;
    }
    return {
      x: rect.left + (rect.width - w) / 2,
      y: rect.top + (rect.height - h) / 2,
      w, h,
    };
  }

  /**
   * Start periodic screenshot submission
   */
  private startScreenshotInterval() {
    const intervalSecs = this.displaySettings?.getSetting('screenshotInterval') || 0;
    if (!intervalSecs || intervalSecs <= 0) return;

    // Pre-load html2canvas module so first capture is instant
    if (!this._html2canvasMod) {
      import('html2canvas').then(m => { this._html2canvasMod = m.default; });
    }

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
    this.timelineOverlay?.setOffline(true);
  }

  private removeOfflineIndicator() {
    this.timelineOverlay?.setOffline(false);
  }

  /**
   * Check if the sync lead is alive (for follower delegation).
   * Returns true if any peer with role 'lead' has been seen in the last 15s.
   */
  private _syncLeadAlive(): boolean {
    if (!this.syncManager) return false;
    for (const [, peer] of this.syncManager.followers) {
      if (peer.role === 'lead' && Date.now() - peer.lastSeen < 15000) {
        return true;
      }
    }
    return false;
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

    if (this.timelineOverlay) {
      this.timelineOverlay.destroy();
    }
  }
}

function startPlayer() {
  const player = new PwaPlayer();
  player.init().catch(error => {
    log.error('Failed to initialize:', error);
  });
  window.addEventListener('beforeunload', () => {
    player.cleanup();
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', startPlayer);
} else {
  startPlayer();
}
