/**
 * Download Progress Overlay
 *
 * Shows download status on hover (configurable, debug feature)
 * Displays: active downloads, progress, chunk status, queue info
 */

export interface DownloadOverlayConfig {
  enabled: boolean;
  position?: 'top-right' | 'top-left' | 'bottom-right' | 'bottom-left';
  updateInterval?: number; // ms between updates
  autoHide?: boolean; // Hide when no downloads
}

export class DownloadOverlay {
  private overlay: HTMLElement | null = null;
  private config: DownloadOverlayConfig;
  private updateTimer: number | null = null;

  constructor(config: DownloadOverlayConfig, _cacheProxy?: any) {
    this.config = {
      position: 'bottom-right',
      updateInterval: 1000,
      autoHide: true,
      ...config
    };
    // cacheProxy not needed - using SW postMessage instead

    if (this.config.enabled) {
      this.createOverlay();
      this.startUpdating();
    }
  }

  private createOverlay() {
    this.overlay = document.createElement('div');
    this.overlay.id = 'download-overlay';
    // Style like top status messages - always visible, clean design
    this.overlay.style.cssText = `
      position: fixed;
      top: 50px;
      left: 10px;
      background: rgba(0, 0, 0, 0.85);
      color: #fff;
      font-family: system-ui, -apple-system, sans-serif;
      font-size: 12px;
      padding: 8px 12px;
      border-radius: 4px;
      border: 1px solid rgba(255, 255, 255, 0.2);
      z-index: 999999;
      max-width: 350px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    `;

    document.body.appendChild(this.overlay);
  }

  private async updateOverlay() {
    if (!this.overlay) return;

    try {
      // Get download progress from Service Worker via postMessage
      if (!navigator.serviceWorker?.controller) {
        throw new Error('No SW controller');
      }

      // Request progress from SW
      const mc = new MessageChannel();
      const progressPromise = new Promise((resolve) => {
        mc.port1.onmessage = (event) => resolve(event.data);
        setTimeout(() => resolve({ success: false }), 500); // Timeout
      });

      navigator.serviceWorker.controller.postMessage(
        { type: 'GET_DOWNLOAD_PROGRESS' },
        [mc.port2]
      );

      const result: any = await progressPromise;

      if (result.success) {
        const html = this.renderStatus(result.progress);

        if (html) {
          this.overlay.innerHTML = html;
          this.overlay.style.display = 'block';
        } else if (this.config.autoHide) {
          this.overlay.style.display = 'none';
        }
      } else {
        throw new Error('Progress request failed');
      }
    } catch (error) {
      // Fallback: Show basic info
      this.overlay.innerHTML = `
        <div style="color: #f80;">
          <strong>Download Status</strong><br>
          Service Worker not ready<br>
          <small>Or no downloads active</small>
        </div>
      `;
    }
  }

  private renderStatus(progress: any): string {
    const downloads = progress || {};

    if (!downloads || Object.keys(downloads).length === 0) {
      if (this.config.autoHide) {
        return ''; // Hide when no downloads
      }
      return `<div style="color: #6c6;">✓ No downloads</div>`;
    }

    const numDownloads = Object.keys(downloads).length;
    let html = `<div style="font-weight: 600; margin-bottom: 6px;">Downloads: ${numDownloads} active</div>`;

    for (const [url, progress] of Object.entries(downloads)) {
      const filename = this.extractFilename(url);
      const percent = Math.round((progress as any).percent || 0);
      const downloaded = this.formatBytes((progress as any).downloaded || 0);
      const total = this.formatBytes((progress as any).total || 0);

      html += `
        <div style="margin-bottom: 6px; padding-bottom: 6px; border-bottom: 1px solid rgba(255,255,255,0.1);">
          <div style="font-size: 11px; margin-bottom: 2px;">${filename}</div>
          <div style="background: rgba(255,255,255,0.1); height: 4px; border-radius: 2px; overflow: hidden;">
            <div style="width: ${percent}%; height: 100%; background: #4a9eff; transition: width 0.3s;"></div>
          </div>
          <div style="color: #999; font-size: 10px; margin-top: 2px;">
            ${percent}% · ${downloaded} / ${total}
          </div>
        </div>
      `;
    }

    return html;
  }

  private extractFilename(url: string): string {
    try {
      const urlObj = new URL(url);
      const filename = urlObj.searchParams.get('file') || url.split('/').pop() || 'unknown';
      return filename.length > 30 ? filename.substring(0, 27) + '...' : filename;
    } catch {
      return 'unknown';
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    const kb = bytes / 1024;
    if (kb < 1024) return `${kb.toFixed(1)} KB`;
    const mb = kb / 1024;
    if (mb < 1024) return `${mb.toFixed(1)} MB`;
    return `${(mb / 1024).toFixed(1)} GB`;
  }

  private startUpdating() {
    this.updateTimer = window.setInterval(() => {
      this.updateOverlay();
    }, this.config.updateInterval);
  }

  public destroy() {
    if (this.updateTimer) {
      clearInterval(this.updateTimer);
      this.updateTimer = null;
    }

    if (this.overlay) {
      this.overlay.remove();
      this.overlay = null;
    }
  }

  public setEnabled(enabled: boolean) {
    this.config.enabled = enabled;

    if (enabled && !this.overlay) {
      this.createOverlay();
      this.startUpdating();
    } else if (!enabled && this.overlay) {
      this.destroy();
    }
  }
}

/**
 * Get default configuration based on environment
 */
export function getDefaultOverlayConfig(): DownloadOverlayConfig {
  const hostname = window.location.hostname;
  const isDevelopment = hostname === 'localhost' || hostname === '127.0.0.1';

  // Check URL parameter override
  const urlParams = new URLSearchParams(window.location.search);
  const showDownloads = urlParams.get('showDownloads');

  if (showDownloads !== null) {
    return { enabled: showDownloads !== '0' && showDownloads !== 'false' };
  }

  // Check localStorage preference
  const savedPref = localStorage.getItem('xibo_show_download_overlay');
  if (savedPref !== null) {
    return { enabled: savedPref === 'true' };
  }

  // Default: enabled in development, disabled in production
  return { enabled: isDevelopment };
}
