/**
 * Download Progress Overlay
 *
 * Shows download status on hover (configurable, debug feature)
 * Displays: active downloads, progress, chunk status, queue info
 */

export interface DownloadOverlayConfig {
  enabled: boolean;
  updateInterval?: number; // ms between updates
  autoHide?: boolean; // Hide when no downloads
}

export class DownloadOverlay {
  private overlay: HTMLElement | null = null;
  private config: DownloadOverlayConfig;
  private updateTimer: number | null = null;

  constructor(config: DownloadOverlayConfig) {
    this.config = {
      updateInterval: 1000,
      autoHide: true,
      ...config
    };

    if (this.config.enabled) {
      this.createOverlay();
      // Don't start polling yet — startUpdating() is called on demand
      // when downloads begin, and stops automatically when idle.
    }
  }

  private createOverlay() {
    this.overlay = document.createElement('div');
    this.overlay.id = 'download-overlay';
    // Style like top status messages - always visible, clean design
    this.overlay.style.cssText = `
      position: fixed;
      top: 1.5vh;
      left: 1.5vw;
      background: rgba(0, 0, 0, 0.88);
      color: #fff;
      font-family: system-ui, -apple-system, sans-serif;
      font-size: 1.4vw;
      padding: 1vh 1.2vw;
      border-radius: 0.4vw;
      border: 1px solid rgba(255, 255, 255, 0.25);
      z-index: 999999;
      max-width: 35vw;
      box-shadow: 0 0.3vh 1.2vw rgba(0, 0, 0, 0.5);
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
        } else {
          // No active downloads — stop polling to avoid SW message noise
          this.stopUpdating();
          if (this.config.autoHide) {
            this.overlay.style.display = 'none';
          }
        }
      } else {
        throw new Error('Progress request failed');
      }
    } catch (error) {
      // No SW controller or request failed — stop polling
      this.stopUpdating();
      if (this.config.autoHide && this.overlay) {
        this.overlay.style.display = 'none';
      }
    }
  }

  private renderStatus(progress: any): string {
    const downloads = progress || {};

    if (Object.keys(downloads).length === 0) {
      if (this.config.autoHide) {
        return ''; // Hide when no downloads
      }
      return `<div style="color: #6c6;">✓ No downloads</div>`;
    }

    const numDownloads = Object.keys(downloads).length;
    let html = `<div style="font-weight: 600; margin-bottom: 0.8vh; font-size: 1.4vw;">Downloads: ${numDownloads} active</div>`;

    for (const [url, progress] of Object.entries(downloads)) {
      const filename = this.extractFilename(url);
      const percent = Math.round((progress as any).percent || 0);
      const downloaded = this.formatBytes((progress as any).downloaded || 0);
      const total = this.formatBytes((progress as any).total || 0);

      html += `
        <div style="margin-bottom: 0.6vh; padding-bottom: 0.6vh; border-bottom: 1px solid rgba(255,255,255,0.1);">
          <div style="font-size: 1.2vw; margin-bottom: 0.2vh;">${filename}</div>
          <div style="background: rgba(255,255,255,0.1); height: 0.4vh; border-radius: 0.2vw; overflow: hidden;">
            <div style="width: ${percent}%; height: 100%; background: #4a9eff; transition: width 0.3s;"></div>
          </div>
          <div style="color: #999; font-size: 1.1vw; margin-top: 0.2vh;">
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

  /**
   * Start polling SW for download progress.
   * Safe to call multiple times — won't create duplicate timers.
   */
  public startUpdating() {
    if (this.updateTimer) return; // Already polling
    this.updateTimer = window.setInterval(() => {
      this.updateOverlay();
    }, this.config.updateInterval);
  }

  /**
   * Stop polling. Called automatically when no downloads are active.
   */
  private stopUpdating() {
    if (this.updateTimer) {
      clearInterval(this.updateTimer);
      this.updateTimer = null;
    }
  }

  public destroy() {
    this.stopUpdating();
    if (this.overlay) {
      this.overlay.remove();
      this.overlay = null;
    }
  }

  public setEnabled(enabled: boolean) {
    this.config.enabled = enabled;

    if (enabled && !this.overlay) {
      this.createOverlay();
      // Polling starts on demand via startUpdating()
    } else if (!enabled && this.overlay) {
      this.destroy();
    }
  }
}

/**
 * Get default configuration based on environment
 */
export function getDefaultOverlayConfig(): DownloadOverlayConfig {
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

  // Default: always enable overlay (autoHide hides it when no downloads active)
  return { enabled: true, autoHide: true };
}
