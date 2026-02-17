/**
 * Timeline Overlay
 *
 * Toggleable debug overlay showing upcoming schedule timeline.
 * Displays: layout IDs, time ranges, durations, current layout highlight.
 * Positioned bottom-left (download overlay is top-left).
 */

interface TimelineEntry {
  layoutFile: string;
  startTime: Date;
  endTime: Date;
  duration: number;
  isDefault: boolean;
}

export class TimelineOverlay {
  private overlay: HTMLElement | null = null;
  private visible: boolean;
  private timeline: TimelineEntry[] = [];
  private currentLayoutId: number | null = null;
  private offline: boolean = false;

  constructor(visible = false) {
    this.visible = visible;
    this.createOverlay();
    if (!this.visible) {
      this.overlay!.style.display = 'none';
    }
  }

  private createOverlay() {
    this.overlay = document.createElement('div');
    this.overlay.id = 'timeline-overlay';
    this.overlay.style.cssText = `
      position: fixed;
      bottom: 1.5vh;
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
      pointer-events: auto;
    `;
    document.body.appendChild(this.overlay);
  }

  toggle() {
    this.visible = !this.visible;
    if (this.overlay) {
      this.overlay.style.display = this.visible ? 'block' : 'none';
    }
    // Re-render when becoming visible (render() skips while hidden)
    if (this.visible) {
      this.render();
    }
    // Persist preference
    localStorage.setItem('xibo_show_timeline_overlay', String(this.visible));
  }

  /**
   * Update the overlay with new timeline data and/or current layout highlight.
   * Pass timeline=null to keep existing timeline and only update the highlight.
   */
  setOffline(offline: boolean) {
    this.offline = offline;
    this.render();
  }

  update(timeline: TimelineEntry[] | null, currentLayoutId: number | null) {
    if (timeline !== null) {
      this.timeline = timeline;
    }
    if (currentLayoutId !== null) {
      this.currentLayoutId = currentLayoutId;
    }
    this.render();
  }

  private render() {
    if (!this.overlay || !this.visible) return;

    const now = new Date();

    // Filter: show current (endTime > now) + future entries only
    const entries = this.timeline.filter(e => e.endTime > now);

    if (entries.length === 0) {
      this.overlay.innerHTML = '<div style="color: #999;">Timeline — no upcoming layouts</div>';
      return;
    }

    const maxVisible = 8;
    const count = entries.length;
    const visible = entries.slice(0, maxVisible);
    const offlineBadge = this.offline ? ' <span style="color: #ff4444; font-size: 1.1vw;">OFFLINE</span>' : '';
    let html = `<div style="font-weight: 600; margin-bottom: 0.8vh; font-size: 1.4vw; color: #ccc;">Timeline (${count} upcoming)${offlineBadge}</div>`;

    for (const entry of visible) {
      const layoutId = parseInt(entry.layoutFile.replace('.xlf', ''), 10);
      // Current = the entry whose time window contains now AND matches current layout
      const isCurrent = layoutId === this.currentLayoutId
        && entry.startTime <= now && entry.endTime > now;

      const startStr = this.formatTime(entry.startTime);
      const endStr = this.formatTime(entry.endTime);
      const durStr = this.formatDuration(entry.duration);
      const marker = isCurrent ? '▶ ' : '  ';

      const borderLeft = isCurrent ? 'border-left: 0.25vw solid #4a9eff; padding-left: 0.6vw;' : 'padding-left: 0.85vw;';
      const color = isCurrent ? 'color: #fff;' : 'color: #ccc;';

      html += `<div style="${borderLeft} ${color} margin-bottom: 0.3vh; font-family: monospace; font-size: 1.3vw; line-height: 1.5; white-space: nowrap;">`;
      html += `${marker}${startStr}–${endStr}  #${layoutId}  ${durStr}`;
      if (entry.isDefault) html += ' <span style="color: #888;">[def]</span>';
      html += '</div>';
    }

    if (count > maxVisible) {
      html += `<div style="padding-left: 0.85vw; color: #888; font-size: 1.1vw; margin-top: 0.3vh;">+${count - maxVisible} more</div>`;
    }

    this.overlay.innerHTML = html;
  }

  private formatTime(date: Date): string {
    return date.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  }

  private formatDuration(seconds: number): string {
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return m > 0 ? `${m}m ${s.toString().padStart(2, '0')}s` : `${s}s`;
  }

  destroy() {
    if (this.overlay) {
      this.overlay.remove();
      this.overlay = null;
    }
  }
}

/**
 * Determine initial visibility from URL param or localStorage.
 */
export function isTimelineVisible(): boolean {
  const urlParams = new URLSearchParams(window.location.search);
  const showTimeline = urlParams.get('showTimeline');
  if (showTimeline !== null) {
    return showTimeline !== '0' && showTimeline !== 'false';
  }

  const saved = localStorage.getItem('xibo_show_timeline_overlay');
  if (saved !== null) {
    return saved === 'true';
  }

  return false;
}
