/**
 * Setup Overlay
 *
 * Two-phase overlay that never navigates away from the player:
 *  1. CMS key gate — verifies identity
 *  2. Full setup form — setup.html in a fullscreen iframe
 *
 * Dismissible with Esc or Cancel at both phases. On successful setup
 * the iframe redirects to index.html, which we intercept to reload.
 */

// @ts-ignore - JavaScript module
import { createLogger, config } from '@xiboplayer/utils';

const log = createLogger('SetupOverlay');

export class SetupOverlay {
  private backdrop: HTMLElement | null = null;
  private gateCard: HTMLElement | null = null;
  private iframe: HTMLIFrameElement | null = null;
  private cancelBtn: HTMLElement | null = null;
  private visible = false;

  show() {
    if (this.visible) return;
    this.visible = true;

    if (!this.backdrop) {
      this.create();
    }

    // Always start with the gate phase
    this.showGate();
    this.backdrop!.style.display = 'flex';
    log.info('[SetupOverlay] Opened');
  }

  hide() {
    if (!this.visible) return;
    this.visible = false;

    if (this.backdrop) {
      this.backdrop.style.display = 'none';
    }
    // Clear iframe to stop any polling timers inside setup.html
    if (this.iframe) {
      this.iframe.src = 'about:blank';
      this.iframe.style.display = 'none';
    }
    log.info('[SetupOverlay] Closed');
  }

  toggle() {
    if (this.visible) {
      this.hide();
    } else {
      this.show();
    }
  }

  isVisible() {
    return this.visible;
  }

  /** Show the CMS key gate card, hide the iframe */
  private showGate() {
    if (this.gateCard) this.gateCard.style.display = 'block';
    if (this.iframe) this.iframe.style.display = 'none';
    if (this.cancelBtn) this.cancelBtn.style.display = 'none';

    const input = this.gateCard?.querySelector('#gate-key') as HTMLInputElement;
    if (input) {
      input.value = '';
      requestAnimationFrame(() => input.focus());
    }
    const err = this.gateCard?.querySelector('#gate-error') as HTMLElement;
    if (err) err.style.display = 'none';
  }

  /** Show the setup iframe, hide the gate card */
  private showSetup() {
    if (this.gateCard) this.gateCard.style.display = 'none';
    if (this.cancelBtn) this.cancelBtn.style.display = 'block';
    if (this.iframe) {
      this.iframe.style.display = 'block';
      this.iframe.src = './setup.html?unlocked=1';
    }
  }

  private create() {
    // ── Backdrop ──
    this.backdrop = document.createElement('div');
    this.backdrop.id = 'setup-overlay-backdrop';
    this.backdrop.style.cssText = `
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.85);
      z-index: 1000000;
      display: none;
      align-items: center;
      justify-content: center;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    `;

    // ── Cancel button (visible in iframe phase) ──
    this.cancelBtn = document.createElement('button');
    this.cancelBtn.textContent = 'Cancel';
    this.cancelBtn.style.cssText = `
      position: absolute;
      top: 12px;
      right: 16px;
      background: transparent;
      border: 1px solid rgba(255, 255, 255, 0.3);
      color: #aaa;
      font-size: 14px;
      padding: 6px 18px;
      border-radius: 6px;
      cursor: pointer;
      z-index: 1000001;
      display: none;
      transition: background 0.2s, color 0.2s;
    `;
    this.cancelBtn.addEventListener('mouseenter', () => {
      this.cancelBtn!.style.background = 'rgba(255,255,255,0.1)';
      this.cancelBtn!.style.color = '#fff';
    });
    this.cancelBtn.addEventListener('mouseleave', () => {
      this.cancelBtn!.style.background = 'transparent';
      this.cancelBtn!.style.color = '#aaa';
    });
    this.cancelBtn.addEventListener('click', () => this.hide());

    // ── Gate card (matches setup.html .container) ──
    this.gateCard = document.createElement('div');
    this.gateCard.style.cssText = `
      background: #2A2A2A;
      border-radius: 16px;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
      padding: 48px;
      max-width: 480px;
      width: 90vw;
      color: #E0E0E0;
    `;
    this.gateCard.innerHTML = `
      <div style="text-align: center; margin-bottom: 32px;">
        <div style="font-size: 36px; font-weight: 700; color: #fff; letter-spacing: -0.5px;">
          <span style="color: #0097D8;">xibo</span> player
        </div>
        <div style="font-size: 14px; color: #888; margin-top: 4px;">PWA Digital Signage</div>
      </div>
      <div style="font-size: 18px; font-weight: 600; color: #fff; margin-bottom: 8px; text-align: center;">
        Reconfigure Display
      </div>
      <div style="font-size: 13px; color: #888; margin-bottom: 20px; text-align: center; line-height: 1.5;">
        Enter the current CMS Key to change settings.
      </div>
      <form id="gate-form">
        <div style="margin-bottom: 20px;">
          <label style="display: block; margin-bottom: 6px; color: #AAA; font-size: 13px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px;">
            CMS Key
          </label>
          <input type="password" id="gate-key" placeholder="Current CMS key" required
            style="width: 100%; padding: 12px 14px; background: #1D1D1D; border: 2px solid #3A3A3A; border-radius: 8px; font-size: 15px; color: #E0E0E0; transition: border-color 0.2s; box-sizing: border-box;">
        </div>
        <button type="submit" style="width: 100%; padding: 14px; background: #0097D8; color: white; border: none; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer; transition: background 0.2s, transform 0.1s;">
          Unlock
        </button>
      </form>
      <div id="gate-error" style="margin-top: 16px; padding: 12px 14px; background: rgba(244, 67, 54, 0.15); border: 1px solid rgba(244, 67, 54, 0.3); border-radius: 8px; color: #EF9A9A; font-size: 14px; display: none;"></div>
      <button id="gate-cancel" style="width: 100%; padding: 14px; background: transparent; border: 1px solid #3A3A3A; color: #AAA; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer; margin-top: 8px; transition: background 0.2s;">
        Cancel
      </button>
    `;

    // ── Iframe (fullscreen, same look as setup.html) ──
    this.iframe = document.createElement('iframe');
    this.iframe.style.cssText = `
      width: 100%;
      height: 100%;
      border: none;
      background: #1D1D1D;
      display: none;
    `;

    // Detect success redirect: setup.html navigates to index.html → reload player
    this.iframe.addEventListener('load', () => {
      try {
        const href = this.iframe!.contentWindow?.location?.href || '';
        if (href.includes('index.html')) {
          this.hide();
          window.location.reload();
          return;
        }

        // Esc inside the iframe dismisses the overlay
        const iframeDoc = this.iframe!.contentDocument;
        if (!iframeDoc) return;
        iframeDoc.addEventListener('keydown', (e: KeyboardEvent) => {
          if (e.key === 'Escape') {
            e.preventDefault();
            this.hide();
          }
        });
      } catch { /* not loaded yet */ }
    });

    this.backdrop.appendChild(this.cancelBtn);
    this.backdrop.appendChild(this.gateCard);
    this.backdrop.appendChild(this.iframe);
    document.body.appendChild(this.backdrop);

    // ── Gate event handlers ──
    const form = this.gateCard.querySelector('#gate-form') as HTMLFormElement;
    const input = this.gateCard.querySelector('#gate-key') as HTMLInputElement;
    const errorEl = this.gateCard.querySelector('#gate-error') as HTMLElement;
    const gateCancelBtn = this.gateCard.querySelector('#gate-cancel') as HTMLButtonElement;

    input.addEventListener('focus', () => { input.style.borderColor = '#0097D8'; });
    input.addEventListener('blur', () => { input.style.borderColor = '#3A3A3A'; });

    form.addEventListener('submit', (e: Event) => {
      e.preventDefault();
      const entered = input.value.trim();

      if (entered === config.cmsKey) {
        this.showSetup();
      } else {
        errorEl.textContent = 'Incorrect CMS key';
        errorEl.style.display = 'block';
        input.focus();
        input.select();
      }
    });

    gateCancelBtn.addEventListener('click', () => this.hide());

    // Esc closes overlay; stopPropagation blocks player shortcuts
    this.backdrop.addEventListener('keydown', (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        this.hide();
      }
      e.stopPropagation();
    });
  }
}
