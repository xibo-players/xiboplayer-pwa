# Xibo Player PWA

Lightweight PWA Xibo digital signage player built on the [`@xiboplayer` SDK](https://github.com/linuxnow/xiboplayer).

## CMS Communication

- **REST API** (primary) — lighter JSON transport with ETag caching (~30% smaller payloads)
- **XMDS SOAP** (fallback) — standard Xibo player protocol when REST is unavailable

## Features

- **Offline-first** — Service Worker caching with parallel chunk downloads and progressive streaming
- **XLF layout rendering** — video (MP4/HLS), images, PDF, text/ticker, web pages via RendererLite
- **Campaign scheduling** — priority-based campaigns, dayparting, interrupts, and overlays
- **Real-time CMS commands** — collectNow, screenshot, changeLayout, overlayLayout via XMR WebSocket
- **Proof of play** — per-layout and per-widget duration tracking with stats reporting
- **Timeline overlay** — toggleable debug overlay showing upcoming schedule (press `D` to toggle)
- **Screenshots** — html2canvas-based capture for non-Electron browsers, Electron uses `capturePage()`
- **Keyboard and presenter remote controls** — interactive actions via touch, click, and keyboard
- **Download overlay** — hover bottom-right corner to see download progress
- **Screen Wake Lock** — prevents display from sleeping during playback
- **Configurable log levels** — `DEBUG`, `INFO`, `WARNING`, `ERROR`, `NONE` (via URL param or CMS settings)

## Service Worker Architecture

The Service Worker (`sw-pwa.js`) provides:

- **Progressive streaming** — large media served via chunk streaming with Range request support
- **BlobCache** — in-memory assembled chunks with LRU eviction for fast video seeking
- **XLF-driven media resolution** — parses layout XLF to download exactly the media each layout needs
- **Layout-ordered downloads** — media queued with barriers so the currently-playing layout downloads first
- **Version-aware activation** — same-version SW update skips activation to preserve in-flight video streams
- **Adaptive chunk sizing** — adjusts chunk size and concurrency based on device RAM (4GB/8GB+ tiers)

## Installation

The PWA can be served from:

1. **CMS origin** — deploy `dist/` to a path under the CMS web server (e.g. `https://your-cms.example.com/player/pwa/`) to avoid CORS
2. **Standalone with proxy** — use `@xiboplayer/proxy` to serve the PWA and proxy CMS requests (used by Electron and Chromium shells)

## Development

### Prerequisites

- Node.js 20+
- pnpm 10+

### Setup

```bash
pnpm install
```

### Build

```bash
pnpm run build
```

### Dev server

```bash
pnpm run dev
```

### Link SDK for local development

```bash
pnpm link ../xiboplayer/packages/{utils,cache,renderer,schedule,xmds,xmr,core,stats,settings}
```

## Testing

### Electron (recommended for development)

```bash
cd ../xiboplayer-electron && npx electron . --dev --no-kiosk
```

### Chromium kiosk

```bash
cd ../xiboplayer-chromium && ./xiboplayer/launch-kiosk.sh --no-kiosk
```

### Playwright E2E

Playwright tests in `playwright-tests/` run against a live CMS with scheduled layouts — not for CI.

```bash
PWA_URL=https://your-cms.example.com/player/pwa/ npx playwright test
```

## License

Apache-2.0
