# Xibo Player PWA

Lightweight PWA Xibo digital signage player built on the [`@xiboplayer` SDK](https://github.com/xibo-players/xiboplayer).

## CMS Communication

- **REST API** (primary) — lighter JSON transport with ETag caching (~30% smaller payloads)
- **XMDS SOAP** (fallback) — standard Xibo player protocol when REST is unavailable

## Features

- **Offline-first** — Service Worker caching with parallel chunk downloads and progressive streaming
- **XLF layout rendering** — video (MP4/HLS), images (scale/align), audio overlay, PDF, text/ticker, web pages via RendererLite
- **Campaign scheduling** — priority-based campaigns, dayparting, interrupts, and overlays
- **Playback control** — skip to next/previous layout via keyboard or click a layout in the timeline overlay
- **Conflict indicators** — timeline overlay highlights overlapping schedule entries
- **Multi-display sync** — BroadcastChannel-based lead/follower synchronized playback for video walls
- **Real-time CMS commands** — collectNow, screenshot, changeLayout, overlayLayout via XMR WebSocket
- **SDK event wiring** — widget duration events, scheduled commands, event-based proof of play
- **Proof of play** — per-layout and per-widget duration tracking with stats reporting
- **Log batching** — aggregated log submission aligned with CMS spec
- **Screenshots** — html2canvas-based capture for non-Electron browsers, Electron uses `capturePage()`
- **Screen Wake Lock** — prevents display from sleeping during playback
- **Configurable log levels** — `DEBUG`, `INFO`, `WARNING`, `ERROR`, `NONE` (via URL param or CMS settings)

## Keyboard Shortcuts

All overlays and controls are hidden by default for clean kiosk operation.

| Key | Action |
|-----|--------|
| `T` | Toggle timeline overlay — shows upcoming scheduled layouts with conflict indicators |
| `D` | Toggle download overlay — shows media download progress |
| `V` | Toggle video controls — show/hide native browser controls on all videos |
| `→` / `PageDown` | Skip to next layout |
| `←` / `PageUp` | Go to previous layout |
| `Space` | Pause / resume playback |
| `R` | Revert to scheduled layout (when manually overridden) |

Timeline overlay also supports **click-to-skip** — click any layout in the timeline to jump directly to it.

## Debug Overlays

Three toggleable overlays provide real-time insight into player operation without leaving the playback screen. All are hidden by default for clean kiosk operation — press the corresponding key to toggle.

### Timeline Overlay (`T`)

Shows the upcoming schedule as a scrollable list (up to 8 entries visible):

- **Current layout** highlighted with a `▶` marker and blue left border
- **Time range** and **duration** for each entry (e.g. `19:25–19:31  #362  6m 15s`)
- **`[def]`** tag on default/fallback layouts (no campaign scheduled)
- **`OFFLINE`** badge when the player has lost CMS connectivity
- **⚠ conflict indicators** — an orange `⚠ N` badge appears when N lower-priority layouts were scheduled for the same time slot but suppressed by a higher-priority campaign. Hover over the ⚠ to see which layouts were hidden and their priorities (e.g. `Hidden: #366 (p0), #362 (p0)`)
- **Click-to-skip** — click any future layout to jump to it immediately. The player enters override mode (the schedule won't auto-advance). Press `R` to return to normal schedule playback

### Download Overlay (`D`)

Shows real-time media download progress:

- **Active downloads** with progress bars, speed, and chunk status
- **Queue depth** — how many files are waiting to download
- **Idle status** when all media is cached and up to date
- Auto-hides when all downloads complete (if auto-hide is enabled)

Useful during initial deployment or after a `purgeAll` command to monitor how quickly content is being cached.

### Video Controls (`V`)

Toggles native browser `<video>` controls on all video elements currently in the DOM. Shows play/pause, seek bar, volume, and fullscreen buttons on each video widget — helpful for debugging video playback issues, checking codec info, or manually seeking within a video.

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
