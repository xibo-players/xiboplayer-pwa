# Xibo Player PWA

Lightweight PWA Xibo digital signage player built on the `@xiboplayer` SDK.

## CMS Communication

- **REST API** (primary) — used when the CMS exposes the REST endpoint
- **XMDS SOAP** (fallback) — the standard Xibo player protocol

## Features

- Offline-first with Service Worker caching and parallel chunk downloads
- XLF layout rendering via RendererLite
- Campaign scheduling with dayparting, interrupts, and overlays
- Real-time CMS commands via XMR WebSocket
- Proof of play tracking and stats reporting
- Configurable log levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `NONE`)

## Installation

The PWA must be served from the same origin as the Xibo CMS (e.g. `https://your-cms.example.com/player/pwa/`) to avoid CORS restrictions when communicating with the XMDS and REST APIs.

Deploy the built `dist/` directory to a path under the CMS web server.

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

## Testing

Playwright E2E tests are in `playwright-tests/`. These are manual QA scripts meant to be run against a live CMS with layouts already scheduled — they are not suitable for CI.

```bash
PWA_URL=https://your-cms.example.com/player/pwa/ npx playwright test
```

## License

Apache-2.0
