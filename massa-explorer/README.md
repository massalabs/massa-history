# massa-explorer

React / TypeScript single-page app that consumes the
[`massa-indexer`](../massa-indexer/) REST + SSE API. Built with Vite,
Tailwind, React Router, and React Query; the production bundle is ~90 KB
gzipped and served by a minimal unprivileged nginx image.

---

## Features

* **Home dashboard** — live node status, latest blocks, latest operations.
* **Slot graph** — 30-second live stream of candidate / final / miss slots
  with SSE.
* **Blocks / operations / endorsements / denunciations** — paginated lists
  plus detail pages for every entity, including embedded transfer, event,
  endorsement, and denunciation panels.
* **Address view** — per-address transfers, received operations, endorsements,
  denunciations, and SC-event stream.
* **Stakers** — node-passthrough leaderboard (served from the indexer's
  slot-aware proxy cache).
* **Charts** — throughput, blocks-per-slot, finality lag, active addresses,
  staker count. Rendered inline with a zero-dependency SVG component.
* **API docs** — embedded rendering of the indexer's OpenAPI spec.
* **Network switcher + custom endpoints** — saved per-browser in localStorage.
* **Runtime configuration** — operator-controlled defaults injected into
  `/config.js` at container start, without rebuilding.

---

## Develop

```bash
cd massa-explorer
npm ci
npm run dev          # http://localhost:5173
```

Vite proxies nothing; it expects the indexer to be reachable from the
browser. For the default dev setup the indexer listens on `http://127.0.0.1:8080`.
Override via the in-app **Settings** page or by editing `public/config.js`.

### Type-checking, tests, and production build

```bash
npm run lint         # tsc --noEmit
npm test             # vitest run
npm run build        # tsc --noEmit && vite build
npm run preview      # serve dist/ on :4173 with `vite preview`
```

---

## Runtime configuration

The bundled defaults are compiled into `src/lib/config.ts`:

```ts
mainnet  -> http://127.0.0.1:8080
buildnet -> http://127.0.0.1:8081
```

A deployment can override these without rebuilding by populating
`window.__MASSA_EXPLORER_CONFIG__` in `/config.js`. The Docker entrypoint
does this automatically from environment variables:

| Env var | Effect |
| --- | --- |
| `MASSA_EXPLORER_DEFAULT_NETWORK` | Sets the initial network (`mainnet` / `buildnet`). |
| `MASSA_EXPLORER_MAINNET_ENDPOINTS` | Comma-separated list of indexer URLs used when the user is on `mainnet`. |
| `MASSA_EXPLORER_BUILDNET_ENDPOINTS` | Same but for `buildnet`. |

User-supplied endpoint overrides saved in localStorage take precedence over
runtime config, which itself takes precedence over the bundled defaults.

---

## Deploy

### Docker

```bash
docker build -f massa-explorer/Dockerfile -t massa-explorer:local .

docker run -d --name explorer \
  -p 127.0.0.1:8081:8080 \
  -e MASSA_EXPLORER_DEFAULT_NETWORK=mainnet \
  -e MASSA_EXPLORER_MAINNET_ENDPOINTS=https://indexer.example.com \
  massa-explorer:local

curl -s http://127.0.0.1:8081/healthz        # "ok"
```

The container runs as the unprivileged `nginx` user from
`nginxinc/nginx-unprivileged`, listens on port 8080 inside the container,
and ships these caching defaults:

* HTML shell and `config.js`: `Cache-Control: no-store`
* Hashed `/assets/*`: `Cache-Control: public, immutable`, 1 year

Security headers set by nginx:

```
X-Content-Type-Options: nosniff
X-Frame-Options:        SAMEORIGIN
Referrer-Policy:        strict-origin-when-cross-origin
Permissions-Policy:     geolocation=(), microphone=(), camera=()
```

### DeWeb / static hosting

```bash
npm run build
cp dist/* /var/www/explorer/
# edit /var/www/explorer/config.js to set endpoints
```

The `dist/` directory is fully self-contained and works on any static host
(S3, Netlify, Cloudflare Pages, DeWeb, IPFS). `config.js` is served next to
`index.html` and can be rewritten on deploy without touching the hashed
bundles.

---

## Accessibility & UX notes

* Dark theme by default; the SPA respects `prefers-color-scheme` via
  `<meta name="color-scheme">`.
* Every detail page sets a page-specific `<title>` through `react-helmet-async`
  for useful browser history and social previews.
* A top-level `ErrorBoundary` catches render-time crashes and shows an
  actionable recovery screen instead of a blank page.
* `<noscript>` copy explains that the explorer is a SPA and links to the
  indexer's OpenAPI spec, so users without JS still have a path to the data.
