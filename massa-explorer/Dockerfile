# syntax=docker/dockerfile:1.6
#
# Production Dockerfile for `massa-explorer`.
#
# Build context is the workspace root (same as the indexer):
#
#     docker build -f massa-explorer/Dockerfile -t massa-explorer:local .
#
# The image is a tiny nginx serving the pre-built Vite `dist/`. Operators can
# override endpoints without a rebuild by either:
#   * setting the MASSA_EXPLORER_DEFAULT_NETWORK / MASSA_EXPLORER_MAINNET_ENDPOINTS
#     / MASSA_EXPLORER_BUILDNET_ENDPOINTS env vars, which `/docker-entrypoint.d/`
#     rewrites into `/usr/share/nginx/html/config.js` before nginx boots, or
#   * bind-mounting a custom `config.js` onto `/usr/share/nginx/html/config.js`.

# ---- builder -----------------------------------------------------------------
FROM node:20-bookworm-slim AS build

WORKDIR /src

# Copy package manifest first to exploit the layer cache.
COPY massa-explorer/package.json massa-explorer/package-lock.json* /src/
RUN npm ci --no-audit --no-fund

COPY massa-explorer/ /src/

# Type-check + production build. Vite emits to /src/dist with fully hashed
# asset filenames so aggressive caching at the CDN/nginx layer is safe.
RUN npm run build

# ---- runtime -----------------------------------------------------------------
FROM nginxinc/nginx-unprivileged:1.27-alpine AS runtime
USER root

# `gettext` gives us `envsubst` for runtime config injection; `wget` is used
# by the HEALTHCHECK instruction below.
RUN apk add --no-cache gettext wget \
    && rm -rf /etc/nginx/conf.d/default.conf

# Serve config + SPA fallback
COPY massa-explorer/nginx.conf /etc/nginx/conf.d/default.conf

COPY --from=build /src/dist /usr/share/nginx/html

# Entrypoint rewrites config.js from env vars at container start.
COPY massa-explorer/docker-entrypoint.sh /docker-entrypoint.d/20-massa-config.sh
RUN chmod +x /docker-entrypoint.d/20-massa-config.sh \
    && chown -R nginx:nginx /usr/share/nginx/html

USER nginx

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1
