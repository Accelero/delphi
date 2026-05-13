# Two-stage build: produce the SPA bundle, serve it with Caddy.
#
# Used for Tier-2 (prod-shape stack) and any real deployment. Tier-1
# keeps the bun + vite-dev-server pattern for HMR; this image is for
# the case where you actually want to test/ship the production bundle.

# ── Stage 1 — build the SPA ──────────────────────────────────────────
FROM oven/bun:1.1 AS build
WORKDIR /app

# Install deps first so bun's layer is cached across source edits.
COPY package.json bun.lockb ./
RUN bun install --frozen-lockfile

# Copy sources after install so source changes don't bust the deps layer.
COPY . .

# Build. We invoke `vite build` directly rather than the package.json
# `build` script (which is `tsc -b && vite build`) because the
# TanStack Router plugin generates `src/routeTree.gen.ts` during the
# Vite run — that file is gitignored and absent at this point. tsc
# would fail trying to type-check imports against it before vite
# gets a chance to generate it.
#
# Type-check is enforced in CI (pr.yml lint job), which generates
# the route tree first and then runs `tsc -b --noEmit`. We don't
# duplicate that gate inside the prod image build.
RUN bunx vite build

# ── Stage 2 — Caddy serves the bundle ────────────────────────────────
FROM caddy:2-alpine
COPY --from=build /app/dist /usr/share/caddy
COPY Caddyfile /etc/caddy/Caddyfile
EXPOSE 80
