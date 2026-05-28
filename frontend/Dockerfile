FROM oven/bun:1.3 AS build
WORKDIR /app
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend ./
RUN bun run build

FROM oven/bun:1.3 AS dev
WORKDIR /app
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend ./
EXPOSE 80
CMD ["bun", "run", "dev"]

FROM caddy:2-alpine
COPY frontend/Caddyfile /etc/caddy/Caddyfile
COPY --from=build /app/dist /usr/share/caddy
