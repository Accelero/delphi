FROM node:24-bookworm AS build
WORKDIR /app
COPY frontend/package.json ./
RUN npm install
COPY frontend ./
RUN npm run build

FROM caddy:2-alpine
COPY frontend/Caddyfile /etc/caddy/Caddyfile
COPY --from=build /app/dist /usr/share/caddy
