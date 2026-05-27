.PHONY: rebuild-up up down ps logs docs-up docs-serve docs-build docs-rust docs-frontend docs-all

SHELL := /usr/bin/bash
COMPOSE ?= docker compose -f docker-compose.t2.yml

rebuild-up:
	$(COMPOSE) build api-service frontend
	$(COMPOSE) up -d

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

docs-up: docs-serve

docs-serve:
	/usr/bin/bash -lc 'cd doc && BUN_TMPDIR=/tmp NEXT_TELEMETRY_DISABLED=1 bun --bun node_modules/next/dist/bin/next dev --hostname 127.0.0.1 --port 8003'

docs-build:
	/usr/bin/bash -lc 'cd doc && BUN_TMPDIR=/tmp NEXT_TELEMETRY_DISABLED=1 bun --bun node_modules/next/dist/bin/next build'

docs-rust:
	cargo doc --workspace --no-deps

docs-frontend:
	cd frontend && npx typedoc

docs-all: docs-build docs-rust docs-frontend
