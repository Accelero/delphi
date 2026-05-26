.PHONY: rebuild-up up down ps logs docs-serve docs-build docs-rust docs-frontend docs-all

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

docs-serve:
	cd doc && UV_CACHE_DIR=../.uv-cache uv run mkdocs serve

docs-build:
	cd doc && UV_CACHE_DIR=../.uv-cache uv run mkdocs build

docs-rust:
	cargo doc --workspace --no-deps

docs-frontend:
	cd frontend && npx typedoc

docs-all: docs-build docs-rust docs-frontend
