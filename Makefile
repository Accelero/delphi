.PHONY: help up down nuke logs ps status wipe surql shell \
        full-up full-down full-nuke full-logs full-ps \
        backend-build backend-test backend-run backend-run-dev \
        frontend-install frontend-dev frontend-build \
        frontend-test frontend-test-watch \
        e2e-install e2e-tier1 e2e-tier2 \
        test-all

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# ----- Tier 1: fast inner-loop dev stack -----------------------------------
# surrealdb + backend (dev-auth: self-injects X-Auth-* headers) + frontend.
# Schema is applied automatically by the backend on startup.
up: ## Tier 1: surrealdb + backend (dev-auth) + frontend
	docker compose up -d --build
	@echo "frontend: http://localhost:5173    backend: http://localhost:8081    surreal: http://localhost:8000"

down: ## Tier 1: stop services (keep volumes)
	docker compose down

nuke: ## Tier 1: stop and delete data volumes
	docker compose down -v

logs: ## Tier 1: tail logs
	docker compose logs -f --tail=200

ps: ## Tier 1: show service status
	docker compose ps

# ----- Tier 2: full prod-shape stack ---------------------------------------
# surrealdb + backend (header mode, no dev-auth) + frontend +
# traefik + keycloak (OIDC IdP) + oauth2-proxy (BFF) + redis (session store).
full-up: ## Tier 2: full prod-shape stack
	docker compose -f docker-compose.full.yml up -d --build
	@echo "open http://localhost  (login: alice / alice  or  bob / bob)"
	@echo "keycloak admin: http://localhost:8088  (admin/admin)"
	@echo "traefik dashboard: http://localhost:8089"

full-down: ## Tier 2: stop services (keep volumes)
	docker compose -f docker-compose.full.yml down

full-nuke: ## Tier 2: stop and delete data volumes
	docker compose -f docker-compose.full.yml down -v

full-logs: ## Tier 2: tail logs
	docker compose -f docker-compose.full.yml logs -f --tail=200

full-ps: ## Tier 2: show service status
	docker compose -f docker-compose.full.yml ps

# ----- backend admin (Tier 1; runs against the live backend container) -----
status: ## Show row counts
	docker compose exec backend /usr/local/bin/delphi admin status

wipe: ## Delete all data, keep schema
	docker compose exec backend /usr/local/bin/delphi admin wipe

surql: ## Open a SurrealQL shell
	docker compose exec surrealdb /surreal sql \
	  --endpoint http://localhost:8000 \
	  --username root --password root \
	  --namespace delphi --database main \
	  --pretty

shell: ## Drop into a shell in the surrealdb container
	docker compose exec surrealdb sh

# ----- local backend (no docker) -------------------------------------------
backend-build: ## cargo build (release; production-shaped — NO dev-auth)
	cd backend && cargo build --release

backend-test: ## cargo test
	cd backend && cargo test

backend-run: ## cargo run delphi serve (header mode; needs an upstream proxy injecting X-Auth-*)
	cd backend && DELPHI_DB_URL=ws://localhost:8000/rpc DELPHI_AUTH_MODE=header cargo run --release -- serve

backend-run-dev: ## cargo run with dev-auth feature (auto-injected identity)
	cd backend && DELPHI_DB_URL=ws://localhost:8000/rpc DELPHI_AUTH_MODE=dev \
	  cargo run --features dev-auth -- serve

# ----- local frontend (no docker) ------------------------------------------
frontend-install: ## bun install
	cd frontend && bun install

frontend-dev: ## bun run dev (http://localhost:5173)
	cd frontend && bun run dev

frontend-build: ## bun run build
	cd frontend && bun run build

frontend-test: ## Vitest (runs via node — Bun's child_process shim breaks tinypool)
	docker run --rm -v "$$(pwd)/frontend:/app" -w /app -u root \
	  node:22-alpine sh -c "node node_modules/.bin/vitest run"

frontend-test-watch: ## Vitest in watch mode
	docker run --rm -it -v "$$(pwd)/frontend:/app" -w /app -u root \
	  node:22-alpine sh -c "node node_modules/.bin/vitest"

# ----- end-to-end tests (Playwright) ----------------------------------------
# Stack must be up before invoking these (`make up` for tier1, `make full-up`
# for tier2). Tests live in /tests; this directory has its own package.json
# so the playwright dependency tree doesn't bleed into the frontend bundle.
e2e-install: ## Install Playwright + browsers
	cd tests && bun install && bunx playwright install --with-deps chromium

e2e-tier1: ## Playwright e2e against Tier 1 (`make up` first)
	cd tests && bun run test:tier1

e2e-tier2: ## Playwright e2e against Tier 2 (`make full-up` first)
	cd tests && bun run test:tier2

# ----- composite -----------------------------------------------------------
test-all: ## Run cargo + vitest (excluding e2e — those need a live stack)
	cd backend && cargo test --features dev-auth
	$(MAKE) frontend-test
