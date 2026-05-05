.PHONY: help up down nuke logs ps build init status wipe surql shell \
        backend-build backend-test backend-run backend-run-dev \
        frontend-install frontend-dev frontend-build

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# ----- compose lifecycle ---------------------------------------------------
up: ## Bring up surrealdb + backend + frontend
	docker compose up -d --build
	@$(MAKE) init
	@echo "frontend: http://localhost:5173    backend: http://localhost:8081    surreal: http://localhost:8000"

down: ## Stop all services (keep volumes)
	docker compose down

nuke: ## Stop and delete all data volumes
	docker compose down -v

logs: ## Tail logs for all services
	docker compose logs -f --tail=200

ps: ## Show service status
	docker compose ps

# ----- backend admin (via compose) -----------------------------------------
init: ## Apply DB schema (idempotent)
	docker compose --profile tools run --rm admin init

status: ## Show row counts
	docker compose --profile tools run --rm admin status

wipe: ## Delete all data, keep schema
	docker compose --profile tools run --rm admin wipe

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

backend-run: ## cargo run delphi serve (against compose surrealdb on localhost:8000)
	cd backend && SURREAL_URL=ws://localhost:8000/rpc cargo run --release -- serve

backend-run-dev: ## cargo run with dev-auth feature (auto-injected identity)
	cd backend && SURREAL_URL=ws://localhost:8000/rpc AUTH_MODE=dev \
	  cargo run --features dev-auth -- serve

# ----- local frontend (no docker) ------------------------------------------
frontend-install: ## bun install
	cd frontend && bun install

frontend-dev: ## bun run dev (http://localhost:5173)
	cd frontend && bun run dev

frontend-build: ## bun run build
	cd frontend && bun run build
