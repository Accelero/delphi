.PHONY: rebuild-up up down ps logs docs-up docs-down docs-build docs-rust docs-frontend docs-all

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

docs-up:
	@/usr/bin/bash -lc 'set -euo pipefail; \
		pidfile=/tmp/delphi-docs.pid; \
		logfile=/tmp/delphi-docs.log; \
		if [[ -f "$$pidfile" ]] && kill -0 "$$(cat "$$pidfile")" 2>/dev/null; then \
			echo "docs server already running: http://127.0.0.1:8003 (pid $$(cat "$$pidfile"))"; \
			exit 0; \
		fi; \
		rm -f "$$pidfile"; \
		cd doc; \
		BUN_TMPDIR=/tmp NEXT_TELEMETRY_DISABLED=1 setsid bun --bun node_modules/next/dist/bin/next dev --hostname 127.0.0.1 --port 8003 > "$$logfile" 2>&1 < /dev/null & \
		pid=$$!; \
		echo "$$pid" > "$$pidfile"; \
		sleep 0.5; \
		if ! kill -0 "$$pid" 2>/dev/null; then \
			rm -f "$$pidfile"; \
			echo "docs server failed to start; see $$logfile" >&2; \
			exit 1; \
		fi; \
		echo "docs server started: http://127.0.0.1:8003 (pid $$pid, log $$logfile)"'

docs-down:
	@/usr/bin/bash -lc 'set -euo pipefail; \
		pidfile=/tmp/delphi-docs.pid; \
		pattern="[b]un --bun node_modules/next/dist/bin/next dev --hostname 127.0.0.1 --port 8003"; \
		stopped=0; \
		if [[ -f "$$pidfile" ]]; then \
			pid="$$(cat "$$pidfile")"; \
			if kill -0 "$$pid" 2>/dev/null; then \
				kill "$$pid" || true; \
				stopped=1; \
			fi; \
		fi; \
		if pkill -f "$$pattern" 2>/dev/null; then \
			stopped=1; \
		fi; \
		if [[ "$$stopped" -eq 0 ]]; then \
			rm -f "$$pidfile"; \
			echo "docs server is not running"; \
			exit 0; \
		fi; \
		for _ in {1..50}; do \
			if ! pgrep -f "$$pattern" >/dev/null 2>&1; then \
				rm -f "$$pidfile"; \
				echo "docs server stopped"; \
				exit 0; \
			fi; \
			sleep 0.1; \
		done; \
		echo "docs server did not stop after SIGTERM" >&2; \
		exit 1'

docs-build:
	/usr/bin/bash -lc 'cd doc && BUN_TMPDIR=/tmp NEXT_TELEMETRY_DISABLED=1 bun --bun node_modules/next/dist/bin/next build'

docs-rust:
	cargo doc --workspace --no-deps

docs-frontend:
	cd frontend && npx typedoc

docs-all: docs-build docs-rust docs-frontend
