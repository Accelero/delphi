.PHONY: rebuild-up up down ps logs

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
