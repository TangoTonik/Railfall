# Shortcuts for the local Railfall platform. `make help` lists the targets.
#
# Variables from `.env` (copy `.env.example`) are exported to every recipe, so
# `make health` talks to the same credentials and ports as `docker compose`.

-include .env
export

COMPOSE ?= docker compose
PYTHON ?= python
WAIT ?= 60

.DEFAULT_GOAL := help
.PHONY: help env build up health smoke logs ps down clean lint test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  %-8s %s\n", $$1, $$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

build: env ## Build the source-built MinIO image
	$(COMPOSE) build minio

up: build ## Start Postgres + MinIO, wait until healthy, create the lake bucket
	$(COMPOSE) up -d --wait postgres minio
	$(COMPOSE) run --rm minio-init

health: ## Check every service answers (retries for WAIT seconds)
	$(PYTHON) -m railfall.healthcheck --wait $(WAIT)

smoke: ## Health checks plus a write/read round-trip on the lake and warehouse
	$(PYTHON) -m railfall.healthcheck --wait $(WAIT) --write

logs: ## Follow service logs
	$(COMPOSE) logs -f

ps: ## Show service status
	$(COMPOSE) ps

down: ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

clean: ## Stop the stack and delete its data volumes
	$(COMPOSE) down -v

lint: ## Run ruff lint + format check
	ruff check .
	ruff format --check .

test: ## Run unit tests
	$(PYTHON) -m pytest -q
