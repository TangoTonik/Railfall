# Shortcuts for the local Railfall platform. `make help` lists the targets.
#
# Variables from `.env` (copy `.env.example`) are exported to every recipe, so
# `make health` talks to the same credentials and ports as `docker compose`.

-include .env
export

COMPOSE ?= docker compose
PYTHON ?= python
WAIT ?= 60
TF ?= terraform
TF_DIR := infra/terraform

# Terraform reaches MinIO on the host port published by compose.
TF_VAR_minio_server ?= localhost:$(or $(MINIO_API_PORT),9000)
TF_VAR_postgres_port ?= $(or $(POSTGRES_PORT),5432)
TF_VAR_postgres_db ?= $(or $(POSTGRES_DB),warehouse)
TF_VAR_lake_bucket ?= $(or $(LAKE_BUCKET),railfall-lake)

.DEFAULT_GOAL := help
.PHONY: help env build up infra-init infra infra-check access health smoke ingest-sncf verify-sncf ingest-meteo verify-meteo silver verify-silver logs ps down \
	clean lint test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  %-12s %s\n", $$1, $$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

build: env ## Build the source-built MinIO image
	$(COMPOSE) build minio

up: build ## Start Postgres + MinIO, wait until healthy, provision them with Terraform
	$(COMPOSE) up -d --wait postgres minio
	$(MAKE) --no-print-directory infra

infra-init: ## Download the pinned Terraform providers
	$(TF) -chdir=$(TF_DIR) init -input=false

infra: infra-init ## Apply Terraform: lake bucket, pipeline user, warehouse schemas + roles
	$(TF) -chdir=$(TF_DIR) apply -input=false -auto-approve

infra-check: ## Fail if the running stack drifted from the Terraform config
	$(TF) -chdir=$(TF_DIR) plan -input=false -detailed-exitcode -lock=false

access: ## Prove each pipeline identity can do its job and nothing more
	$(PYTHON) -m railfall.access

health: ## Check every service answers (retries for WAIT seconds)
	$(PYTHON) -m railfall.healthcheck --wait $(WAIT)

smoke: ## Health checks plus a write/read round-trip on the lake and warehouse
	$(PYTHON) -m railfall.healthcheck --wait $(WAIT) --write

ingest-sncf: ## Download the SNCF exports and land them in bronze (idempotent per day)
	$(PYTHON) -m railfall.ingest.run sncf $(INGEST_ARGS)

verify-sncf: ## Read today's SNCF bronze back and check it against the contract
	$(PYTHON) -m railfall.ingest.verify sncf $(INGEST_ARGS)

ingest-meteo: ## Download Météo-France daily files (METEO_DEPARTMENTS) into bronze
	$(PYTHON) -m railfall.ingest.run meteo $(INGEST_ARGS)

verify-meteo: ## Read today's Météo-France bronze back and check it against the contract
	$(PYTHON) -m railfall.ingest.verify meteo $(INGEST_ARGS)

silver: ## Build silver Parquet tables from bronze with Spark (SILVER_ARGS=--json, --table ...)
	$(PYTHON) -m railfall.spark.job $(SILVER_ARGS)

verify-silver: ## Read silver back and check schema, key uniqueness and non-emptiness
	$(PYTHON) -m railfall.spark.verify $(SILVER_ARGS)

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
