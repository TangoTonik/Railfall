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
DAG ?= railfall_ingest
# Where `make monitor` writes its JSON reports.
MONITOR_DIR ?= monitoring-reports

# Terraform reaches MinIO on the host port published by compose.
TF_VAR_minio_server ?= localhost:$(or $(MINIO_API_PORT),9000)
TF_VAR_postgres_port ?= $(or $(POSTGRES_PORT),5432)
TF_VAR_postgres_db ?= $(or $(POSTGRES_DB),warehouse)
TF_VAR_lake_bucket ?= $(or $(LAKE_BUCKET),railfall-lake)
TF_VAR_airflow_db_name ?= $(or $(AIRFLOW_DB_NAME),airflow)
TF_VAR_airflow_db_user ?= $(or $(AIRFLOW_DB_USER),airflow)
TF_VAR_metabase_db_name ?= $(or $(METABASE_DB_NAME),metabase)
TF_VAR_metabase_db_user ?= $(or $(METABASE_DB_USER),metabase)

# Airflow services live behind a compose profile (they need `make infra` first).
AIRFLOW_PROFILE := --profile airflow
# Metabase also needs `make infra` first (Terraform creates its database).
BI_PROFILE := --profile bi

.DEFAULT_GOAL := help
.PHONY: help env build core up urls airflow-build airflow metabase dashboards dashboards-check infra-init infra infra-check access health smoke ingest-sncf verify-sncf ingest-meteo verify-meteo dag-run silver verify-silver load verify-load dbt-debug dbt-build dbt-freshness dbt-docs dbt-docs-bundle readme-catalog quality quality-canary verify-staging verify-marts weather-report weather-stats freshness run-metrics monitor logs ps down \
	clean lint test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  %-12s %s\n", $$1, $$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

build: env ## Build the source-built MinIO image
	$(COMPOSE) build minio

core: build ## Start Postgres + MinIO, wait until healthy, provision them with Terraform
	$(COMPOSE) up -d --wait postgres minio
	$(MAKE) --no-print-directory infra

up: ## One command: core + Terraform + Airflow + Metabase, wait until all healthy
	$(MAKE) --no-print-directory core
	$(MAKE) --no-print-directory airflow
	$(MAKE) --no-print-directory metabase
	$(PYTHON) -m railfall.healthcheck --all --wait $(WAIT)
	@$(MAKE) --no-print-directory urls

urls: ## Print where each service of the running stack answers
	@echo "Airflow   http://localhost:$(or $(AIRFLOW_WEB_PORT),8080)  (user $(or $(AIRFLOW_ADMIN_USER),admin))"
	@echo "Metabase  http://localhost:$(or $(METABASE_PORT),3000)  (dashboards: make dashboards, once marts exist)"
	@echo "MinIO     http://localhost:$(or $(MINIO_CONSOLE_PORT),9001)  (bucket $(or $(LAKE_BUCKET),railfall-lake))"
	@echo "Postgres  localhost:$(or $(POSTGRES_PORT),5432)/$(or $(POSTGRES_DB),warehouse)"

airflow-build: env ## Build the Airflow image (official image + the railfall package)
	$(COMPOSE) $(AIRFLOW_PROFILE) build airflow-init

airflow: airflow-build ## Start Airflow (after `make core`): migrate its DB, wait until healthy
	$(COMPOSE) $(AIRFLOW_PROFILE) up -d --wait airflow-webserver airflow-scheduler

metabase: env ## Start Metabase (after `make core`) and wait until it is healthy
	$(COMPOSE) $(BI_PROFILE) up -d --wait metabase

dashboards: ## Bootstrap Metabase and provision the dashboards in bi/dashboards (idempotent)
	$(PYTHON) -m railfall.bi.metabase bootstrap $(METABASE_ARGS)

dashboards-check: ## Validate bi/dashboards offline (DASHBOARDS_ARGS="--manifest dbt/target/manifest.json")
	$(PYTHON) -m railfall.bi.metabase check $(DASHBOARDS_ARGS)

dag-run: ## Trigger a DAG and wait until it finishes (DAG=railfall_ingest DAG_ARGS=--conf '{...}')
	$(PYTHON) -m railfall.orchestration.airflow_api $(DAG) $(DAG_ARGS)

infra-init: ## Download the pinned Terraform providers (retried: the registry is flaky)
	@for i in 1 2 3; do \
		$(TF) -chdir=$(TF_DIR) init -input=false && exit 0; \
		echo "terraform init failed (attempt $$i/3), retrying in $$((i * 15))s" >&2; \
		sleep $$((i * 15)); \
	done; exit 1

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

load: ## Merge silver into the warehouse raw schema, idempotently (LOAD_ARGS=--year 2025 --json)
	$(PYTHON) -m railfall.load.job $(LOAD_ARGS)

verify-load: ## Check raw tables against silver: columns, primary key, no duplicates, counts
	$(PYTHON) -m railfall.load.verify $(LOAD_ARGS)

dbt-debug: ## Check dbt can reach the warehouse as the transformer role
	$(PYTHON) -m railfall.transform.dbt debug

dbt-build: ## Build and test the dbt models (DBT_ARGS=--select staging --json)
	$(PYTHON) -m railfall.transform.dbt build $(DBT_ARGS)

dbt-freshness: ## Check raw sources were loaded recently (warn > 7 days, error > 40)
	$(PYTHON) -m railfall.transform.dbt source freshness $(DBT_ARGS)

dbt-docs: ## Generate dbt docs (manifest + catalog from the live warehouse)
	$(PYTHON) -m railfall.transform.dbt docs generate $(DBT_ARGS)

dbt-docs-bundle: ## Lineage, doc coverage, schema drift and catalog.md (DOCS_ARGS=--strict)
	$(PYTHON) -m railfall.transform.docs $(DOCS_ARGS)

readme-catalog: ## Regenerate the README data catalog from `dbt parse` (no database needed)
	$(PYTHON) -m railfall.transform.dbt parse
	$(PYTHON) -m railfall.transform.docs --readme README.md

quality: ## Validate raw tables with the Great Expectations suites (QUALITY_ARGS=--report q.json)
	$(PYTHON) -m railfall.quality.checkpoint $(QUALITY_ARGS)

quality-canary: ## Prove the gate fails on corrupted copies of the raw tables (copies are dropped)
	$(PYTHON) -m railfall.quality.canary $(QUALITY_ARGS)

verify-staging: ## Check staging views against raw: view, row counts, unique grain
	$(PYTHON) -m railfall.transform.verify $(DBT_VERIFY_ARGS)

verify-marts: ## Check the marts: tables, unique grain, facts match sources, trains reconcile
	$(PYTHON) -m railfall.transform.verify --layer marts $(DBT_VERIFY_ARGS)

weather-report: ## Print the weather-impact marts as Markdown, read as railfall_reader
	$(PYTHON) -m railfall.transform.report $(REPORT_ARGS)

weather-stats: ## Test whether rain / wind / frost significantly lower punctuality: overall, by season, dose-response
	$(PYTHON) -m railfall.analysis.weather $(STATS_ARGS)

freshness: ## Check every raw table was loaded recently and its newest period is within SLA
	$(PYTHON) -m railfall.monitoring.freshness $(MONITOR_ARGS)

run-metrics: ## Summarise recent load runs and flag failed tables or abnormal staged volumes
	$(PYTHON) -m railfall.monitoring.runs $(MONITOR_ARGS)

monitor: ## Freshness + run metrics, then one anomaly summary (LLM wording if LLM_SUMMARY_ENABLED=1)
	mkdir -p $(MONITOR_DIR)
	-$(PYTHON) -m railfall.monitoring.freshness --report $(MONITOR_DIR)/freshness.json > /dev/null
	-$(PYTHON) -m railfall.monitoring.runs --report $(MONITOR_DIR)/runs.json > /dev/null
	$(PYTHON) -m railfall.monitoring.summary --freshness $(MONITOR_DIR)/freshness.json \
		--runs $(MONITOR_DIR)/runs.json --report $(MONITOR_DIR)/summary.json $(SUMMARY_ARGS)

logs: ## Follow service logs
	$(COMPOSE) $(AIRFLOW_PROFILE) $(BI_PROFILE) logs -f

ps: ## Show service status
	$(COMPOSE) $(AIRFLOW_PROFILE) $(BI_PROFILE) ps

down: ## Stop the stack (keeps data volumes)
	$(COMPOSE) $(AIRFLOW_PROFILE) $(BI_PROFILE) down

clean: ## Stop the stack and delete its data volumes
	$(COMPOSE) $(AIRFLOW_PROFILE) $(BI_PROFILE) down -v

lint: ## Run ruff lint + format check
	ruff check .
	ruff format --check .

test: ## Run unit tests
	$(PYTHON) -m pytest -q
