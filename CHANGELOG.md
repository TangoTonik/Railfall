# Changelog

All notable changes to Railfall. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). A release is published by pushing a
`vX.Y.Z` tag; the `release` workflow refuses it unless the tag matches
`railfall.__version__` and this file has a dated section for that version.

## [Unreleased]

### Added
- Local stack in Docker Compose: Postgres warehouse, MinIO lake, Airflow,
  Metabase; `make up` starts and health-checks everything idempotently.
- Terraform (MinIO + Postgres providers) for buckets, schemas, roles and
  least-privilege pipeline identities, with a drift check.
- Ingestion of SNCF monthly TGV punctuality, SNCF stations and Météo-France
  daily climatology into bronze, validated against data contracts.
- PySpark bronze → silver jobs: typed, deduplicated tables, monthly weather by
  department and the route ↔ department mapping.
- Incremental, idempotent silver → Postgres loader with a run audit table.
- Airflow DAGs for ingestion and the full ingest → Spark → load → dbt → quality
  pipeline, with retries only for transient failures.
- dbt staging models, a star schema and weather-impact marts, with tests,
  freshness checks, docs, lineage and a generated data catalog.
- Great Expectations checkpoint as a DAG task, plus a canary proving the gate
  fails on corrupted data.
- Metabase dashboards provisioned as code and validated against the dbt manifest.
- Statistical analysis of punctuality vs rain, wind and frost (route/month
  fixed effects, bootstrap CIs, Holm correction, season and dose-response).
- Monitoring: run metrics, freshness SLAs and an optional local-LLM anomaly
  summary (off by default).
- Results page and SVG charts generated from the live e2e run.
- `e2e` workflow proving every stage on the real open data and real services.
