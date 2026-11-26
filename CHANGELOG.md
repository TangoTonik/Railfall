# Changelog

All notable changes to Railfall. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). A release is published by pushing a
`vX.Y.Z` tag; the `release` workflow refuses it unless the tag matches
`railfall.__version__` and this file has a dated section for that version.

## [Unreleased]

## [1.0.0] - 2026-11-26

First complete release: every stage runs on the real SNCF and Météo-France open
data inside the local stack, proven end to end by the `e2e` workflow. The
results page and charts of the tagged commit's e2e run are attached to the
GitHub release.

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
- Statistical analysis of punctuality vs rain, wind and frost (gaps stratified
  by route x service x season, within-strata permutation tests, route-level
  bootstrap CIs, Benjamini-Hochberg correction, season and dose-response).
- Monitoring: run metrics, freshness SLAs and an optional local-LLM anomaly
  summary (off by default).
- Results page and SVG charts generated from the live e2e run, attached to
  each GitHub release.
- Release workflow: a `vX.Y.Z` tag is checked against the package version and
  this changelog, then published with the wheel, sdist and live results.
- `e2e` workflow proving every stage on the real open data and real services.

[Unreleased]: https://github.com/TangoTonik/Railfall/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/TangoTonik/Railfall/releases/tag/v1.0.0
