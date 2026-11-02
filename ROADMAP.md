# Roadmap

One month, 20 working days, ~6 h of human-equivalent work per day.
Each day ends with one push. A day is **done** when its box is checked here.

## Week 1 · Foundations & ingestion
- [x] Day 1 — Repo, README, architecture plan, Python project setup, pre-commit, CI (GitHub Actions)
- [ ] Day 2 — Docker Compose stack: Postgres warehouse + MinIO (local S3-compatible lake)
- [ ] Day 3 — Terraform: provision MinIO buckets and Postgres schemas as code
- [ ] Day 4 — Ingestion: SNCF punctuality API → raw JSON/Parquet in the lake (retries, logging)
- [ ] Day 5 — Ingestion: Météo-France weather data → lake (TDD with fixtures)

## Week 2 · Processing & orchestration
- [ ] Day 6 — PySpark: clean and normalize raw data into the silver layer
- [ ] Day 7 — PySpark: join trains × weather by region/date, partitioned output
- [ ] Day 8 — Load silver into Postgres, incremental and idempotent
- [ ] Day 9 — Airflow in Docker: first ingestion DAG
- [ ] Day 10 — Full Airflow DAG: ingest → Spark → load, with backfill, retries, alerts

## Week 3 · Modeling & quality
- [ ] Day 11 — dbt project: sources and staging models
- [ ] Day 12 — dbt: star schema (fact_delays, dim_station, dim_weather, dim_date)
- [ ] Day 13 — dbt marts: delay rate by weather type, region, season
- [ ] Day 14 — Data quality: dbt tests + Great Expectations checks in the DAG
- [ ] Day 15 — dbt docs, lineage graph, data catalog in the README

## Week 4 · Delivery & polish
- [ ] Day 16 — Metabase dashboards on the marts
- [ ] Day 17 — Statistical analysis: does weather significantly affect delays?
- [ ] Day 18 — Monitoring + LLM anomaly summaries (behind a flag, no-AI fallback)
- [ ] Day 19 — Full CI: lint, unit tests, dbt build on sample data
- [ ] Day 20 — Polish: architecture diagram, results write-up, demo, v1.0.0 release
