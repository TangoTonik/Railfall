# Roadmap

One month, 20 working days, ~6 h of human-equivalent work per day.

**Definition of done for every day:** the feature works on *real data* and
*real services*, proven by the `e2e` GitHub Actions workflow (Docker Compose
stack + real downloads), not only by unit tests on fixtures.

## Data sources (real, free, no API key)
- **SNCF — Régularité mensuelle TGV par liaisons** (monthly punctuality per TGV
  route, with delay-cause percentages), ODbL — ressources.data.sncf.com,
  dataset `regularite-mensuelle-tgv-aqst`.
- **SNCF — Gares de voyageurs** (station → coordinates / department), to place
  routes geographically.
- **Météo-France — Données climatologiques de base, quotidiennes**
  (per-department `csv.gz`: precipitation, wind, temperature, frost) —
  meteo.data.gouv.fr / data.gouv.fr.

Train data is monthly per route; weather is daily per station. The platform
aggregates weather to month × department and links routes to the departments
of their endpoints.

## Week 1 · Foundations & ingestion
- [x] Day 1 — Repo, README, architecture plan, Python project setup, CI (lint + unit tests)
- [x] Day 2 — Docker Compose stack (Postgres + MinIO) + `e2e` workflow that boots it in GitHub Actions and health-checks it
- [x] Day 3 — Terraform (MinIO + Postgres providers) applied inside the e2e workflow
- [x] Day 4 — SNCF ingestion (punctuality + stations) → bronze in MinIO; e2e downloads the real data and asserts row counts / schema
- [x] Day 5 — Météo-France ingestion (daily, per department) → bronze; e2e on 2–3 real departments

## Week 2 · Processing & orchestration
- [x] Day 6 — PySpark: clean and type bronze → silver (Parquet), run in e2e on the real bronze data
- [x] Day 7 — PySpark: monthly weather by department, route ↔ department mapping, joined silver table
- [ ] Day 8 — Incremental, idempotent load silver → Postgres; e2e runs it twice and asserts no duplicates
- [ ] Day 9 — Airflow in Docker Compose: ingestion DAG
- [ ] Day 10 — Full DAG ingest → Spark → load, triggered and awaited in e2e until success

## Week 3 · Modeling & quality
- [ ] Day 11 — dbt (dbt-postgres): sources and staging models, `dbt build` in e2e
- [ ] Day 12 — dbt star schema: fact_route_month, dim_route, dim_department, dim_month, weather facts
- [ ] Day 13 — dbt marts: punctuality vs rain / wind / frost, by region and season
- [ ] Day 14 — Data quality: dbt tests + Great Expectations checkpoint as a DAG task
- [ ] Day 15 — dbt docs + lineage published as a CI artifact; data catalog in README

## Week 4 · Delivery & polish
- [ ] Day 16 — Metabase in Docker Compose with dashboards provisioned as code
- [ ] Day 17 — Statistical analysis on the real marts: does bad weather significantly lower punctuality?
- [ ] Day 18 — Monitoring (run metrics, freshness checks) + optional LLM anomaly summaries (off by default)
- [ ] Day 19 — Hardening: retries, failure scenarios tested in e2e, `make up` one-command start
- [ ] Day 20 — Final write-up with real results and charts, architecture diagram, v1.0.0 release
