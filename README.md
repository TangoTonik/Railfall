# Railfall

**Do French train delays follow the weather?** A data platform that tracks how rain, wind and frost hit SNCF punctuality.

> 🚧 Work in progress — day 1 of 20 done, see [ROADMAP.md](ROADMAP.md).

## Architecture (target)

```
 SNCF open data ─┐                                   ┌─► dbt (staging → star schema → marts)
                 ├─► Python ingestion ─► MinIO lake ─┤                 │
 Météo-France  ──┘     (bronze)        Spark (silver)└─► Postgres ─────┴─► Metabase dashboards
                                                          warehouse
            └──────────── orchestrated by Airflow · quality gates: dbt tests + Great Expectations ───┘
```

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python (`httpx`, retries) | Simple, testable, API-agnostic |
| Data lake | MinIO (S3-compatible) | Free locally, same API as AWS S3 |
| Processing | PySpark | Scales to the full historical dataset |
| Warehouse | PostgreSQL | Standard SQL, easy to run anywhere |
| Modeling | dbt | Versioned, tested SQL transformations |
| Orchestration | Airflow | Industry standard scheduler |
| Infra | Docker Compose, Terraform | Reproducible, infrastructure as code |
| CI | GitHub Actions | Lint + tests on every push |

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
pytest
```

### Local platform (Postgres + MinIO)

Needs Docker with Compose v2. The first `make up` builds MinIO from pinned
source (no public images exist any more), which takes a few minutes.

```bash
make up       # build, start Postgres + MinIO, wait until healthy, create the lake bucket
make health   # check every service answers
make smoke    # also write to the lake and warehouse and read back
make down     # stop (keeps data); `make clean` also deletes the volumes
```

| Service | URL | Default credentials (`.env`) |
|---|---|---|
| Postgres warehouse | `localhost:5432/warehouse` | `rail` / `rail` |
| MinIO S3 API | http://localhost:9000 | `railfall` / `railfall-secret` |
| MinIO console | http://localhost:9001 | same as above |

## How this project is built

This project is built with an AI pair-programmer (Claude) in a deliberate,
reviewable workflow — see [docs/AI_WORKFLOW.md](docs/AI_WORKFLOW.md).
Every design choice is logged in [docs/DECISIONS.md](docs/DECISIONS.md).

## Project layout

```
src/railfall/   Python package (config, ingestion, processing)
tests/              Unit tests (pytest)
docs/               Architecture decisions and workflow
```
