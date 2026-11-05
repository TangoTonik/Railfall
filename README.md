# Railfall

**Do French train delays follow the weather?** A data platform that tracks how rain, wind and frost hit SNCF punctuality.

> 🚧 Work in progress — day 4 of 20 done, see [ROADMAP.md](ROADMAP.md).

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

Needs Docker with Compose v2 and Terraform ≥ 1.6. The first `make up` builds
MinIO from pinned source (no public images exist any more), which takes a few
minutes.

```bash
make up       # build, start Postgres + MinIO, wait until healthy, terraform apply
make health   # check every service answers
make access   # prove each pipeline identity can do its job and nothing more
make infra-check  # fail if the running stack drifted from the Terraform config
make smoke    # also write to the lake and warehouse and read back
make ingest-sncf  # download SNCF punctuality + stations into bronze (idempotent per day)
make verify-sncf  # re-check today's bronze objects against their manifests and contract
make down     # stop (keeps data); `make clean` also deletes the volumes
```

| Service | URL | Default credentials (`.env`) |
|---|---|---|
| Postgres warehouse | `localhost:5432/warehouse` | `rail` / `rail` |
| MinIO S3 API | http://localhost:9000 | `railfall` / `railfall-secret` |
| MinIO console | http://localhost:9001 | same as above |

Those are the admin accounts, used only by Terraform. Terraform
(`infra/terraform`) provisions what the pipelines actually connect with:

| Resource | Provisioned by Terraform |
|---|---|
| Lake bucket `railfall-lake` | `bronze/`, `silver/` prefixes; user `railfall-pipeline` limited to this bucket |
| Warehouse schemas | `raw` (owner `railfall_loader`), `staging` + `marts` (owner `railfall_transformer`) |
| Read access | transformer reads `raw`; `railfall_reader` (dashboards) reads `marts` only |

### Bronze layout

```
s3://railfall-lake/bronze/sncf/<dataset>/ingest_date=YYYY-MM-DD/<dataset>.json
s3://railfall-lake/bronze/sncf/<dataset>/ingest_date=YYYY-MM-DD/_manifest.json
```

Raw exports are stored byte-for-byte after a schema check; the manifest records
row count, SHA-256 and source URL. Datasets: `punctuality`
(`regularite-mensuelle-tgv-aqst`) and `stations` (`gares-de-voyageurs`).

## How this project is built

This project is built with an AI pair-programmer (Claude) in a deliberate,
reviewable workflow — see [docs/AI_WORKFLOW.md](docs/AI_WORKFLOW.md).
Every design choice is logged in [docs/DECISIONS.md](docs/DECISIONS.md).

## Project layout

```
src/railfall/   Python package (config, ingestion, processing)
tests/              Unit tests (pytest)
infra/terraform/    Terraform: lake bucket, IAM, warehouse schemas and roles
infra/minio/        Source build of the pinned MinIO image
docs/               Architecture decisions and workflow
```
