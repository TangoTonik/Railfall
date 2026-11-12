# Railfall

**Do French train delays follow the weather?** A data platform that tracks how rain, wind and frost hit SNCF punctuality.

> 🚧 Work in progress — day 8 of 20 done, see [ROADMAP.md](ROADMAP.md).

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

Needs Docker with Compose v2 and Terraform ≥ 1.6; the silver job also needs
Java 17+ and `pip install -e ".[spark]"`. The first `make up` builds
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
make ingest-meteo # download Météo-France daily files for METEO_DEPARTMENTS into bronze
make verify-meteo # re-check today's Météo-France bronze against manifests and contract
make silver       # Spark: clean, type and de-duplicate bronze into silver Parquet
make verify-silver # read silver back: schema contract, unique non-null keys, not empty
make load         # merge silver into the warehouse raw schema (LOAD_ARGS="--year 2025" for one year)
make verify-load  # check raw against silver: columns, primary key, no duplicates, row counts
make airflow  # build the Airflow image, migrate its metadata DB, start webserver + scheduler
make dag-run DAG=railfall_ingest  # trigger a DAG over the REST API and wait for it
make down     # stop (keeps data); `make clean` also deletes the volumes
```

| Service | URL | Default credentials (`.env`) |
|---|---|---|
| Postgres warehouse | `localhost:5432/warehouse` | `rail` / `rail` |
| MinIO S3 API | http://localhost:9000 | `railfall` / `railfall-secret` |
| MinIO console | http://localhost:9001 | same as above |
| Airflow UI / REST API (`make airflow`) | http://localhost:8080 | `admin` / `admin` |

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

```
s3://railfall-lake/bronze/meteo/daily_<dept>_<period>/ingest_date=YYYY-MM-DD/daily_<dept>_<period>.csv.gz
s3://railfall-lake/bronze/meteo/daily_<dept>_<period>/ingest_date=YYYY-MM-DD/_manifest.json
```

Météo-France BASE/QUOT `RR-T-Vent` files (rain, temperature, wind), one per
department, resolved from the data.gouv.fr catalogue and kept as the original
`csv.gz`. `METEO_DEPARTMENTS` (default `75,69,13`) picks the departments;
`python -m railfall.ingest.run meteo --department 33 --period previous` fetches
the long history for a backfill.

### Silver layout

```
s3://railfall-lake/silver/punctuality_monthly/year=YYYY/part-*.parquet
s3://railfall-lake/silver/weather_daily/department=DD/year=YYYY/part-*.parquet
s3://railfall-lake/silver/stations/part-*.parquet
s3://railfall-lake/silver/weather_monthly/year=YYYY/part-*.parquet
s3://railfall-lake/silver/route_departments/part-*.parquet
s3://railfall-lake/silver/route_weather_monthly/year=YYYY/part-*.parquet
s3://railfall-lake/silver/_rejected/<table>/run_date=YYYY-MM-DD/part-*.parquet
```

| Table | Key | Content |
|---|---|---|
| `punctuality_monthly` | month, service, departure, arrival | typed counts, `route_id`, `trains_run`, arrival punctuality rate |
| `weather_daily` | station_id, day | rain, min/max/mean temperature, wind; doubtful or impossible values nulled |
| `stations` | uic_code | name, accent-free `name_key`, department, coordinates |
| `weather_monthly` | department, month | rain total, rain / heavy-rain / frost / strong-wind days, mean temperature and wind, stations per measure |
| `route_departments` | route_id | department of each endpoint and how it was resolved (`alias`, `foreign`, `exact`, `prefix`, `unmatched`) |
| `route_weather_monthly` | month, service, route_id | punctuality joined with weather at both ends (`dep_*`, `arr_*`, `route_*`), `weather_endpoints` 0–2 |

SNCF tables are rebuilt from the newest bronze snapshot; weather reads every
bronze day and keeps the newest row per station-day. Writes overwrite
partitions, so re-running never appends. Rows with an untrustworthy key go to
`_rejected/` with a `reject_reason` instead of being dropped silently.

The three derived tables are built in the same job from silver itself, after
their inputs. `route_weather_monthly` keeps exactly the rows of
`punctuality_monthly` (left joins on unique keys); routes without weather keep
null measures and `weather_endpoints = 0` rather than disappearing.
`verify-silver` reports how many route-months got weather on 0, 1 or 2 ends.

### Warehouse `raw` layout

Every silver table is loaded into `raw.<table>` by `railfall_loader`, with the
same columns (DDL derived from the Parquet schema) and the silver business key
as primary key, plus `_loaded_at` / `_updated_at`.

Each table is merged in its own transaction: `COPY` into a temporary stage,
`INSERT ... ON CONFLICT DO UPDATE` only where a value changed, then delete the
rows of the replaced scope that silver no longer has. Re-running a load
therefore changes nothing. `--year 2025` replaces only that year's partitions;
the small reference tables (`stations`, `route_departments`) always load in full.

Every run appends one row per table to `raw._load_runs` (run id, scope,
staged / inserted / updated / unchanged / deleted rows, outcome, timings), so
"what did the last load do?" has an answer even when it changed nothing.

### Orchestration (Airflow)

Airflow 2.10 (LocalExecutor) runs behind the `airflow` compose profile, so
`make up` stays light; `make airflow` starts it once Terraform has created its
metadata database (`airflow`, its own role, no access to the warehouse). The
image is the official one plus this package, so tasks run the same code as the
`make` targets.

| DAG | Schedule | Tasks |
|---|---|---|
| `railfall_ingest` | daily 05:15 UTC | `ingest_sncf → verify_sncf`; `ingest_meteo[dept]` (mapped over `METEO_DEPARTMENTS`) `→ verify_meteo` |

Each task returns a JSON summary per dataset as its XCom and fails only after
trying every dataset; retries are safe because bronze landing is idempotent.
To force a partition and keep the run report with each task's result:

```bash
make dag-run DAG=railfall_ingest \
  DAG_ARGS="--conf '{\"ingest_date\": \"2026-11-12\"}' --xcom --report run.json"
```

## How this project is built

This project is built with an AI pair-programmer (Claude) in a deliberate,
reviewable workflow — see [docs/AI_WORKFLOW.md](docs/AI_WORKFLOW.md).
Every design choice is logged in [docs/DECISIONS.md](docs/DECISIONS.md).

## Project layout

```
src/railfall/       Python package (config, ingestion, Spark silver jobs, warehouse load)
dags/               Airflow DAGs (thin wiring over railfall.orchestration)
tests/              Unit tests (pytest)
infra/terraform/    Terraform: lake bucket, IAM, warehouse schemas and roles
infra/minio/        Source build of the pinned MinIO image
infra/airflow/      Airflow image: official image + the railfall package
docs/               Architecture decisions and workflow
```
