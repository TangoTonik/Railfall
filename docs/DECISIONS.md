# Decision log

## Day 1 — Project foundations

**Medallion lake (bronze / silver / gold).**
Raw API payloads land untouched in *bronze*, so any bug downstream can be fixed
and replayed without re-calling the APIs. *Silver* holds cleaned, typed data;
*gold* lives in the warehouse as dbt models.

**MinIO instead of a cloud bucket.**
Same S3 API as AWS, free and local. Switching to real S3 is a config change
(`LAKE_ENDPOINT_URL`), not a code change.

**Config from environment variables.**
Twelve-factor style: the same code runs in tests, Docker and CI. Secrets never
enter the repo.

**`src/` layout + `pyproject.toml`.**
Prevents tests from accidentally importing the working directory instead of the
installed package; one file configures build, lint and tests.

**Ruff + pre-commit + GitHub Actions from day 1.**
Quality gates are cheapest to add before there is code to fix.

## Day 2 — Local stack and the `e2e` workflow

**Docker Compose with pinned images, health-gated startup.**
Postgres (warehouse) and MinIO (lake) start with `docker compose up --wait`, so
nothing runs against a half-booted service. Data lives in named volumes: `make
down` keeps it, `make clean` wipes it.

**MinIO built from pinned source.**
The first e2e runs failed because MinIO no longer publishes community images
(Docker Hub denies `minio/minio`, quay.io is unauthorized). Rather than depend on
an unofficial mirror, `infra/minio/Dockerfile` builds the exact server and `mc`
releases from upstream tags. Slower first build, but reproducible and free.

**Bucket creation as a one-shot `minio-init` job.**
Infrastructure setup belongs to the stack, not to pipeline code. The job uses
`mc mb --ignore-existing`, and e2e runs it twice to prove it is idempotent.
Day 3 moves this to Terraform.

**Health checks in Python, not only in Compose.**
`railfall.healthcheck` uses the same `Settings` as the pipelines, so it proves the
*application* can reach the services with its own credentials. `--write` adds a
real round-trip (object put/get/delete, temp-table insert/count) without leaving
data behind.

**e2e drives the stack through `make`.**
CI and local development use the same entry points (`make up`, `health`,
`smoke`), so the workflow doubles as tested documentation. A restart step
(`down` + `up` without init) asserts the bucket survives on its volume.

## Day 3 — Infrastructure as code with Terraform

**Terraform owns provisioning; Compose only runs the services.**
The `minio-init` job is gone. `infra/terraform` creates the lake bucket (with
`bronze/` and `silver/` layer prefixes), a bucket-scoped MinIO policy and
pipeline user, and in Postgres the `raw` / `staging` / `marts` schemas plus one
login role per actor (`railfall_loader`, `railfall_transformer`,
`railfall_reader`). `make up` boots the containers, then runs `terraform apply`,
so there is still a single entry point.

**Community providers, pinned to a patch series.**
`aminueza/minio` and `cyrilgdn/postgresql` are the de-facto providers for these
services. Constraints are `~> x.y.0` and CI pins Terraform itself (1.9.8).
`.terraform.lock.hcl` is not committed yet: it should be generated for every
platform (`terraform providers lock`) and checked in as a follow-up so laptops
and CI verify the same provider hashes.

**Local state, not a remote backend.**
The stack is ephemeral and single-user; a remote backend (S3 on MinIO itself)
would be a chicken-and-egg problem and adds nothing here. State files are
gitignored.

**Secrets only through `TF_VAR_*`.**
No `tfvars` file is committed; the only variable defaults are non-secret local
values, and validation blocks reject bad bucket names and short MinIO secrets
before anything is applied.

**Each writer owns its schema; reads go through default privileges.**
Ownership lets the loader and dbt create/replace tables without per-table
grants; `postgresql_default_privileges` makes future tables readable by the
next actor in the chain without a re-apply. `destroy` protections
(`force_destroy = false`, `drop_cascade = false`) keep data safe.

**Proving it in e2e, three ways.**
1. *Drift:* `terraform plan -detailed-exitcode` must be empty after apply, after
   a re-apply and after a stack restart (the Terraform wrapper is disabled so
   exit code 2 is not swallowed).
2. *Behaviour:* `railfall.access` connects as every identity and checks allowed
   actions succeed and forbidden ones fail with a real permission error — an
   outage can never pass as "denied".
3. *Use:* the application itself now connects as the pipeline user and the
   loader role (`.env.example`), and the smoke round-trip asserts it ran as
   `railfall_loader`. Admin credentials are used by Terraform only.

Unit tests parse the HCL with `python-hcl2` so the layout, grants and secret
hygiene are checked on every push without a Terraform binary.

## Day 4 — SNCF ingestion into bronze

**Full JSON export, not paged `records`.**
Both SNCF datasets are small (~10k punctuality rows, ~2.8k stations), so one
call to the Opendatasoft `exports/json` endpoint returns everything. The
`records` endpoint caps at 100 rows per page and a 10k offset, which would mean
pagination code that buys nothing. The downloader is stdlib `urllib` with
timeouts and exponential backoff on 408/425/429/5xx and connection resets;
other 4xx fail fast because they are bugs in our request.

**Validate before landing, store byte-for-byte.**
Each export is checked against a small per-dataset contract (required fields,
types, a row floor that catches truncated downloads) before it touches the
lake, so bronze never holds an error page or half a file. What lands is the raw
payload, unchanged: bronze is the replayable source of truth, cleaning is
silver's job (Day 6).

**The contract is structural, not a data-quality gate.**
The first real e2e run rejected the punctuality export over two rows with a
*negative* `retard_moyen_arrivee` (-30.5 and -40.1 min). The AQST average runs
over all trains, early ones included, so a negative mean is a legitimate value
(even if those two look like outliers). Bronze now only requires a finite
number there; counts must still be non-negative integers-or-null and booleans
are rejected outright. Plausibility rules (outlier delays, cancellations above
planned trains) belong in silver and in the Day 14 quality checks, where a bad
row can be flagged instead of blocking a whole dataset.

**One version per dataset per day, idempotent by content hash.**
Layout: `bronze/sncf/<dataset>/ingest_date=YYYY-MM-DD/<dataset>.json` plus a
`_manifest.json` (rows, SHA-256, source URL, landed-at). Re-running on the
same day with the same bytes is a no-op (`skipped`); different bytes (the
source was republished) replace that day's object. Day-partitioned history
keeps earlier snapshots for replays without duplicating within a day.

**Proven on the real portal in e2e.**
The workflow downloads both datasets as the least-privilege `railfall-pipeline`
lake user, asserts row floors (≥ 5,000 punctuality, ≥ 2,000 stations), reads
bronze back with `verify-sncf` (manifest hash = object hash = contract still
holds), then re-runs ingestion and asserts the prefix still holds exactly one
payload and one manifest per dataset.

## Day 5 — Météo-France ingestion into bronze

**Resolve file URLs from the data.gouv.fr catalogue, never hard-code them.**
BASE/QUOT files are named after their period (`Q_75_latest-2025-2026_RR-T-Vent.csv.gz`,
`Q_75_previous-1950-2024_…`), and those bounds move every January. A hard-coded
URL would silently 404 once a year. Ingestion makes one call to the dataset API,
parses every resource name with a strict regex and picks the department/period
it needs; a missing department is a `ResolveError` reported per department.

**Only the `RR-T-Vent` files.**
Rain (`RR`), temperature (`TN`/`TX`/`TM`) and wind (`FFM`/`FXY`) are exactly
what the punctuality-vs-weather question needs. The `autres-parametres` files
(sunshine, humidity, snow…) roughly double the volume for columns no planned
model uses; adding them later is a new resource filter, not a redesign.

**`latest` by default, `previous` on demand.**
The `latest` file (current + previous year) is refreshed daily and small enough
for e2e; the `previous` history goes back to 1950 and is many times larger per
department. The SNCF punctuality series starts in 2018, so the history is only
needed for a backfill: `--period previous` is there for that, not for the
daily run.

**Three departments by default (`METEO_DEPARTMENTS=75,69,13`).**
Paris, Lyon and Marseille are the endpoints of the busiest TGV axis, so they
give Day 7's route ↔ department join something real to match while keeping a
local run and e2e at three files instead of ~100. The full list is one env var
away.

**Same bronze contract as SNCF: validate, then store byte-for-byte.**
Each file must be a complete gzip with the 58-column header, contain the
columns silver needs, have every `NUM_POSTE` start with the requested
department's prefix (Corsica's 2A/2B stations still use `20`), hold numbers or
blanks in the measure columns (rain-gauge-only stations leave temperature and
wind empty, which is legitimate) and clear a 1,000-row floor. The original
`csv.gz` lands unchanged under
`bronze/meteo/daily_<dept>_<period>/ingest_date=YYYY-MM-DD/` with a manifest,
reusing Day 4's content-hash idempotency. One bad department is reported
without blocking the others.

**Proven on real data in e2e.**
The workflow downloads the live files for 75, 69 and 13 as the pipeline
identity, reads them back with `verify-meteo` (hash, size, contract against the
department the object is stored under), re-runs ingestion and asserts exactly
one payload and one manifest per department, then re-verifies both sources
after a stack restart.

## Day 6 — PySpark: bronze → silver

**Plain DataFrame transforms, one module per table, valid/rejected split.**
`punctuality`, `weather` and `stations` each expose `read_bronze` and a pure
`to_silver(df) -> Split(valid, rejected)`. The transforms never touch storage,
so they run on fixtures with a one-core local session in unit tests and on the
real lake in e2e unchanged. Rows whose key cannot be trusted (bad month or day,
missing station, station outside its department, impossible counts) go to
`silver/_rejected/<table>/run_date=…` with a `reject_reason`; individual
measures that are doubtful (Météo-France quality flag 2) or physically
impossible are nulled instead, so one bad sensor does not cost the whole day.

**Explicit schemas, no inference.** Bronze JSON is read with a declared schema
and Météo-France CSVs as strings cast column by column: inference guessed
different types for the same column from one department to the next.

**Which bronze to read.** SNCF exports are full snapshots, so silver is rebuilt
from the newest `ingest_date` only; older days are history, not extra rows.
Météo-France `latest`/`previous` files overlap, so every day is read and one
row per station-day is kept (prefer `latest`, then newest ingest).

**Idempotency by partition overwrite.** `partitionOverwriteMode=dynamic`
(punctuality by year, weather by department × year) means a run replaces the
partitions it produces and nothing else. No bookkeeping table is needed for a
re-run to be harmless; e2e proves it by running the job twice.

**S3A against MinIO, hadoop-aws pulled at start-up.** Spark 4.0 ships Hadoop
3.4.1, so the matching `hadoop-aws` package is fetched through
`spark.jars.packages` rather than vendoring jars; path-style access and SSL
only for `https://` endpoints. The job runs as the least-privilege
`railfall-pipeline` user, whose bucket policy already allows the
put/delete that S3A's commit needs.

**An independent read-back check (`make verify-silver`).** The job's own
summary could agree with itself while the files are wrong, so a separate
command re-reads the Parquet and checks a per-table contract: required columns
with exact Spark types, business key never null and never repeated, table not
empty. e2e asserts row floors on the real data, that silver punctuality never
exceeds the bronze rows it came from, that a second run yields identical counts
with keys still unique, and that silver survives a stack restart.

**JSON reports go to a file (`--report`).** The JVM started by PySpark inherits
stdout, so Ivy/Spark output could interleave with a printed JSON summary; e2e
reads the report file instead of parsing stdout.

