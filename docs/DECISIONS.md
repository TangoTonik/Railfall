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


## Day 7 — Monthly weather, route ↔ department mapping, joined silver table

**Meet punctuality at its grain: department-month.** SNCF publishes TGV
punctuality per route and month, so daily weather is rolled up to
`weather_monthly` in two steps. Each station's month is summarised first, with
day counts scaled to the month from the days it reported and the measure
dropped below 80 % coverage (scaling a half-empty month only amplifies noise,
and the month still in progress falls out by itself). Covered station-months
are then averaged per department, keeping how many stations each measure rests
on, so a department-month built on one rain gauge does not look as solid as
Paris. Thresholds follow Météo-France conventions (rain day ≥ 1 mm, heavy rain
≥ 10 mm, frost TN < 0 °C); strong wind is FXY ≥ 10.8 m/s (Beaufort 6) because
gusts are not in the files silver keeps.

**Route mapping by deterministic rules, never by fuzzy matching.** AQST names
(`MARSEILLE ST CHARLES`) and SNCF names (`Marseille Saint-Charles`) are folded
to a common key and resolved in order: a small curated alias table, a list of
foreign stations (no department on purpose), exact key, then prefix — accepted
only if every candidate station lies in the same department. Fuzzy string
similarity was rejected: a wrong department would silently attach the wrong
weather, which is worse than a null. Every endpoint records its
`match_method`, and `unmatched` ones stay in the table so coverage is
measurable.

**The join cannot change the punctuality row count.** `route_weather_monthly`
uses only left joins on keys unique on the right (`route_id`,
`(department, month)`), so it has exactly the grain and rows of
`punctuality_monthly`. Weather is kept per endpoint (`dep_*`, `arr_*`) plus a
route-level `route_*` value: mean of the covered ends, max/min for extremes. The
two ends are a proxy for the line, not a measurement of it; keeping both lets
the dbt models choose a different combination later.

**Derived tables are built from silver, not from bronze.** They run in the same
Spark job after their inputs, reading silver back with typed partitions. They
reuse the cleaned, de-duplicated data and its contracts instead of repeating the
bronze logic, and partition overwrite keeps the re-run idempotent.

**Prove the join matches, not just that it keeps its grain.** Equal row counts
would also pass with every weather column null, so `verify-silver` profiles
`weather_endpoints` and e2e asserts on the real lake that at least 12
route-months have weather at both ends and 24 at one or more (Paris, Lyon and
Marseille are ingested), that the profile sums to the row count, and that a
second run gives identical counts with unique keys.


## Day 8 — Incremental, idempotent load silver → Postgres

**Postgres mirrors silver, derived not hand-written.** The `raw` DDL is generated
from each silver table's Spark schema with an explicit type map; an unknown Spark
type is refused instead of widened. Only the primary key comes from the silver
contract, because Parquet has no notion of one. New silver columns are added on
the next load; a column whose type changed fails the load rather than being
altered behind the transformer's back.

**Merge, not truncate-and-insert.** Each table is loaded in one transaction:
`COPY` into a temporary stage, `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE
IS DISTINCT FROM`, then delete the rows of the replaced scope that the stage
lacks. Truncate-and-insert was simpler but leaves the table empty to readers
mid-load and makes `_updated_at` meaningless. With the merge, a re-run of the
same silver inserts, updates and deletes nothing, and the primary key makes a
duplicate impossible rather than merely unlikely.

**Incremental by year partition.** `--year` filters the year-partitioned tables
and replaces only those partitions, which is how a late correction from SNCF or
Météo-France gets in without reloading history. Reference tables (`stations`,
`route_departments`) always load in full, so a route-month never points at a
route the warehouse lacks. Spark reads silver and the rows are streamed to
Postgres with psycopg `COPY` from the driver: the data is a few hundred thousand
rows, and a JDBC writer would add a driver jar without giving us `ON CONFLICT`.

**An append-only audit log.** An idempotent re-run leaves no trace in the data,
so every run appends one row per table to `raw._load_runs` (scope, counts,
outcome, timings) in autocommit, so a table that rolled back is still recorded
as failed. The loader role does not create the `raw` schema: Terraform owns it,
and `create schema if not exists` needs CREATE on the database even when the
schema exists (found by e2e run 38).

**Proof on the real warehouse.** `verify-load` checks `raw` against silver
independently of the loader: columns and types from the contract, primary key,
no null or repeated key, and row counts per table and per year. e2e loads the
real silver, verifies, loads again and asserts zero inserts/updates/deletes and
an identical report, checks each run got its own `run_id`, then deletes the
latest punctuality year and repairs it with `load --year`, which must re-insert
exactly the dropped rows.

## Day 9 — Airflow in Docker Compose: ingestion DAG

**Airflow 2.10 with the LocalExecutor, behind a compose profile.** One scheduler
and one webserver are enough for a daily DAG of a handful of tasks; Celery would
add Redis and workers for no gain. Airflow 3 was considered and left for later:
its API and provider ecosystem were still moving, and the DAG only uses the
TaskFlow API, which carries over. The services sit behind the `airflow` profile
so `make up` stays light and the Spark stages of e2e keep their memory; `make
airflow` starts them after Terraform has run.

**Terraform owns Airflow's metadata database too.** Airflow gets its own
database and role on the existing Postgres server, provisioned like the
warehouse roles, instead of a second Postgres container or Airflow's
`create-db` with the admin account. The `airflow` role has no grant on the
warehouse; the pipeline identities used by the tasks are the same ones the CLI
uses (`railfall-pipeline` on the lake).

**The image is the official one plus the railfall package.** It is installed
against Airflow's published constraints so the two dependency trees cannot
drift silently, and the DAG file stays a thin wiring layer: every task calls
`railfall.orchestration.ingest`, which wraps the same functions as `make
ingest-*` / `make verify-*` and is unit-tested without Airflow. A test parses
the DAG with `ast` so a renamed callable breaks CI rather than the scheduler;
the DagBag tests run only where Airflow is installed.

**Failures are partial, retries are safe.** A task tries every dataset, returns
a JSON summary (its XCom) and raises only at the end if any failed, so one bad
department does not hide the state of the others. Météo-France is a mapped task
over `METEO_DEPARTMENTS`, one retryable instance per department. Two retries with
exponential backoff are cheap because bronze landing is idempotent: identical
content is skipped, so a retry or a re-triggered day never duplicates a file.

**e2e drives the DAG through the REST API, not the CLI.** `railfall.orchestration.airflow_api`
(also `make dag-run`) triggers a run with a forced `ingest_date`, polls until a
terminal state with a timeout, and writes a report with every task instance and
its XCom. e2e runs the DAG on the real SNCF and Météo-France sources after the
CLI ingestion of the same day and asserts: run `success`, every task (including
the three mapped departments) `success`, and every dataset reported as skipped
or replaced, never newly duplicated; bronze is then re-verified. The Airflow
containers are removed afterwards so Spark gets the runner's memory back.

## Day 10 — Full DAG: ingest → Spark → load

**One DAG for the whole chain, ingestion DAG kept.** `railfall_pipeline` repeats
the ingestion tasks of `railfall_ingest` and adds `build_silver → verify_silver →
load_warehouse → verify_warehouse`. Splitting into DAGs linked by datasets or
`TriggerDagRunOperator` was considered; one DAG keeps a day's lineage in a single
run, a single retry surface and a single report for e2e. `railfall_ingest` stays
for re-landing bronze without paying for Spark.

**Spark runs in local mode inside the Airflow worker.** A Spark standalone
cluster (or `SparkSubmitOperator`) would add two services and a submit path for
a dataset that fits in one JVM. The image carries Java 17 and PySpark 4.0 —
installed after, and outside, Airflow's constraints, which pin 3.5 for a provider
we don't use — and resolves the hadoop-aws jars at build time so a task never
waits on Maven Central. Each Spark task opens and always stops its own session;
the task callables in `railfall.orchestration.pipeline` are unit-tested with an
injected session and connection.

**Spark waits for both bronze checks.** A silver build on a half-landed day would
publish partial months, so `build_silver` depends on `verify_sncf` *and*
`verify_meteo`. Spark tasks get a 60-minute `execution_timeout` (the others 30);
retries stay safe because silver overwrites partitions and the load merges on keys.

**e2e proves convergence, not just success.** The DAG is triggered after the CLI
chain has already filled the lake and warehouse, so a correct run must change
nothing: no new bronze, silver counts equal to the CLI build, a merge with zero
inserts/updates/deletes and the same per-year warehouse counts. If a source
republished in between, the internal checks (staged = built = loaded, unique
keys) still have to hold. The warehouse is then re-read with the CLI after the
Airflow containers are gone.

**Timetable runs are switched off in e2e.** Unpausing a daily DAG with
`catchup=False` still creates a run for the latest interval; it would queue next
to the triggered run or keep writing silver after the awaited one finished.
`AIRFLOW_USE_JOB_SCHEDULE` maps to `[scheduler] use_job_schedule` (on by
default); e2e boots Airflow with it off so only the runs it triggers execute.

## Day 11 — dbt sources and staging models

**dbt-postgres on the existing warehouse, as its own role.** dbt runs as
`railfall_transformer`, which reads `raw` and owns `staging`/`marts` but cannot
create schemas. Terraform keeps owning the schemas, and a `generate_schema_name`
override makes dbt build into `staging` verbatim instead of `staging_staging`.
Every profile value comes from `DBT_*` variables, defaulting to the local stack.

**dbt runs in-process, not as a subprocess.** `railfall.transform.dbt` wraps
`dbtRunner` and returns a typed per-node report (status, failures, timings) that
the CLI, the future Airflow tasks and e2e share. dbt can report `success` with
skipped nodes; the wrapper treats `skipped` as a failure so a silent partial
build cannot pass. Node results are unit-tested with dbt-shaped fakes, and one
test runs the real `dbt parse`, which needs no database.

**Staging = views, one per raw table, no joins and no filters.** Silver already
de-duplicates and rejects impossible rows; staging only types, renames, lists
columns explicitly (a new raw column must be opted into) and derives obvious
rates. Views cost nothing to rebuild and can never be staler than raw. Because
staging is 1:1, `make verify-staging` checks from outside dbt that each view
holds exactly the loader's raw row count with a unique, non-null grain; a dbt
bug cannot then pass its own tests unnoticed.

**Tests mirror the silver contract, with one deliberate warning.** Sources carry
the silver keys (`not_null`, unique grain); models re-check Spark's physical
bounds (a unit test keeps the YAML and `railfall.spark.weather.BOUNDS` in sync),
accepted values and a cross-column consistency test. Two small generic tests
(`accepted_range`, `unique_combination_of_columns`) live in the project rather
than pulling `dbt_utils`: no package download in CI, and both are a few lines.
AQST cause shares are only range-checked with `severity: warn`, since the
published percentages are rounded and occasionally drift past 100; e2e prints
those warnings and fails if any *other* test warns.

**Freshness is checked, and strict in e2e.** Sources declare `warn_after` 7 days
and `error_after` 40 days on the loader's `_updated_at` (the SNCF data is
monthly, so a week without a load is worth a look, a missed month is an error).
`make dbt-freshness` runs it; e2e requires every raw source to `pass` right after
the DAG has loaded, then builds staging twice and asserts identical nodes,
results and row counts to prove the build is idempotent.

## Day 12 — dbt star schema

**Two facts at their natural grains, not one wide table.** `fact_route_month`
(route × service × month) and `fact_weather_department_month` (department ×
month) share `dim_month` and `dim_department`; a route reaches the weather
through the departments of its endpoints in `dim_route`. Joining weather onto
every route-month in the core layer would duplicate each department's weather
across dozens of routes and bake in one join rule (departure? arrival? both?)
before the analysis marts have decided it. Day 13's marts make that choice.

**Additive measures only in facts.** Counts and a total of arrival-delay minutes
are stored, so a region or season mart recomputes rates and weighted averages
from sums instead of averaging averages. Rates are kept on the fact for
convenience but never aggregated.

**Departments are a seed, regions come with them.** The department → region
mapping (INSEE codes, post-2016 regions) is small, stable reference data, so it
is versioned as `seeds/departments.csv` rather than downloaded. `dim_route`
keeps routes whose endpoints Spark could not place, with a null department and
`match = unmatched`, so the fact never loses a route-month to an inner join.

**Climatology from the warehouse itself.** Weather anomalies are computed
against each department's own mean for that calendar month over the years
loaded. It is not a 30-year WMO normal, but it is one consistent baseline for
every route and needs no extra data source.

**Row preservation is tested twice.** Singular dbt tests assert each fact holds
every staging row through its dimension joins, and `verify --layer marts`
re-checks tables, unique grain and fact-vs-staging row counts as the transformer
role, outside dbt. e2e builds the marts twice and requires identical nodes,
statuses and row counts, and re-verifies them after the stack restart.

**`buildable` indirect selection.** The first e2e run with marts failed:
dbt's default `eager` selection ran the marts' singular tests (which read a
staging view *and* a fact) during `--select staging`, before the facts existed.
The runner now passes `--indirect-selection buildable` on selected
`build`/`test` runs: a test runs only when all its parents are selected or
upstream of the selection, so staging builds alone and `--select marts` still
runs the tests that also read staging. A unit test resolves the real graph with
`dbt ls` to pin this.

## Day 13 — dbt marts: punctuality vs rain / wind / frost

**The worse end of the route decides.** A TGV is exposed to the weather where it
leaves and where it arrives, so `int_route_month_weather` joins both endpoint
departments and keeps the worse value (wet if either end is wet, the larger
rain ratio, the more frost and windy days). Averaging the two ends would dilute
a storm at one terminus; the departure end alone would ignore half the line.
Routes without weather at either end are kept as `unknown` so the coverage of
the comparison stays visible, and only the comparisons filter them out.

**Three marts, three questions.** A binary adverse-vs-normal mart per factor
(the headline), the same split by region × meteorological season (frost in an
Alpine winter is not rain in a Breton autumn), and a graded intensity mart for
dose-response. The factors overlap on purpose (a month can be wet and windy):
each factor is its own comparison rather than one exclusive label, which would
hide wind behind frost. The single label is still on the intermediate model for
dashboards that need one colour per route-month.

**Rates from counts, through one macro.** Every mart sums trains and delay
minutes and recomputes rates with `punctuality_rates()`, so a route with 600
trains weighs 600 times a route with one; the macro came out of a refactor once
the second mart duplicated the formulas.

**Band edges reuse the flag thresholds.** The intensity bands include the
thresholds of the star schema's adverse flags (1.5 × normal rain, 10 frost days,
5 windy days), so the binary and graded marts agree on what "adverse" means.
Each factor has one baseline band and the gaps are measured against it.

**Sparse cells stay, flagged.** Region × season cells with only one exposure
keep their row with a null gap and `has_comparison = false`, instead of
disappearing. Pooling seasons across years (`years` column) keeps cells large
enough; Day 17 will test whether the gaps are significant.

**Proved from the reader's side.** `verify --layer marts` checks every mart
covers the three factors and that trains reconcile with the route-months that
had weather. e2e then runs `railfall.transform.report` as `railfall_reader` on
the real marts: it must find every factor and baseline band, and publishes its
Markdown to the job summary, so the findings are visible on each run.

## Day 14 — Data quality: dbt tests + Great Expectations checkpoint

**Each tool guards the layer it can see.** dbt tests stay on the models dbt
builds (staging, marts); Great Expectations validates `raw.*`, the layer dbt
reads but does not build, right after the Spark load. Running GX over the marts
too would duplicate dbt's tests in a second syntax; running dbt tests on raw
would only happen once dbt had already started. The checkpoint is the last task
of `railfall_pipeline`, so a broken load stops the run before any model reads it.

**Suites as code, context ephemeral.** Expectations are declared as plain
dataclasses in `railfall.quality.suites` and turned into a GX checkpoint at run
time. There is no `gx/` project directory to initialise in the Airflow image or
keep in sync with the code, the spec is unit-tested (every column exists in the
silver contract, every kind exists in GX) without building a context, and it
reads like a data contract in review.

**Physical bounds, not statistical ones.** Ranges come from the Spark cleaner's
`BOUNDS` and from definitions (a month has at most 31 days, trains run never
exceed trains planned). An unusually wet month is what the project studies, so
a z-score expectation would flag the signal as noise.

**Read-only gate.** The checkpoint connects as `railfall_transformer`, which can
select from `raw` but not write to it. A quality task should not be able to
"fix" or damage what it judges.

**dbt tests stay dependency-free.** `expression_is_true` and
`row_count_in_range` are local generic tests, like `accepted_range` before them,
instead of adding `dbt_utils`: no `dbt deps` network fetch inside the Airflow
image or e2e, and the four macros are short enough to own. Every model now
fails the build when empty, which is the failure mode a partial ingest
actually produces.

**A canary proves the gate bites.** A checkpoint that has only ever passed
could be pointing at the wrong column. `railfall.quality.canary` copies each
real raw table as the loader, plants one defect per table, runs the real suite
on the copy and requires a failure on the targeted expectation; copies are
dropped in a `finally`. The corruptions are portable SQL so the same flow is
unit-tested on SQLite. In e2e it runs on the real data, and the gate must report
exactly the same before and after, proving the canary left `raw` untouched.

**Results are visible, not just green.** The checkpoint renders a Markdown
table per run; e2e appends it to the job summary and keeps the JSON report as
an artifact, so a reviewer sees what was checked without reading logs.

## Day 15 — dbt docs, lineage and data catalog

**Read dbt's artifacts, don't re-derive them.** Lineage, coverage and the
catalog come from `manifest.json` and `catalog.json`, the files `dbt docs
generate` already writes. `railfall.transform.docs` never imports dbt or opens a
connection, so it is unit-tested on a small hand-written manifest and runs the
same on a laptop and in CI. Lineage is model-level (sources, seeds, models);
tests are left out because they would triple the graph without adding a path.

**Docs on the real warehouse, and strict.** e2e generates the docs after the
marts are rebuilt on the real data, as the transformer role, and fails on any
catalog error. The bundle then requires every declared relation and column to
be described and to exist in Postgres. Columns that exist but are not declared
(`_loaded_at`, `_updated_at`) are reported, not fatal: describing technical
columns on every model would be noise. Drift the other way, docs describing a
column that is gone, is the bug that matters and fails the run.

**Publish as an artifact, not a Pages site.** The `dbt-docs` artifact holds
dbt's `static_index.html` (a single file that opens from disk, no server), the
raw JSON and the bundle; the lineage is also drawn in the run summary as
Mermaid. GitHub Pages would need a deploy job and repository settings outside
the code; an artifact per run also keeps the docs tied to the exact commit.

**The README catalog is generated and checked.** A hand-written catalog would
go stale with the first renamed column. The README block between markers is
rendered from the manifest only (`dbt parse`, no database), so it is identical
on any machine and diffable in review; warehouse types stay in the artifact's
`catalog.md`, where they come from the real Postgres. `make readme-catalog`
rewrites it, a unit test (when dbt is installed) and e2e (against the manifest
of the live run) fail when it is stale. Writing only between markers keeps the
rest of the README hand-edited, and missing markers are an error, never a
silent append.

## Day 16 — Metabase with dashboards provisioned as code

**Metabase, on Postgres, managed by Terraform.** Metabase is free, runs from
one official image and talks to Postgres natively; Superset would add Redis,
Celery workers and its own image build for no gain at this size. Its
application database is a `metabase` database and role that Terraform creates
next to the warehouse, not the default H2 file inside the container: state
survives `make down`, and e2e asserts Metabase's tables are there and not in
the warehouse. Metabase reads the warehouse as `railfall_reader`, restricted to
`marts`, so a dashboard can never see raw or staging data.

**Bootstrap through the REST API, not a database dump.** Exports of Metabase's
application database are version-specific and unreviewable. The bootstrap uses
the documented API instead: the one-time setup token creates the admin on a
fresh instance, later runs log in; the warehouse connection is found by name and
updated in place; the sync is awaited until every mart a dashboard reads is
visible. Standard library only, like the Airflow client, so it runs on the CI
host without extra dependencies.

**Specs are JSON + plain SQL files.** One directory per dashboard: the JSON
says how to show things (cards, display, grid), the SQL says what to show and
is diffable in review. Specs are validated before any API call (one read-only
statement, the tables in the SQL equal the ones declared, cards fit the grid and
do not overlap), so a typo fails in unit tests rather than half-way through a
run. Questions are matched by name inside the Railfall collection and updated
in place; existing dashcard ids are reused so a re-run causes no churn; cards no
spec claims are archived rather than deleted, keeping their history. The spec is
the source of truth: UI edits to provisioned items are overwritten, by design.

**A dashboard that renders is not a dashboard that works.** After provisioning,
every card is executed by Metabase against the real marts; a failed query or an
empty result makes the command exit 1 and names the card. e2e runs the whole
bootstrap twice and requires the second run to create nothing, archive nothing
and keep the same card and dashcard ids, checked both in the JSON report and in
Metabase's application database.

**Fail before Metabase when dbt and the specs disagree.** `make
dashboards-check` validates the specs offline and, given dbt's `manifest.json`,
requires every table they read to be a relation dbt materialises in `marts`.
e2e runs it on the manifest of the live run, before Metabase boots, so renaming a
mart breaks the build with the card that reads it instead of an empty tile.
