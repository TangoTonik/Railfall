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
