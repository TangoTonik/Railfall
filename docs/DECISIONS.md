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
