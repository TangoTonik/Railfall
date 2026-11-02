# Instructions for the AI pair-programmer

## Daily workflow
1. Read `ROADMAP.md`; the next unchecked day is today's scope. Do only that day.
2. Implement in small, logical commits (conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`).
3. Every new module gets unit tests. `ruff check .` and `pytest` must pass before pushing.
4. Add one entry to `docs/DECISIONS.md` explaining the day's key choice(s).
5. Tick the day in `ROADMAP.md` in the last commit, then push to `main`.

## Definition of done: fully functional
- Every feature must work on real data and real services, not only on fixtures.
- The proof is the `e2e` GitHub Actions workflow: it boots the Docker Compose
  stack, downloads the real open data and runs the pipeline as far as it exists.
  Extend it every day to cover the day's feature with real assertions
  (row counts, schemas, no duplicates, DAG success).
- Fixtures are for fast unit tests only. Never mock something in e2e.
- Before starting new work, check the latest CI runs (`gh run list -R TangoTonik/Railfall`,
  `gh run view <id> --log-failed`). If anything is red, fixing it is the first job.
- A day is only ticked once its e2e coverage is in place and the previous e2e run is green.

## Conventions
- Python 3.11+, type hints, `ruff` for lint/format, line length 100.
- Config through environment variables (`src/railfall/config.py`), never hard-coded secrets.
- Pipelines must be idempotent: re-running a day must not duplicate data.
- Unit tests mock external APIs with fixtures under `tests/fixtures/`; e2e uses the real ones.

## Don't
- Don't rewrite history or force-push.
- Don't skip a failing test; fix it or explain in DECISIONS.md.
- Don't add paid services; everything must run locally for free.
