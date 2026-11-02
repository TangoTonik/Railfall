# Instructions for the AI pair-programmer

## Daily workflow
1. Read `ROADMAP.md`; the next unchecked day is today's scope. Do only that day.
2. Implement in small, logical commits (conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`).
3. Every new module gets unit tests. `ruff check .` and `pytest` must pass before pushing.
4. Add one entry to `docs/DECISIONS.md` explaining the day's key choice(s).
5. Tick the day in `ROADMAP.md` in the last commit, then push to `main`.

## Conventions
- Python 3.11+, type hints, `ruff` for lint/format, line length 100.
- Config through environment variables (`src/railfall/config.py`), never hard-coded secrets.
- Pipelines must be idempotent: re-running a day must not duplicate data.
- External APIs are mocked in tests with fixtures under `tests/fixtures/`.

## Don't
- Don't rewrite history or force-push.
- Don't skip a failing test; fix it or explain in DECISIONS.md.
- Don't add paid services; everything must run locally for free.
