# AI-assisted workflow

This project is built the way a 2026 data engineer works with AI: the human owns
the scope and architecture, the AI accelerates implementation, and automated
checks gate every change.

## Loop
1. **Scope** — the roadmap (`ROADMAP.md`) fixes what gets built each day.
2. **Rules** — `CLAUDE.md` sets conventions, testing requirements and limits.
3. **Draft** — the AI implements the day's scope in small commits.
4. **Gate** — lint (`ruff`) and tests (`pytest`) run locally and in CI.
5. **Explain** — each day adds a decision record to `DECISIONS.md`.
6. **Ship** — one push per day.

## Why work this way
- Speed: boilerplate and plumbing take minutes instead of hours.
- Safety: nothing lands without passing tests and lint.
- Traceability: commits carry co-author attribution; decisions are written down.
