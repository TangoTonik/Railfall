# AI-assisted workflow

This project is built with an AI agent (Claude) running on a schedule.
The human sets the direction; the agent implements; automated checks gate
every change.

## Roles
- **Human:** chose the project and its scope from current data engineering
  job requirements, fixed the roadmap, and set the rules the agent follows.
- **Agent:** implements one roadmap day per working day, writes the tests and
  the decision record, and pushes.
- **Automation:** `ruff` and `pytest` must pass before any push; CI re-runs
  them on GitHub.

## Loop
1. **Scope** — `ROADMAP.md`: the next unchecked day is the day's work.
2. **Rules** — `CLAUDE.md`: conventions, testing requirements, limits.
3. **Build** — small conventional commits, co-authored by the agent.
4. **Gate** — lint and tests locally, then CI.
5. **Explain** — one entry per day in `DECISIONS.md`.
6. **Ship** — one push per day.
