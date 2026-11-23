# AGENTS.md

Instructions for any agent (Claude Code or otherwise) working in this repo.

## What this is

A take-home solution: a service that synchronizes customer callback
requests into an external CRM, safely. PostgreSQL is both the system of
record and the queue (see `DESIGN.md` for the full rationale). Read
`README.md` first for how to run it, `DESIGN.md` for the design writeup,
and `TRADEOFFS.md` for scope cuts and time spent.

## Layout

- `app/api.py` - FastAPI HTTP layer. Thin: translates `repository`
  exceptions to HTTP status codes, nothing else.
- `app/service.py` - orchestration layer, framework-free on purpose so
  the API, the worker, and tests all call the same functions and get
  identical behavior. This is where incident-relevant logging lives.
- `app/repository.py` - raw-SQL data access. Deliberately un-ORMed: the
  correctness of this service lives in a small number of exact
  transaction boundaries and locking clauses that an ORM tends to hide.
- `app/worker.py` / `app/worker_main.py` - the polling loop, run as its
  own process, safe to scale horizontally.
- `app/crm/` - fake CRM adapters (`crm_a` supports idempotency keys,
  `crm_b` does not) plus the registry that maps a company to its adapter.
- `migrations/*.sql` - applied in filename order by `scripts/migrate.py`.
  Add a new numbered file for schema changes; don't edit an applied one.
- `tests/test_scenario_*.py` - the six required Part 2 failure
  demonstrations, one file each.
- `tests/test_baseline_reproduces_incidents.py` - runs the *original*
  naive implementation (`original_problem/baseline.py`) against fake
  db/queue/crm backends and reproduces each incident directly, so the
  "before" state is demonstrated running, not just asserted in prose.
- `original_problem/baseline.py` - the brief's naive code, kept verbatim
  for diffing. Never "fix" this file; it's the fixed point everything
  else is diffed against. See `scripts/diff_solutions.py`.

## Before changing code

- Run `docker compose up -d` and `python -m scripts.migrate` against
  both `handoff_dev` and `handoff_test` if you haven't already (see
  README's "Running it locally"). The test suite needs a real Postgres -
  it exercises `SELECT ... FOR UPDATE SKIP LOCKED` and transaction-boundary
  behavior that no in-memory fake reproduces faithfully.
- `python -m pytest -q` must pass before and after your change. If it
  doesn't pass before you start, that's a pre-existing issue - say so,
  don't silently fix unrelated things in the same change.
- `ruff check .` must pass (config in `pyproject.toml`). CI
  (`.github/workflows/ci.yml`) runs both lint and the full suite against
  a real Postgres service container on every push/PR.

## Conventions to follow

- Every state transition on `handoff_requests` goes through
  `repository._audit(...)` - the append-only `handoff_audit_log` table is
  what makes "inspect a request" and "healthy queue vs. silently lost
  work" possible. If you add a new transition, audit it.
- Every operator-facing read or write in `repository.py` scopes by
  `company_id` in the `WHERE` clause itself, not as a filter applied
  after the fact. A new operator action must follow this - see
  `operator_resolve` and `test_scenario_6_cross_company_isolation.py`.
- Any write that finalizes a claimed row's outcome fences on
  `lease_token` (`WHERE lease_token = $2 AND status = 'processing'`).
  Don't add a write path that skips this - it's what makes a stalled
  worker's delayed write a safe no-op instead of data corruption.
- Log, don't just audit, anything that corresponds to one of the
  assignment's five named production incidents (duplicate callback
  tasks, accepted-but-unreached requests, stuck-after-restart requests,
  one company delaying others, operator-retry duplicates) - see the
  `log.*` calls already in `app/service.py`, `app/worker.py`, and
  `app/api.py`'s `_watch_workers` for the pattern (structured, one line,
  names the incident in a comment above the call).
- Keep `app/service.py` framework-free - no FastAPI imports, no
  asyncio-loop-specific globals that would only work inside a request
  handler. Tests and the worker depend on calling it directly.

## Testing a new behavior

Prefer a new test over a new comment explaining what *should* happen.
Match the existing style: one invariant per test, a docstring naming
which assignment requirement or incident it covers, and - if it's a
race - a real `await` yield point in the fake/setup so the race is
deterministic rather than theoretical (see
`tests/test_baseline_reproduces_incidents.py`'s fakes for the pattern).

## Do not

- Don't add a message broker, Celery, Redis, or any new infrastructure
  dependency. The whole point of this design is that Postgres alone is
  enough; adding a broker back in undoes the thing being demonstrated.
- Don't loosen a `WHERE company_id = $n` clause, even temporarily, even
  in a script - that's the entire isolation guarantee.
- Don't give `original_problem/baseline.py` real `db`/`queue`/`crm`
  implementations so it "actually runs" in production code. It stays
  pseudocode-with-fakes-in-tests on purpose; see `scripts/diff_solutions.py`
  for why a line-for-line diff against it doesn't make sense.
