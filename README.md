# Unreliable handoff integration - made safe to operate

Take-home: a service that synchronizes customer callback requests from an
AI assistant into an external CRM, redesigned so that duplicate
submissions, crashes mid-flight, worker restarts, and ambiguous CRM
timeouts can't corrupt state or create duplicate CRM records (where the
CRM allows us to prevent that at all - see `DESIGN.md` for where it
can't).

See `DESIGN.md` for the full design writeup (transaction boundaries,
fairness, rollout, what's implemented vs. design-only, and a dedicated
"Known downsides and possible solutions" section), `TRADEOFFS.md` for
time spent, scope cuts, and AI tool disclosure, and `AGENTS.md` for the
conventions an agent (or a human) should follow before changing code
here.

CI (`.github/workflows/ci.yml`) runs `ruff check .` and the full test
suite against a real Postgres service container on every push and PR.
To run the same checks locally:
```bash
pip install ruff
ruff check .
```

## Architecture in one paragraph

PostgreSQL is both the system of record and the queue: `handoff_requests`
rows ARE the units of work, discovered by workers polling for
`status = 'pending'` (or a `processing` row whose lease has expired).
There is no separate "publish to a broker" step after insert, which
removes an entire class of bug (accepted-but-never-scheduled) by
construction. Workers claim one row at a time with
`SELECT ... FOR UPDATE SKIP LOCKED`, stamp it with a fresh fencing token
and a time-boxed lease, and that fencing token - not just the lease - is
what's checked on write-back, so a worker that stalls and wakes up after
losing its lease can't overwrite what a newer claimant already decided.

## Running it locally

Prerequisites: Python 3.11+, and either Docker or a local PostgreSQL 14+.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Database - option A, Docker:**
```bash
docker compose up -d
# creates handoff_dev and handoff_test automatically
```

No local `psql` needed - run it inside the running container:
```bash
docker compose exec postgres psql -U handoff -d handoff_dev
```

**Database - option B, a Postgres you already have:**
```bash
createuser handoff --superuser          # or: give it CREATEDB + the usual grants
createdb handoff_dev  -O handoff
createdb handoff_test -O handoff
```

**Apply the schema** (idempotent, safe to re-run):
```bash
python -m scripts.migrate "postgresql://handoff:handoff@localhost:5432/handoff_dev"
python -m scripts.migrate "postgresql://handoff:handoff@localhost:5432/handoff_test"
```

**Seed two demo companies** (one on each fake CRM):
```bash
DATABASE_URL="postgresql://handoff:handoff@localhost:5432/handoff_dev" python -m scripts.seed
```

**Run the API and the worker pool as separate processes**, exactly as
they'd run in production:
```bash
# terminal 1
DATABASE_URL="postgresql://handoff:handoff@localhost:5432/handoff_dev" \
  uvicorn app.api:app --reload --port 8000

# terminal 2
DATABASE_URL="postgresql://handoff:handoff@localhost:5432/handoff_dev" \
  python -m app.worker_main
```

**Try it:**
```bash
curl -X POST localhost:8000/companies/acme/handoffs \
  -H 'content-type: application/json' \
  -d '{"event_id":"evt-1","payload":{"customer":"Jane","phone":"+1555..."}}'
# -> {"id": "...", "accepted": true, "newly_created": true}

curl localhost:8000/companies/acme/handoffs/<id>
# status goes pending -> completed within a poll interval (0.5s default)
```

**Check that workers are actually alive** (not just that the API is up):
```bash
curl -s localhost:8000/health/workers
# -> {"ok": true, "healthy_worker_count": 1}      once a worker has sent
#                                                  at least one heartbeat
# -> 503 {"error": "no_healthy_workers", ...}      before that, or if every
#                                                  worker has been dead for
#                                                  longer than the staleness
#                                                  window (default 15s)
```
Each worker upserts its own row in `worker_heartbeats` every
`worker_heartbeat_interval_seconds` (default 5s); the API polls that
table on the same cadence in a background task and caches the verdict,
so `/health/workers` never blocks on a live round-trip to a worker
process - the API only ever talks to Postgres, which both sides already
depend on. `/health` stays a plain liveness probe for the API process
itself; `/health/workers` is the one that answers "is the backlog
actually moving". The API keeps accepting new requests either way -
acceptance and processing are decoupled by design (see `DESIGN.md`) - but
this is what distinguishes "queue is quiet because there's no work" from
"queue is quiet because nobody is consuming it".

**Inspect state directly:**
```bash
# via Docker (no local psql install needed)
docker compose exec postgres psql -U handoff -d handoff_dev \
  -c "select id, company_id, status, attempt_count, crm_reference_id from handoff_requests order by created_at desc limit 20;"

# or, with a local psql client installed
psql postgresql://handoff:handoff@localhost:5432/handoff_dev \
  -c "select id, company_id, status, attempt_count, crm_reference_id from handoff_requests order by created_at desc limit 20;"
```

## Running the tests

The test suite runs against a real Postgres (`handoff_test`) because the
whole point of this design is `SELECT ... FOR UPDATE SKIP LOCKED` and
transaction-boundary behavior that SQLite cannot reproduce.

```bash
TEST_DATABASE_URL="postgresql://handoff:handoff@localhost:5432/handoff_test" pytest -v
```

Each file under `tests/test_scenario_*.py` corresponds to one of the six
required fault-injection scenarios in the assignment, with a docstring at
the top stating the invariant it checks. `tests/test_fairness.py` and
`tests/test_idempotency_key_expiry_safety_margin.py` are extra, not
required by the brief, demonstrating two of the Part 3 design answers
(backpressure/fairness, and idempotency-key-expiry handling) as running
code rather than just prose.

## Comparing the original naive solution to this one

```bash
python -m scripts.diff_solutions               # static comparison, no DB needed
TEST_DATABASE_URL="postgresql://handoff:handoff@localhost:5432/handoff_test" \
  python -m scripts.diff_solutions --run-demo   # also runs two scenarios live
```

Prints the verbatim baseline from `original_problem/baseline.py` next to a
`diff -u` against the specific new-solution function responsible for each
of five behaviors (duplicate handling, crash recovery, worker
coordination, company isolation, retries/ambiguous outcomes), each with a
one-paragraph explanation of why the new side is safer and which test
proves it. `--run-demo` additionally executes two of those scenarios live
against a real Postgres (concurrent duplicate submissions collapsing to
one row, and a stalled worker's write being fenced out after reclaim) so
the claims are shown running, not just asserted in prose.

## Operator recovery API

```bash
# Inspect a request (company-scoped - a wrong company_id gets a 404, not
# someone else's data):
curl localhost:8000/companies/acme/handoffs/<id>

# Resolve a request stuck in needs_review (CRM B ambiguous outcome, or an
# idempotency key that aged past the safety margin), after checking the
# CRM directly:
curl -X POST localhost:8000/companies/acme/handoffs/<id>/resolve \
  -H 'content-type: application/json' \
  -d '{"action": "mark_completed", "external_reference": "crm-side-id-you-found"}'

curl -X POST localhost:8000/companies/acme/handoffs/<id>/resolve \
  -H 'content-type: application/json' \
  -d '{"action": "retry"}'

curl -X POST localhost:8000/companies/acme/handoffs/<id>/resolve \
  -H 'content-type: application/json' \
  -d '{"action": "mark_failed", "reason": "customer requested cancellation"}'
```

## Repository layout

```
app/
  api.py             FastAPI app: accept / inspect / resolve endpoints
  service.py         orchestration: claim -> call CRM -> finalize, all the
                      "is this safe to retry" decisions live here
  repository.py       raw-SQL data access: every transaction boundary and
                      locking clause, with comments on why each exists
  worker.py           the polling worker loop, per-company fan-out
  worker_main.py       standalone worker process entrypoint
  crm/                fake CRM A (idempotent) and CRM B (not) adapters,
                      plus a per-company fault injector for tests
migrations/001_init.sql  schema
scripts/              migrate.py, seed.py, inspect.py, diff_solutions.py
tests/                one file per required scenario, plus two extra
DESIGN.md              Part 3 writeup
TRADEOFFS.md           time spent, scope cuts, AI disclosure
```
