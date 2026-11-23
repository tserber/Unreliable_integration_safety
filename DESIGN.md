# Design document: making the handoff integration safe to operate

## 1. Diagnosing the original design

```python
async def accept_handoff(company_id, payload):
    if not await db.event_exists(payload["event_id"]):
        request = await db.insert_handoff(company_id, payload)
        await queue.publish({"handoff_id": request.id})
    return {"accepted": True}

async def process_handoff(handoff_id):
    request = await db.get_handoff(handoff_id)
    if request.status == "completed":
        return
    result = await crm.create_callback(request.payload)
    await db.mark_completed(request.id, result.id)
```

Each production incident maps to a specific gap, reproduced directly
against this exact code in `tests/test_baseline_reproduces_incidents.py`:

- **Duplicate callback tasks** - `event_exists` then `insert_handoff` is a
  check-then-act race with no shared transaction or constraint.
- **Accepted but never reach the CRM** - `insert_handoff` and
  `queue.publish` are two separate steps; a crash between them leaves a
  persisted row with no scheduled work, forever.
- **Stuck in processing after restarts** - no claim, no lease, no
  fencing; a worker that dies mid-call leaves `status` wherever it was.
- **One company's backlog delays others** - a single global
  queue/worker pool with no per-company fairness concept.
- **Operator retries create duplicates** - `create_callback` carries no
  idempotency key and no record of a prior attempt.

The rest of this document is the replacement's design, in `app/`.

## 2. Transaction boundaries and delivery guarantees

**The database row is the queue.** `handoff_requests` is populated with
`INSERT ... ON CONFLICT (company_id, event_id) DO NOTHING`, one
statement, one transaction - no second "publish" step, so a committed
row is by definition visible to every worker's next poll. This removes
the "accepted but never scheduled" failure mode entirely.

**Claiming** is `SELECT ... FOR UPDATE SKIP LOCKED`, scoped per company,
followed by an `UPDATE` in the same short transaction stamping
`status='processing'`, a fresh `lease_token` (re-minted every claim),
and `lease_expires_at`. `SKIP LOCKED` means two workers racing the same
company never block each other or double-claim a row.

**The CRM call happens outside any DB transaction** - holding one open
across an unbounded network call risks long-lived locks and
pool exhaustion. The cost is that "claim" and "record the outcome" are
two separate commits with an external call in between; the fencing
token and lease make that gap safe:

- **Finalizing checks the fencing token**: `UPDATE ... WHERE id = $1 AND
  lease_token = $2 AND status = 'processing'`. If the lease expired and
  another worker reclaimed the row while we were blocked on the CRM, our
  token no longer matches and the write affects zero rows - our result
  is discarded rather than risking an overwrite. This is what makes a
  stalled-worker resume safe.
- **A reclaimed row is handled differently by CRM support**: CRM A
  (idempotency keys) is retried normally - the same `crm_idempotency_key`
  is reused on every attempt, so its own dedup makes a retry safe even
  if the abandoned attempt actually succeeded. CRM B (no dedup, no
  lookup) is never retried blindly; the row goes straight to
  `needs_review`. This asymmetry is a branch in `service.py`, not a
  comment.

**Delivery guarantee actually provided**: at-least-once work attempts;
at-most-once committed CRM-side effect for CRM A; for CRM B, at-most-once
*correct outcome* but not a guaranteed single CRM-side effect in the one
case our own process dies between a successful CRM B call and recording
it (§5).

**Why Postgres-as-queue, not a broker**: one dependency for a 6-hour
exercise, and exact, fast fault-injection tests (backdate a timestamp
instead of waiting out a real visibility timeout). Also legitimate at
this scale - 200 companies, 1000 rps burst, is comfortably within a
well-indexed Postgres table. The natural upgrade is `LISTEN/NOTIFY` for
low-latency wakeups, or a broker purely as a "go look" signal in front of
the same claim/fence/finalize logic, which is where correctness actually
lives.

## 3. Fairness and backpressure

Each company has a `max_concurrency`. The worker's `tick()`, once per
pass, lists every company with claimable work, shuffles the order, and
dispatches up to each company's own slice - it does not drain one
company's backlog before considering the next. `tests/test_fairness.py`
shows a 50-request flood from one company does not delay another's
pending request.

**Implemented vs. design-only**: the concurrency counter
(`Worker._inflight`) is in-process - correct for one worker, but doesn't
coordinate across multiple worker processes/machines, which the stated
scale implies. A real deployment needs a shared limiter (Redis or
Postgres-backed token bucket keyed by `company_id`); not built here.

API-side backpressure is handled by construction: accepting a request is
just a row insert, decoupled from processing, so the API never blocks on
a slow CRM. The backlog grows visibly in `pending` rows; queue age (§6),
not API latency, is what to monitor.

## 4. Behavior when an idempotency key expires before recovery

CRM A retains idempotency keys 24h. A retry landing after that boundary
would lose its dedup protection - the exact bug the key otherwise
prevents. So automatic retries stop well before it: `retry_cutoff_hours`
(default 20h, a 4h margin) is checked against `first_claimed_at` before
every attempt. Past that, the request escalates to `needs_review` with
the reason recorded, instead of retried again
(`tests/test_idempotency_key_expiry_safety_margin.py`). The 4h margin is
a guess, not a measured number - in production I'd tune it against
observed CRM latency and alert before the cutoff, not just at it.

## 5. Safe recovery for CRM B's ambiguous outcomes

CRM B has no idempotency key and no lookup by our reference, so an
uncertain outcome (our timeout, a reclaimed abandoned attempt) has no
safe automatic action - retrying risks a duplicate, not retrying risks
silently dropping a real request. This needs a human, and the design
makes that path fast rather than pretending to automate around it: the
row moves to `needs_review` with a specific reason, is excluded from
every claim query, and the operator endpoint offers exactly three
resolutions - `mark_completed` (found it in CRM B), `retry` (confirmed
CRM B doesn't have it), `mark_failed` (give up, with a reason) - each
backed by the full audit log on the inspect endpoint.

**Not solved, because CRM B's constraints don't allow it**: if our own
process crashes between a successful CRM B call and recording that
success, the row lands in `needs_review` via the normal reclaim path,
costing one operator look with no way to pre-fill the answer. The
prioritized fix: if CRM B exposes any search (customer, phone, time
window), add a best-effort reconciliation check before presenting the
item, downgrading "go check manually" to "here's what we found, confirm
or reject." Not built - the fake adapter has no such lookup, matching
the brief's stated constraint.

## 6. Metrics and alerts: healthy queue vs. silently lost work

Implemented as the audit log plus the status column; these are the
queries I'd wire into a dashboard, not a built exporter:

- **Queue age** (`now() - created_at` for the oldest `pending` row, per
  company) - climbing under steady volume means workers aren't keeping
  up.
- **Reclaim rate** (`handoff_audit_log` rows with `event = 'reclaimed'`
  per minute) - near zero in healthy operation; a sustained rise is a
  worker-fleet health signal.
- **`needs_review` inflow rate and age**, per company, split by reason -
  this directly answers "is work silently disappearing," since
  `needs_review` is exactly the set we refused to resolve automatically.
- **`attempt_count` distribution** for completions - a shift toward
  higher counts means CRM flakiness is rising before it's an incident.
- **Worker-fleet liveness**: each worker now upserts a heartbeat row
  (`worker_heartbeats`), and the API polls it on an interval, exposing
  `GET /health/workers` (503 with a structured body if no worker has
  reported in). This is the piece that tells "queue is quiet because
  there's no work" apart from "queue is quiet because nobody is
  consuming it" - and the API logs every healthy↔unhealthy transition.
- **The dangerous silent-loss case**: a row `completed` with no matching
  CRM-side record. Every `finalize_completed` stores `crm_reference_id`
  specifically so a periodic reconciliation job (sampling against the
  CRM's own records, where supported) can check this before fully
  trusting the data. A request that never reached `accept_handoff` at
  all is outside this service's visibility by definition - that has to
  be caught upstream, at the sender's own send path.

## 7. Deployment and migration

**Alongside old workers**: the schema is purely additive (there was no
explicit schema before). Deploy the new schema and acceptance path first;
leave old workers running until the new ones are confirmed healthy. Both
acceptance paths can run concurrently as long as neither bypasses the
`(company_id, event_id)` unique constraint. A row an old worker left
`processing` with no lease needs a one-time backfill
(`UPDATE ... SET status='pending' WHERE status='processing' AND
lease_expires_at IS NULL`) before cutover, since the new claim query only
reclaims *expired* leases, and `NULL < now()` isn't true.

**Migration compatibility**: `migrations/*.sql` use `CREATE TABLE/INDEX
IF NOT EXISTS` so a partially-applied, retried run never fails or
duplicates objects. There is deliberately no down-migration - rolling
back a schema already taking writes isn't something a blind `DROP
COLUMN` can do safely once workers are fencing on it. A real rollback is:
stop new workers, let in-flight leases expire naturally, then roll back
code, and only drop columns in a later migration once nothing references
them. Any future `NOT NULL` column at this table's eventual size needs
add-nullable-then-backfill-then-constrain, not a single blocking
`ALTER TABLE`.

## 8. Implemented, design-only, and what's next

**Implemented and tested**: atomic accept-or-dedup; claim/lease/fence
lifecycle; CRM-A-vs-CRM-B asymmetric retry policy for both the ambiguous-
timeout and reclaimed-after-crash cases; idempotency-key-expiry safety
cutoff; company-scoped inspect and three-way operator resolution (now
transaction-isolated end-to-end - see §9); per-company concurrency
fan-out; a full audit trail; worker-fleet heartbeat/health-check;
incident-labeled structured logging.

**Design-only**: a distributed per-company rate limiter for multi-process
deployments; a metrics exporter wired to §6's queries; CRM-B-side
reconciliation lookups; `LISTEN/NOTIFY` or a real broker as a lower-
latency wakeup; a canarying rollout runbook beyond §7's sequencing.

**Priority order**: (1) the metrics/alerts - a stuck `needs_review` pile
is only visible today if someone queries for it, the gap most likely to
become a real silent-loss incident; (2) the distributed rate limiter,
since the single-process cap won't hold once there's more than one
worker process at the stated scale; (3) CRM-B reconciliation lookups,
since every crash-during-a-CRM-B-call costs a manual check today.

## 9. Known downsides and possible solutions

Stated against KISS/ACID/SOLID rather than left implicit:

- **Polling has a latency floor** (`poll_interval_seconds`, 0.5s default)
  and claim-query cost grows with table size. The direct cost of the
  KISS tradeoff in §2. *Fix*: `LISTEN/NOTIFY`, or a broker purely as a
  wakeup signal, once a single instance is confirmed to be the
  bottleneck - not before.
- **The outbox-style insert pays index-maintenance cost on every
  accept**, not just under contention - the right ACID tradeoff
  (atomicity over raw throughput), not free. *Fix*: partition
  `handoff_requests` by company or by month if insert volume ever
  dominates.
- **CRM B's handling is a permanent, not temporary, human bottleneck**;
  `needs_review` size is structurally tied to CRM B's own reliability.
  *Fix*: the §5 reconciliation lookup, or at minimum alert on inflow rate
  so an outage is visible immediately.
- **The idempotency-key cutoff is one global number**, not tuned per
  company or observed latency - a slow company gets escalated for no
  real safety reason. *Fix*: make it a per-company column, fed from
  measured p99s; a config change, not an architecture change.
- **Fairness is per-worker-process, not cluster-wide**, so N processes
  could each independently under-count and collectively exceed a real
  per-company rate limit - `Worker` currently owns both scheduling policy
  and the counter it should be consulting externally (a SOLID-flavored
  gap). *Fix*: extract the counter behind
  `RateLimiter.try_acquire(company_id) -> bool`, in-process for tests,
  shared-store for production.
- **The audit log is append-only and unbounded.** Fine at current scale,
  not indefinitely. *Fix*: a scheduled archival/retention job for
  `completed`/`failed` rows past a window, as its own reviewable
  migration.
- **No backpressure signal reaches the caller**, by design - a 202 says
  nothing about a climbing queue age for that company. *Fix*: an
  `estimated_processing_lag` field in the 202 response, read from the
  same queue-age query already used operationally.

## 10. A fixed ACID bug, as a worked example of the review process

`operator_resolve` originally did `SELECT ... FOR UPDATE` then a status
check then an `UPDATE`, without an explicit `conn.transaction()`. On
asyncpg, a statement outside an explicit transaction auto-commits on its
own - so the row lock was released the instant the `SELECT` returned,
turning the status check into a TOCTOU race rather than a guard. Two
operators racing conflicting actions (`mark_completed` vs `mark_failed`)
on the same row could both pass the check and both write, with no 409
for either - reproduced directly before the fix. Fixed by wrapping the
whole read-check-write in one transaction and adding `status IN (...)`
to each `UPDATE` itself as a second, independent fence, mirroring the
lease-token fence used elsewhere in this file. Regression test:
`tests/test_operator_recovery.py::test_two_operators_racing_conflicting_actions_only_one_wins`.
