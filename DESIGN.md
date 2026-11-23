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

Each production incident maps to a specific gap:

- **Duplicate callback tasks** - `event_exists` then `insert_handoff` is a
  check-then-act race. Two concurrent submissions of the same event can
  both pass the check before either inserts.
- **Accepted but never reach the CRM** - `insert_handoff` and
  `queue.publish` are two separate steps with no shared transaction. A
  crash, or any failure, between them leaves a persisted row with no
  scheduled work.
- **Stuck in processing after restarts** - nothing marks a row as claimed,
  nothing leases it, and nothing ever revisits it. A worker that dies
  mid-`create_callback` leaves `status` wherever it was, forever.
- **One company's backlog delays others** - this reads like a single
  global queue/worker pool with no per-company fairness or concurrency
  isolation; one slow or bursty company's work necessarily sits in front
  of everyone else's in FIFO order.
- **Operator retries create duplicates** - `create_callback` is called
  with no idempotency key and no record of a prior attempt, so a manual
  retry is indistinguishable from the first attempt, and nothing stops an
  operator from calling it twice.

The replacement in this repo addresses each of these directly; the rest
of this document is the design behind that replacement.

## 2. Transaction boundaries and delivery guarantees

**The database row is the queue.** `handoff_requests` is populated with
`INSERT ... ON CONFLICT (company_id, event_id) DO NOTHING`, one
statement, one transaction. There is no second "publish" step - a
committed row is, by definition, visible to every worker's next poll.
This is what eliminates the "accepted but never scheduled" failure mode
entirely rather than mitigating it: there is no window in which the row
exists but no worker will ever find it.

**Claiming is `SELECT ... FOR UPDATE SKIP LOCKED` scoped per company**,
followed by an `UPDATE` in the same short transaction that sets
`status='processing'`, a fresh `lease_token` (a random UUID, re-minted on
every claim), and `lease_expires_at = now() + lease_seconds`. SKIP LOCKED
means two workers racing the same company never block each other or
double-claim a row - one gets it, the other's query simply doesn't see it
and moves to its own candidate.

**The CRM call happens outside any database transaction.** This is
deliberate: holding a DB transaction open across a network call with an
unknown timeout is how you get long-lived locks and connection-pool
exhaustion under load. The cost is that "claim" and "record the outcome"
are two separate commits, with an external call in between that can fail
in ways the database can't see. That gap is exactly what the fencing
token and the lease exist to make safe:

- **Finalizing a write checks the fencing token**: `UPDATE ... WHERE id =
  $1 AND lease_token = $2 AND status = 'processing'`. If the lease
  expired and another worker reclaimed the row while we were blocked on
  the CRM, our token no longer matches and this UPDATE affects zero rows.
  We discard our own result rather than risk overwriting whatever the
  newer claimant decided. This is what makes scenario 4 (stalled worker
  resumes) safe.
- **A reclaimed row (lease expired while still `processing`) is treated
  differently depending on whether the CRM can deduplicate**: if it can
  (CRM A, idempotency keys), the retry is dispatched normally - the same
  `crm_idempotency_key` was generated once at insert time and is reused
  on every attempt, so the CRM's own dedup makes the retry safe even if
  the abandoned attempt actually succeeded. If it can't (CRM B), we do
  not call the CRM again at all; the row goes straight to
  `needs_review`. This is the asymmetry the assignment is explicitly
  testing for, and it is implemented as a branch in `service.py`, not
  left as a comment.

**Delivery guarantee actually provided: at-least-once work attempts,
at-most-once committed CRM-side effect for CRM A, at-most-once *correct*
outcome but not a guaranteed-single CRM-side effect for CRM B** in the
one case where our own process dies between a successful CRM B call and
recording it (see §4 - this is a real, acknowledged gap, not hidden).

**Why Postgres-as-queue instead of a real broker for this take-home:** it
removes an infrastructure dependency from a 6-hour exercise, and it makes
the fault-injection tests exact and fast (backdate a timestamp with SQL
instead of waiting out a real visibility timeout). It is also a
legitimate production choice at this scale - 200 companies, burst 1000
req/s, is comfortably within what a well-indexed Postgres table handles
as a queue. The worker's `tick()` currently polls on an interval; the
natural production upgrade is `LISTEN/NOTIFY` for low-latency wakeups
while keeping Postgres as the authority, or fronting it with SQS/RabbitMQ
purely as a "go look" signal while the database stays the source of
truth. Either way, correctness lives in the claim/fence/finalize logic,
not in whatever delivers the wakeup - which is also why the delivery
mechanism can be swapped later without touching that logic.

## 3. Fairness and backpressure

Each company has a `max_concurrency` (seeded from `companies.max_concurrency`,
standing in for a real per-company CRM rate limit). The worker's `tick()`
method, once per pass: lists every company with claimable work, shuffles
the order, and dispatches up to each company's own concurrency slice -
it does not drain one company's backlog before considering the next.
A company that floods the system simply stops being dispatched once its
slice is full; every other company still gets served in the same pass.
`tests/test_fairness.py` demonstrates this directly: a 50-request flood
from one company does not delay a single pending request from another.

**What's implemented vs. design-only here:** the concurrency counter in
this repo is in-process (`Worker._inflight`), which is correct for a
single worker process but does not coordinate across multiple worker
processes/machines. At 200 companies and bursts of 1000 req/s you will
run several worker processes, so the real per-company rate limit needs a
shared counter - a Redis token bucket keyed by `company_id`, or a
Postgres-backed one (e.g. a `company_rate_state` row updated with
`UPDATE ... SET tokens = tokens - 1 WHERE tokens > 0`). I did not build
that distributed limiter; the in-process version is what's exercised by
the tests, and the design intent above is what I'd extend.

Backpressure on the API side (bursts of 1000 req/s against companies
whose downstream CRM can't keep up) is handled by construction: accepting
a request is just a row insert, decoupled from processing, so the API
never blocks on a CRM. The backlog simply grows in `pending` rows,
visible and boring. The thing to actually monitor is queue age (§5), not
API latency.

## 4. Behavior when an idempotency key expires before recovery

CRM A retains idempotency keys for 24h. If a request kept failing and
getting auto-retried right up against that boundary, a retry landing
after the key expired would lose its dedup protection and could create a
genuine duplicate - the exact bug CRM A otherwise prevents. So retries
stop well before the boundary: `retry_cutoff_hours` (default 20h, a 4h
margin) is checked against `first_claimed_at` before every attempt, for
both CRMs. Once a request has been in flight that long without a clean
outcome, it is escalated to `needs_review` instead of retried again, with
the reason recorded. `tests/test_idempotency_key_expiry_safety_margin.py`
exercises this. The 4h margin is a guess, not a measured number - in
production I'd tune it against observed CRM A latency/retry patterns and
alert well before the cutoff is hit, not just when it is.

## 5. Safe recovery for CRM B's ambiguous outcomes

CRM B has no idempotency key and no lookup by our reference, so once a
call to it has an uncertain outcome (our own timeout, a connection drop,
an abandoned attempt we're now reclaiming), there is no safe automatic
action - calling again might create a real duplicate, and *not* calling
again might leave a real customer callback request silently dropped. The
honest answer is that the external system's own limitations mean this
case needs a human, and the design makes that path as fast and safe as
possible rather than pretending to automate around it:

- The row moves to `needs_review` with a specific `review_reason` (what
  happened and why it's ambiguous), and is excluded from every claim
  query - it will never be silently retried or picked up by a worker again.
- The operator endpoint (`POST .../resolve`) gives exactly three
  resolutions, each requiring the operator to have actually looked:
  `mark_completed` (found the record in CRM B, provide its reference id),
  `retry` (confirmed CRM B does *not* have it, safe to requeue), or
  `mark_failed` (give up, with a reason).
- The full audit log (`handoff_audit_log`) is attached to the inspect
  endpoint response, so the operator can see exactly when it was claimed,
  by which worker, and what the recorded error was, without needing
  separate log access.

What's explicitly **not** solved, because CRM B's constraints don't allow
it to be solved automatically: if our *own process* dies between a
successful CRM B call and the moment we would have recorded that success
(not a timeout - a real crash, after `create_callback` already returned),
the row is left `processing`, its lease eventually expires, and the
reclaim logic routes it to `needs_review` rather than guessing - which is
correct, but it means every such crash costs one operator look, with no
way to pre-fill the likely answer. A production mitigation worth
prioritizing: if CRM B exposes *any* search (by customer, phone number,
timestamp window), add a best-effort reconciliation check that queries it
before presenting the item to the operator, downgrading "go check
manually" to "here's what we found, confirm or reject." I did not build
this because the fake adapter has no such lookup, matching the
assignment's stated constraint, but it's the first thing I'd ask the real
CRM B integration for.

## 6. Metrics and alerts: healthy queue vs. silently lost work

Implemented as the audit log plus the status column; the queries below
are what I'd wire into a dashboard but did not build a metrics exporter
for in the 6-hour box:

- **Queue age**: `now() - created_at` for the oldest `pending` row, per
  company. A healthy queue has this bounded by roughly
  `lease_seconds + CRM latency`; a climbing age under steady request
  volume means workers aren't keeping up (undersized pool, a company's
  concurrency limit set too low, or a CRM that's degraded).
- **Reclaim rate**: count of `handoff_audit_log` rows with
  `event = 'reclaimed'` per minute. This should be near zero in healthy
  operation - every one represents a worker that died or stalled mid-task.
  A sustained increase is a worker-fleet health signal, independent of
  whether any individual request's outcome was correct.
- **needs_review inflow rate and age**, per company and split by reason
  prefix (reclaim-after-timeout vs. idempotency-cutoff vs. CRM rejection).
  This is the metric that most directly answers "is work silently
  disappearing": `needs_review` is specifically the set of requests we
  refused to resolve automatically, so its size *is* the count of
  customer-visible work currently blocked on a human, as opposed to
  `pending`/`processing`, which is just normal queueing.
- **attempt_count distribution** for completed requests - most should
  complete on attempt 1; a shift toward higher attempt counts means CRM
  flakiness is rising before it becomes an incident.
- **The dangerous silent-loss case to alert on specifically**: a row that
  is `completed` with no matching CRM-side record, or (harder to detect
  from our own DB alone) a customer-reported missing callback with no
  corresponding row at all. The first is why every `finalize_completed`
  call stores `crm_reference_id` - a periodic reconciliation job that
  samples completed `crm_reference_id`s against the CRM's own records
  (where CRM A supports it) is the check I'd add before fully trusting
  this in production. The second - a request that never even reached
  `accept_handoff` - is outside this service's visibility by definition;
  it has to be caught upstream, at the AI assistant's own send path
  (e.g. an outbox pattern there too, or a client-side delivery receipt).

## 7. Deployment alongside old workers and existing pending requests

The schema is additive relative to the old implicit model (there was no
explicit schema before - just `db.insert_handoff` / `db.mark_completed`
calls against something), so there's no "old rows in a different shape"
problem to migrate. The sequencing that matters is: deploy the new schema
and the new acceptance path (new `accept_handoff`) before deploying any
new workers, and leave old workers running against whatever they were
already consuming from until the new workers are confirmed healthy. Both
old and new acceptance paths can run concurrently against the same table
as long as the old path never writes a row that bypasses the
`(company_id, event_id)` unique constraint. Once new workers are serving
traffic correctly, drain and retire old workers; any row an old worker
was mid-processing simply has no lease and gets picked up normally by
the new claim query (it'll look exactly like an organically pending row,
since it predates `lease_token` and `lease_expires_at`, which is fine -
`NULL` lease_expires_at doesn't match `lease_expires_at < now()` in the
reclaim branch but a `status='pending'` row doesn't need that branch
anyway). The one case to watch is a row an old worker left `processing`
with no lease at all - those need a one-time backfill
(`UPDATE ... SET status='pending' WHERE status='processing' AND
lease_expires_at IS NULL`) before the cutover, or the new claim query
will never pick them up (it only reclaims *expired* leases, and
`NULL < now()` is `NULL`, not true).

## 8. Database migration compatibility and rollback limitations

`migrations/001_init.sql` is written to be safely re-run
(`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`), which
matters more than it looks: a migration runner that partially applies and
retries should not fail or duplicate objects on replay. There is
deliberately no down-migration. Rolling back a schema that's already
taking production writes is not something a blind `DROP COLUMN` can do
safely once there's data depending on it (a rollback that drops
`lease_token` after workers have been fencing writes on it will let two
workers double-process everything the moment it lands) - a real rollback
plan is: stop new workers first, let in-flight leases expire naturally
(bounded by `lease_seconds`), *then* roll back code, and only drop
columns in a later migration once you're certain nothing will reference
them again. Any future migration that adds a `NOT NULL` column to
`handoff_requests` at this table's eventual size needs a default-and-backfill
in two steps (add nullable, backfill, add constraint) rather than a
single blocking `ALTER TABLE ... ADD COLUMN ... NOT NULL`, to avoid a
long table lock under load - not needed yet since the table is new, but
worth stating as the standing rule for this table.

## 9. What's implemented, what's design-only, and what I'd prioritize next

**Implemented and tested**: atomic accept-or-dedup; claim/lease/fence
lifecycle with SKIP LOCKED; the CRM-A-vs-CRM-B asymmetric retry policy for
both the "ambiguous timeout" and "reclaimed after crash" cases;
idempotency-key-expiry safety cutoff; company-scoped inspect and
three-way operator resolution; per-company concurrency fan-out in the
worker loop; a full audit trail per request.

**Design-only** (described above, not built): a distributed per-company
rate limiter (Redis/DB-backed token bucket) for multi-process deployments;
a metrics exporter wired to the queries in §6; CRM-B-side reconciliation
lookups before handing an item to an operator; `LISTEN/NOTIFY` or a real
broker as a lower-latency wakeup signal in front of the same claim logic;
an actual rollout runbook beyond the sequencing described in §7 (e.g.
canarying new workers against a subset of companies first).

**What I'd prioritize next, in order**: (1) the metrics/alerts, because
right now a stuck `needs_review` pile is only visible if someone queries
for it - that's the gap most likely to turn into a real "silently lost
work" incident; (2) the distributed rate limiter, because the
single-process concurrency cap genuinely will not hold at the stated
scale once there's more than one worker process; (3) CRM-B reconciliation
lookups, because every crash-during-a-CRM-B-call currently costs a manual
check with no way to speed that up, and that's the one place the design
asks a human to do work that a slightly richer CRM API could do instead.

## 10. Known downsides and possible solutions

Honest architectural downsides of the choices above, stated against
KISS/ACID/SOLID rather than left implicit, plus what I'd actually do
about each:

- **Postgres-as-queue has a polling floor on latency.** `poll_interval_seconds`
  (0.5s default) is the best-case time between a row becoming claimable
  and a worker noticing it, and under a 1000 rps burst the queries in
  `companies_with_claimable_work` and `claim_next_for_company` add real
  per-poll cost against one instance as the table grows. This is the
  direct cost of the KISS tradeoff in §2 (one dependency, exact tests) -
  a push-based broker has no such floor. Mitigation: `LISTEN/NOTIFY` on
  insert for near-immediate wakeup while keeping Postgres as the system
  of record (cheap, no new infra), or a real broker in front purely as a
  "go look now" signal once a single Postgres instance is confirmed to be
  the bottleneck - not before, since 200 companies / 1000 rps burst is
  comfortably inside what a well-indexed table handles today.

- **The outbox-pattern overhead is paid on every single accept, not just
  under contention.** Collapsing "persist" and "schedule" into one
  `INSERT ... ON CONFLICT` (§2) buys atomicity by construction, but every
  accepted row now carries index maintenance on `(company_id, event_id)`
  and `(company_id, status, created_at)` whether or not it was ever a
  duplicate. This is the right ACID tradeoff (durability and atomicity
  over raw insert throughput) but it is not free. Mitigation: if insert
  volume ever dominates, partition `handoff_requests` by `company_id`
  range or by month of `created_at` so index maintenance and vacuum stay
  bounded per partition instead of growing against one ever-larger table.

- **CRM B's ambiguous-outcome handling is a permanent, not temporary,
  human bottleneck.** §5 is explicit that this is the honest answer given
  CRM B's constraints, but it means the `needs_review` queue's size is
  structurally tied to CRM B's own reliability - a bad day for CRM B
  becomes a bad day for the operator team, with no automated relief
  valve. Mitigation: the reconciliation-lookup idea in §5 (query CRM B by
  customer/phone/time-window before surfacing to a human) is the direct
  fix; short of that, at minimum alert on `needs_review` inflow rate (§6)
  so a CRM B outage is visible as an operator-queue problem immediately,
  not discovered days later as a backlog.

- **Idempotency-key expiry forces escalation, which is a blunt
  instrument.** The `retry_cutoff_hours` safety margin (§4) is a single
  global number, not tuned per company or per observed CRM A latency. A
  company whose CRM A instance is unusually slow could legitimately need
  longer than 20h and get escalated to `needs_review` for no real safety
  reason, while a different company's genuinely-stuck request might
  deserve escalation sooner. Mitigation: make the cutoff per-company
  (same place `max_concurrency` already lives, on the `companies` table)
  and feed it from measured p99 time-to-completion per company instead of
  one fixed default - this is a config change, not an architecture
  change, so it is low-risk to add later.

- **Fairness is per-worker-process, not cluster-wide (§3), so it degrades
  exactly when it matters most.** At one worker process the round-robin
  fan-out in `tick()` genuinely prevents starvation; at N worker
  processes each has its own `_inflight` counter, so N processes could
  each independently decide a company is under its concurrency limit and
  collectively blow past the real per-company CRM rate limit. This is a
  SOLID-flavored gap too: `Worker` currently owns both scheduling policy
  and the concurrency counter it should be consulting from a shared
  source. Mitigation: extract the counter behind a small interface
  (`RateLimiter.try_acquire(company_id) -> bool`) with an in-process
  implementation for tests/dev and a Redis- or Postgres-backed
  implementation for production - the `Worker` loop does not need to
  change, only what it asks.

- **At-least-once work attempts mean `attempt_count` and the audit log
  are append-only and unbounded.** `handoff_audit_log` has no retention
  policy; a request that bounces between `pending`/`processing` many
  times before completing writes one row per transition, forever. Fine at
  current scale, not fine indefinitely. Mitigation: a scheduled job that
  archives or deletes audit rows for `completed`/`failed` requests older
  than a retention window (e.g. 90 days), kept as a separate migration so
  it can be reviewed and rolled back independently of the core schema.

- **No backpressure signal back to the caller, by design, which can hide
  a real problem behind a "successful" 202.** Accept never blocking on
  CRM health (§3) is correct for API latency, but it also means a caller
  gets `{"accepted": true}` even while that company's queue age is
  climbing into hours - KISS at the API boundary, at the cost of the
  caller having no way to know. Mitigation: this is exactly what queue
  age (§6) is for operators, but a caller-facing signal (e.g. an
  `estimated_processing_lag` field in the 202 response, read from the
  same queue-age query) would let the AI assistant upstream make its own
  decision about setting customer expectations, instead of only the
  operator finding out.
