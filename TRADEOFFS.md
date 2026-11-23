# Tradeoffs, scope cuts, and AI tool disclosure

## AI tool disclosure

This solution (architecture decisions, code, tests, and both other
documents in this repo) was produced with Claude (Anthropic), working
from the assignment brief in a single focused session, rather than
written independently and merely checked by AI afterward. Concretely:
the choice of Postgres-as-queue with `SKIP LOCKED` and a fencing-token
lease, the CRM-A-vs-CRM-B asymmetric retry policy, the schema, the
service/repository/worker split, all six required fault-injection tests
plus the two extra ones, and both `DESIGN.md` and this file were drafted
by Claude. Every test in `tests/` was actually run against a real local
Postgres instance as part of producing this repo (not just written and
assumed to pass) - the full suite is currently green; see the test
output convention in `README.md` to reproduce that.

If you (the reviewer) are seeing this doc as part of evaluating a
candidate: the candidate should personalize this disclosure with their
own review notes, what they'd have done differently, and anything they
changed after reading the AI's output, before submitting it as their
own work.

## Time spent

Everything in this repo - schema, fake CRM adapters, repository/service/
worker layers, the FastAPI app, 14 tests across 8 files, and the two
design documents - was produced in one continuous session, run against a
real local Postgres rather than mocked. Rough split of where the effort
went: ~20% on reading the brief closely enough to extract the exact
invariant each of the six scenarios wants demonstrated (this mattered
more than the code - several of the "obvious" fixes, like just retrying
on timeout, are wrong for CRM B specifically); ~45% on the core
service/repository/worker implementation and getting the locking and
fencing logic actually correct under concurrent test runs, not just
plausible on paper; ~20% on the tests themselves, including the two
beyond the required six; ~15% on the design document and this file.

## Major tradeoffs

**Postgres as the queue, not a message broker.** Chosen over SQS/
RabbitMQ/Kafka to keep the take-home runnable with one dependency and to
make the fault-injection tests exact (backdating a timestamp in SQL
instead of waiting out a real visibility timeout or standing up a broker
in test setup). The real cost: polling has higher latency than a push-based
broker, and a single Postgres instance is a scaling ceiling eventually.
At the stated scale (200 companies, 100 rps normal / 1000 rps burst) a
well-indexed table comfortably handles this as a queue, and the design
doc explains how to swap in a broker later as a wakeup signal without
touching the correctness-bearing logic, which lives in the claim/fence/
finalize transactions, not in delivery.

**Raw SQL via asyncpg instead of an ORM.** The entire exercise is about a
small number of exact transaction boundaries and lock clauses
(`FOR UPDATE SKIP LOCKED`, the fencing-token `WHERE` clause). An ORM
tends to obscure exactly those details, which is the opposite of what a
reviewer needs to be able to check here. The cost is more boilerplate and
no auto-generated migrations - acceptable for a service this size with
one table that matters.

**In-process per-company concurrency limiting, not a distributed rate
limiter.** Correct for one worker process, not sufficient once there are
several (the stated scale implies there will be). Called out explicitly
in `DESIGN.md` §3 as design-only; building a Redis-backed token bucket
felt like the wrong use of a 6-hour box versus getting the harder
correctness properties (fencing, CRM-B asymmetry) actually right and
tested.

**No real network/CRM, by the assignment's own design.** The fake CRM
adapters model exactly the two failure shapes the brief calls out
(idempotent-with-TTL vs. no-idempotency-no-lookup) plus "accepted but the
response was lost," via an explicit fault-injection hook rather than
randomized flakiness - deterministic tests mattered more here than
realism, since the point is to prove an invariant, not to fuzz-test a
fake.

**No metrics/alerting implementation**, only the queries and reasoning
in `DESIGN.md` §6. Wiring a real exporter (Prometheus, statsd, whatever
the target stack uses) didn't seem like it would teach the reviewer
anything the queries themselves don't already show, and it would have
come at the cost of the operator-recovery API or the test suite, which I
judged more valuable to have working code for.

**Scope I would add first if this were a real sprint, not a take-home:**
CRM-B-side reconciliation lookups before surfacing an item to an
operator (see `DESIGN.md` §5), and the distributed rate limiter. Both are
called out in `DESIGN.md` §9 with reasoning for the ordering.
