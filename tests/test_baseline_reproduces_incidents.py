"""
Runs the ORIGINAL naive solution (original_problem/baseline.py, verbatim
from the assignment brief) against minimal fake db/queue/crm backends,
one test per Part 2 failure scenario from the brief, to show the bug
actually reproduces when the code runs - not just that it looks wrong on
a read-through.

baseline.py references `db`, `queue`, `crm` as bare globals with no
import (they are the brief's "undefined externals"). monkeypatch injects
fakes into the module's namespace so the exact, unmodified baseline code
executes against them.

Each fake is deliberately unlocked and includes a real `await` yield
point on every call (even "instant" ones), which is what turns the
baseline's missing synchronization from a theoretical gap into an actual,
deterministically-reproducible race when two coroutines are scheduled
concurrently via asyncio.gather.

Every test here has a named counterpart in tests/test_scenario_*.py that
exercises the SAME scenario against the new solution (app/service.py)
and asserts the invariant holds there instead.
"""
from __future__ import annotations

import asyncio
import inspect
import types
import uuid

import pytest

from original_problem import baseline


class FakeRequest:
    def __init__(self, id, company_id, payload):
        self.id = id
        self.company_id = company_id
        self.payload = payload
        self.status = "pending"
        self.crm_reference_id = None


class FakeDB:
    """Mirrors exactly the db.* calls baseline.py makes - no transactions,
    no locking, no unique constraint. That absence is the point: it is
    what the real Postgres schema (migrations/001_init.sql) replaces."""

    def __init__(self):
        self.by_event: dict[str, FakeRequest] = {}
        self.by_id: dict[uuid.UUID, FakeRequest] = {}
        self.insert_calls = 0

    async def event_exists(self, event_id):
        await asyncio.sleep(0)  # yield here = the TOCTOU window the brief describes
        return event_id in self.by_event

    async def insert_handoff(self, company_id, payload):
        await asyncio.sleep(0)
        self.insert_calls += 1
        req = FakeRequest(uuid.uuid4(), company_id, payload)
        self.by_event[payload["event_id"]] = req
        self.by_id[req.id] = req
        return req

    async def get_handoff(self, handoff_id):
        await asyncio.sleep(0)
        return self.by_id[handoff_id]

    async def mark_completed(self, handoff_id, external_id):
        await asyncio.sleep(0)
        req = self.by_id[handoff_id]
        req.status = "completed"
        req.crm_reference_id = external_id


class FakeQueue:
    def __init__(self):
        self.messages: list[dict] = []

    async def publish(self, message):
        self.messages.append(message)


class FakeCRM:
    def __init__(self):
        self.calls: list[dict] = []

    async def create_callback(self, payload):
        await asyncio.sleep(0)
        self.calls.append(payload)
        return types.SimpleNamespace(id=f"crm-ref-{len(self.calls)}")


@pytest.fixture
def wired_baseline(monkeypatch):
    db, queue, crm = FakeDB(), FakeQueue(), FakeCRM()
    monkeypatch.setattr(baseline, "db", db, raising=False)
    monkeypatch.setattr(baseline, "queue", queue, raising=False)
    monkeypatch.setattr(baseline, "crm", crm, raising=False)
    return db, queue, crm


# ---------------------------------------------------------------------------
# Scenario 1: two concurrent submissions of the same event.
# Counterpart: tests/test_scenario_1_concurrent_duplicate.py (new solution:
# exactly one row, via INSERT ... ON CONFLICT DO NOTHING).
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scenario_1_baseline_creates_two_rows_for_one_concurrent_duplicate(wired_baseline):
    db, queue, crm = wired_baseline
    payload = {"event_id": "evt-dup", "customer": "Jane"}

    await asyncio.gather(
        baseline.accept_handoff("acme", payload),
        baseline.accept_handoff("acme", payload),
    )

    # BUG: check-then-insert with no shared transaction or constraint -
    # both callers observe "not exists" before either has inserted.
    assert db.insert_calls == 2
    assert len(queue.messages) == 2


# ---------------------------------------------------------------------------
# Scenario 2: a crash after persistence but before work is scheduled.
# Counterpart: tests/test_scenario_2_crash_before_schedule.py (new solution:
# the row itself is the schedulable unit, no second "publish" step exists).
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scenario_2_baseline_loses_the_request_if_publish_never_runs(wired_baseline):
    db, queue, crm = wired_baseline
    payload = {"event_id": "evt-crash", "customer": "Jane"}

    # accept_handoff does insert_handoff, THEN queue.publish, as two
    # separate steps. We reproduce "crashed between them" directly by
    # calling only the first step - exactly the window a real process
    # crash would land in.
    req = await db.insert_handoff("acme", payload)

    # BUG: the row survived the "crash", but nothing will ever process
    # it - there is no polling fallback, only queue-driven dispatch, and
    # the queue never got a message.
    assert req.id in db.by_id
    assert queue.messages == []


# ---------------------------------------------------------------------------
# Scenario 3: a crash after external success but before local completion.
# Counterpart: tests/test_scenario_3_crash_after_crm_success.py (new
# solution: a reused idempotency key makes the CRM-A retry safe; CRM-B's
# ambiguous case routes to needs_review instead of being retried blindly).
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scenario_3_baseline_retry_duplicates_the_crm_call(wired_baseline):
    db, queue, crm = wired_baseline
    req = await db.insert_handoff("acme", {"event_id": "evt-1"})

    # First attempt: the CRM call succeeds, but the process crashes on
    # the very next line, before mark_completed ever runs.
    await crm.create_callback(req.payload)
    assert req.status == "pending"

    # An operator (or a naive "retry everything pending" sweep) redelivers
    # the same handoff_id.
    await baseline.process_handoff(req.id)

    # BUG: no idempotency key, no record of the earlier attempt - the
    # retry calls the CRM a second time for what may already be a real,
    # successful callback on the CRM's side.
    assert len(crm.calls) == 2


# ---------------------------------------------------------------------------
# Scenario 4: a worker stalls, loses ownership of its work, then resumes.
# Counterpart: tests/test_scenario_4_stalled_worker_resumes.py (new
# solution: a fencing token minted on every claim makes the stale worker's
# delayed write a guaranteed no-op).
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scenario_4_baseline_two_workers_racing_one_handoff_both_call_crm(wired_baseline):
    db, queue, crm = wired_baseline
    req = await db.insert_handoff("acme", {"event_id": "evt-race"})

    # "Worker stalls, a second worker picks the same row up, the first
    # one eventually resumes" is indistinguishable, from the baseline's
    # point of view, from two workers racing process_handoff concurrently -
    # there is no claim/lease concept to tell them apart in the first place.
    await asyncio.gather(
        baseline.process_handoff(req.id),
        baseline.process_handoff(req.id),
    )

    # BUG: both read status == 'pending' before either writes, both call
    # the CRM, both call mark_completed - whichever write lands last
    # silently wins, with no way to tell it apart from the "stale" one.
    assert len(crm.calls) == 2


# ---------------------------------------------------------------------------
# Scenario 5: duplicate or delayed queue delivery.
# Counterpart: tests/test_scenario_5_duplicate_delivery.py (new solution:
# a reclaimed/already-finalized row is excluded from claiming entirely,
# and a fenced-out finalize is a safe no-op).
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scenario_5_baseline_duplicate_delivery_calls_crm_twice(wired_baseline):
    db, queue, crm = wired_baseline
    req = await db.insert_handoff("acme", {"event_id": "evt-redelivered"})

    # A broker redelivering the same message while the first delivery is
    # still in flight: two concurrent process_handoff(same handoff_id)
    # calls, same shape as scenario 4's race, driven by delivery semantics
    # (at-least-once) rather than a worker stall.
    await asyncio.gather(
        baseline.process_handoff(req.id),
        baseline.process_handoff(req.id),
    )
    assert len(crm.calls) == 2  # BUG: duplicate delivery, duplicate CRM call

    # Delayed delivery AFTER the row is genuinely completed is the one
    # case the baseline does guard, because of the explicit early-return -
    # worth keeping honest about what *does* already work:
    calls_before = len(crm.calls)
    await baseline.process_handoff(req.id)  # a third, late-arriving redelivery
    assert len(crm.calls) == calls_before  # no new CRM call once status == completed


# ---------------------------------------------------------------------------
# Scenario 6: an attempted cross-company read or recovery action.
# Counterpart: tests/test_scenario_6_cross_company_isolation.py (new
# solution: company_id is part of every operator-facing WHERE clause).
# ---------------------------------------------------------------------------
def test_scenario_6_baseline_has_no_company_scoping_concept_at_all():
    # BUG, structurally rather than by race: process_handoff takes only a
    # bare handoff_id, with no company_id anywhere in its signature or
    # body. Nothing stops a caller (or a misrouted message) from acting on
    # any handoff_id regardless of which company it belongs to, because
    # the baseline has no notion of "belongs to" to check in the first
    # place - and there is no inspect/resolve surface at all for an
    # operator to make a controlled, company-scoped recovery decision.
    assert list(inspect.signature(baseline.process_handoff).parameters) == ["handoff_id"]
    assert not hasattr(baseline, "inspect_handoff")
    assert not hasattr(baseline, "resolve_handoff")
