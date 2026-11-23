"""
Part 1 bullet: 'A way for an operator to inspect a request and make a
controlled recovery decision.' tests/test_scenario_6_* already covers the
isolation half of this; this file covers the recovery actions themselves
(mark_completed, retry, mark_failed) on the new solution, including the
guardrails - wrong-state rejection and the required reason on
mark_failed - that make it a *controlled* decision rather than an
unconditional override.
"""
import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app import repository, service
from app.api import app


async def _force_needs_review(pool, handoff_id, reason="ambiguous outcome, needs a human"):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE handoff_requests SET status = 'needs_review', review_reason = $2 WHERE id = $1",
            handoff_id,
            reason,
        )


async def _force_failed(pool, handoff_id, error="gave up after max attempts"):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE handoff_requests SET status = 'failed', last_error = $2 WHERE id = $1",
            handoff_id,
            error,
        )


@pytest.mark.asyncio
async def test_operator_can_mark_a_needs_review_request_completed(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-1", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    row = await service.resolve_handoff(
        pool, "acme", handoff_id, "mark_completed", external_reference="crm-ref-manual-1"
    )

    assert row["status"] == "completed"
    assert row["crm_reference_id"] == "crm-ref-manual-1"
    assert row["review_reason"] is None  # cleared once resolved


@pytest.mark.asyncio
async def test_operator_can_retry_a_failed_request_and_a_worker_can_claim_it_again(pool, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-2", {"customer": "Jane"})
    await _force_failed(pool, handoff_id)

    row = await service.resolve_handoff(pool, "acme", handoff_id, "retry")
    assert row["status"] == "pending"
    assert row["claimed_by"] is None
    assert row["lease_token"] is None

    async with pool.acquire() as conn:
        claimed = await repository.claim_next_for_company(conn, "acme", "worker-x", settings.lease_seconds)
    assert claimed is not None
    assert claimed.id == handoff_id


@pytest.mark.asyncio
async def test_operator_mark_failed_requires_a_reason(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-3", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    with pytest.raises(ValueError):
        await service.resolve_handoff(pool, "acme", handoff_id, "mark_failed", reason=None)

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "needs_review"  # the bad request didn't change anything


@pytest.mark.asyncio
async def test_operator_mark_failed_with_a_reason_records_it(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-4", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    row = await service.resolve_handoff(
        pool, "acme", handoff_id, "mark_failed", reason="customer requested cancellation"
    )
    assert row["status"] == "failed"
    assert row["last_error"] == "customer requested cancellation"


@pytest.mark.asyncio
async def test_operator_cannot_resolve_a_request_that_is_still_pending(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-5", {"customer": "Jane"})

    with pytest.raises(repository.Conflict):
        await service.resolve_handoff(pool, "acme", handoff_id, "mark_completed", external_reference="x")


@pytest.mark.asyncio
async def test_operator_cannot_resolve_a_request_that_is_already_completed(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-6", {"customer": "Jane"})
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE handoff_requests SET status = 'completed', completed_at = now() WHERE id = $1",
            handoff_id,
        )

    with pytest.raises(repository.Conflict):
        await service.resolve_handoff(pool, "acme", handoff_id, "retry")


@pytest.mark.asyncio
async def test_two_operators_racing_conflicting_actions_only_one_wins(pool):
    """
    ACID regression: operator_resolve's read-check-write must run as one
    isolated transaction. Without that, the FOR UPDATE lock is released
    the instant the SELECT returns, the status check becomes a TOCTOU
    race, and two operators issuing conflicting actions on the same row
    at once (one mark_completed, one mark_failed) could both pass the
    check and both write - exactly the opposite of a *controlled*
    recovery decision.
    """
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-race", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    results = await asyncio.gather(
        service.resolve_handoff(pool, "acme", handoff_id, "mark_completed", external_reference="ref-A"),
        service.resolve_handoff(pool, "acme", handoff_id, "mark_failed", reason="ref-B reason"),
        return_exceptions=True,
    )

    outcomes = [r["status"] if isinstance(r, dict) else type(r) for r in results]
    successes = [r for r in results if isinstance(r, dict)]
    conflicts = [r for r in results if isinstance(r, repository.Conflict)]

    assert len(successes) == 1, f"expected exactly one action to win, got {outcomes}"
    assert len(conflicts) == 1, f"expected exactly one Conflict, got {outcomes}"

    row = await service.get_handoff(pool, "acme", handoff_id)
    events = [entry["event"] for entry in row["audit_log"]]
    operator_events = [e for e in events if e.startswith("operator_")]
    assert len(operator_events) == 1  # only the winning action was ever applied


@pytest.mark.asyncio
async def test_resolve_rejects_an_unknown_action(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-7", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    with pytest.raises(ValueError):
        await service.resolve_handoff(pool, "acme", handoff_id, "do_something_weird")


@pytest.mark.asyncio
async def test_every_resolve_action_is_audited(pool):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-8", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)
    await service.resolve_handoff(pool, "acme", handoff_id, "mark_completed", external_reference="crm-ref")

    row = await service.get_handoff(pool, "acme", handoff_id)
    events = [entry["event"] for entry in row["audit_log"]]
    assert "operator_marked_completed" in events


# --- same three actions, exercised through the HTTP layer -----------------

@pytest.mark.asyncio
async def test_resolve_via_http_mark_completed(pool, registry):
    app.state.pool = pool
    app.state.crm_registry = registry

    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-http-1", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/companies/acme/handoffs/{handoff_id}/resolve",
            json={"action": "mark_completed", "external_reference": "crm-ref-http"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed"


@pytest.mark.asyncio
async def test_resolve_via_http_rejects_wrong_state_with_409(pool, registry):
    app.state.pool = pool
    app.state.crm_registry = registry

    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-http-2", {"customer": "Jane"})
    # still pending - never forced into needs_review/failed

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/companies/acme/handoffs/{handoff_id}/resolve",
            json={"action": "mark_completed", "external_reference": "x"},
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_resolve_via_http_rejects_missing_reason_with_400(pool, registry):
    app.state.pool = pool
    app.state.crm_registry = registry

    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-op-http-3", {"customer": "Jane"})
    await _force_needs_review(pool, handoff_id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/companies/acme/handoffs/{handoff_id}/resolve",
            json={"action": "mark_failed"},
        )
    assert resp.status_code == 400
