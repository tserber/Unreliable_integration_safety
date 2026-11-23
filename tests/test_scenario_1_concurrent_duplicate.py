"""
Scenario 1: two (here, twenty) concurrent submissions of the same event.

Invariant: exactly one handoff_requests row is created, and exactly one
CRM callback is ever made for it, no matter how many callers race the
acceptance endpoint at once with the same (company_id, event_id).
"""
import asyncio

import pytest

from app import service


@pytest.mark.asyncio
async def test_concurrent_identical_submissions_create_one_row(pool):
    results = await asyncio.gather(
        *[
            service.accept_handoff(pool, "acme", "evt-dup-1", {"customer": "Jane"})
            for _ in range(20)
        ]
    )

    ids = {str(r[0]) for r in results}
    created_flags = [r[1] for r in results]

    assert len(ids) == 1, "all callers must agree on a single handoff id"
    assert sum(created_flags) == 1, "exactly one caller should have actually inserted a row"

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM handoff_requests WHERE company_id = 'acme' AND event_id = 'evt-dup-1'"
        )
    assert count == 1


@pytest.mark.asyncio
async def test_duplicate_submission_results_in_exactly_one_crm_call(pool, registry, settings):
    await asyncio.gather(
        *[
            service.accept_handoff(pool, "acme", "evt-dup-2", {"customer": "Jane"})
            for _ in range(10)
        ]
    )

    outcome = await service.process_one(pool, registry, settings, "acme", "worker-1")
    assert outcome == "completed"

    adapter = registry.get("acme")
    assert len(adapter.calls) == 1

    # A second processing pass finds no more work for this company.
    outcome2 = await service.process_one(pool, registry, settings, "acme", "worker-1")
    assert outcome2 == "no_work"
    assert len(adapter.calls) == 1
