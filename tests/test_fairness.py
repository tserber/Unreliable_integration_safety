"""
Not one of the six required scenarios, but demonstrates the "one
company's backlog delaying other companies" production incident directly,
and answers the Part 3 fairness/backpressure question with a test.

acme floods the system with 50 pending requests. initech has exactly one
pending request and a max_concurrency of 1. A single scheduling pass
(Worker.tick()) must still dispatch initech's request - it must not have
to wait for acme's backlog to drain first.
"""
import asyncio

import pytest

from app import service
from app.worker import Worker


@pytest.mark.asyncio
async def test_small_company_is_not_starved_by_a_large_backlog(pool, registry, settings):
    for i in range(50):
        await service.accept_handoff(pool, "acme", f"evt-flood-{i}", {"n": i})
    await service.accept_handoff(pool, "initech", "evt-small-1", {"customer": "Bob"})

    worker = Worker(pool, registry, settings, worker_id="fairness-test-worker")
    tasks = await worker.tick()
    await asyncio.gather(*tasks)

    initech_row_status = None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status FROM handoff_requests WHERE company_id = 'initech' AND event_id = 'evt-small-1'"
        )
        initech_row_status = row["status"]

    assert initech_row_status == "completed", (
        "initech's single request must be served in the same scheduling "
        "pass as acme's flood, not queued behind it"
    )

    adapter = registry.get("acme")
    # acme's own max_concurrency (5, per conftest) bounds how many of its
    # 50 requests got dispatched in one pass - the rest wait for the next
    # tick, which is the backpressure half of the same mechanism.
    assert len(adapter.calls) <= 5
