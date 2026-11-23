"""
Scenario 2: a crash after persistence but before work is scheduled.

In the original design this was a real gap: accept_handoff did
`insert -> publish to queue` as two steps, and a crash between them left
a row that no worker would ever look at. Our design removes the second
step entirely - the inserted row, once committed, IS the schedulable
unit of work, discovered by polling status='pending'. So "crash right
after persistence" is indistinguishable from "normal operation" here;
there is nothing left to lose.

Invariant: a request that was successfully accepted (the API call
returned 202) will be found and processed by a worker, even if literally
nothing else happens between acceptance and the next poll.
"""
import pytest

from app import service


@pytest.mark.asyncio
async def test_pending_row_is_discoverable_with_no_further_action(pool, registry, settings):
    handoff_id, created = await service.accept_handoff(
        pool, "acme", "evt-crash-before-publish", {"customer": "Jane"}
    )
    assert created

    # Simulate "the process that accepted this request died right here" -
    # literally nothing else runs before a worker (which could be a brand
    # new process, started independently) comes along.
    outcome = await service.process_one(pool, registry, settings, "acme", "a-different-worker")
    assert outcome == "completed"

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "completed"
    assert row["crm_reference_id"] is not None
