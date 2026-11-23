"""
Not one of the six required scenarios, but directly answers a Part 3
question with a test instead of just prose: "behavior when an
idempotency key expires before recovery."

CRM A retains idempotency keys for 24h. If we kept blindly retrying a
stuck request right up to and past that window, a retry landing after
the key expired could create a genuine duplicate - exactly the bug we
otherwise avoid. So retries stop with margin to spare (retry_cutoff_hours,
default 20h) and the request is escalated instead of gambling.
"""
import pytest

from app import service
from tests.conftest import backdate_first_claimed


@pytest.mark.asyncio
async def test_old_in_flight_request_escalates_instead_of_retrying(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-old", {"customer": "Jane"})

    # Put it in 'processing' with a long-expired lease, as if it's been
    # bouncing between workers for a long time without a clean outcome.
    from app import repository

    async with pool.acquire() as conn:
        claimed = await repository.claim_next_for_company(conn, "acme", "w1", settings.lease_seconds)
    await backdate_first_claimed(pool, handoff_id, hours=settings.retry_cutoff_hours + 1)
    from tests.conftest import force_expire_lease

    await force_expire_lease(pool, handoff_id)

    outcome = await service.process_one(pool, registry, settings, "acme", "w2")
    assert outcome == "needs_review"

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "needs_review"
    assert "idempotency" in row["review_reason"]

    adapter = registry.get("acme")
    assert len(adapter.calls) == 0, "must not call the CRM once we're past the safety margin"
