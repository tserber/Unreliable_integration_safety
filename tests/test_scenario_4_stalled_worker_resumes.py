"""
Scenario 4: a worker stalls, loses ownership of its work, then resumes.

Invariant: once a second worker has reclaimed a row and finalized it, the
first (stalled) worker's eventual, delayed write must be a no-op. The
fencing token (lease_token, re-minted on every claim) is what guarantees
this - the stale worker's UPDATE carries the old token and matches zero
rows.
"""
import pytest

from app import repository, service
from tests.conftest import force_expire_lease


@pytest.mark.asyncio
async def test_stale_worker_write_is_fenced_out(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-stall", {"customer": "Jane"})

    async with pool.acquire() as conn:
        worker1_claim = await repository.claim_next_for_company(conn, "acme", "worker-1", settings.lease_seconds)
    assert worker1_claim is not None

    # worker-1 stalls (GC pause, blocked on a hung socket, whatever) long
    # enough for its lease to lapse.
    await force_expire_lease(pool, handoff_id)

    # worker-2 comes along, reclaims the same row, and finishes it cleanly.
    outcome = await service.process_one(pool, registry, settings, "acme", "worker-2")
    assert outcome == "completed"
    row_after_worker2 = await service.get_handoff(pool, "acme", handoff_id)
    assert row_after_worker2["status"] == "completed"
    real_reference = row_after_worker2["crm_reference_id"]

    # worker-1 now wakes back up and tries to write ITS OWN result using
    # the lease token it was handed a long time ago.
    async with pool.acquire() as conn:
        fenced = await repository.finalize_completed(
            conn, handoff_id, worker1_claim.lease_token, "worker-1-would-be-reference"
        )
    assert fenced is False, "the stale worker's write must be rejected"

    # Final state is exactly what worker-2 wrote. worker-1's write left no trace.
    final_row = await service.get_handoff(pool, "acme", handoff_id)
    assert final_row["status"] == "completed"
    assert final_row["crm_reference_id"] == real_reference
    assert final_row["crm_reference_id"] != "worker-1-would-be-reference"
