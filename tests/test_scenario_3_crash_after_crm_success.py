"""
Scenario 3: a crash after external success but before local completion.

We simulate this directly rather than via a real kill -9: call the CRM
adapter ourselves and get a real success back, then deliberately do NOT
call finalize_completed (as if the process died right there, before the
write). Then let the lease expire and hand the row to a second attempt,
exactly as a second worker would pick it up after the first one's lease
lapses.

The two CRMs genuinely cannot be given the same guarantee here, and the
test asserts the honest, different outcome for each:

  * CRM A (idempotency keys, 24h retention): the retry is safe. It calls
    create_callback again with the SAME idempotency key, the CRM
    deduplicates server-side, no second record is created, and we
    converge on 'completed' with the original reference id.

  * CRM B (no idempotency, no lookup): we do NOT retry at all. Retrying
    blind could create a second, real callback with no way to detect or
    merge it afterward. Instead the request is routed to needs_review,
    status stays out of the auto-retry pool, and a human has to check
    CRM B directly. This is the assignment's "do not claim stronger
    guarantees than the external systems allow" in its most literal form.
"""
import pytest

from app import repository, service
from tests.conftest import force_expire_lease


@pytest.mark.asyncio
async def test_crm_a_converges_safely_after_crash(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-crash-a", {"customer": "Jane"})

    async with pool.acquire() as conn:
        claimed = await repository.claim_next_for_company(conn, "acme", "worker-doomed", settings.lease_seconds)

    adapter = registry.get("acme")
    result = await adapter.create_callback(claimed.payload, claimed.crm_idempotency_key)
    # <-- "crash" happens right here: we never call finalize_completed.

    await force_expire_lease(pool, handoff_id)

    outcome = await service.process_one(pool, registry, settings, "acme", "worker-recovered")
    assert outcome == "completed"

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "completed"
    assert row["crm_reference_id"] == result.external_id
    assert adapter.created_record_count() == 1, "must not create a second CRM-side record"
    assert len(adapter.calls) == 2, "both the doomed call and the recovery retry reached the CRM"


@pytest.mark.asyncio
async def test_crm_b_escalates_to_review_instead_of_risking_a_duplicate(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "globex", "evt-crash-b", {"customer": "Jane"})

    async with pool.acquire() as conn:
        claimed = await repository.claim_next_for_company(conn, "globex", "worker-doomed", settings.lease_seconds)

    adapter = registry.get("globex")
    result = await adapter.create_callback(claimed.payload, claimed.crm_idempotency_key)
    # <-- "crash" happens right here, same as above.

    await force_expire_lease(pool, handoff_id)

    outcome = await service.process_one(pool, registry, settings, "globex", "worker-recovered")
    assert outcome == "needs_review"

    row = await service.get_handoff(pool, "globex", handoff_id)
    assert row["status"] == "needs_review"
    assert row["crm_reference_id"] is None
    # Exactly the one real side effect from the doomed call - the recovery
    # path must NOT have called CRM B a second time.
    assert adapter.created_record_count() == 1
    assert len(adapter.calls) == 1

    # An operator who checks CRM B's dashboard and finds the record
    # (matching `result.external_id`) resolves it by hand:
    resolved = await service.resolve_handoff(
        pool, "globex", handoff_id, "mark_completed", external_reference=result.external_id
    )
    assert resolved["status"] == "completed"
    assert resolved["crm_reference_id"] == result.external_id
