"""
Scenario 6: an attempted cross-company read or recovery action.

Invariant: company_id is part of the WHERE clause on every operator-facing
read and write, not just a filter applied after the fact. A request for
another company's handoff_id, however it's spelled, must come back
identical to "that id doesn't exist" - no row contents leak, and no
recovery action is accepted.
"""
import pytest

from app import repository, service


@pytest.mark.asyncio
async def test_cross_company_read_is_not_found(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-isolation-read", {"customer": "Jane", "ssn": "secret"})

    with pytest.raises(repository.NotFound):
        await service.get_handoff(pool, "globex", handoff_id)

    # The owning company can still read it fine.
    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["id"] == handoff_id


@pytest.mark.asyncio
async def test_cross_company_resolve_is_rejected(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "globex", "evt-isolation-resolve", {"customer": "Bob"})
    # Get it into needs_review so there's something a recovery action could act on.
    async with pool.acquire() as conn:
        claimed = await repository.claim_next_for_company(conn, "globex", "w1", settings.lease_seconds)
        await repository.finalize_needs_review(conn, claimed.id, claimed.lease_token, "test setup")

    with pytest.raises(repository.NotFound):
        await service.resolve_handoff(pool, "acme", handoff_id, "mark_completed", external_reference="forged-id")

    # Row is untouched by the rejected cross-company attempt.
    row = await service.get_handoff(pool, "globex", handoff_id)
    assert row["status"] == "needs_review"
    assert row["crm_reference_id"] is None

    # The owning company can resolve it.
    resolved = await service.resolve_handoff(pool, "globex", handoff_id, "mark_completed", external_reference="real-id")
    assert resolved["status"] == "completed"


@pytest.mark.asyncio
async def test_cross_company_specific_delivery_is_ignored(pool, registry, settings):
    """A message that names the right handoff_id but the wrong company_id
    (corrupted message, or an attempt to process one company's work under
    another company's quota/credentials) must not be processed."""
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-isolation-process", {"customer": "Jane"})

    outcome = await service.process_specific(pool, registry, settings, "globex", handoff_id, "worker-x")
    assert outcome == "no_work"

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "pending", "the row must remain untouched, not claimed under the wrong company"

    adapter = registry.get("globex")
    assert len(adapter.calls) == 0
