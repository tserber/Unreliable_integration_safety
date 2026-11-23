"""
Scenario 5: duplicate or delayed queue delivery.

We model a queue message as "go process this specific handoff_id"
(service.process_specific), the same shape the original buggy
`process_handoff(handoff_id)` took. Two things a real broker will do to
us, and that our handling must survive:

  * duplicate delivery: the same message arrives twice, possibly
    concurrently (e.g. a consumer's ack was lost and the broker
    redelivers while the first delivery is still being handled).
  * delayed delivery: the message arrives long after the request was
    already finished some other way (a different message, an operator
    action, or a worker that processed it via the normal company-queue
    poll before this specific message ever showed up).

Invariant: no matter how many times or how late a message for the same
handoff_id arrives, the CRM is called at most once and the request ends
up completed exactly once.
"""
import asyncio

import pytest

from app import service


@pytest.mark.asyncio
async def test_concurrent_duplicate_delivery_processes_once(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-duplicate-delivery", {"customer": "Jane"})

    outcomes = await asyncio.gather(
        *[
            service.process_specific(pool, registry, settings, "acme", handoff_id, f"worker-{i}")
            for i in range(10)
        ]
    )

    assert outcomes.count("completed") == 1
    assert all(o in ("completed", "no_work") for o in outcomes)

    adapter = registry.get("acme")
    assert len(adapter.calls) == 1

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "completed"


@pytest.mark.asyncio
async def test_delayed_delivery_after_completion_is_a_noop(pool, registry, settings):
    handoff_id, _ = await service.accept_handoff(pool, "acme", "evt-delayed-delivery", {"customer": "Jane"})

    first = await service.process_specific(pool, registry, settings, "acme", handoff_id, "worker-1")
    assert first == "completed"

    adapter = registry.get("acme")
    assert len(adapter.calls) == 1

    # The same message, delivered again much later.
    late = await service.process_specific(pool, registry, settings, "acme", handoff_id, "worker-2")
    assert late == "no_work"
    assert len(adapter.calls) == 1, "a delayed redelivery after completion must not call the CRM again"

    row = await service.get_handoff(pool, "acme", handoff_id)
    assert row["status"] == "completed"
