from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field

from app.crm.base import CallRecord, CRMRejected, CRMResult, CRMTimeout, FaultInjector


@dataclass
class FakeCRMB:
    """
    Models a CRM with NO idempotency key support and NO way to look up
    an existing callback by our reference. Every call that reaches this
    adapter and isn't rejected creates a brand new record - there is no
    server-side dedup safety net at all.

    This is intentional: CRM B's fake does nothing clever to protect us.
    The protection for CRM B has to live in our own worker policy (never
    blindly retry an ambiguous or reclaimed CRM-B call), not in this
    adapter. That is the point of the exercise.
    """

    name: str = "crm_b"
    supports_idempotency: bool = False
    latency_seconds: float = 0.0

    created: list[str] = field(default_factory=list)
    calls: list[CallRecord] = field(default_factory=list)
    # Keyed by an arbitrary caller-chosen tag (we use crm_idempotency_key
    # as a convenient *local* correlation id for fault injection only -
    # CRM B itself never sees or honors it).
    faults: FaultInjector = field(default_factory=FaultInjector)

    async def create_callback(self, payload: dict, idempotency_key: str) -> CRMResult:
        if self.latency_seconds:
            await asyncio.sleep(self.latency_seconds)

        mode = self.faults.consume(idempotency_key)

        if mode == "rejected":
            self.calls.append(CallRecord(idempotency_key, payload, "rejected"))
            raise CRMRejected("CRM B rejected the payload")

        if mode == "timeout_then_not_created":
            self.calls.append(CallRecord(idempotency_key, payload, "timeout"))
            raise CRMTimeout("CRM B call timed out, request never arrived")

        if mode == "timeout_then_succeeded":
            external_id = str(uuid.uuid4())
            self.created.append(external_id)  # the side effect really happens
            self.calls.append(CallRecord(idempotency_key, payload, "timeout"))
            raise CRMTimeout("CRM B call timed out, response lost after acceptance")

        external_id = str(uuid.uuid4())
        self.created.append(external_id)
        self.calls.append(CallRecord(idempotency_key, payload, "success"))
        return CRMResult(external_id=external_id)

    def created_record_count(self) -> int:
        return len(self.created)
