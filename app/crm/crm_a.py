from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field

from app.crm.base import CallRecord, CRMRejected, CRMResult, CRMTimeout, FaultInjector


@dataclass
class FakeCRMA:
    """
    Models a CRM that supports idempotency keys, retained for 24h
    (per the assignment's stated constraint). Calling create_callback
    twice with the same idempotency_key returns the SAME external_id and
    creates no second record - this is what makes blind retries safe for
    this CRM, including retries that happen after we already got a
    timeout back once.
    """

    name: str = "crm_a"
    supports_idempotency: bool = True
    latency_seconds: float = 0.0

    _store: dict[str, str] = field(default_factory=dict)  # idempotency_key -> external_id
    calls: list[CallRecord] = field(default_factory=list)
    faults: FaultInjector = field(default_factory=FaultInjector)

    async def create_callback(self, payload: dict, idempotency_key: str) -> CRMResult:
        if self.latency_seconds:
            await asyncio.sleep(self.latency_seconds)

        if idempotency_key in self._store:
            # True CRM-side dedup: no new record, same reference returned.
            self.calls.append(CallRecord(idempotency_key, payload, "success"))
            return CRMResult(external_id=self._store[idempotency_key])

        mode = self.faults.consume(idempotency_key)

        if mode == "rejected":
            self.calls.append(CallRecord(idempotency_key, payload, "rejected"))
            raise CRMRejected("CRM A rejected the payload")

        if mode == "timeout_then_not_created":
            self.calls.append(CallRecord(idempotency_key, payload, "timeout"))
            raise CRMTimeout("CRM A call timed out, request never arrived")

        if mode == "timeout_then_succeeded":
            # The side effect happens server-side, we just never hear
            # back about it.
            external_id = str(uuid.uuid4())
            self._store[idempotency_key] = external_id
            self.calls.append(CallRecord(idempotency_key, payload, "timeout"))
            raise CRMTimeout("CRM A call timed out, response lost after acceptance")

        external_id = str(uuid.uuid4())
        self._store[idempotency_key] = external_id
        self.calls.append(CallRecord(idempotency_key, payload, "success"))
        return CRMResult(external_id=external_id)

    def created_record_count(self) -> int:
        return len(self._store)
