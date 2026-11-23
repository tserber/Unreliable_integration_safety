from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class CRMError(Exception):
    """Base class for CRM adapter errors."""


class CRMTimeout(CRMError):
    """
    We do not know what happened. The call may have never reached the CRM,
    or the CRM may have processed it and the response was lost. This is
    the case the assignment calls out explicitly: "External network calls
    can time out after the CRM has accepted the request." Callers MUST
    NOT treat this the same as a clean failure.
    """


class CRMRejected(CRMError):
    """The CRM understood the request and definitively refused it
    (bad payload, auth failure, etc). Safe to treat as a clean failure -
    there is no ambiguity about whether a side effect happened."""


@dataclass
class CRMResult:
    external_id: str


@dataclass
class CallRecord:
    idempotency_key: str
    payload: dict
    outcome: str  # "success" | "timeout" | "rejected"


class CRMAdapter(Protocol):
    supports_idempotency: bool
    name: str

    async def create_callback(self, payload: dict, idempotency_key: str) -> CRMResult: ...


@dataclass
class FaultPlan:
    """
    Test/demo hook: lets a scenario script exactly what should happen the
    next time a given idempotency_key (CRM A) or call index (CRM B) is
    used, without needing a real flaky network to reproduce it.

    mode:
      None      -> normal success
      "timeout_then_succeeded" -> the CRM-side side effect happens (a
                   record is created / stored), but the adapter raises
                   CRMTimeout instead of returning it to the caller. This
                   is the "accepted but we never found out" case.
      "timeout_then_not_created" -> the adapter raises CRMTimeout and no
                   side effect happens at all (request never arrived).
      "rejected" -> raises CRMRejected, no side effect.
    """

    mode: str | None = None
    uses_remaining: int = 1


@dataclass
class FaultInjector:
    _plans: dict[str, FaultPlan] = field(default_factory=dict)

    def arm(self, key: str, mode: str, uses: int = 1) -> None:
        self._plans[key] = FaultPlan(mode=mode, uses_remaining=uses)

    def consume(self, key: str) -> str | None:
        plan = self._plans.get(key)
        if plan is None or plan.mode is None:
            return None
        mode = plan.mode
        plan.uses_remaining -= 1
        if plan.uses_remaining <= 0:
            del self._plans[key]
        return mode
