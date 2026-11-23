from __future__ import annotations

from dataclasses import dataclass, field

from app.crm.base import CRMAdapter
from app.crm.crm_a import FakeCRMA
from app.crm.crm_b import FakeCRMB


@dataclass
class CRMRegistry:
    """
    Each company has its own CRM credentials (and, in a real system, its
    own HTTP client / connection pool / rate limiter) even when several
    companies share the same CRM *product*. We model that here with one
    adapter instance per company_id, keyed off the company's configured
    provider, so a fault injected for company A's CRM A instance never
    leaks into company B's CRM A instance.
    """

    providers: dict[str, str] = field(default_factory=dict)  # company_id -> "crm_a"|"crm_b"
    _instances: dict[str, CRMAdapter] = field(default_factory=dict)

    def register(self, company_id: str, provider: str) -> None:
        self.providers[company_id] = provider

    def get(self, company_id: str) -> CRMAdapter:
        if company_id not in self._instances:
            provider = self.providers.get(company_id, "crm_a")
            self._instances[company_id] = FakeCRMA() if provider == "crm_a" else FakeCRMB()
        return self._instances[company_id]
