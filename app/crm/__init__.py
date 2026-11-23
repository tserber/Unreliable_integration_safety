from app.crm.base import CRMAdapter, CRMError, CRMRejected, CRMResult, CRMTimeout
from app.crm.crm_a import FakeCRMA
from app.crm.crm_b import FakeCRMB

__all__ = [
    "CRMAdapter",
    "CRMError",
    "CRMTimeout",
    "CRMRejected",
    "CRMResult",
    "FakeCRMA",
    "FakeCRMB",
]
