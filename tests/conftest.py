import os

import asyncpg
import pytest
import pytest_asyncio

from app.config import Settings
from app.crm.registry import CRMRegistry
from app.db import create_pool

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://handoff:handoff@localhost:5432/handoff_test"
)


@pytest.fixture
def settings() -> Settings:
    # Short lease and short cutoffs so tests don't need to sleep for real
    # minutes to observe expiry - tests instead backdate timestamps
    # directly via SQL (see force_expire_lease below) which is faster and
    # deterministic, but a short default is still handy for a couple of
    # real-time-based checks.
    return Settings(
        database_url=TEST_DATABASE_URL,
        lease_seconds=2,
        idempotency_key_ttl_hours=24,
        retry_cutoff_hours=20,
        crm_call_timeout_seconds=2.0,
        poll_interval_seconds=0.05,
    )


@pytest_asyncio.fixture
async def pool(settings):
    pool = await create_pool(settings)
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE handoff_audit_log, handoff_requests, companies, "
            "worker_heartbeats RESTART IDENTITY CASCADE"
        )
        await conn.execute(
            """
            INSERT INTO companies (id, name, crm_provider, max_concurrency) VALUES
                ('acme', 'Acme (CRM A)', 'crm_a', 5),
                ('globex', 'Globex (CRM B)', 'crm_b', 5),
                ('initech', 'Initech (CRM A)', 'crm_a', 1)
            """
        )
    yield pool
    await pool.close()


@pytest.fixture
def registry() -> CRMRegistry:
    r = CRMRegistry()
    r.register("acme", "crm_a")
    r.register("globex", "crm_b")
    r.register("initech", "crm_a")
    return r


async def force_expire_lease(pool: asyncpg.Pool, handoff_id) -> None:
    """Test helper simulating 'time has passed / the worker stalled past
    its lease' without an actual sleep: backdate lease_expires_at."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE handoff_requests SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
            handoff_id,
        )


async def backdate_first_claimed(pool: asyncpg.Pool, handoff_id, hours: float) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE handoff_requests SET first_claimed_at = now() - ($2 || ' hours')::interval WHERE id = $1",
            handoff_id,
            str(hours),
        )
