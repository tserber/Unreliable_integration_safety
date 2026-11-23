"""
Periodic worker healthcheck: workers upsert a liveness row on an
interval, and the API reads that row (not the worker directly - there is
no RPC between them) to tell 'queue is quiet because there's no work'
apart from 'queue is quiet because nobody is consuming it'.
"""
import pytest
from httpx import ASGITransport, AsyncClient

from app import repository
from app.api import NoHealthyWorkers, app
from app.worker import Worker


@pytest.mark.asyncio
async def test_heartbeat_counts_as_healthy_until_stale(pool, settings):
    async with pool.acquire() as conn:
        assert await repository.healthy_worker_count(conn, settings.worker_heartbeat_stale_seconds) == 0

        await repository.record_worker_heartbeat(conn, "worker-1")
        assert await repository.healthy_worker_count(conn, settings.worker_heartbeat_stale_seconds) == 1

        # A heartbeat older than the staleness window no longer counts -
        # the worker is presumed dead rather than just quiet.
        await conn.execute(
            "UPDATE worker_heartbeats SET last_seen_at = now() - interval '1 hour' "
            "WHERE worker_id = 'worker-1'"
        )
        assert await repository.healthy_worker_count(conn, settings.worker_heartbeat_stale_seconds) == 0


@pytest.mark.asyncio
async def test_worker_send_heartbeat_upserts_its_own_row(pool, registry, settings):
    worker = Worker(pool, registry, settings, worker_id="worker-abc")
    await worker.send_heartbeat()
    await worker.send_heartbeat()  # second call must upsert, not duplicate

    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT worker_id FROM worker_heartbeats")
    assert [r["worker_id"] for r in rows] == ["worker-abc"]


@pytest.mark.asyncio
async def test_health_workers_endpoint_reports_custom_error_when_nobody_is_alive(pool):
    app.state.pool = pool
    app.state.healthy_worker_count = 0

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health/workers")

    assert resp.status_code == 503
    body = resp.json()
    assert body["error"] == "no_healthy_workers"
    assert body["healthy_worker_count"] == 0


@pytest.mark.asyncio
async def test_health_workers_endpoint_ok_when_a_worker_is_alive(pool):
    app.state.pool = pool
    app.state.healthy_worker_count = 1

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health/workers")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "healthy_worker_count": 1}


def test_no_healthy_workers_exception_carries_the_count():
    exc = NoHealthyWorkers(0)
    assert exc.healthy_worker_count == 0
    assert "count=0" in str(exc)
