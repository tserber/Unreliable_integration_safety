"""
Not a fault-injection scenario - just a sanity check that the HTTP layer
wires accept -> inspect -> resolve correctly on top of the service layer
already covered by the scenario tests.
"""
import pytest
from httpx import ASGITransport, AsyncClient

from app.api import app


@pytest.mark.asyncio
async def test_accept_and_inspect_roundtrip(pool, monkeypatch):
    app.state.pool = pool
    from app.crm.registry import CRMRegistry

    registry = CRMRegistry()
    registry.register("acme", "crm_a")
    registry.register("globex", "crm_b")
    app.state.crm_registry = registry

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/companies/acme/handoffs", json={"event_id": "evt-http-1", "payload": {"customer": "Jane"}}
        )
        assert resp.status_code == 202
        handoff_id = resp.json()["id"]

        resp = await client.get(f"/companies/acme/handoffs/{handoff_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"

        resp = await client.get(f"/companies/globex/handoffs/{handoff_id}")
        assert resp.status_code == 404

        resp = await client.post("/companies/unknown-co/handoffs", json={"event_id": "x", "payload": {}})
        assert resp.status_code == 404
