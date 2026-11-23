from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app import repository, service
from app.config import load_settings
from app.crm.registry import CRMRegistry
from app.db import create_pool


class AcceptRequest(BaseModel):
    event_id: str
    payload: dict


class ResolveRequest(BaseModel):
    action: str  # "mark_completed" | "retry" | "mark_failed"
    external_reference: str | None = None
    reason: str | None = None


class NoHealthyWorkers(Exception):
    """Raised when the periodic worker-liveness check finds nobody
    consuming the queue. The API keeps accepting requests regardless -
    acceptance and processing are decoupled by design, see DESIGN.md -
    but callers hitting the health endpoint need to know the backlog
    isn't moving."""

    def __init__(self, healthy_worker_count: int) -> None:
        self.healthy_worker_count = healthy_worker_count
        super().__init__(f"no healthy workers (count={healthy_worker_count})")


async def _watch_workers(app: FastAPI) -> None:
    """Background task, one per API process: on an interval, ask Postgres
    how many workers have reported in recently and cache the verdict on
    app.state. This is the 'periodic healthcheck from the main app to the
    workers' - indirect (through their heartbeat rows, not a direct RPC
    to each worker), which keeps the API from depending on workers being
    reachable at all, only on the database both sides already depend on."""
    settings = app.state.settings
    while True:
        try:
            async with app.state.pool.acquire() as conn:
                count = await repository.healthy_worker_count(
                    conn, settings.worker_heartbeat_stale_seconds
                )
            app.state.healthy_worker_count = count
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a failed check must not kill the loop
            app.state.healthy_worker_count = None
        await asyncio.sleep(settings.worker_heartbeat_interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    app.state.settings = settings
    app.state.pool = await create_pool(settings)
    app.state.crm_registry = CRMRegistry()
    app.state.healthy_worker_count = None
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, crm_provider FROM companies")
        for r in rows:
            app.state.crm_registry.register(r["id"], r["crm_provider"])
    watcher = asyncio.create_task(_watch_workers(app))
    yield
    watcher.cancel()
    await asyncio.gather(watcher, return_exceptions=True)
    await app.state.pool.close()


app = FastAPI(title="handoff-sync", lifespan=lifespan)


@app.exception_handler(NoHealthyWorkers)
async def _no_healthy_workers_handler(request: Request, exc: NoHealthyWorkers):
    return JSONResponse(
        status_code=503,
        content={
            "error": "no_healthy_workers",
            "message": "no worker has reported a heartbeat within the staleness window",
            "healthy_worker_count": exc.healthy_worker_count,
        },
    )


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/health/workers")
async def health_workers():
    count = app.state.healthy_worker_count
    if not count:
        raise NoHealthyWorkers(count or 0)
    return {"ok": True, "healthy_worker_count": count}


@app.post("/companies/{company_id}/handoffs", status_code=202)
async def accept(company_id: str, body: AcceptRequest):
    try:
        handoff_id, created = await service.accept_handoff(
            app.state.pool, company_id, body.event_id, body.payload
        )
    except repository.NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"id": str(handoff_id), "accepted": True, "newly_created": created}


@app.get("/companies/{company_id}/handoffs/{handoff_id}")
async def inspect(company_id: str, handoff_id: uuid.UUID):
    try:
        row = await service.get_handoff(app.state.pool, company_id, handoff_id)
    except repository.NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _serialize(row)


@app.post("/companies/{company_id}/handoffs/{handoff_id}/resolve")
async def resolve(company_id: str, handoff_id: uuid.UUID, body: ResolveRequest):
    try:
        row = await service.resolve_handoff(
            app.state.pool,
            company_id,
            handoff_id,
            body.action,
            external_reference=body.external_reference,
            reason=body.reason,
        )
    except repository.NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except repository.Conflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _serialize(row)


def _serialize(row: dict) -> dict:
    out = dict(row)
    for key, value in list(out.items()):
        if isinstance(value, uuid.UUID):
            out[key] = str(value)
    if "audit_log" in out:
        out["audit_log"] = [
            {**entry, "created_at": entry["created_at"].isoformat()} for entry in out["audit_log"]
        ]
    for ts_field in ("created_at", "updated_at", "completed_at", "lease_expires_at", "first_claimed_at"):
        if out.get(ts_field) is not None and hasattr(out[ts_field], "isoformat"):
            out[ts_field] = out[ts_field].isoformat()
    return out
