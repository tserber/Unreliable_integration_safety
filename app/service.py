"""
Orchestration layer: combines the repository (DB transactions) with a CRM
adapter to decide what "safe" looks like for a given attempt. This module
is deliberately framework-free (no FastAPI, no asyncio-loop-specific
globals) so that both the API, the worker, and the tests can call the same
functions directly and get the exact same behavior.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg

from app import repository
from app.config import Settings
from app.crm.base import CRMRejected, CRMTimeout
from app.crm.registry import CRMRegistry


async def accept_handoff(
    pool: asyncpg.Pool, company_id: str, event_id: str, payload: dict
) -> tuple[uuid.UUID, bool]:
    async with pool.acquire() as conn:
        if not await repository.company_exists(conn, company_id):
            raise repository.NotFound(f"unknown company {company_id!r}")
        return await repository.insert_handoff_if_new(conn, company_id, event_id, payload)


async def get_handoff(pool: asyncpg.Pool, company_id: str, handoff_id: uuid.UUID) -> dict:
    async with pool.acquire() as conn:
        row = await repository.get_handoff_for_company(conn, company_id, handoff_id)
        if row is None:
            raise repository.NotFound(f"handoff {handoff_id} not found for company {company_id!r}")
        row["audit_log"] = await repository.get_audit_log(conn, handoff_id)
        return row


async def resolve_handoff(
    pool: asyncpg.Pool,
    company_id: str,
    handoff_id: uuid.UUID,
    action: str,
    *,
    external_reference: str | None = None,
    reason: str | None = None,
) -> dict:
    async with pool.acquire() as conn:
        return await repository.operator_resolve(
            conn, company_id, handoff_id, action,
            external_reference=external_reference, reason=reason,
        )


async def process_one(
    pool: asyncpg.Pool,
    crm_registry: CRMRegistry,
    settings: Settings,
    company_id: str,
    worker_id: str,
) -> str:
    """
    Claim and attempt at most one handoff for `company_id`. Returns a short
    status string describing what happened, mainly so tests can assert on
    it without re-reading the DB. See module docstring in repository.py
    for why each of these branches exists.
    """
    async with pool.acquire() as conn:
        claimed = await repository.claim_next_for_company(conn, company_id, worker_id, settings.lease_seconds)
    if claimed is None:
        return "no_work"

    return await _process_claimed(pool, crm_registry, settings, claimed)


async def process_specific(
    pool: asyncpg.Pool,
    crm_registry: CRMRegistry,
    settings: Settings,
    company_id: str,
    handoff_id: uuid.UUID,
    worker_id: str,
) -> str:
    """
    Handle one delivery of a message naming a specific handoff_id - the
    shape a real queue message takes, and the shape that lets us
    demonstrate duplicate/delayed delivery directly: calling this twice
    (concurrently, or long after the first call already finished the
    row) is always safe. If the row isn't in a claimable state - already
    completed/needs_review/failed, or currently leased by someone else -
    this is a silent no-op, which is the correct response to a duplicate
    or a delayed delivery, not an error.
    """
    async with pool.acquire() as conn:
        claimed = await repository.claim_specific(conn, company_id, handoff_id, worker_id, settings.lease_seconds)
    if claimed is None:
        return "no_work"
    return await _process_claimed(pool, crm_registry, settings, claimed)


async def _process_claimed(pool, crm_registry: CRMRegistry, settings: Settings, claimed) -> str:
    company_id = claimed.company_id
    adapter = crm_registry.get(company_id)

    if claimed.was_reclaimed and not adapter.supports_idempotency:
        async with pool.acquire() as conn:
            await repository.finalize_needs_review(
                conn,
                claimed.id,
                claimed.lease_token,
                "reclaimed after a lease expiry while a prior attempt was in flight; "
                "this CRM has no idempotency key or lookup support, so we cannot tell "
                "whether the abandoned attempt already created a callback. Check the "
                "CRM directly before resolving.",
            )
        return "needs_review"

    if adapter.supports_idempotency and claimed.first_claimed_at is not None:
        age = datetime.now(timezone.utc) - claimed.first_claimed_at
        if age > timedelta(hours=settings.retry_cutoff_hours):
            async with pool.acquire() as conn:
                await repository.finalize_needs_review(
                    conn,
                    claimed.id,
                    claimed.lease_token,
                    f"in flight for over {settings.retry_cutoff_hours}h; the CRM's idempotency "
                    f"key is only retained {settings.idempotency_key_ttl_hours}h, so a further "
                    "automatic retry is no longer provably safe.",
                )
            return "needs_review"

    if claimed.attempt_count > claimed.max_attempts:
        async with pool.acquire() as conn:
            await repository.finalize_needs_review(
                conn,
                claimed.id,
                claimed.lease_token,
                f"exceeded max_attempts ({claimed.max_attempts}) without a clean outcome.",
            )
        return "needs_review"

    try:
        result = await asyncio.wait_for(
            adapter.create_callback(claimed.payload, claimed.crm_idempotency_key),
            timeout=settings.crm_call_timeout_seconds,
        )
    except CRMTimeout as exc:
        return await _handle_ambiguous(pool, adapter, claimed, str(exc))
    except asyncio.TimeoutError:
        return await _handle_ambiguous(pool, adapter, claimed, "client-side timeout waiting for CRM response")
    except CRMRejected as exc:
        async with pool.acquire() as conn:
            await repository.finalize_failed(conn, claimed.id, claimed.lease_token, str(exc))
        return "failed"

    async with pool.acquire() as conn:
        ok = await repository.finalize_completed(conn, claimed.id, claimed.lease_token, result.external_id)
    return "completed" if ok else "fenced_out"


async def _handle_ambiguous(pool, adapter, claimed, error: str) -> str:
    async with pool.acquire() as conn:
        if adapter.supports_idempotency:
            await repository.finalize_retry_later(conn, claimed.id, claimed.lease_token, error)
            return "retry_scheduled"
        await repository.finalize_needs_review(conn, claimed.id, claimed.lease_token, error)
        return "needs_review"
