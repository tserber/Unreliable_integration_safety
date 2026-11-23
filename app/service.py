"""
Orchestration layer: combines the repository (DB transactions) with a CRM
adapter to decide what "safe" looks like for a given attempt. This module
is deliberately framework-free (no FastAPI, no asyncio-loop-specific
globals) so that both the API, the worker, and the tests can call the same
functions directly and get the exact same behavior.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg

from app import repository
from app.config import Settings
from app.crm.base import CRMRejected, CRMTimeout
from app.crm.registry import CRMRegistry

log = logging.getLogger("service")


async def accept_handoff(
    pool: asyncpg.Pool, company_id: str, event_id: str, payload: dict
) -> tuple[uuid.UUID, bool]:
    async with pool.acquire() as conn:
        if not await repository.company_exists(conn, company_id):
            raise repository.NotFound(f"unknown company {company_id!r}")
        handoff_id, created = await repository.insert_handoff_if_new(conn, company_id, event_id, payload)

    if not created:
        # Incident: "Duplicate callback tasks." - a second (or Nth)
        # submission of an event we already have on file. Not an error -
        # the unique constraint already made this a no-op - but worth a
        # log line so a sudden spike in dedup hits (a retried client, a
        # broker redelivering upstream of us) is visible without having
        # to query the audit table.
        log.info(
            "duplicate submission suppressed: company=%s event_id=%s handoff_id=%s",
            company_id, event_id, handoff_id,
        )
    return handoff_id, created


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
    # Incident: "Operators retrying requests and accidentally creating
    # duplicates." - every manual recovery decision is logged here,
    # separately from the DB audit trail, so it stands out operationally
    # (e.g. a spike in operator "retry" actions against one company is a
    # signal worth alerting on even though each individual action is safe).
    log.info(
        "operator action requested: company=%s handoff_id=%s action=%s",
        company_id, handoff_id, action,
    )
    try:
        async with pool.acquire() as conn:
            row = await repository.operator_resolve(
                conn, company_id, handoff_id, action,
                external_reference=external_reference, reason=reason,
            )
    except (repository.NotFound, repository.Conflict, ValueError) as exc:
        log.warning(
            "operator action rejected: company=%s handoff_id=%s action=%s reason=%s",
            company_id, handoff_id, action, exc,
        )
        raise
    log.info(
        "operator action applied: company=%s handoff_id=%s action=%s new_status=%s",
        company_id, handoff_id, action, row["status"],
    )
    return row


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

    if claimed.was_reclaimed:
        # Incident: "Requests stuck in processing after worker restarts."
        # This IS that incident, caught in the act rather than left stuck:
        # a prior claim's lease expired (its worker crashed, stalled, or
        # was killed mid-call) and this is a different attempt picking the
        # row back up. Not an error by itself - it's exactly what the
        # lease/fencing design is for - but a sustained rate of reclaims
        # for one worker_id is a signal that worker is unhealthy.
        log.warning(
            "reclaimed handoff whose lease had expired: company=%s handoff_id=%s "
            "worker=%s attempt=%s",
            company_id, claimed.id, claimed.lease_token, claimed.attempt_count,
        )
        if not adapter.supports_idempotency:
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
            log.warning(
                "escalated to needs_review: company=%s handoff_id=%s reason=%s",
                company_id, claimed.id, "reclaimed row on a non-idempotent CRM",
            )
            return "needs_review"

    if adapter.supports_idempotency and claimed.first_claimed_at is not None:
        age = datetime.now(UTC) - claimed.first_claimed_at
        if age > timedelta(hours=settings.retry_cutoff_hours):
            # Incident: "Accepted requests that never reach the CRM." -
            # this is the guardrail against the automatic retry loop
            # itself becoming that incident: we stop retrying before the
            # CRM's idempotency key goes stale, rather than risk a real
            # duplicate, and hand it to a human instead.
            async with pool.acquire() as conn:
                await repository.finalize_needs_review(
                    conn,
                    claimed.id,
                    claimed.lease_token,
                    f"in flight for over {settings.retry_cutoff_hours}h; the CRM's idempotency "
                    f"key is only retained {settings.idempotency_key_ttl_hours}h, so a further "
                    "automatic retry is no longer provably safe.",
                )
            log.warning(
                "escalated to needs_review: company=%s handoff_id=%s reason=%s age_hours=%.1f",
                company_id, claimed.id, "approaching idempotency key expiry", age.total_seconds() / 3600,
            )
            return "needs_review"

    if claimed.attempt_count > claimed.max_attempts:
        # Incident: "Accepted requests that never reach the CRM." - the
        # request was accepted, has been retried repeatedly, and still
        # hasn't cleanly reached the CRM. Surfaced loudly rather than left
        # to silently keep retrying forever.
        async with pool.acquire() as conn:
            await repository.finalize_needs_review(
                conn,
                claimed.id,
                claimed.lease_token,
                f"exceeded max_attempts ({claimed.max_attempts}) without a clean outcome.",
            )
        log.warning(
            "escalated to needs_review: company=%s handoff_id=%s reason=%s attempts=%s",
            company_id, claimed.id, "exceeded max_attempts", claimed.attempt_count,
        )
        return "needs_review"

    try:
        result = await asyncio.wait_for(
            adapter.create_callback(claimed.payload, claimed.crm_idempotency_key),
            timeout=settings.crm_call_timeout_seconds,
        )
    except CRMTimeout as exc:
        return await _handle_ambiguous(pool, adapter, claimed, str(exc))
    except TimeoutError:
        return await _handle_ambiguous(pool, adapter, claimed, "client-side timeout waiting for CRM response")
    except CRMRejected as exc:
        # Incident: "Accepted requests that never reach the CRM." - a
        # clean rejection (not ambiguous, not retryable) still means the
        # customer's callback never got created, so it's worth its own
        # log line distinct from a retry-scheduled or needs_review outcome.
        async with pool.acquire() as conn:
            await repository.finalize_failed(conn, claimed.id, claimed.lease_token, str(exc))
        log.error(
            "handoff failed, CRM rejected it: company=%s handoff_id=%s error=%s",
            company_id, claimed.id, exc,
        )
        return "failed"

    async with pool.acquire() as conn:
        ok = await repository.finalize_completed(conn, claimed.id, claimed.lease_token, result.external_id)
    if not ok:
        # Incident: "Duplicate callback tasks." - this IS the case where a
        # duplicate was prevented rather than created: the CRM call we
        # just made succeeded, but a newer claim already decided this
        # row's fate (our lease expired while we were waiting on the CRM),
        # so our own fencing check discards this result instead of
        # overwriting whatever the newer claimant wrote.
        log.warning(
            "fenced out on completion: company=%s handoff_id=%s crm_reference_id=%s "
            "(a newer claim already finalized this row; this result was discarded)",
            company_id, claimed.id, result.external_id,
        )
        return "fenced_out"
    return "completed"


async def _handle_ambiguous(pool, adapter, claimed, error: str) -> str:
    company_id = claimed.company_id
    async with pool.acquire() as conn:
        if adapter.supports_idempotency:
            await repository.finalize_retry_later(conn, claimed.id, claimed.lease_token, error)
            # Safe retry - the idempotency key makes this not a duplicate
            # risk, so an info line (expected, routine) rather than a
            # warning is enough.
            log.info(
                "ambiguous outcome, retry scheduled (idempotent CRM): company=%s handoff_id=%s error=%s",
                company_id, claimed.id, error,
            )
            return "retry_scheduled"
        await repository.finalize_needs_review(conn, claimed.id, claimed.lease_token, error)
        # Incident: "Operators retrying requests and accidentally creating
        # duplicates." - this is exactly the ambiguous-outcome case that
        # incident came from: a CRM with no idempotency support timed out
        # and we genuinely cannot tell whether it already created the
        # callback. Escalated to a human instead of auto-retrying blindly.
        log.warning(
            "ambiguous outcome, escalated to needs_review (non-idempotent CRM): "
            "company=%s handoff_id=%s error=%s",
            company_id, claimed.id, error,
        )
        return "needs_review"
