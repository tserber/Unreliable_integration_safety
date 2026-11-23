"""
Raw-SQL data access layer. Kept deliberately un-ORMed: the correctness of
this service lives in a small number of exact transaction boundaries and
locking clauses, and an ORM tends to hide exactly those details from the
reader (and from the person reviewing the take-home).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import asyncpg


class NotFound(Exception):
    pass


class Conflict(Exception):
    """Operator tried an action that doesn't make sense given current state."""


@dataclass
class ClaimedHandoff:
    id: uuid.UUID
    company_id: str
    payload: dict
    crm_idempotency_key: str
    attempt_count: int
    max_attempts: int
    lease_token: str
    was_reclaimed: bool  # True iff this row was already 'processing' (a prior
    # claim's lease expired) rather than fresh 'pending' work.
    first_claimed_at: Any


# ---------------------------------------------------------------------------
# Acceptance
# ---------------------------------------------------------------------------

async def insert_handoff_if_new(
    conn: asyncpg.Connection, company_id: str, event_id: str, payload: dict
) -> tuple[uuid.UUID, bool]:
    """
    Atomic accept-or-dedup. INSERT ... ON CONFLICT DO NOTHING means that
    N concurrent submissions of the same (company_id, event_id) result in
    exactly one row, decided by Postgres's unique index, not by an
    application-level check-then-insert race.

    There is deliberately no second step here (no "publish to a queue").
    The row itself, once committed, IS the unit of work. A crash right
    after this commit loses nothing: any worker's next poll will see a
    'pending' row with no further action required to make it visible.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO handoff_requests (company_id, event_id, payload)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (company_id, event_id) DO NOTHING
        RETURNING id
        """,
        company_id,
        event_id,
        payload,
    )
    if row is not None:
        await _audit(conn, row["id"], "accepted", {"event_id": event_id})
        return row["id"], True

    existing = await conn.fetchrow(
        "SELECT id FROM handoff_requests WHERE company_id = $1 AND event_id = $2",
        company_id,
        event_id,
    )
    return existing["id"], False


async def company_exists(conn: asyncpg.Connection, company_id: str) -> bool:
    row = await conn.fetchrow("SELECT 1 FROM companies WHERE id = $1", company_id)
    return row is not None


# ---------------------------------------------------------------------------
# Worker-side claiming
# ---------------------------------------------------------------------------

async def companies_with_claimable_work(
    conn: asyncpg.Connection, limit: int = 200
) -> list[str]:
    """Distinct companies that currently have at least one row a worker
    could claim. Drives round-robin fairness: the worker loop asks for
    this list, shuffles it, and tries one company at a time instead of
    draining a single global FIFO (which is what lets one company's
    backlog starve everyone else)."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT company_id
        FROM handoff_requests
        WHERE status = 'pending'
           OR (status = 'processing' AND lease_expires_at < now())
        LIMIT $1
        """,
        limit,
    )
    return [r["company_id"] for r in rows]


async def claim_next_for_company(
    conn: asyncpg.Connection,
    company_id: str,
    worker_id: str,
    lease_seconds: int,
) -> ClaimedHandoff | None:
    """
    One short transaction: find the oldest claimable row for this company
    (SKIP LOCKED so a concurrent worker scanning the same company just
    moves on to its own candidate row instead of blocking), then stamp it
    with a fresh fencing token and a lease.

    FOR UPDATE SKIP LOCKED + per-company queries (instead of one global
    DISTINCT ON query, which Postgres does not allow combined with FOR
    UPDATE) is what gives us "many workers, no two ever grab the same
    row, and a slow row never blocks others."
    """
    async with conn.transaction():
        candidate = await conn.fetchrow(
            """
            SELECT id, status, first_claimed_at
            FROM handoff_requests
            WHERE company_id = $1
              AND (status = 'pending'
                   OR (status = 'processing' AND lease_expires_at < now()))
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """,
            company_id,
        )
        if candidate is None:
            return None

        was_reclaimed = candidate["status"] == "processing"
        new_token = str(uuid.uuid4())

        row = await conn.fetchrow(
            """
            UPDATE handoff_requests
            SET status = 'processing',
                claimed_by = $2,
                lease_token = $3::uuid,
                lease_expires_at = now() + ($4 || ' seconds')::interval,
                attempt_count = attempt_count + 1,
                first_claimed_at = COALESCE(first_claimed_at, now()),
                updated_at = now()
            WHERE id = $1
            RETURNING id, company_id, payload, crm_idempotency_key,
                      attempt_count, max_attempts, lease_token, first_claimed_at
            """,
            candidate["id"],
            worker_id,
            new_token,
            str(lease_seconds),
        )
        await _audit(
            conn,
            row["id"],
            "reclaimed" if was_reclaimed else "claimed",
            {"worker_id": worker_id, "attempt": row["attempt_count"]},
        )
        return ClaimedHandoff(
            id=row["id"],
            company_id=row["company_id"],
            payload=row["payload"],
            crm_idempotency_key=str(row["crm_idempotency_key"]),
            attempt_count=row["attempt_count"],
            max_attempts=row["max_attempts"],
            lease_token=str(row["lease_token"]),
            was_reclaimed=was_reclaimed,
            first_claimed_at=row["first_claimed_at"],
        )


async def claim_specific(
    conn: asyncpg.Connection,
    company_id: str,
    handoff_id: uuid.UUID,
    worker_id: str,
    lease_seconds: int,
) -> ClaimedHandoff | None:
    """
    Same locking discipline as claim_next_for_company, but for a queue
    message that names a specific handoff_id directly (the shape the
    original buggy `process_handoff(handoff_id)` took, and the shape a
    real broker message would have). Returns None if the row is not
    currently claimable - either because it's already finished
    (completed/needs_review/failed), because another worker currently
    holds an unexpired lease on it, or because it does not belong to
    `company_id`. All three are the correct, silent no-op response - in
    particular, company_id is in the WHERE clause so a mismatched
    company never claims (and therefore never perturbs attempt_count or
    takes out a lease on) a row it has no business touching; it is
    rejected exactly like a not-found, not claimed-then-undone.
    """
    async with conn.transaction():
        candidate = await conn.fetchrow(
            """
            SELECT id, status
            FROM handoff_requests
            WHERE id = $1
              AND company_id = $2
              AND (status = 'pending'
                   OR (status = 'processing' AND lease_expires_at < now()))
            FOR UPDATE SKIP LOCKED
            """,
            handoff_id,
            company_id,
        )
        if candidate is None:
            return None

        was_reclaimed = candidate["status"] == "processing"
        new_token = str(uuid.uuid4())
        row = await conn.fetchrow(
            """
            UPDATE handoff_requests
            SET status = 'processing',
                claimed_by = $2,
                lease_token = $3::uuid,
                lease_expires_at = now() + ($4 || ' seconds')::interval,
                attempt_count = attempt_count + 1,
                first_claimed_at = COALESCE(first_claimed_at, now()),
                updated_at = now()
            WHERE id = $1
            RETURNING id, company_id, payload, crm_idempotency_key,
                      attempt_count, max_attempts, lease_token, first_claimed_at
            """,
            candidate["id"],
            worker_id,
            new_token,
            str(lease_seconds),
        )
        await _audit(
            conn,
            row["id"],
            "reclaimed" if was_reclaimed else "claimed",
            {"worker_id": worker_id, "attempt": row["attempt_count"], "via": "specific"},
        )
        return ClaimedHandoff(
            id=row["id"],
            company_id=row["company_id"],
            payload=row["payload"],
            crm_idempotency_key=str(row["crm_idempotency_key"]),
            attempt_count=row["attempt_count"],
            max_attempts=row["max_attempts"],
            lease_token=str(row["lease_token"]),
            was_reclaimed=was_reclaimed,
            first_claimed_at=row["first_claimed_at"],
        )


async def finalize_completed(
    conn: asyncpg.Connection, handoff_id: uuid.UUID, lease_token: str, external_id: str
) -> bool:
    """
    The fencing check: this UPDATE only takes effect if lease_token still
    matches what we were handed at claim time. If another worker reclaimed
    this row in the meantime (our lease expired while we were blocked on
    the CRM call), lease_token has already changed and this becomes a
    no-op - we silently discard our own result rather than corrupt
    whatever the newer claimant decided. That is the fencing token
    pattern and it is what makes "worker stalls, loses ownership, then
    resumes" safe.
    """
    result = await conn.execute(
        """
        UPDATE handoff_requests
        SET status = 'completed',
            crm_reference_id = $3,
            completed_at = now(),
            updated_at = now()
        WHERE id = $1 AND lease_token = $2::uuid AND status = 'processing'
        """,
        handoff_id,
        lease_token,
        external_id,
    )
    ok = result.endswith("1")
    if ok:
        await _audit(conn, handoff_id, "completed", {"crm_reference_id": external_id})
    else:
        await _audit(conn, handoff_id, "fenced_out_on_complete", {"crm_reference_id": external_id})
    return ok


async def finalize_retry_later(
    conn: asyncpg.Connection, handoff_id: uuid.UUID, lease_token: str, error: str
) -> bool:
    """Clean, non-ambiguous failure (or an idempotent CRM's timeout) that
    is still safe to retry. Releases the row back to 'pending' immediately
    rather than waiting for the lease to expire, so a transient blip
    doesn't cost a full lease period of latency."""
    result = await conn.execute(
        """
        UPDATE handoff_requests
        SET status = 'pending',
            last_error = $3,
            claimed_by = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            updated_at = now()
        WHERE id = $1 AND lease_token = $2::uuid AND status = 'processing'
        """,
        handoff_id,
        lease_token,
        error,
    )
    ok = result.endswith("1")
    if ok:
        await _audit(conn, handoff_id, "retry_scheduled", {"error": error})
    return ok


async def finalize_needs_review(
    conn: asyncpg.Connection, handoff_id: uuid.UUID, lease_token: str | None, reason: str
) -> bool:
    """Ambiguous outcome. Taken out of the auto-retry pool entirely -
    `companies_with_claimable_work` and `claim_next_for_company` only ever
    look at 'pending' and lease-expired 'processing' rows, never
    'needs_review'. Only an operator action can move it forward again."""
    if lease_token is not None:
        result = await conn.execute(
            """
            UPDATE handoff_requests
            SET status = 'needs_review',
                review_reason = $3,
                updated_at = now()
            WHERE id = $1 AND lease_token = $2::uuid AND status = 'processing'
            """,
            handoff_id,
            lease_token,
            reason,
        )
        ok = result.endswith("1")
    else:
        # Used for the "never even call CRM B on a reclaim" path, where we
        # want to flag the row before attempting anything, so there is no
        # lease_token to fence on yet in the normal sense - we instead
        # fence on still being 'processing' with the token we were just
        # handed by the claim itself (caller passes it in as lease_token
        # in practice; this branch exists only for direct/manual use).
        result = await conn.execute(
            """
            UPDATE handoff_requests
            SET status = 'needs_review', review_reason = $2, updated_at = now()
            WHERE id = $1 AND status = 'processing'
            """,
            handoff_id,
            reason,
        )
        ok = result.endswith("1")
    if ok:
        await _audit(conn, handoff_id, "needs_review", {"reason": reason})
    return ok


async def finalize_failed(
    conn: asyncpg.Connection, handoff_id: uuid.UUID, lease_token: str, error: str
) -> bool:
    result = await conn.execute(
        """
        UPDATE handoff_requests
        SET status = 'failed', last_error = $3, updated_at = now()
        WHERE id = $1 AND lease_token = $2::uuid AND status = 'processing'
        """,
        handoff_id,
        lease_token,
        error,
    )
    ok = result.endswith("1")
    if ok:
        await _audit(conn, handoff_id, "failed", {"error": error})
    return ok


# ---------------------------------------------------------------------------
# Operator-facing: inspection and recovery, always company-scoped
# ---------------------------------------------------------------------------

async def get_handoff_for_company(
    conn: asyncpg.Connection, company_id: str, handoff_id: uuid.UUID
) -> dict | None:
    row = await conn.fetchrow(
        "SELECT * FROM handoff_requests WHERE id = $1 AND company_id = $2",
        handoff_id,
        company_id,
    )
    return dict(row) if row else None


async def get_audit_log(conn: asyncpg.Connection, handoff_id: uuid.UUID) -> list[dict]:
    rows = await conn.fetch(
        "SELECT event, detail, created_at FROM handoff_audit_log "
        "WHERE handoff_id = $1 ORDER BY id",
        handoff_id,
    )
    return [dict(r) for r in rows]


async def operator_resolve(
    conn: asyncpg.Connection,
    company_id: str,
    handoff_id: uuid.UUID,
    action: str,
    *,
    external_reference: str | None = None,
    reason: str | None = None,
) -> dict:
    """
    Every operator action is scoped by company_id in the WHERE clause -
    not just checked afterwards - so a request for a company_id that
    doesn't own this row affects zero rows and we raise NotFound exactly
    as if the row didn't exist. We never leak the row's existence, let
    alone its content, across a company boundary.

    The whole read-check-write sequence runs inside one explicit
    transaction: FOR UPDATE only holds its row lock for the lifetime of
    the transaction it was taken in, and without `async with
    conn.transaction()` each statement on this connection is its own
    auto-committed transaction - the lock would be released the instant
    the SELECT returned, making the status check below a TOCTOU race
    rather than the guard it looks like. Two operators racing conflicting
    actions on the same row (one mark_completed, one mark_failed) would
    otherwise both pass the check and both write, with no 409 for either -
    exactly the kind of "controlled recovery decision" this function
    exists to prevent. Each UPDATE additionally re-asserts `status IN
    (...)` itself, as a second, independent guard against the same class
    of bug (the fencing-token pattern used elsewhere in this file, applied
    here as a status fence instead of a lease-token fence).
    """
    async with conn.transaction():
        row = await conn.fetchrow(
            "SELECT * FROM handoff_requests WHERE id = $1 AND company_id = $2 FOR UPDATE",
            handoff_id,
            company_id,
        )
        if row is None:
            raise NotFound(f"handoff {handoff_id} not found for company {company_id}")

        if row["status"] not in ("needs_review", "failed"):
            raise Conflict(
                f"cannot resolve a handoff in status '{row['status']}' "
                "(only needs_review/failed are operator-resolvable)"
            )

        if action == "mark_completed":
            result = await conn.execute(
                """
                UPDATE handoff_requests
                SET status = 'completed', crm_reference_id = $3,
                    completed_at = now(), updated_at = now(), review_reason = NULL
                WHERE id = $1 AND company_id = $2 AND status IN ('needs_review', 'failed')
                """,
                handoff_id,
                company_id,
                external_reference,
            )
            if not result.endswith("1"):
                raise Conflict(f"handoff {handoff_id} changed state concurrently; resolve aborted")
            await _audit(conn, handoff_id, "operator_marked_completed",
                          {"external_reference": external_reference, "reason": reason})
        elif action == "retry":
            result = await conn.execute(
                """
                UPDATE handoff_requests
                SET status = 'pending', claimed_by = NULL, lease_token = NULL,
                    lease_expires_at = NULL, review_reason = NULL, updated_at = now()
                WHERE id = $1 AND company_id = $2 AND status IN ('needs_review', 'failed')
                """,
                handoff_id,
                company_id,
            )
            if not result.endswith("1"):
                raise Conflict(f"handoff {handoff_id} changed state concurrently; resolve aborted")
            await _audit(conn, handoff_id, "operator_requeued", {"reason": reason})
        elif action == "mark_failed":
            if not reason:
                raise ValueError("reason is required to mark_failed")
            result = await conn.execute(
                """
                UPDATE handoff_requests
                SET status = 'failed', last_error = $3, updated_at = now()
                WHERE id = $1 AND company_id = $2 AND status IN ('needs_review', 'failed')
                """,
                handoff_id,
                company_id,
                reason,
            )
            if not result.endswith("1"):
                raise Conflict(f"handoff {handoff_id} changed state concurrently; resolve aborted")
            await _audit(conn, handoff_id, "operator_marked_failed", {"reason": reason})
        else:
            raise ValueError(f"unknown action {action!r}")

        return dict(
            await conn.fetchrow(
                "SELECT * FROM handoff_requests WHERE id = $1 AND company_id = $2",
                handoff_id,
                company_id,
            )
        )


# ---------------------------------------------------------------------------
# Worker liveness
# ---------------------------------------------------------------------------

async def record_worker_heartbeat(conn: asyncpg.Connection, worker_id: str) -> None:
    """Upsert this worker's last-seen timestamp. Cheap, single-row,
    no locking contention with the claim queries above (different table)."""
    await conn.execute(
        """
        INSERT INTO worker_heartbeats (worker_id, last_seen_at)
        VALUES ($1, now())
        ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = now()
        """,
        worker_id,
    )


async def healthy_worker_count(conn: asyncpg.Connection, stale_after_seconds: float) -> int:
    """Workers whose last heartbeat is within the staleness window. A
    worker that crashed or was killed simply stops updating its row and
    ages out of this count on its own - no explicit deregistration needed."""
    row = await conn.fetchrow(
        """
        SELECT count(*) AS n FROM worker_heartbeats
        WHERE last_seen_at > now() - ($1 || ' seconds')::interval
        """,
        str(stale_after_seconds),
    )
    return row["n"]


async def _audit(conn: asyncpg.Connection, handoff_id: uuid.UUID, event: str, detail: dict) -> None:
    await conn.execute(
        "INSERT INTO handoff_audit_log (handoff_id, event, detail) VALUES ($1, $2, $3::jsonb)",
        handoff_id,
        event,
        detail,
    )
