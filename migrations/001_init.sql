-- Initial schema for the handoff synchronization service.
--
-- Design notes (see DESIGN.md for the full rationale):
--   * handoff_requests IS the queue. There is no separate "publish to a
--     broker" step after insert, which removes an entire class of bugs
--     (accepted-but-never-scheduled) by construction.
--   * Dedup on acceptance is a DB constraint (company_id, event_id), not
--     an application-level check-then-insert.
--   * crm_idempotency_key is generated once, at insert time, and reused
--     on every retry attempt for the lifetime of the row. It is what we
--     hand to CRM A. CRM B ignores it (CRM B has no idempotency support).
--   * lease_token is a fencing token: regenerated on every claim, and
--     required (via WHERE lease_token = $x) on the write that finalizes
--     a claim's outcome, so a stale worker that wakes up after losing its
--     lease cannot silently overwrite what a newer claimant already did.

CREATE TABLE IF NOT EXISTS companies (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    crm_provider    TEXT NOT NULL CHECK (crm_provider IN ('crm_a', 'crm_b')),
    max_concurrency INTEGER NOT NULL DEFAULT 5,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS handoff_requests (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id          TEXT NOT NULL REFERENCES companies(id),
    event_id            TEXT NOT NULL,
    payload             JSONB NOT NULL,

    status              TEXT NOT NULL DEFAULT 'pending'
                             CHECK (status IN
                                 ('pending', 'processing', 'completed',
                                  'needs_review', 'failed')),

    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 8,

    crm_idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),
    crm_reference_id    TEXT,

    claimed_by          TEXT,
    lease_token         UUID,
    lease_expires_at    TIMESTAMPTZ,
    first_claimed_at    TIMESTAMPTZ,

    last_error          TEXT,
    review_reason       TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,

    UNIQUE (company_id, event_id)
);

-- Used by the claim query: "give me the oldest claimable row for this
-- company" where claimable = pending, or processing-with-expired-lease.
CREATE INDEX IF NOT EXISTS idx_handoff_company_status_created
    ON handoff_requests (company_id, status, created_at);

-- Used by the worker's "which companies currently have claimable work"
-- scan, which drives round-robin fairness across companies.
CREATE INDEX IF NOT EXISTS idx_handoff_status_company
    ON handoff_requests (status, company_id);

-- Append-only audit trail. This is what makes "inspect a request" and
-- "distinguish a healthy queue from silently lost work" possible: every
-- state transition is recorded, not just the current row.
CREATE TABLE IF NOT EXISTS handoff_audit_log (
    id          BIGSERIAL PRIMARY KEY,
    handoff_id  UUID NOT NULL REFERENCES handoff_requests(id),
    event       TEXT NOT NULL,
    detail      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_handoff_id
    ON handoff_audit_log (handoff_id, created_at);
