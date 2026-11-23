-- Worker liveness tracking, used by the API's periodic health check to
-- tell "queue is healthy but quiet" apart from "nothing is consuming it".
--
-- Each worker process upserts its own row on every poll tick. The API
-- does not talk to workers directly (there is no RPC between them by
-- design - see DESIGN.md on why workers stay dumb pollers); it only
-- reads this table on an interval and compares last_seen_at against a
-- staleness threshold.

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id    TEXT PRIMARY KEY,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
