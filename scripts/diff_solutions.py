"""
Reproducible comparison between the ORIGINAL naive solution
(`original_problem/baseline.py`, verbatim from the assignment brief) and
the NEW safe implementation in `app/`.

The two aren't line-for-line comparable (the baseline is two bare
functions against an undefined `db`/`queue`/`crm`; the replacement is a
real schema plus a service/repository/worker split), so a plain `diff -u`
across files would be noise. Instead this script:

  1. Prints the baseline source verbatim.
  2. Prints the equivalent new-solution source for the same two
     operations (accept, process), pulled live from app/ so this never
     drifts out of sync with the actual code.
  3. Walks a fixed table of behavior differences (duplicate handling,
     crash recovery, worker coordination, company isolation, retries),
     each backed by a `diff -u` between the specific baseline lines and
     the specific new-solution lines responsible for that behavior, plus
     a one-line explanation of why the new side is safer.
  4. Optionally (--run-demo) executes a couple of the real fault
     scenarios against a live Postgres (the same fixtures tests/ use) so
     the "safer" claims are demonstrated running, not just asserted in
     prose.

Usage:
    python -m scripts.diff_solutions
    python -m scripts.diff_solutions --run-demo   # needs TEST_DATABASE_URL
"""
from __future__ import annotations

import argparse
import asyncio
import difflib
import inspect
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO_ROOT / "original_problem" / "baseline.py"


def _read(path: Path) -> str:
    return path.read_text()


def _section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def _unified_diff(label_a: str, lines_a: list[str], label_b: str, lines_b: list[str]) -> str:
    return "".join(
        difflib.unified_diff(lines_a, lines_b, fromfile=label_a, tofile=label_b, lineterm="\n")
    )


# ---------------------------------------------------------------------------
# The behavior-difference table. Each entry names the baseline snippet and
# the new-solution function(s) responsible for the corresponding behavior,
# plus why the new side is safer. Source is pulled live via `inspect` so
# this stays accurate as app/ changes.
# ---------------------------------------------------------------------------

def _get_source(dotted: str) -> list[str]:
    """
    'app.repository.insert_handoff_if_new' -> source of that function.
    'app.worker.Worker.tick' -> source of that method, resolved by walking
    attributes one at a time rather than assuming a flat module.function
    shape, so this also works for a Class.method path.
    """
    parts = dotted.split(".")
    # Find the longest importable module prefix, then walk the rest as
    # attribute access (handles both plain functions and Class.method).
    for split_at in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:split_at])
        try:
            obj = __import__(module_name, fromlist=["_"])
        except ImportError:
            continue
        for attr in parts[split_at:]:
            obj = getattr(obj, attr)
        return inspect.getsource(obj).splitlines(keepends=True)
    raise ImportError(f"could not resolve {dotted!r}")


BASELINE_ACCEPT = textwrap.dedent(
    """\
    async def accept_handoff(company_id, payload):
        if not await db.event_exists(payload["event_id"]):
            request = await db.insert_handoff(company_id, payload)
            await queue.publish({"handoff_id": request.id})
        return {"accepted": True}
    """
).splitlines(keepends=True)

BASELINE_PROCESS = textwrap.dedent(
    """\
    async def process_handoff(handoff_id):
        request = await db.get_handoff(handoff_id)
        if request.status == "completed":
            return
        result = await crm.create_callback(request.payload)
        await db.mark_completed(request.id, result.id)
    """
).splitlines(keepends=True)

COMPARISONS = [
    {
        "title": "Duplicate handling (concurrent identical submissions)",
        "baseline_label": "original_problem/baseline.py:accept_handoff",
        "baseline_lines": BASELINE_ACCEPT,
        "new_label": "app/repository.py:insert_handoff_if_new",
        "new_source": lambda: _get_source("app.repository.insert_handoff_if_new"),
        "why": (
            "The baseline does `event_exists` THEN `insert_handoff` as two separate "
            "awaits with no shared transaction or constraint - two concurrent callers "
            "can both see 'not exists' and both insert, producing two handoff rows and "
            "two queue messages for the same event. The new solution replaces the "
            "check with a single `INSERT ... ON CONFLICT (company_id, event_id) DO "
            "NOTHING`: Postgres's unique index resolves the race, not application "
            "code, so N concurrent identical submissions always produce exactly one "
            "row (demonstrated in tests/test_scenario_1_concurrent_duplicate.py)."
        ),
    },
    {
        "title": "Crash recovery (accepted but never scheduled)",
        "baseline_label": "original_problem/baseline.py:accept_handoff",
        "baseline_lines": BASELINE_ACCEPT,
        "new_label": "app/repository.py:insert_handoff_if_new",
        "new_source": lambda: _get_source("app.repository.insert_handoff_if_new"),
        "why": (
            "The baseline's `insert_handoff` and `queue.publish` are two separate "
            "steps; a crash between them leaves a persisted row with no scheduled "
            "work, forever. The new solution has no second 'publish' step at all - "
            "the row IS the unit of work, discovered by any worker's next poll "
            "against `status = 'pending'`. There is no window in which the row exists "
            "but nothing will ever find it (tests/test_scenario_2_crash_before_schedule.py)."
        ),
    },
    {
        "title": "Worker coordination (stall, lose ownership, resume)",
        "baseline_label": "original_problem/baseline.py:process_handoff",
        "baseline_lines": BASELINE_PROCESS,
        "new_label": "app/repository.py:claim_next_for_company + finalize_completed",
        "new_source": lambda: (
            _get_source("app.repository.claim_next_for_company")
            + ["\n"]
            + _get_source("app.repository.finalize_completed")
        ),
        "why": (
            "The baseline has no claim/lease concept at all - `process_handoff` just "
            "reads a row and writes to it, so two workers racing the same handoff_id "
            "(or one worker stalling mid-call and a second one picking the same row up "
            "after a naive 'retry everything pending' sweep) can both call the CRM and "
            "both write 'completed', in either order, with no way to tell which write "
            "is stale. The new solution claims with `FOR UPDATE SKIP LOCKED` and stamps "
            "a fresh `lease_token` on every claim; `finalize_completed` only takes "
            "effect `WHERE lease_token = $2` - a worker that stalls past its lease and "
            "wakes up later has a token that no longer matches, so its write affects "
            "zero rows and is silently discarded (tests/test_scenario_4_stalled_worker_resumes.py)."
        ),
    },
    {
        "title": "Company isolation (one company's backlog, or its operator actions)",
        "baseline_label": "original_problem/baseline.py (whole file - no concept exists)",
        "baseline_lines": BASELINE_ACCEPT + ["...\n"] + BASELINE_PROCESS,
        "new_label": "app/worker.py:Worker.tick + app/repository.py:operator_resolve",
        "new_source": lambda: (
            _get_source("app.worker.Worker.tick")
            + ["\n"]
            + _get_source("app.repository.operator_resolve")
        ),
        "why": (
            "The baseline has no company concept in its scheduling or data access at "
            "all - a global `process_handoff(handoff_id)` implies one global FIFO "
            "where one company's backlog necessarily sits in front of everyone else's, "
            "and nothing stops a lookup/recovery action against a handoff_id from "
            "resolving regardless of who owns it. The new solution's `Worker.tick()` "
            "lists every company with claimable work and fans out up to each one's own "
            "concurrency slice per pass (tests/test_fairness.py), and every operator "
            "action in `operator_resolve` is scoped by `company_id` IN THE WHERE "
            "CLAUSE, so a mismatched company_id affects zero rows and is rejected "
            "exactly like a 404, never someone else's data "
            "(tests/test_scenario_6_cross_company_isolation.py)."
        ),
    },
    {
        "title": "Retries and uncertain external outcomes",
        "baseline_label": "original_problem/baseline.py:process_handoff",
        "baseline_lines": BASELINE_PROCESS,
        "new_label": "app/service.py:_process_claimed + _handle_ambiguous",
        "new_source": lambda: (
            _get_source("app.service._process_claimed")
            + ["\n"]
            + _get_source("app.service._handle_ambiguous")
        ),
        "why": (
            "The baseline calls `crm.create_callback` with no idempotency key and no "
            "record of a prior attempt - a timeout after the CRM actually accepted the "
            "request looks identical to a timeout where it didn't, and a blind retry "
            "(automatic or an operator's manual one) can create a real duplicate on a "
            "CRM with no dedup. The new solution generates one `crm_idempotency_key` "
            "at insert time and reuses it on every attempt for an idempotent CRM, so a "
            "retry after an ambiguous timeout is provably safe there; for a CRM with no "
            "idempotency support, the same ambiguous outcome routes to `needs_review` "
            "instead of ever being retried blindly - the asymmetry is an explicit "
            "branch, not a shared retry-everything path "
            "(tests/test_scenario_3_crash_after_crm_success.py)."
        ),
    },
]


def print_static_comparison() -> None:
    _section("ORIGINAL naive solution (verbatim, original_problem/baseline.py)")
    print(_read(BASELINE_PATH))

    for entry in COMPARISONS:
        _section(entry["title"])
        new_lines = entry["new_source"]()
        diff = _unified_diff(entry["baseline_label"], entry["baseline_lines"], entry["new_label"], new_lines)
        print(diff if diff.strip() else "(sources are identical - unexpected)")
        print("-" * 78)
        print("Why the new side is safer:")
        print(textwrap.fill(entry["why"], width=78))


async def run_demo() -> None:
    """
    Executes two of the real fault scenarios against a live Postgres
    (same TEST_DATABASE_URL the test suite uses) to show the behavior
    difference running, not just asserted. This does NOT run the
    baseline - the baseline has no working db/queue/crm to run against,
    which is itself part of the point: it is pseudocode from the brief,
    not an executable alternative. What this demonstrates is the new
    solution's actual behavior for two of the scenarios the comparison
    table above claims it handles.
    """
    import os
    import uuid

    from app.config import Settings
    from app.crm.registry import CRMRegistry
    from app.db import create_pool
    from app import repository, service

    db_url = os.environ.get(
        "TEST_DATABASE_URL", "postgresql://handoff:handoff@localhost:5432/handoff_test"
    )
    settings = Settings(database_url=db_url, lease_seconds=2)
    pool = await create_pool(settings)
    registry = CRMRegistry()
    registry.register("acme", "crm_a")

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO companies (id, name, crm_provider, max_concurrency) "
            "VALUES ('acme', 'Acme', 'crm_a', 5) ON CONFLICT (id) DO NOTHING"
        )

    _section("LIVE DEMO 1: concurrent identical submissions -> exactly one row")
    event_id = f"demo-{uuid.uuid4().hex[:8]}"
    results = await asyncio.gather(
        *[
            service.accept_handoff(pool, "acme", event_id, {"customer": "Jane"})
            for _ in range(10)
        ]
    )
    ids = {str(r[0]) for r in results}
    created_flags = [r[1] for r in results]
    print(f"10 concurrent accepts of the same event_id -> {len(ids)} distinct row id(s)")
    print(f"newly_created flags: {created_flags} (exactly one True expected)")
    assert len(ids) == 1
    assert created_flags.count(True) == 1

    _section("LIVE DEMO 2: stalled worker's write is fenced out after reclaim")
    handoff_id, _ = await service.accept_handoff(pool, "acme", f"demo-stall-{uuid.uuid4().hex[:8]}", {})
    async with pool.acquire() as conn:
        worker1 = await repository.claim_next_for_company(conn, "acme", "worker-1", settings.lease_seconds)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE handoff_requests SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
            handoff_id,
        )
    outcome = await service.process_one(pool, registry, settings, "acme", "worker-2")
    print(f"worker-2 reclaims and processes: outcome={outcome}")
    async with pool.acquire() as conn:
        fenced_ok = await repository.finalize_completed(
            conn, handoff_id, worker1.lease_token, "worker-1-stale-reference"
        )
    print(f"worker-1's late write using its old lease_token succeeded: {fenced_ok} (expected: False)")
    assert fenced_ok is False

    await pool.close()
    print("\nBoth live demos match the behavior claimed in the comparison table above.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-demo",
        action="store_true",
        help="also execute two scenarios live against TEST_DATABASE_URL (default: handoff_test)",
    )
    args = parser.parse_args()

    print_static_comparison()

    if args.run_demo:
        asyncio.run(run_demo())
    else:
        print("\n(run with --run-demo to execute two of these scenarios live against Postgres)")


if __name__ == "__main__":
    main()
