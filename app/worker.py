"""
The long-running worker process. Tests exercise `app.service.process_one`
directly for determinism; this module is the thing that actually runs in
production, wiring that same function into a polling loop with
per-company fan-out so one company's backlog can't starve the rest.
"""
from __future__ import annotations

import asyncio
import logging
import random
import uuid
from collections import defaultdict

import asyncpg

from app import repository, service
from app.config import Settings
from app.crm.registry import CRMRegistry

log = logging.getLogger("worker")


class Worker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        crm_registry: CRMRegistry,
        settings: Settings,
        worker_id: str | None = None,
        default_max_concurrency: int = 5,
    ) -> None:
        self.pool = pool
        self.crm_registry = crm_registry
        self.settings = settings
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.default_max_concurrency = default_max_concurrency
        self._inflight: dict[str, int] = defaultdict(int)
        self._max_concurrency: dict[str, int] = {}
        self._stop = asyncio.Event()

    async def _concurrency_limit(self, company_id: str) -> int:
        if company_id not in self._max_concurrency:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT max_concurrency FROM companies WHERE id = $1", company_id
                )
            self._max_concurrency[company_id] = (
                row["max_concurrency"] if row else self.default_max_concurrency
            )
        return self._max_concurrency[company_id]

    async def _run_one(self, company_id: str) -> None:
        try:
            outcome = await service.process_one(
                self.pool, self.crm_registry, self.settings, company_id, self.worker_id
            )
            log.info("company=%s outcome=%s worker=%s", company_id, outcome, self.worker_id)
        except Exception:  # noqa: BLE001 - a crashed task must not take the loop down
            log.exception("unhandled error processing company=%s", company_id)
        finally:
            self._inflight[company_id] -= 1

    async def tick(self) -> list[asyncio.Task]:
        """One scheduling pass. Returns the tasks it dispatched (tests
        await these directly; run_forever just checks whether the list
        was empty). Exposed separately from run_forever so tests/demos
        can step the worker deterministically instead of racing a
        background loop.

        Crucially, this loops over EVERY company with claimable work and
        gives each one up to its own concurrency slice before moving on -
        it does not drain one company's backlog before looking at the
        next. That is what stops one flooding company from delaying
        everyone else: a company hitting its own concurrency/rate limit
        simply stops being dispatched this tick, while every other
        company's companies still get a turn in the same pass.
        """
        async with self.pool.acquire() as conn:
            companies = await repository.companies_with_claimable_work(conn)
        random.shuffle(companies)  # no fixed company gets first dibs every tick

        tasks: list[asyncio.Task] = []
        for company_id in companies:
            limit = await self._concurrency_limit(company_id)
            if self._inflight[company_id] >= limit:
                # Incident: "One company's backlog delaying other
                # companies." - this is the backpressure guard against
                # exactly that: this company has claimable work but is
                # already at its own concurrency slice for this worker, so
                # it's skipped THIS tick rather than drained first. Logged
                # at debug (expected under a legitimate burst) - a
                # sustained rate of these for one company_id is the signal
                # an operator would alert on to catch that company
                # genuinely flooding the system versus a one-tick blip.
                log.debug(
                    "company=%s at concurrency limit (%d), deferring to next tick "
                    "without blocking other companies",
                    company_id, limit,
                )
                continue
            self._inflight[company_id] += 1
            tasks.append(asyncio.create_task(self._run_one(company_id)))
        return tasks

    def stop(self) -> None:
        self._stop.set()

    async def send_heartbeat(self) -> None:
        """Upsert this worker's liveness row. Exposed as its own method
        (not folded into run_forever) so tests/demos can call it on demand
        instead of racing a background loop's timing."""
        async with self.pool.acquire() as conn:
            await repository.record_worker_heartbeat(conn, self.worker_id)

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        last_heartbeat = 0.0
        while not self._stop.is_set():
            now = loop.time()
            if now - last_heartbeat >= self.settings.worker_heartbeat_interval_seconds:
                await self.send_heartbeat()
                last_heartbeat = now

            tasks = await self.tick()
            if not tasks:
                await asyncio.sleep(self.settings.poll_interval_seconds)
