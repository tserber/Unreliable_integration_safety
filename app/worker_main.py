"""Standalone worker process entrypoint: `python -m app.worker_main`.

Run this as its own process (and scale it horizontally - multiple
instances are safe by construction, see DESIGN.md) separately from the
API process.
"""
from __future__ import annotations

import asyncio
import logging
import signal

from app.config import load_settings
from app.crm.registry import CRMRegistry
from app.db import create_pool
from app.worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main() -> None:
    settings = load_settings()
    pool = await create_pool(settings)
    registry = CRMRegistry()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, crm_provider FROM companies")
        for r in rows:
            registry.register(r["id"], r["crm_provider"])

    worker = Worker(pool, registry, settings)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, worker.stop)
        loop.add_signal_handler(signal.SIGTERM, worker.stop)
    except NotImplementedError:
        pass  # Windows

    logging.info("worker %s starting", worker.worker_id)
    await worker.run_forever()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
