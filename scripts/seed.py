"""Seed a couple of demo companies. Run with: python -m scripts.seed"""
import asyncio

from app.config import load_settings
from app.db import create_pool


async def main() -> None:
    settings = load_settings()
    pool = await create_pool(settings)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO companies (id, name, crm_provider, max_concurrency)
            VALUES
                ('acme', 'Acme Inc (CRM A - idempotent)', 'crm_a', 5),
                ('globex', 'Globex Corp (CRM B - no idempotency)', 'crm_b', 3)
            ON CONFLICT (id) DO NOTHING
            """
        )
    await pool.close()
    print("seeded companies: acme (crm_a), globex (crm_b)")


if __name__ == "__main__":
    asyncio.run(main())
