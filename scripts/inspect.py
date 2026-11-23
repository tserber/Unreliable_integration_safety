"""
Quick state dump with no psql / Postgres client install required - just
the asyncpg dependency this project already needs.

Usage:
    python -m scripts.inspect
    python -m scripts.inspect --company acme
    python -m scripts.inspect --id 5ed4c13b-61ea-482f-b6d5-cd42437affc0
"""
import argparse
import asyncio

from app.config import load_settings
from app.db import create_pool


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company", default=None, help="filter to one company_id")
    parser.add_argument("--id", default=None, help="show one handoff (and its audit log) by id")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    settings = load_settings()
    pool = await create_pool(settings)

    if args.id:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM handoff_requests WHERE id = $1", args.id)
            if row is None:
                print(f"no handoff with id {args.id}")
            else:
                for key, value in dict(row).items():
                    print(f"{key:>22}: {value}")
                print("\naudit log:")
                audit = await conn.fetch(
                    "SELECT created_at, event, detail FROM handoff_audit_log WHERE handoff_id = $1 ORDER BY id",
                    args.id,
                )
                for a in audit:
                    print(f"  {a['created_at']}  {a['event']:<28} {a['detail']}")
        await pool.close()
        return

    query = (
        "SELECT id, company_id, status, attempt_count, crm_reference_id, "
        "review_reason, created_at FROM handoff_requests "
    )
    params: list = []
    if args.company:
        query += "WHERE company_id = $1 "
        params.append(args.company)
    query += "ORDER BY created_at DESC LIMIT " + str(args.limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    print(f"{'id':<38} {'company':<10} {'status':<14} {'attempts':<9} {'crm_ref':<18} reason")
    for r in rows:
        print(
            f"{str(r['id']):<38} {r['company_id']:<10} {r['status']:<14} "
            f"{r['attempt_count']:<9} {str(r['crm_reference_id'] or '-'):<18} "
            f"{(r['review_reason'] or '')[:40]}"
        )

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
