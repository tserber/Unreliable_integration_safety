"""Apply migrations/*.sql in order against DATABASE_URL.
Usage: python -m scripts.migrate [database_url]
"""
import asyncio
import sys
from pathlib import Path

import asyncpg


async def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "postgresql://handoff:handoff@localhost:5432/handoff_dev"
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    conn = await asyncpg.connect(url)
    try:
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text()
            print(f"applying {path.name} -> {url}")
            await conn.execute(sql)
    finally:
        await conn.close()
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
