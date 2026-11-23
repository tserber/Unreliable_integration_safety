import asyncpg

from app.config import Settings


async def create_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=2,
        max_size=20,
        # jsonb payloads are passed as Python dicts; codec set up below.
        init=_init_connection,
    )


async def _init_connection(conn: asyncpg.Connection) -> None:
    import json

    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
