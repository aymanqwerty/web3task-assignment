import json

import asyncpg

from .config import DATABASE_URL


async def _init_connection(conn):
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool():
    dsn = DATABASE_URL
    ssl = None
    if "sslmode=require" in dsn:
        dsn = dsn.replace("?sslmode=require", "").replace("&sslmode=require", "")
        ssl = True
    return await asyncpg.create_pool(dsn=dsn, ssl=ssl, init=_init_connection)


async def init_schema(pool):
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            destination_url TEXT NOT NULL,
            event_id TEXT NOT NULL,
            payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            attempts INT NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS delivery_attempts (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(id),
            attempt_number INT NOT NULL,
            status TEXT NOT NULL,
            response_code INT,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_pending ON jobs (next_attempt_at)
            WHERE status IN ('queued', 'retrying');
        """
    )
