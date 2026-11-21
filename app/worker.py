import asyncio
import logging
from urllib.parse import urlparse

import httpx

from .config import BATCH_SIZE, HTTP_TIMEOUT, MAX_CONCURRENCY, MAX_RETRIES, POLL_INTERVAL

log = logging.getLogger("worker")


async def claim_jobs(pool, limit):
    rows = await pool.fetch(
        """
        UPDATE jobs SET status = 'delivering', updated_at = now()
        WHERE id IN (
            SELECT id FROM jobs
            WHERE status IN ('queued', 'retrying') AND next_attempt_at <= now()
            ORDER BY next_attempt_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        RETURNING id, destination_url, event_id, payload, attempts
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def log_attempt(pool, job_id, attempt, status, code, error):
    await pool.execute(
        "INSERT INTO delivery_attempts (job_id, attempt_number, status, response_code, error) VALUES ($1, $2, $3, $4, $5)",
        job_id,
        attempt,
        status,
        code,
        error,
    )


async def mark_success(pool, job_id, attempt, code):
    await pool.execute(
        "UPDATE jobs SET status = 'success', attempts = $2, updated_at = now() WHERE id = $1",
        job_id,
        attempt,
    )
    await log_attempt(pool, job_id, attempt, "success", code, None)


async def handle_failure(pool, job_id, attempt, code, error, retryable):
    if retryable and attempt <= MAX_RETRIES:
        delay = 2 ** attempt
        await pool.execute(
            "UPDATE jobs SET status = 'retrying', attempts = $2, next_attempt_at = now() + ($3 || ' seconds')::interval, updated_at = now() WHERE id = $1",
            job_id,
            attempt,
            str(delay),
        )
        await log_attempt(pool, job_id, attempt, "retrying", code, error)
    else:
        await pool.execute(
            "UPDATE jobs SET status = 'failed', attempts = $2, updated_at = now() WHERE id = $1",
            job_id,
            attempt,
        )
        await log_attempt(pool, job_id, attempt, "failed", code, error)


async def deliver(pool, limiter, client, job):
    domain = urlparse(job["destination_url"]).netloc
    await limiter.acquire(domain)
    attempt = job["attempts"] + 1
    try:
        resp = await client.post(job["destination_url"], json=job["payload"])
        if 200 <= resp.status_code < 300:
            await mark_success(pool, job["id"], attempt, resp.status_code)
        else:
            retryable = resp.status_code >= 500 or resp.status_code == 429
            await handle_failure(pool, job["id"], attempt, resp.status_code, None, retryable)
    except httpx.HTTPError as exc:
        await handle_failure(pool, job["id"], attempt, None, str(exc), True)


async def _run_job(pool, limiter, client, job):
    try:
        await deliver(pool, limiter, client, job)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("delivery failed for job %s", job["id"])


async def run_worker(pool, limiter):
    inflight = set()
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            while True:
                free = MAX_CONCURRENCY - len(inflight)
                if free > 0:
                    jobs = await claim_jobs(pool, min(free, BATCH_SIZE))
                    for job in jobs:
                        task = asyncio.create_task(_run_job(pool, limiter, client, job))
                        inflight.add(task)
                        task.add_done_callback(inflight.discard)
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            for task in inflight:
                task.cancel()
            raise
