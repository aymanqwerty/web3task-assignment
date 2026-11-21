import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .config import RATE_LIMIT_CAPACITY, RATE_LIMIT_REFILL
from .db import create_pool, init_schema
from .models import DispatchRequest
from .ratelimit import RateLimiter
from .worker import run_worker


@asynccontextmanager
async def lifespan(app):
    pool = await create_pool()
    await init_schema(pool)
    limiter = RateLimiter(RATE_LIMIT_CAPACITY, RATE_LIMIT_REFILL)
    app.state.pool = pool
    task = asyncio.create_task(run_worker(pool, limiter))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await pool.close()


app = FastAPI(title="Webhook Dispatcher", lifespan=lifespan)


@app.get("/")
async def health():
    return {"status": "ok"}


@app.post("/api/webhooks/dispatch", status_code=202)
async def dispatch(req: DispatchRequest):
    job_id = str(uuid4())
    await app.state.pool.execute(
        "INSERT INTO jobs (id, destination_url, event_id, payload) VALUES ($1, $2, $3, $4)",
        job_id,
        str(req.destinationUrl),
        req.eventId,
        req.payload,
    )
    return {"status": "queued", "jobId": job_id}


@app.get("/api/webhooks/{job_id}")
async def job_status(job_id: str):
    job = await app.state.pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    attempts = await app.state.pool.fetch(
        "SELECT attempt_number, status, response_code, error, created_at FROM delivery_attempts WHERE job_id = $1 ORDER BY id",
        job_id,
    )
    return {
        "jobId": job["id"],
        "destinationUrl": job["destination_url"],
        "eventId": job["event_id"],
        "status": job["status"],
        "attempts": job["attempts"],
        "log": [dict(a) for a in attempts],
    }


@app.post("/api/test/sink")
async def sink(code: int = 200):
    return JSONResponse(status_code=code, content={"received": True, "code": code})
