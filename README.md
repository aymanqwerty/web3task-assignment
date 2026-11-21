# Webhook Dispatcher

This is a small service that takes webhook events and delivers them to other servers in the background. The main idea is that the API accepts the event and replies right away, and a background worker does the actual sending. If the other server is down or slow, the worker retries a few times with increasing delays. It also keeps a log of every attempt and makes sure it does not hit the same target too fast.

Live URL: https://web3task-assignment1.onrender.com

## Tech stack

- Python + FastAPI
- PostgreSQL (asyncpg)
- httpx for outgoing requests
- Deployed on Render (free tier)

## How it works

**Ingestion is non-blocking.** When you call `POST /api/webhooks/dispatch`, the API only saves the job in the database and returns `202 Accepted` with a job id. It does not wait for the delivery. So even if the target takes 10 seconds, the API still responds instantly.

**A background worker does the delivery.** When the app starts, it also starts a worker loop in the same process. The loop checks the database for jobs that are ready, claims them, and sends the POST request to the `destinationUrl`. Each delivery runs as its own task, so a slow target does not block the other jobs.

**Retries with backoff.** If the target returns a `5xx`, a `429`, or the request times out, the job is retried up to 3 times. The wait grows each time: 2 seconds, then 4 seconds, then 8 seconds. After that the job is marked as failed. A normal `4xx` (like a bad request) is not retried because retrying would not help.

**State is kept in the database.** The job queue and the delivery log both live in Postgres. Because of this, if the service restarts, the jobs that were still pending are picked up again. The `delivery_attempts` table stores every attempt with its status, the response code, and the attempt number.

**Rate limiting per domain.** Before sending, the worker passes through a token bucket for the target domain. Each domain gets its own bucket, so the service does not flood a single domain with too many requests at once.

### Why I chose this setup

Render's free tier does not give a separate background worker service, so I run the worker inside the same web service using asyncio. The database acts as both the queue and the log, which keeps everything in one place and makes the jobs survive a restart. If this needed to scale later, the queue could be moved to Redis and the worker could run as its own service, but for this task one service plus Postgres is enough.

## API

### POST /api/webhooks/dispatch

Queues a webhook for delivery.

Body:

```json
{
  "destinationUrl": "https://target-server.com/endpoint",
  "eventId": "evt_123456",
  "payload": { "event": "user.signup", "userId": "usr_999" }
}
```

Response (202):

```json
{ "status": "queued", "jobId": "..." }
```

### GET /api/webhooks/{jobId}

Shows the job status and the full list of delivery attempts.

### POST /api/test/sink

A small target used for testing. By default it returns 200. Pass `?code=500` to make it return 500 so you can see the retries.

## How to test

Replace the URL with the live one if you are testing the deployment.

Queue a job that succeeds:

```bash
curl -X POST https://web3task-assignment1.onrender.com/api/webhooks/dispatch \
  -H "Content-Type: application/json" \
  -d '{"destinationUrl":"https://web3task-assignment1.onrender.com/api/test/sink","eventId":"evt_1","payload":{"event":"user.signup"}}'
```

Check its status (use the jobId from the response above):

```bash
curl https://web3task-assignment1.onrender.com/api/webhooks/<jobId>
```

Queue a job that keeps failing so you can watch the retries:

```bash
curl -X POST https://web3task-assignment1.onrender.com/api/webhooks/dispatch \
  -H "Content-Type: application/json" \
  -d '{"destinationUrl":"https://web3task-assignment1.onrender.com/api/test/sink?code=500","eventId":"evt_2","payload":{"event":"ping"}}'
```

Wait around 15 seconds, then check the status again. You will see 4 attempts (3 retries) with the response code and the growing delays.

## Run locally

You need Python 3.12 and a running Postgres.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
set DATABASE_URL=postgresql://postgres:postgres@localhost:5432/webhooks
uvicorn app.main:app --reload
```

The tables are created automatically on startup.
