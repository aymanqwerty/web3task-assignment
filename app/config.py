import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/webhooks")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "50"))

RATE_LIMIT_CAPACITY = int(os.getenv("RATE_LIMIT_CAPACITY", "5"))
RATE_LIMIT_REFILL = float(os.getenv("RATE_LIMIT_REFILL", "5"))
