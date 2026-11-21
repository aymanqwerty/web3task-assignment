import asyncio
import time


class TokenBucket:
    def __init__(self, capacity, refill_rate):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.refill_rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.refill_rate)


class RateLimiter:
    def __init__(self, capacity, refill_rate):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.buckets = {}
        self.lock = asyncio.Lock()

    async def acquire(self, key):
        async with self.lock:
            bucket = self.buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self.capacity, self.refill_rate)
                self.buckets[key] = bucket
        await bucket.acquire()
