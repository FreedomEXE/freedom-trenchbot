from __future__ import annotations

import asyncio
import random
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import aiohttp

from .config import Config


class RetryableError(Exception):
    pass


class TTLCache:
    def __init__(self, ttl_sec: int, max_size: int = 512):
        self.ttl_sec = ttl_sec
        self.max_size = max_size
        self.store: OrderedDict[str, Any] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        item = self.store.get(key)
        if not item:
            return None
        expires_at, value = item
        if expires_at < time.monotonic():
            self.store.pop(key, None)
            return None
        self.store.move_to_end(key)
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl_val = self.ttl_sec if ttl is None else ttl
        expires_at = time.monotonic() + ttl_val
        self.store[key] = (expires_at, value)
        self.store.move_to_end(key)
        while len(self.store) > self.max_size:
            self.store.popitem(last=False)


class AsyncRateLimiter:
    def __init__(self, max_rps: int, max_concurrency: int):
        self.min_interval = 1.0 / max(1, max_rps)
        self.sem = asyncio.Semaphore(max_concurrency)
        self.lock = asyncio.Lock()
        self.last_ts = 0.0

    async def run(self, coro_fn):
        async with self.sem:
            async with self.lock:
                now = time.monotonic()
                wait = self.min_interval - (now - self.last_ts)
                if wait > 0:
                    await asyncio.sleep(wait)
                self.last_ts = time.monotonic()
            return await coro_fn()


class DexscreenerClient:
    def __init__(self, session: aiohttp.ClientSession, config: Config, logger, db=None):
        self.session = session
        self.logger = logger
        self.base_url = "https://api.dexscreener.com"
        self.cache = TTLCache(config.dex_cache_ttl_sec)
        self.limiter = AsyncRateLimiter(config.dex_max_rps, config.dex_max_concurrency)
        self.retry_attempts = config.dex_retry_attempts
        self.base_delay = config.dex_retry_base_delay_sec
        self.db = db

    async def _inc(self, key: str, amount: int) -> None:
        if self.db is None:
            return
        try:
            await self.db.increment_state_int(f"metrics_{key}", amount)
        except Exception:
            self.logger.exception("metrics_increment_failed", extra={"key": key})

    async def fetch_json(self, path: str, cache_ttl: Optional[int] = None) -> Optional[Any]:
        url = f"{self.base_url}{path}"
        cached = self.cache.get(url)
        if cached is not None:
            return cached

        for attempt in range(self.retry_attempts):
            try:
                async def _do_request():
                    return await self.session.get(url, headers={"Accept": "application/json"})

                resp = await self.limiter.run(_do_request)
                try:
                    await self._inc("api_requests", 1)
                    if resp.status == 429 or resp.status >= 500:
                        if resp.status == 429:
                            await self._inc("rate_limited_count", 1)
                        text = await resp.text()
                        raise RetryableError(f"status {resp.status}: {text}")
                    if resp.status != 200:
                        self.logger.warning(
                            "dexscreener_non_200", extra={"status": resp.status, "url": url}
                        )
                        return None
                    data = await resp.json()
                    if self.db is not None:
                        try:
                            await self.db.set_state("last_api_success", str(int(time.time())))
                        except Exception:
                            self.logger.exception("metrics_last_api_success_failed")
                    self.cache.set(url, data, ttl=cache_ttl)
                    return data
                finally:
                    resp.release()
            except RetryableError as exc:
                if attempt + 1 >= self.retry_attempts:
                    self.logger.warning(
                        "dexscreener_retry_exhausted", extra={"url": url, "error": str(exc)}
                    )
                    return None
                delay = self.base_delay * (2 ** attempt) + random.random() * 0.1
                await asyncio.sleep(delay)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt + 1 >= self.retry_attempts:
                    self.logger.warning(
                        "dexscreener_request_failed", extra={"url": url, "error": str(exc)}
                    )
                    return None
                delay = self.base_delay * (2 ** attempt) + random.random() * 0.1
                await asyncio.sleep(delay)
        return None

    async def get_pair(self, chain_id: str, pair_id: str) -> Optional[Any]:
        return await self.fetch_json(f"/latest/dex/pairs/{chain_id}/{pair_id}")

    async def search(self, query: str) -> Optional[Any]:
        q = quote(query)
        return await self.fetch_json(f"/latest/dex/search?q={q}")

    async def get_token_pairs(self, chain_id: str, token_address: str) -> List[Dict[str, Any]]:
        data = await self.fetch_json(f"/token-pairs/v1/{chain_id}/{token_address}")
        if data is None:
            return []
        if isinstance(data, dict) and "pairs" in data:
            return data.get("pairs", [])
        if isinstance(data, list):
            return data
        return []

    async def get_latest_token_profiles(self) -> Optional[Any]:
        return await self.fetch_json("/token-profiles/latest/v1")

    async def get_latest_token_boosts(self) -> Optional[Any]:
        return await self.fetch_json("/token-boosts/latest/v1")
