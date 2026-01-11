from __future__ import annotations

import asyncio
import json
import random
import statistics
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp

from .config import Config
from .dexscreener import AsyncRateLimiter, RetryableError, TTLCache

LAMP = 1_000_000_000
MAX_TX_PAGE_SIZE = 100
DEFAULT_CACHE_TTL_SEC = 300
ALLOWED_SOURCES = ("pump", "raydium", "orca")


@dataclass(frozen=True)
class WalletAnalysisResult:
    sample_size: int
    unique_buyers: int
    fresh_wallets: int
    fresh_ratio: Optional[float]
    avg_sol: Optional[float]
    median_sol: Optional[float]
    min_sol: Optional[float]
    max_sol: Optional[float]
    earliest_buy_ts: Optional[int]
    partial: bool
    source: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sample_size": self.sample_size,
            "unique_buyers": self.unique_buyers,
            "fresh_wallets": self.fresh_wallets,
            "fresh_ratio": self.fresh_ratio,
            "avg_sol": self.avg_sol,
            "median_sol": self.median_sol,
            "min_sol": self.min_sol,
            "max_sol": self.max_sol,
            "earliest_buy_ts": self.earliest_buy_ts,
            "partial": self.partial,
            "source": self.source,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=True)


class HeliusClient:
    def __init__(self, session: aiohttp.ClientSession, config: Config, logger, db=None) -> None:
        self.session = session
        self.logger = logger
        self.db = db
        self.api_key = config.helius_api_key
        self.base_url = "https://api.helius.xyz"
        self.rpc_url = f"https://rpc.helius.xyz/?api-key={self.api_key}"
        self.cache = TTLCache(DEFAULT_CACHE_TTL_SEC, max_size=1024)
        self.limiter = AsyncRateLimiter(config.dex_max_rps, config.dex_max_concurrency)
        self.retry_attempts = config.dex_retry_attempts
        self.base_delay = config.dex_retry_base_delay_sec

    async def _inc(self, key: str, amount: int) -> None:
        if self.db is None:
            return
        try:
            await self.db.increment_state_int(f"metrics_wallet_{key}", amount)
        except Exception:
            self.logger.exception("wallet_metrics_increment_failed", extra={"key": key})

    async def _fetch_json(
        self,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        cache_key: Optional[str] = None,
        cache_ttl: Optional[int] = None,
    ) -> Optional[Any]:
        if cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached

        for attempt in range(self.retry_attempts):
            try:
                async def _do_request():
                    if method == "GET":
                        return await self.session.get(url, headers={"Accept": "application/json"})
                    return await self.session.post(
                        url, json=payload, headers={"Accept": "application/json"}
                    )

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
                            "helius_non_200", extra={"status": resp.status, "url": url}
                        )
                        return None
                    data = await resp.json()
                    if cache_key:
                        self.cache.set(cache_key, data, ttl=cache_ttl)
                    return data
                finally:
                    resp.release()
            except RetryableError as exc:
                if attempt + 1 >= self.retry_attempts:
                    self.logger.warning("helius_retry_exhausted", extra={"url": url, "error": str(exc)})
                    return None
                delay = self.base_delay * (2 ** attempt) + random.random() * 0.1
                await asyncio.sleep(delay)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt + 1 >= self.retry_attempts:
                    self.logger.warning("helius_request_failed", extra={"url": url, "error": str(exc)})
                    return None
                delay = self.base_delay * (2 ** attempt) + random.random() * 0.1
                await asyncio.sleep(delay)
        return None

    async def get_address_transactions(
        self, address: str, before: Optional[str], limit: int
    ) -> Optional[List[Dict[str, Any]]]:
        params = [f"api-key={self.api_key}", f"limit={limit}"]
        if before:
            params.append(f"before={before}")
        url = f"{self.base_url}/v0/addresses/{address}/transactions?{'&'.join(params)}"
        cache_key = f"helius:txs:{address}:{before}:{limit}"
        data = await self._fetch_json("GET", url, cache_key=cache_key, cache_ttl=5)
        if isinstance(data, list):
            return data
        return None

    async def rpc(self, method: str, params: Sequence[Any], cache_key: Optional[str] = None) -> Optional[Any]:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}
        data = await self._fetch_json("POST", self.rpc_url, payload=payload, cache_key=cache_key)
        if not isinstance(data, dict):
            return None
        return data.get("result")

    async def get_balance_sol(self, address: str) -> Optional[float]:
        cache_key = f"helius:balance:{address}"
        result = await self.rpc("getBalance", [address, {"commitment": "finalized"}], cache_key=cache_key)
        if not isinstance(result, dict):
            return None
        value = result.get("value")
        if not isinstance(value, (int, float)):
            return None
        return float(value) / LAMP

    async def get_signatures_for_address(
        self, address: str, before: Optional[str], limit: int
    ) -> Optional[List[Dict[str, Any]]]:
        params: Dict[str, Any] = {"limit": limit}
        if before:
            params["before"] = before
        result = await self.rpc("getSignaturesForAddress", [address, params])
        if isinstance(result, list):
            return result
        return None


class WalletAnalyzer:
    def __init__(self, session: aiohttp.ClientSession, config: Config, logger, db=None) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.enabled = config.wallet_analysis_enabled and config.wallet_analysis_provider == "helius"
        self.client = HeliusClient(session, config, logger, db=db)
        self.sample_size = max(1, config.wallet_analysis_sample)
        self.max_pages = max(1, config.wallet_analysis_max_pages)
        self.max_age_days = max(1, config.fresh_wallet_max_age_days)
        self.max_tx = max(1, config.fresh_wallet_max_tx)

    async def analyze(
        self, pair_address: str, token_address: str
    ) -> Optional[WalletAnalysisResult]:
        if not self.enabled or not self.client.api_key:
            return None
        buyers, earliest_ts, partial = await self._fetch_first_buyers(
            pair_address, token_address
        )
        if not buyers:
            return None

        tasks = [self._analyze_wallet(addr) for addr, _ts in buyers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        balances: List[float] = []
        fresh_wallets = 0
        any_partial = partial
        for result in results:
            if isinstance(result, Exception):
                any_partial = True
                continue
            balance_sol, is_fresh, partial_wallet = result
            if balance_sol is not None:
                balances.append(balance_sol)
            if is_fresh:
                fresh_wallets += 1
            if partial_wallet:
                any_partial = True

        unique_buyers = len(buyers)
        fresh_ratio = None
        if unique_buyers > 0:
            fresh_ratio = fresh_wallets / unique_buyers

        avg_sol = statistics.fmean(balances) if balances else None
        median_sol = statistics.median(balances) if balances else None
        min_sol = min(balances) if balances else None
        max_sol = max(balances) if balances else None

        return WalletAnalysisResult(
            sample_size=self.sample_size,
            unique_buyers=unique_buyers,
            fresh_wallets=fresh_wallets,
            fresh_ratio=fresh_ratio,
            avg_sol=avg_sol,
            median_sol=median_sol,
            min_sol=min_sol,
            max_sol=max_sol,
            earliest_buy_ts=earliest_ts,
            partial=any_partial,
            source="helius",
        )

    async def _fetch_first_buyers(
        self, pair_address: str, token_address: str
    ) -> Tuple[List[Tuple[str, Optional[int]]], Optional[int], bool]:
        buyer_times: Dict[str, Optional[int]] = {}
        before: Optional[str] = None
        partial = False
        token_lc = token_address.lower()

        for page in range(self.max_pages):
            txs = await self.client.get_address_transactions(pair_address, before, MAX_TX_PAGE_SIZE)
            if not txs:
                break
            for tx in txs:
                buyer, ts = self._extract_buyer(tx, token_lc)
                if not buyer:
                    continue
                prev = buyer_times.get(buyer)
                if prev is None or (ts is not None and ts < prev):
                    buyer_times[buyer] = ts
            before = txs[-1].get("signature")
            if not before:
                break
        else:
            partial = True

        buyers = sorted(
            buyer_times.items(),
            key=lambda item: item[1] if item[1] is not None else float("inf"),
        )
        buyers = buyers[: self.sample_size]
        earliest_ts = None
        for _addr, ts in buyers:
            if ts is None:
                continue
            if earliest_ts is None or ts < earliest_ts:
                earliest_ts = ts
        return buyers, earliest_ts, partial

    def _extract_buyer(self, tx: Dict[str, Any], token_lc: str) -> Tuple[Optional[str], Optional[int]]:
        source = str(tx.get("source") or "").lower()
        if not any(key in source for key in ALLOWED_SOURCES):
            return None, None
        tx_type = str(tx.get("type") or "").upper()
        events = tx.get("events")
        if tx_type != "SWAP" and not isinstance(events, dict):
            return None, None
        swap = events.get("swap") if isinstance(events, dict) else None
        if not isinstance(swap, dict):
            return None, None

        token_outputs = swap.get("tokenOutputs") or []
        token_inputs = swap.get("tokenInputs") or []
        if self._has_token(token_outputs, token_lc):
            buyer = swap.get("user") or tx.get("feePayer")
            if not isinstance(buyer, str) or not buyer:
                return None, None
            ts = _to_int(ts=tx.get("timestamp"))
            return buyer, ts
        if self._has_token(token_inputs, token_lc):
            return None, None
        return None, None

    @staticmethod
    def _has_token(items: Any, token_lc: str) -> bool:
        if not isinstance(items, list):
            return False
        for item in items:
            if not isinstance(item, dict):
                continue
            mint = item.get("mint")
            if isinstance(mint, str) and mint.lower() == token_lc:
                return True
        return False

    async def _analyze_wallet(self, address: str) -> Tuple[Optional[float], bool, bool]:
        balance = await self.client.get_balance_sol(address)
        is_fresh, partial = await self._is_fresh_wallet(address)
        return balance, is_fresh, partial

    async def _is_fresh_wallet(self, address: str) -> Tuple[bool, bool]:
        cutoff = int(time.time()) - self.max_age_days * 86400
        before: Optional[str] = None
        total = 0
        partial = False

        for page in range(self.max_pages):
            sigs = await self.client.get_signatures_for_address(address, before, MAX_TX_PAGE_SIZE)
            if not sigs:
                break
            total += len(sigs)
            if total > self.max_tx:
                return False, partial
            oldest = _min_block_time(sigs)
            if oldest is not None and oldest < cutoff:
                return False, partial
            before = sigs[-1].get("signature")
            if not before:
                break
        else:
            partial = True

        if partial:
            return False, True
        return True, False


def _min_block_time(sigs: List[Dict[str, Any]]) -> Optional[int]:
    times = []
    for sig in sigs:
        ts = sig.get("blockTime")
        if isinstance(ts, (int, float)):
            times.append(int(ts))
    return min(times) if times else None


def _to_int(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    try:
        return int(ts)
    except (TypeError, ValueError):
        return None
