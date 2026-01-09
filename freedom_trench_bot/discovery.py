from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple, Optional, Set

from .types import PairCandidate

DEFAULT_BASE_TOKENS: List[Tuple[str, str]] = [
    ("So11111111111111111111111111111111111111112", "WSOL"),
    ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "USDC"),
    ("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "USDT"),
    ("4oRwqhNroh7kgwNXCnu9idZ861zdbWLVfv7aERUcuzU3", "USD1"),
]


class DiscoveryEngine:
    def __init__(self, dex, config, logger):
        self.dex = dex
        self.config = config
        self.logger = logger
        self._last_search_ts = 0.0
        self._last_profiles_ts = 0.0
        self._last_boosts_ts = 0.0
        self._cached_search_pairs: List[PairCandidate] = []
        self._cached_profile_pairs: List[PairCandidate] = []
        self._cached_boost_pairs: List[PairCandidate] = []

    async def discover_pairs(self) -> List[PairCandidate]:
        mode = self.config.discovery_mode
        if mode == "hybrid":
            return await self._hybrid()
        if mode == "market_sampler":
            return await self._market_sampler()
        if mode == "fallback_search":
            return self._from_search(await self._search_queries(), "fallback_search")
        self.logger.warning("unknown_discovery_mode", extra={"mode": mode})
        return await self._market_sampler()

    async def _search_queries(self) -> Any:
        if not self.config.search_queries:
            self.logger.warning("fallback_search_no_queries")
            return {"pairs": []}
        results = []
        for query in self.config.search_queries:
            data = await self.dex.search(query)
            if isinstance(data, dict):
                results.extend(data.get("pairs", []))
        return {"pairs": results}

    async def _hybrid(self) -> List[PairCandidate]:
        candidates: List[PairCandidate] = []
        candidates.extend(await self._market_sampler())
        candidates.extend(await self._hybrid_search_pairs())
        candidates.extend(await self._hybrid_profile_pairs())
        candidates.extend(await self._hybrid_boost_pairs())
        dedup: Dict[str, PairCandidate] = {}
        for candidate in candidates:
            key = candidate.pair_address.lower()
            if key not in dedup:
                dedup[key] = candidate
        return list(dedup.values())

    async def _hybrid_search_pairs(self) -> List[PairCandidate]:
        if not self.config.search_queries:
            return []
        refresh_sec = self.config.hybrid_search_refresh_sec
        now = time.monotonic()
        if now - self._last_search_ts < refresh_sec:
            return self._cached_search_pairs
        pairs = self._from_search(await self._search_queries(), "hybrid_search")
        self._cached_search_pairs = pairs
        self._last_search_ts = now
        return pairs

    async def _hybrid_profile_pairs(self) -> List[PairCandidate]:
        refresh_sec = self.config.hybrid_refresh_sec
        now = time.monotonic()
        if now - self._last_profiles_ts < refresh_sec:
            return self._cached_profile_pairs
        data = await self.dex.get_latest_token_profiles()
        tokens = self._extract_tokens_from_latest(data)
        tokens = tokens[: self.config.hybrid_max_tokens]
        pairs = await self._pairs_from_tokens(tokens, "token_profiles")
        self._cached_profile_pairs = pairs
        self._last_profiles_ts = now
        return pairs

    async def _hybrid_boost_pairs(self) -> List[PairCandidate]:
        refresh_sec = self.config.hybrid_refresh_sec
        now = time.monotonic()
        if now - self._last_boosts_ts < refresh_sec:
            return self._cached_boost_pairs
        data = await self.dex.get_latest_token_boosts()
        tokens = self._extract_tokens_from_latest(data)
        tokens = tokens[: self.config.hybrid_max_tokens]
        pairs = await self._pairs_from_tokens(tokens, "token_boosts")
        self._cached_boost_pairs = pairs
        self._last_boosts_ts = now
        return pairs

    async def _market_sampler(self) -> List[PairCandidate]:
        base_tokens = self._get_base_tokens()
        base_set = {address.lower() for address, _ in base_tokens}
        candidates: List[PairCandidate] = []
        for address, label in base_tokens:
            pairs = await self.dex.get_token_pairs(self.config.chain_id, address)
            for pair in pairs:
                candidate = self._pair_candidate_from_base(pair, address, label, base_set)
                if candidate:
                    candidates.append(candidate)
        dedup: Dict[str, PairCandidate] = {}
        for candidate in candidates:
            key = candidate.pair_address.lower()
            if key not in dedup:
                dedup[key] = candidate
        return list(dedup.values())

    def _get_base_tokens(self) -> List[Tuple[str, str]]:
        tokens = list(DEFAULT_BASE_TOKENS)
        if self.config.market_base_tokens:
            tokens.extend([(address, "BASE") for address in self.config.market_base_tokens])
        dedup: Dict[str, Tuple[str, str]] = {}
        for address, label in tokens:
            key = address.lower()
            if key not in dedup:
                dedup[key] = (address, label)
        return list(dedup.values())

    def _pair_candidate_from_base(
        self,
        pair: Dict[str, Any],
        base_address: str,
        label: str,
        base_set: Set[str],
    ) -> Optional[PairCandidate]:
        if not isinstance(pair, dict):
            return None
        chain_id = pair.get("chainId")
        if chain_id != self.config.chain_id:
            return None
        pair_address = pair.get("pairAddress")
        if not pair_address:
            return None

        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}
        base_token_address = base.get("address")
        quote_token_address = quote.get("address")
        base_address_lc = base_address.lower()

        token_address = None
        if base_token_address and base_token_address.lower() == base_address_lc:
            token_address = quote_token_address
        elif quote_token_address and quote_token_address.lower() == base_address_lc:
            token_address = base_token_address
        else:
            token_address = base_token_address or quote_token_address

        if not token_address:
            return None
        token_address_lc = token_address.lower()
        if token_address_lc == base_address_lc or token_address_lc in base_set:
            return None

        hot_score = self._hot_score(pair)
        return PairCandidate(
            pair_address=pair_address,
            chain_id=chain_id,
            token_address=token_address,
            pair=pair,
            source=f"market:{label}",
            hot_score=hot_score,
        )

    def _from_search(self, data: Any, source: str) -> List[PairCandidate]:
        if not data or not isinstance(data, dict):
            return []
        pairs = data.get("pairs", [])
        base_set = {address.lower() for address, _ in self._get_base_tokens()}
        results: List[PairCandidate] = []
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            candidate = self._pair_candidate_from_generic(pair, source, base_set)
            if candidate:
                results.append(candidate)
        return results

    async def _pairs_from_tokens(self, tokens: List[str], source: str) -> List[PairCandidate]:
        results: List[PairCandidate] = []
        for token_address in tokens:
            pairs = await self.dex.get_token_pairs(self.config.chain_id, token_address)
            for pair in pairs:
                candidate = self._pair_candidate_from_token(pair, token_address, source)
                if candidate:
                    results.append(candidate)
        return results

    def _extract_tokens_from_latest(self, data: Any) -> List[str]:
        if not data or not isinstance(data, list):
            return []
        tokens: List[str] = []
        seen: Set[str] = set()
        for item in data:
            if not isinstance(item, dict):
                continue
            if item.get("chainId") != self.config.chain_id:
                continue
            token_address = item.get("tokenAddress")
            if not token_address:
                continue
            key = token_address.lower()
            if key in seen:
                continue
            seen.add(key)
            tokens.append(token_address)
        return tokens

    def _pair_candidate_from_token(
        self, pair: Dict[str, Any], token_address: str, source: str
    ) -> Optional[PairCandidate]:
        if not isinstance(pair, dict):
            return None
        chain_id = pair.get("chainId")
        if chain_id != self.config.chain_id:
            return None
        pair_address = pair.get("pairAddress")
        if not pair_address:
            return None
        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}
        base_addr = base.get("address") or ""
        quote_addr = quote.get("address") or ""
        token_lc = token_address.lower()
        if base_addr.lower() != token_lc and quote_addr.lower() != token_lc:
            return None
        return PairCandidate(
            pair_address=pair_address,
            chain_id=chain_id,
            token_address=token_address,
            pair=pair,
            source=source,
            hot_score=self._hot_score(pair),
        )

    def _pair_candidate_from_generic(
        self, pair: Dict[str, Any], source: str, base_set: Set[str]
    ) -> Optional[PairCandidate]:
        if not isinstance(pair, dict):
            return None
        chain_id = pair.get("chainId")
        if chain_id != self.config.chain_id:
            return None
        pair_address = pair.get("pairAddress")
        if not pair_address:
            return None
        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}
        base_addr = base.get("address")
        quote_addr = quote.get("address")
        token_address = None
        if base_addr and base_addr.lower() in base_set and quote_addr:
            token_address = quote_addr
        elif quote_addr and quote_addr.lower() in base_set and base_addr:
            token_address = base_addr
        else:
            token_address = base_addr or quote_addr
        if not token_address:
            return None
        return PairCandidate(
            pair_address=pair_address,
            chain_id=chain_id,
            token_address=token_address,
            pair=pair,
            source=source,
            hot_score=self._hot_score(pair),
        )

    def _hot_score(self, pair: Dict[str, Any]) -> float:
        liquidity = 0.0
        if isinstance(pair.get("liquidity"), dict):
            liquidity = pair["liquidity"].get("usd") or 0.0
        volume = 0.0
        if isinstance(pair.get("volume"), dict):
            volume = pair["volume"].get("h1") or pair["volume"].get("1h") or 0.0
        return float(liquidity) + float(volume)
