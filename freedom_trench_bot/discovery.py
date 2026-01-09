from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional, Set

from .types import PairCandidate

DEFAULT_BASE_TOKENS: List[Tuple[str, str]] = [
    ("So11111111111111111111111111111111111111112", "WSOL"),
    ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "USDC"),
]


class DiscoveryEngine:
    def __init__(self, dex, config, logger):
        self.dex = dex
        self.config = config
        self.logger = logger

    async def discover_pairs(self) -> List[PairCandidate]:
        mode = self.config.discovery_mode
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
        if self.config.market_base_tokens:
            return [(address, "BASE") for address in self.config.market_base_tokens]
        return DEFAULT_BASE_TOKENS

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
        results: List[PairCandidate] = []
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            chain_id = pair.get("chainId")
            pair_address = pair.get("pairAddress")
            base = pair.get("baseToken") or {}
            token_address = base.get("address")
            if chain_id and token_address and pair_address:
                results.append(
                    PairCandidate(
                        pair_address=pair_address,
                        chain_id=chain_id,
                        token_address=token_address,
                        pair=pair,
                        source=source,
                        hot_score=self._hot_score(pair),
                    )
                )
        return results

    def _hot_score(self, pair: Dict[str, Any]) -> float:
        liquidity = 0.0
        if isinstance(pair.get("liquidity"), dict):
            liquidity = pair["liquidity"].get("usd") or 0.0
        volume = 0.0
        if isinstance(pair.get("volume"), dict):
            volume = pair["volume"].get("h1") or pair["volume"].get("1h") or 0.0
        return float(liquidity) + float(volume)
