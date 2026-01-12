from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING, Dict, Any

if TYPE_CHECKING:
    import aiohttp
    import logging

    from .config import Config
    from .db import Database
    from .dexscreener import DexscreenerClient
    from .discovery import DiscoveryEngine
    from .wallet_analysis import HeliusClient
    from .wallet_analysis import WalletAnalyzer


@dataclass
class Candidate:
    token_address: str
    chain_id: str
    source: str
    pair_id: Optional[str] = None
    pair_url: Optional[str] = None


@dataclass
class PairCandidate:
    pair_address: str
    chain_id: str
    token_address: str
    pair: Dict[str, Any]
    source: str
    hot_score: float


@dataclass
class FilterMetrics:
    market_cap_value: Optional[float]
    market_cap_label: str
    volume_1h: Optional[float]
    change_1h: Optional[float]
    change_6h: Optional[float]
    change_24h: Optional[float]


@dataclass
class FilterResult:
    passed: bool
    reasons: List[str]
    metrics: FilterMetrics


@dataclass
class AppContext:
    config: "Config"
    logger: "logging.Logger"
    db: "Database"
    session: "aiohttp.ClientSession"
    dex: "DexscreenerClient"
    discovery: "DiscoveryEngine"
    wallet_analyzer: Optional["WalletAnalyzer"] = None
    helius_client: Optional["HeliusClient"] = None
