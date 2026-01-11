from __future__ import annotations

from dataclasses import dataclass
import os
from typing import List, Set

from .utils import parse_bool, parse_csv_ints, parse_csv_strs


@dataclass(frozen=True)
class FilterConfig:
    max_market_cap: float
    min_change_24h: float
    min_change_6h: float
    min_change_1h: float
    min_volume_1h: float
    require_profile: bool = True


@dataclass(frozen=True)
class Config:
    bot_token: str
    allowed_chat_ids: Set[int]
    allowed_thread_ids: Set[int]
    admin_user_ids: Set[int]
    sqlite_path: str
    log_level: str
    dry_run: bool
    scan_interval_sec: int
    dedup_window_sec: int
    max_alerts_per_scan: int
    min_ineligible_duration_sec: int
    candidate_pool_max: int
    hot_recheck_top_n: int
    chain_id: str
    display_timezone: str
    eligible_retention_sec: int
    eligible_list_limit: int
    called_list_limit: int
    alert_tagline: str
    wallet_analysis_enabled: bool
    wallet_analysis_provider: str
    wallet_analysis_sample: int
    wallet_analysis_label: str
    wallet_analysis_max_pages: int
    wallet_analysis_ttl_sec: int
    fresh_wallet_max_age_days: int
    fresh_wallet_max_tx: int
    helius_api_key: str

    dex_max_rps: int
    dex_max_concurrency: int
    dex_timeout_sec: int
    dex_retry_attempts: int
    dex_retry_base_delay_sec: float
    dex_cache_ttl_sec: int

    discovery_mode: str
    market_base_tokens: List[str]
    search_queries: List[str]
    hybrid_search_refresh_sec: int
    hybrid_refresh_sec: int
    hybrid_max_tokens: int

    use_fdv_as_mc_proxy: bool
    metrics_sample_size: int
    filters: FilterConfig


def load_config() -> Config:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    allowed_chat_ids = parse_csv_ints(os.getenv("ALLOWED_CHAT_IDS", ""))
    admin_user_ids = parse_csv_ints(os.getenv("ADMIN_USER_IDS", ""))
    allowed_thread_ids = parse_csv_ints(os.getenv("ALLOWED_THREAD_IDS", ""))

    db_path = os.getenv("DB_PATH", "").strip()
    if not db_path:
        db_path = os.getenv("SQLITE_PATH", "").strip()
    if not db_path:
        db_path = "./data/freedom_trench_bot.db"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    dry_run = parse_bool(os.getenv("DRY_RUN", "false"), False)

    scan_interval_sec = int(
        os.getenv("SCAN_INTERVAL_SECONDS", os.getenv("POLL_INTERVAL_SEC", "20"))
    )
    dedup_window_sec = int(os.getenv("DEDUP_WINDOW_HOURS", "24")) * 3600
    max_alerts_per_scan = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
    min_ineligible_duration_sec = int(os.getenv("MIN_INELIGIBLE_MINUTES_TO_REARM", "30")) * 60
    candidate_pool_max = int(os.getenv("CANDIDATE_POOL_MAX", "1000"))
    hot_recheck_top_n = int(os.getenv("HOT_RECHECK_TOP_N", "150"))
    if hot_recheck_top_n > candidate_pool_max:
        hot_recheck_top_n = candidate_pool_max

    chain_id = "solana"
    display_timezone = os.getenv("DISPLAY_TIMEZONE", "America/New_York")
    eligible_retention_sec = int(os.getenv("ELIGIBLE_RETENTION_HOURS", "24")) * 3600
    eligible_list_limit = int(os.getenv("ELIGIBLE_LIST_LIMIT", "20"))
    called_list_limit = int(os.getenv("CALLED_LIST_LIMIT", "50"))
    alert_tagline = os.getenv("ALERT_TAGLINE", "Trenches Call").strip()

    wallet_analysis_enabled = parse_bool(os.getenv("WALLET_ANALYSIS_ENABLED", "false"), False)
    wallet_analysis_provider = os.getenv("WALLET_ANALYSIS_PROVIDER", "helius").strip().lower()
    wallet_analysis_sample = int(os.getenv("WALLET_ANALYSIS_SAMPLE", "20"))
    wallet_analysis_label = os.getenv("WALLET_ANALYSIS_LABEL", "Top Wallet Call").strip()
    wallet_analysis_max_pages = int(os.getenv("WALLET_ANALYSIS_MAX_PAGES", "10"))
    wallet_analysis_ttl_sec = int(os.getenv("WALLET_ANALYSIS_TTL_HOURS", "24")) * 3600
    fresh_wallet_max_age_days = int(os.getenv("FRESH_WALLET_MAX_AGE_DAYS", "7"))
    fresh_wallet_max_tx = int(os.getenv("FRESH_WALLET_MAX_TX", "20"))
    helius_api_key = os.getenv("HELIUS_API_KEY", "").strip()

    dex_max_rps = int(os.getenv("DEX_MAX_RPS", "5"))
    dex_max_concurrency = int(os.getenv("DEX_MAX_CONCURRENCY", "2"))
    dex_timeout_sec = int(os.getenv("DEX_TIMEOUT_SEC", "10"))
    dex_retry_attempts = int(os.getenv("DEX_RETRY_ATTEMPTS", "3"))
    dex_retry_base_delay_sec = float(os.getenv("DEX_RETRY_BASE_DELAY_SEC", "0.5"))
    dex_cache_ttl_sec = int(os.getenv("DEX_CACHE_TTL_SEC", "15"))

    discovery_mode = os.getenv("DISCOVERY_MODE", "hybrid").strip().lower()
    market_base_tokens = parse_csv_strs(os.getenv("MARKET_BASE_TOKENS", ""))
    search_queries = parse_csv_strs(os.getenv("SEARCH_QUERIES", ""))
    hybrid_search_refresh_sec = int(os.getenv("HYBRID_SEARCH_REFRESH_SECONDS", "30"))
    hybrid_refresh_sec = int(os.getenv("HYBRID_REFRESH_SECONDS", "60"))
    hybrid_max_tokens = int(os.getenv("HYBRID_MAX_TOKENS", "50"))

    use_fdv_as_mc_proxy = parse_bool(os.getenv("USE_FDV_AS_MC_PROXY", "false"), False)
    metrics_sample_size = int(os.getenv("METRICS_SAMPLE_SIZE", "200"))

    filters = FilterConfig(
        max_market_cap=float(os.getenv("FILTER_MARKETCAP_MAX", "100000")),
        min_change_24h=float(os.getenv("FILTER_CHANGE_24H_MIN", "1")),
        min_change_6h=float(os.getenv("FILTER_CHANGE_6H_MIN", "1")),
        min_change_1h=float(os.getenv("FILTER_CHANGE_1H_MIN", "1")),
        min_volume_1h=float(os.getenv("FILTER_VOLUME_1H_MIN", "10000")),
        require_profile=parse_bool(os.getenv("FILTER_REQUIRE_PROFILE", "true"), True),
    )

    return Config(
        bot_token=bot_token,
        allowed_chat_ids=allowed_chat_ids,
        allowed_thread_ids=allowed_thread_ids,
        admin_user_ids=admin_user_ids,
        sqlite_path=db_path,
        log_level=log_level,
        dry_run=dry_run,
        scan_interval_sec=scan_interval_sec,
        dedup_window_sec=dedup_window_sec,
        max_alerts_per_scan=max_alerts_per_scan,
        min_ineligible_duration_sec=min_ineligible_duration_sec,
        candidate_pool_max=candidate_pool_max,
        hot_recheck_top_n=hot_recheck_top_n,
        chain_id=chain_id,
        display_timezone=display_timezone,
        eligible_retention_sec=eligible_retention_sec,
        eligible_list_limit=eligible_list_limit,
        called_list_limit=called_list_limit,
        alert_tagline=alert_tagline,
        wallet_analysis_enabled=wallet_analysis_enabled,
        wallet_analysis_provider=wallet_analysis_provider,
        wallet_analysis_sample=wallet_analysis_sample,
        wallet_analysis_label=wallet_analysis_label,
        wallet_analysis_max_pages=wallet_analysis_max_pages,
        wallet_analysis_ttl_sec=wallet_analysis_ttl_sec,
        fresh_wallet_max_age_days=fresh_wallet_max_age_days,
        fresh_wallet_max_tx=fresh_wallet_max_tx,
        helius_api_key=helius_api_key,
        dex_max_rps=dex_max_rps,
        dex_max_concurrency=dex_max_concurrency,
        dex_timeout_sec=dex_timeout_sec,
        dex_retry_attempts=dex_retry_attempts,
        dex_retry_base_delay_sec=dex_retry_base_delay_sec,
        dex_cache_ttl_sec=dex_cache_ttl_sec,
        discovery_mode=discovery_mode,
        market_base_tokens=market_base_tokens,
        search_queries=search_queries,
        hybrid_search_refresh_sec=hybrid_search_refresh_sec,
        hybrid_refresh_sec=hybrid_refresh_sec,
        hybrid_max_tokens=hybrid_max_tokens,
        use_fdv_as_mc_proxy=use_fdv_as_mc_proxy,
        metrics_sample_size=metrics_sample_size,
        filters=filters,
    )
