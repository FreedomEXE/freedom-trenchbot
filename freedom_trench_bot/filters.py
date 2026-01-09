from __future__ import annotations

from typing import Any, Dict, List, Optional

from .config import FilterConfig
from .types import FilterMetrics, FilterResult


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_nested_number(obj: Dict[str, Any], *keys: str) -> Optional[float]:
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return _to_float(cur)


def extract_metrics(pair: Dict[str, Any], use_fdv_proxy: bool) -> FilterMetrics:
    market_cap = _to_float(pair.get("marketCap"))
    label = "Market Cap" if market_cap is not None else "Market Cap (missing)"

    fdv = _to_float(pair.get("fdv"))
    if market_cap is None and use_fdv_proxy and fdv is not None:
        market_cap = fdv
        label = "FDV (proxy)"

    volume_1h = _get_nested_number(pair, "volume", "h1")
    if volume_1h is None:
        volume_1h = _get_nested_number(pair, "volume", "1h")

    change_1h = _get_nested_number(pair, "priceChange", "h1")
    change_6h = _get_nested_number(pair, "priceChange", "h6")
    change_24h = _get_nested_number(pair, "priceChange", "h24")

    return FilterMetrics(
        market_cap_value=market_cap,
        market_cap_label=label,
        volume_1h=volume_1h,
        change_1h=change_1h,
        change_6h=change_6h,
        change_24h=change_24h,
    )


def evaluate_pair(pair: Dict[str, Any], filters: FilterConfig, use_fdv_proxy: bool) -> FilterResult:
    metrics = extract_metrics(pair, use_fdv_proxy)
    reasons: List[str] = []

    if metrics.market_cap_value is None:
        reasons.append("market cap missing")
        if _to_float(pair.get("fdv")) is not None and not use_fdv_proxy:
            reasons.append("fdv proxy disabled")
    elif metrics.market_cap_value > filters.max_market_cap:
        reasons.append("market cap above max")

    if metrics.change_24h is None:
        reasons.append("24h change missing")
    elif metrics.change_24h < filters.min_change_24h:
        reasons.append("24h change below min")

    if metrics.change_6h is None:
        reasons.append("6h change missing")
    elif metrics.change_6h < filters.min_change_6h:
        reasons.append("6h change below min")

    if metrics.change_1h is None:
        reasons.append("1h change missing")
    elif metrics.change_1h < filters.min_change_1h:
        reasons.append("1h change below min")

    if metrics.volume_1h is None:
        reasons.append("1h volume missing")
    elif metrics.volume_1h < filters.min_volume_1h:
        reasons.append("1h volume below min")

    return FilterResult(passed=len(reasons) == 0, reasons=reasons, metrics=metrics)
