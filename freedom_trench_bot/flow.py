from __future__ import annotations

from typing import Any, Dict, Optional


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_txn_count(pair: Dict[str, Any], window: str, side: str) -> Optional[int]:
    txns = pair.get("txns")
    if not isinstance(txns, dict):
        return None
    bucket = txns.get(window)
    if not isinstance(bucket, dict):
        return None
    return _to_int(bucket.get(side))


def _get_volume_1h(pair: Dict[str, Any]) -> Optional[float]:
    volume = pair.get("volume")
    if not isinstance(volume, dict):
        return None
    value = volume.get("h1")
    if value is None:
        value = volume.get("1h")
    return _to_float(value)


def compute_flow(pair: Dict[str, Any]) -> Dict[str, Any]:
    buys_1h_raw = _get_txn_count(pair, "h1", "buys")
    sells_1h_raw = _get_txn_count(pair, "h1", "sells")
    buys_5m_raw = _get_txn_count(pair, "m5", "buys")
    volume_1h_raw = _get_volume_1h(pair)

    partial = False
    if buys_1h_raw is None or sells_1h_raw is None or buys_5m_raw is None or volume_1h_raw is None:
        partial = True

    has_buys_1h = buys_1h_raw is not None
    has_sells_1h = sells_1h_raw is not None
    has_buys_5m = buys_5m_raw is not None
    has_volume_1h = volume_1h_raw is not None

    buys_1h = buys_1h_raw or 0
    sells_1h = sells_1h_raw or 0
    buys_5m = buys_5m_raw or 0
    volume_1h = volume_1h_raw or 0.0

    buy_sell_ratio = buys_1h / max(1, sells_1h)
    buy_velocity = (buys_5m * 12) / max(1, buys_1h)
    volume_per_buy = volume_1h / max(1, buys_1h)

    score = 0
    if has_buys_1h and buys_1h >= 20:
        score += 15
    if has_buys_1h and has_sells_1h and buy_sell_ratio >= 1.5:
        score += 35
    if has_buys_1h and has_buys_5m and buy_velocity >= 1.2:
        score += 25
    if has_buys_1h and has_volume_1h and volume_per_buy <= 1500:
        score += 25

    if score >= 70:
        label = "High"
    elif score >= 45:
        label = "Medium"
    else:
        label = "Low"

    return {
        "score": score,
        "max_score": 100,
        "label": label,
        "buys_1h": buys_1h,
        "sells_1h": sells_1h,
        "buys_5m": buys_5m,
        "volume_1h": volume_1h,
        "buy_sell_ratio": buy_sell_ratio,
        "buy_velocity": buy_velocity,
        "volume_per_buy": volume_per_buy,
        "partial": partial,
    }


def flow_from_snapshot(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        import json

        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    flow = data.get("flow")
    return flow if isinstance(flow, dict) else None
