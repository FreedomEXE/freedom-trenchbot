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
    if bucket is None and window == "m5":
        bucket = txns.get("5m")
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

def _get_volume_5m(pair: Dict[str, Any]) -> Optional[float]:
    volume = pair.get("volume")
    if not isinstance(volume, dict):
        return None
    value = volume.get("m5")
    if value is None:
        value = volume.get("5m")
    return _to_float(value)


def compute_flow(pair: Dict[str, Any]) -> Dict[str, Any]:
    buys_5m_raw = _get_txn_count(pair, "m5", "buys")
    sells_5m_raw = _get_txn_count(pair, "m5", "sells")
    volume_5m_raw = _get_volume_5m(pair)
    buys_1h_raw = _get_txn_count(pair, "h1", "buys")
    sells_1h_raw = _get_txn_count(pair, "h1", "sells")
    volume_1h_raw = _get_volume_1h(pair)

    partial = False
    if (
        buys_5m_raw is None
        or sells_5m_raw is None
        or volume_5m_raw is None
        or buys_1h_raw is None
        or sells_1h_raw is None
        or volume_1h_raw is None
    ):
        partial = True

    has_buys_5m = buys_5m_raw is not None
    has_sells_5m = sells_5m_raw is not None
    has_volume_5m = volume_5m_raw is not None
    has_buys_1h = buys_1h_raw is not None
    has_sells_1h = sells_1h_raw is not None
    has_volume_1h = volume_1h_raw is not None

    buys_5m = buys_5m_raw or 0
    sells_5m = sells_5m_raw or 0
    volume_5m = volume_5m_raw or 0.0
    buys_1h = buys_1h_raw or 0
    sells_1h = sells_1h_raw or 0
    volume_1h = volume_1h_raw or 0.0

    buy_pressure = buys_5m / max(1, sells_5m)
    avg_buy = volume_5m / max(1, buys_5m)
    buy_pressure_1h = buys_1h / max(1, sells_1h)
    avg_buy_1h = volume_1h / max(1, buys_1h)

    gate_5m = (
        has_buys_5m
        and has_sells_5m
        and has_volume_5m
        and buys_5m >= 6
        and volume_5m >= 10000
        and buys_5m > sells_5m
    )
    gate_1h = (
        has_buys_1h
        and has_sells_1h
        and has_volume_1h
        and buys_1h >= 40
        and volume_1h >= 50000
        and buys_1h > sells_1h
    )

    score = 0
    if gate_5m:
        if buys_5m >= 8:
            score += 30
        if buys_5m >= 12:
            score += 20
        if buy_pressure >= 1.8:
            score += 25
        if buy_pressure >= 2.5:
            score += 15
        if 300 <= avg_buy <= 2000:
            score += 20
        elif avg_buy < 150 or avg_buy > 4000:
            score -= 20
    if gate_1h:
        score += 30
        if buys_1h >= 80:
            score += 15
        if buy_pressure_1h >= 1.4:
            score += 15
        if buy_pressure_1h >= 1.8:
            score += 10
        if 300 <= avg_buy_1h <= 2500:
            score += 15
        elif avg_buy_1h < 150 or avg_buy_1h > 5000:
            score -= 15

    if score < 0:
        score = 0
    if score > 100:
        score = 100

    if score >= 75:
        label = "Trade-Eligible"
    elif score >= 55:
        label = "Watch"
    else:
        label = "Ignore"

    return {
        "score": score,
        "max_score": 100,
        "label": label,
        "buys_5m": buys_5m,
        "sells_5m": sells_5m,
        "volume_5m": volume_5m,
        "buy_pressure": buy_pressure,
        "avg_buy": avg_buy,
        "buys_1h": buys_1h,
        "sells_1h": sells_1h,
        "volume_1h": volume_1h,
        "buy_pressure_1h": buy_pressure_1h,
        "avg_buy_1h": avg_buy_1h,
        "gate_5m": gate_5m,
        "gate_1h": gate_1h,
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
