from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from aiohttp import web

from .db import Database


@dataclass
class SummaryStats:
    recoup_p50_sec: Optional[float]
    recoup_p75_sec: Optional[float]
    recoup_p90_sec: Optional[float]
    signals_by_hour: List[int]
    recoup_p50_by_hour: List[Optional[float]]


def _parse_snapshot(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _snapshot_price(raw: Optional[str]) -> Optional[float]:
    snapshot = _parse_snapshot(raw)
    value = snapshot.get("priceUsd")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _snapshot_metric(raw: Optional[str], key: str) -> Optional[float]:
    snapshot = _parse_snapshot(raw)
    value = snapshot.get(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_iso(ts: Optional[int]) -> Optional[str]:
    if not ts:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _compute_percentile(values: List[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    sorted_vals = sorted(values)
    pct = max(0.0, min(100.0, percentile))
    idx = (pct / 100.0) * (len(sorted_vals) - 1)
    lower = int(idx)
    upper = min(len(sorted_vals) - 1, lower + 1)
    if lower == upper:
        return sorted_vals[lower]
    weight = idx - lower
    return sorted_vals[lower] * (1 - weight) + sorted_vals[upper] * weight


def _get_utc_hour(ts: Optional[int]) -> Optional[int]:
    if ts is None:
        return None
    try:
        return time.gmtime(ts).tm_hour
    except (TypeError, ValueError):
        return None


async def _fetch_rows(db: Database, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    assert db.conn is not None
    cur = await db.conn.execute(query, params)
    rows = await cur.fetchall()
    await cur.close()
    return [dict(row) for row in rows]


async def _fetch_row(db: Database, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    assert db.conn is not None
    cur = await db.conn.execute(query, params)
    row = await cur.fetchone()
    await cur.close()
    return dict(row) if row else None


async def build_summary(db: Database) -> Dict[str, Any]:
    now = int(time.time())
    reset_at = await db.get_state_int("sim_reset_at", 0)
    start_balance = await db.get_state_float("sim_start_balance", 100.0)
    position_size = await db.get_state_float("sim_position_size", 1.0)
    cash = await db.get_state_float("sim_cash", start_balance)

    counts_row = await _fetch_row(
        db,
        """
        SELECT
          SUM(CASE WHEN sim_taken = 1 THEN 1 ELSE 0 END) AS taken,
          SUM(CASE WHEN sim_taken = 1 AND recouped_at IS NOT NULL THEN 1 ELSE 0 END) AS recouped,
          SUM(CASE WHEN sim_taken = 1 AND recouped_at IS NULL THEN 1 ELSE 0 END) AS open
        FROM tokens
        WHERE eligible_first_at >= ?
        """,
        (reset_at,),
    )
    taken = int(counts_row.get("taken") or 0)
    recouped = int(counts_row.get("recouped") or 0)
    open_count = int(counts_row.get("open") or 0)

    recoup_rows = await _fetch_rows(
        db,
        """
        SELECT eligible_first_at, recouped_at
        FROM tokens
        WHERE sim_taken = 1
          AND recouped_at IS NOT NULL
          AND eligible_first_at >= ?
        """,
        (reset_at,),
    )
    durations = [
        max(0, row["recouped_at"] - row["eligible_first_at"])
        for row in recoup_rows
        if row.get("eligible_first_at") and row.get("recouped_at")
    ]
    signals_by_hour = [0 for _ in range(24)]
    for row in recoup_rows:
        hour = _get_utc_hour(row.get("eligible_first_at"))
        if hour is not None:
            signals_by_hour[hour] += 1

    recoup_by_hour: List[List[float]] = [[] for _ in range(24)]
    for row in recoup_rows:
        hour = _get_utc_hour(row.get("eligible_first_at"))
        if hour is None:
            continue
        if row.get("eligible_first_at") and row.get("recouped_at"):
            recoup_by_hour[hour].append(
                max(0, row["recouped_at"] - row["eligible_first_at"])
            )

    stats = SummaryStats(
        recoup_p50_sec=_compute_percentile(durations, 50),
        recoup_p75_sec=_compute_percentile(durations, 75),
        recoup_p90_sec=_compute_percentile(durations, 90),
        signals_by_hour=signals_by_hour,
        recoup_p50_by_hour=[
            _compute_percentile(bucket, 50) for bucket in recoup_by_hour
        ],
    )

    recent_rows = await _fetch_rows(
        db,
        """
        SELECT token_address, eligible_first_at, last_name, last_symbol,
               called_price_usd, max_price_usd, min_price_usd,
               last_seen_metrics, eligible_first_metrics, recouped_at,
               stoploss_at, stoploss_price_usd,
               post_alert_price_usd, post_alert_at,
               moonbag_tokens, moonbag_sold_at, moonbag_sold_value
        FROM tokens
        WHERE sim_taken = 1
          AND eligible_first_at >= ?
        ORDER BY eligible_first_at DESC
        LIMIT 50
        """,
        (reset_at,),
    )

    moonbag_rows = await _fetch_rows(
        db,
        """
        SELECT token_address, eligible_first_at, last_name, last_symbol,
               called_price_usd, max_price_usd, min_price_usd,
               last_seen_metrics, eligible_first_metrics, recouped_at,
               stoploss_at, stoploss_price_usd,
               post_alert_price_usd, post_alert_at,
               moonbag_tokens, moonbag_sold_at, moonbag_sold_value
        FROM tokens
        WHERE sim_taken = 1
          AND moonbag_tokens IS NOT NULL
          AND eligible_first_at >= ?
        ORDER BY eligible_first_at DESC
        LIMIT 200
        """,
        (reset_at,),
    )

    def format_token(row: Dict[str, Any]) -> Dict[str, Any]:
        entry = row.get("called_price_usd")
        current = _snapshot_price(row.get("last_seen_metrics")) or row.get(
            "post_alert_price_usd"
        )
        max_price = row.get("max_price_usd")
        min_price = row.get("min_price_usd")

        def multiple(value: Optional[float]) -> Optional[float]:
            if not value or not entry:
                return None
            try:
                return value / entry
            except ZeroDivisionError:
                return None

        return {
            "token_address": row.get("token_address"),
            "name": row.get("last_name") or "",
            "symbol": row.get("last_symbol") or "",
            "eligible_first_at": row.get("eligible_first_at"),
            "eligible_first_iso": _to_iso(row.get("eligible_first_at")),
            "entry_price_usd": entry,
            "current_price_usd": current,
            "max_price_usd": max_price,
            "min_price_usd": min_price,
            "current_multiple": multiple(current),
        "max_multiple": multiple(max_price),
        "min_multiple": multiple(min_price),
        "recouped_at": row.get("recouped_at"),
        "recouped_iso": _to_iso(row.get("recouped_at")),
        "stoploss_at": row.get("stoploss_at"),
        "stoploss_iso": _to_iso(row.get("stoploss_at")),
        "stoploss_price_usd": row.get("stoploss_price_usd"),
        "moonbag_tokens": row.get("moonbag_tokens"),
            "moonbag_sold_at": row.get("moonbag_sold_at"),
            "moonbag_sold_iso": _to_iso(row.get("moonbag_sold_at")),
            "moonbag_sold_value": row.get("moonbag_sold_value"),
        }

    lookback_24h = now - 24 * 3600
    snapshot_rows = await _fetch_rows(
        db,
        """
        SELECT last_seen_metrics
        FROM tokens
        WHERE last_seen >= ?
          AND last_seen_metrics IS NOT NULL
        """,
        (lookback_24h,),
    )
    snapshots = [row["last_seen_metrics"] for row in snapshot_rows if row.get("last_seen_metrics")]

    def metric_values(key: str) -> List[float]:
        values: List[float] = []
        for raw in snapshots:
            val = _snapshot_metric(raw, key)
            if val is not None:
                values.append(val)
        return values

    volume_values = metric_values("volume1h")
    solana_stats = {
        "sample_tokens_24h": len(snapshots),
        "volume1h_total": sum(volume_values) if volume_values else None,
        "marketcap_median": _compute_percentile(metric_values("marketCap"), 50),
        "change1h_median": _compute_percentile(metric_values("change1h"), 50),
        "change6h_median": _compute_percentile(metric_values("change6h"), 50),
        "change24h_median": _compute_percentile(metric_values("change24h"), 50),
        "price_median": _compute_percentile(metric_values("priceUsd"), 50),
        "holders_median": _compute_percentile(metric_values("holderCount"), 50),
    }

    return {
        "updated_at": _to_iso(now),
        "sim": {
            "reset_at": reset_at,
            "reset_iso": _to_iso(reset_at),
            "start_balance": start_balance,
            "position_size": position_size,
            "cash": cash,
        },
        "counts": {"taken": taken, "recouped": recouped, "open": open_count},
        "stats": asdict(stats),
        "solana": solana_stats,
        "moonbags": [format_token(row) for row in moonbag_rows],
        "recent": [format_token(row) for row in recent_rows],
    }


def _get_auth_token(request: web.Request) -> Optional[str]:
    header = request.headers.get("x-api-key")
    if header:
        return header
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:]
    return None


async def summary_handler(request: web.Request) -> web.Response:
    token = os.getenv("SUMMARY_API_TOKEN", "").strip()
    if token:
        provided = _get_auth_token(request)
        if provided != token:
            return web.json_response({"error": "unauthorized"}, status=401)

    app_ctx = request.app["app_ctx"]
    summary = await build_summary(app_ctx.db)
    return web.json_response(summary)


def create_app(app_ctx) -> web.Application:
    app = web.Application()
    app["app_ctx"] = app_ctx
    app.router.add_get("/api/summary", summary_handler)
    app.router.add_get("/health", lambda _: web.json_response({"ok": True}))
    return app


async def start_http_server(app_ctx, logger) -> Optional[web.AppRunner]:
    port_raw = os.getenv("PORT", "").strip()
    if not port_raw:
        return None
    try:
        port = int(port_raw)
    except ValueError:
        logger.warning("http_port_invalid", extra={"port": port_raw})
        return None

    app = create_app(app_ctx)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("http_server_ready", extra={"port": port})
    return runner


async def stop_http_server(runner: Optional[web.AppRunner]) -> None:
    if runner:
        await runner.cleanup()
