from __future__ import annotations

import html
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$", re.IGNORECASE)


def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    value = value.strip().lower()
    if value in ("1", "true", "yes", "y", "on"):
        return True
    if value in ("0", "false", "no", "n", "off"):
        return False
    return default


def parse_csv_ints(value: str) -> set:
    if not value:
        return set()
    result = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            continue
    return result


def parse_csv_strs(value: str) -> List[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_duration(value: str) -> Optional[int]:
    if not value:
        return None
    match = _DURATION_RE.match(value)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2).lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return amount * mult


def format_usd(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.0f}"


def format_pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None or seconds <= 0:
        return "n/a"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def escape_html(value: str) -> str:
    return html.escape(value or "")


def utc_now_ts() -> int:
    return int(time.time())


def format_ts(ts: Optional[int], tz_name: str) -> str:
    if not ts:
        return "n/a"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    if tz_name and ZoneInfo is not None:
        try:
            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")
