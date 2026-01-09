from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from typing import Optional


def _day_key(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


async def increment_counter(db, key: str, amount: int) -> int:
    state_key = f"metrics_{key}"
    current = await db.get_state_int(state_key, 0)
    new_value = current + amount
    await db.set_state(state_key, str(new_value))
    return new_value


async def update_rate_counter(
    db,
    key_prefix: str,
    amount: int,
    now: int,
    window_sec: int = 60,
    min_elapsed_sec: int = 5,
) -> float:
    start_key = f"{key_prefix}_window_start"
    count_key = f"{key_prefix}_window_count"

    start = await db.get_state_int(start_key, 0)
    if start == 0 or now - start >= window_sec:
        start = now
        count = amount
        await db.set_state(start_key, str(start))
        await db.set_state(count_key, str(count))
    else:
        count = await db.get_state_int(count_key, 0) + amount
        await db.set_state(count_key, str(count))

    elapsed = max(1, now - start)
    if elapsed < min_elapsed_sec:
        existing = await db.get_state(f"metrics_{key_prefix}_per_min")
        try:
            return float(existing) if existing else 0.0
        except (TypeError, ValueError):
            return 0.0

    rate = (count * 60.0) / min(window_sec, elapsed)
    await db.set_state(f"metrics_{key_prefix}_per_min", f"{rate:.2f}")
    return rate


async def increment_daily_counter(db, key_prefix: str, amount: int, now: int) -> int:
    day_key = _day_key(now)
    key_day = f"{key_prefix}_day"
    key_count = f"{key_prefix}_day_count"

    current_day = await db.get_state(key_day)
    if current_day != day_key:
        count = amount
        await db.set_state(key_day, day_key)
    else:
        count = await db.get_state_int(key_count, 0) + amount

    await db.set_state(key_count, str(count))
    await db.set_state(f"metrics_{key_prefix}_per_day", str(count))
    return count


async def add_lag_sample(db, lag_sec: int, max_samples: int) -> Optional[int]:
    if lag_sec <= 0:
        return None

    raw = await db.get_state("alert_lag_samples")
    try:
        samples = json.loads(raw) if raw else []
    except json.JSONDecodeError:
        samples = []

    samples.append(int(lag_sec))
    if len(samples) > max_samples:
        samples = samples[-max_samples:]

    await db.set_state("alert_lag_samples", json.dumps(samples))

    median_val = int(statistics.median(samples)) if samples else 0
    await db.set_state("metrics_alert_lag_median_sec", str(median_val))
    return median_val
