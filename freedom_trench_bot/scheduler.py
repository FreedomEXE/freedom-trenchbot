from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from telegram.constants import ParseMode

from .bot import (
    build_alert_keyboard,
    format_alert_message,
    build_trigger_reason,
    format_wallet_analysis_update,
    format_intent_update,
)
from .filters import evaluate_pair, extract_metrics
from .metrics import add_lag_sample, increment_counter, increment_daily_counter, update_rate_counter
from .types import AppContext, PairCandidate
from .utils import utc_now_ts

POOL_RETENTION_SEC = 6 * 3600
PERFORMANCE_LOOKBACK_DAYS = 7
PERFORMANCE_REFRESH_INTERVAL_SEC = 300
PERFORMANCE_BATCH_SIZE = 50


def _pair_sort_key(pair: Dict[str, Any]) -> float:
    liquidity = 0.0
    if isinstance(pair.get("liquidity"), dict):
        liquidity = pair["liquidity"].get("usd") or 0.0
    volume = 0.0
    if isinstance(pair.get("volume"), dict):
        volume = pair["volume"].get("h1") or pair["volume"].get("1h") or 0.0
    return float(liquidity) + float(volume)


def _coerce_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return None
    if ts > 10**12:
        ts = int(ts / 1000)
    return ts


def _extract_pair(payload: Any) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    if isinstance(payload, dict):
        if "pair" in payload and isinstance(payload["pair"], dict):
            return payload["pair"]
        if "pairs" in payload and isinstance(payload["pairs"], list) and payload["pairs"]:
            first = payload["pairs"][0]
            return first if isinstance(first, dict) else None
        return payload if "pairAddress" in payload else None
    return None


def _metrics_snapshot(pair: Dict[str, Any], metrics) -> str:
    price_usd = _to_float(pair.get("priceUsd"))
    data = {
        "pairAddress": pair.get("pairAddress"),
        "marketCap": metrics.market_cap_value,
        "marketCapLabel": metrics.market_cap_label,
        "volume1h": metrics.volume_1h,
        "change1h": metrics.change_1h,
        "change6h": metrics.change_6h,
        "change24h": metrics.change_24h,
        "priceUsd": price_usd,
    }
    return json.dumps(data, ensure_ascii=True)


def _parse_wallet_analysis(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


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
    return _to_float(snapshot.get("priceUsd"))


def _snapshot_mcap(raw: Optional[str]) -> Optional[float]:
    snapshot = _parse_snapshot(raw)
    return _to_float(snapshot.get("marketCap"))


def _snapshot_pair_address(raw: Optional[str]) -> Optional[str]:
    snapshot = _parse_snapshot(raw)
    pair_address = snapshot.get("pairAddress")
    return str(pair_address) if pair_address else None


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_token_meta(pair: Dict[str, Any], token_address: str) -> Tuple[str, str]:
    base = pair.get("baseToken") if isinstance(pair, dict) else None
    quote = pair.get("quoteToken") if isinstance(pair, dict) else None
    token_address_lc = token_address.lower()
    token_obj = base if isinstance(base, dict) else {}
    if isinstance(base, dict) and base.get("address") and base["address"].lower() == token_address_lc:
        token_obj = base
    elif (
        isinstance(quote, dict)
        and quote.get("address")
        and quote["address"].lower() == token_address_lc
    ):
        token_obj = quote
    name = token_obj.get("name") or "Unknown"
    symbol = token_obj.get("symbol") or "?"
    return str(name), str(symbol)


@dataclass(frozen=True)
class AlertMessageRef:
    chat_id: int
    message_id: int
    thread_id: Optional[int]


class Scanner:
    def __init__(self, app_ctx: AppContext, bot):
        self.ctx = app_ctx
        self.bot = bot
        self._scan_lock = asyncio.Lock()
        self._analysis_lock = asyncio.Lock()
        self._analysis_inflight: set[str] = set()
        concurrency = max(1, min(4, app_ctx.config.dex_max_concurrency))
        self._analysis_sem = asyncio.Semaphore(concurrency)
        self._performance_lock = asyncio.Lock()
        self._backfill_lock = asyncio.Lock()
        self._intent_lock = asyncio.Lock()
        self._intent_inflight: set[str] = set()

    async def scan_job(self, context) -> None:
        if self._scan_lock.locked():
            self.ctx.logger.warning("scan_overlap_skip")
            await increment_counter(self.ctx.db, "scan_overlap", 1)
            return
        async with self._scan_lock:
            try:
                await self.scan_once()
            except Exception:
                self.ctx.logger.exception("scan_error")

    async def performance_job(self, context) -> None:
        if self._performance_lock.locked():
            return
        async with self._performance_lock:
            try:
                await self.refresh_performance_batch()
            except Exception:
                self.ctx.logger.exception("performance_refresh_error")

    async def scan_once(self) -> None:
        config = self.ctx.config
        db = self.ctx.db
        logger = self.ctx.logger
        now = utc_now_ts()

        await db.set_state("last_scan_at", str(now))

        paused = await db.get_state_bool("paused", False)
        if paused:
            logger.info("scan_paused")
            return

        mute_until = await db.get_state_int("mute_until", 0)
        muted = mute_until > now

        fresh_pairs = await self.ctx.discovery.discover_pairs()
        await increment_counter(db, "scans", 1)
        await update_rate_counter(db, "candidates", len(fresh_pairs), now)

        candidates = self._dedup_candidates(fresh_pairs, config.candidate_pool_max)
        for candidate in candidates:
            metrics = extract_metrics(candidate.pair, config.use_fdv_as_mc_proxy)
            await db.upsert_pair_pool(
                pair_address=candidate.pair_address,
                chain_id=candidate.chain_id,
                token_address=candidate.token_address,
                last_seen_at=now,
                last_hot_score=candidate.hot_score,
                last_metrics=_metrics_snapshot(candidate.pair, metrics),
                source=candidate.source,
            )

        await db.purge_pair_pool(now - POOL_RETENTION_SEC)
        await db.trim_pair_pool(config.candidate_pool_max)

        hot_rows = await db.get_hot_pairs(config.hot_recheck_top_n, now - POOL_RETENTION_SEC)
        hot_fetch_count = 0
        candidate_by_pair = {cand.pair_address.lower(): cand for cand in candidates}
        for row in hot_rows:
            row_key = row["pair_address"].lower()
            existing = candidate_by_pair.get(row_key)
            if existing is not None:
                metrics = extract_metrics(existing.pair, config.use_fdv_as_mc_proxy)
                await db.update_pair_checked(
                    existing.pair_address,
                    now,
                    existing.hot_score,
                    _metrics_snapshot(existing.pair, metrics),
                )
                continue
            payload = await self.ctx.dex.get_pair(config.chain_id, row["pair_address"])
            hot_fetch_count += 1
            pair = _extract_pair(payload)
            if not pair:
                continue
            hot_score = _pair_sort_key(pair)
            candidate = PairCandidate(
                pair_address=row["pair_address"],
                chain_id=row["chain_id"],
                token_address=row["token_address"],
                pair=pair,
                source="hot_pool",
                hot_score=hot_score,
            )
            candidates.append(candidate)
            metrics = extract_metrics(pair, config.use_fdv_as_mc_proxy)
            await db.update_pair_checked(
                candidate.pair_address,
                now,
                candidate.hot_score,
                _metrics_snapshot(pair, metrics),
            )

        await update_rate_counter(db, "pairs_fetched", hot_fetch_count, now)
        await increment_counter(db, "scanned_pairs", len(candidates))

        token_groups = self._group_pairs_by_token(candidates)
        await increment_counter(db, "unique_tokens_checked", len(token_groups))

        for token_address, token_candidates in token_groups.items():
            await db.upsert_token_seen(token_address, config.chain_id, now)
            token_row = await db.get_token(token_address)
            if token_row is None:
                continue

            best_candidate: Optional[PairCandidate] = None
            best_result = None
            best_pass_candidate: Optional[PairCandidate] = None
            best_pass_result = None
            for candidate in token_candidates:
                result = evaluate_pair(candidate.pair, config.filters, config.use_fdv_as_mc_proxy)
                if best_candidate is None or candidate.hot_score > best_candidate.hot_score:
                    best_candidate = candidate
                    best_result = result
                if result.passed and (
                    best_pass_candidate is None or candidate.hot_score > best_pass_candidate.hot_score
                ):
                    best_pass_candidate = candidate
                    best_pass_result = result

            if best_candidate is None or best_result is None:
                continue

            if best_pass_candidate is not None and best_pass_result is not None:
                primary_candidate = best_pass_candidate
                primary_result = best_pass_result
                eligible = True
            else:
                primary_candidate = best_candidate
                primary_result = best_result
                eligible = False
                logger.info(
                    "filter_reject",
                    extra={
                        "token": token_address,
                        "pair": primary_candidate.pair_address,
                        "reasons": primary_result.reasons,
                        "metrics": primary_result.metrics.__dict__,
                    },
                )

            name, symbol = _extract_token_meta(primary_candidate.pair, token_address)
            last_seen_metrics = _metrics_snapshot(primary_candidate.pair, primary_result.metrics)
            price_usd = _to_float(primary_candidate.pair.get("priceUsd"))
            market_cap_value = primary_result.metrics.market_cap_value

            eligible_first_at = token_row["eligible_first_at"]
            eligible_first_metrics = token_row["eligible_first_metrics"]
            newly_eligible = False
            if eligible and not eligible_first_at:
                eligible_first_at = now
                eligible_first_metrics = last_seen_metrics
                newly_eligible = True

            called_price_usd = token_row["called_price_usd"]
            if eligible_first_at and called_price_usd is None and price_usd is not None:
                called_price_usd = price_usd

            max_price_usd = token_row["max_price_usd"]
            if eligible_first_at and price_usd is not None:
                if max_price_usd is None or price_usd > max_price_usd:
                    max_price_usd = price_usd

            max_market_cap = token_row["max_market_cap"]
            if eligible_first_at and market_cap_value is not None:
                if max_market_cap is None or market_cap_value > max_market_cap:
                    max_market_cap = market_cap_value

            last_eligible_at = token_row["last_eligible_at"]
            last_ineligible_at = token_row["last_ineligible_at"]
            if eligible:
                last_eligible_at = now
            else:
                last_ineligible_at = now

            hit_2x_at = token_row["hit_2x_at"]
            hit_3x_at = token_row["hit_3x_at"]
            hit_5x_at = token_row["hit_5x_at"]
            if called_price_usd and price_usd and called_price_usd > 0:
                multiple = price_usd / called_price_usd
                if hit_2x_at is None and multiple >= 2.0:
                    hit_2x_at = now
                if hit_3x_at is None and multiple >= 3.0:
                    hit_3x_at = now
                if hit_5x_at is None and multiple >= 5.0:
                    hit_5x_at = now

            await db.update_token_state(
                token_address=token_address,
                last_checked_at=now,
                last_eligible=eligible,
                last_eligible_at=last_eligible_at,
                last_ineligible_at=last_ineligible_at,
                last_seen_metrics=last_seen_metrics,
                eligible_first_at=eligible_first_at,
                eligible_first_metrics=eligible_first_metrics,
                last_name=name,
                last_symbol=symbol,
                called_price_usd=called_price_usd,
                max_price_usd=max_price_usd,
                max_market_cap=max_market_cap,
                hit_2x_at=hit_2x_at,
                hit_3x_at=hit_3x_at,
                hit_5x_at=hit_5x_at,
            )
            await db.update_pair_checked(
                primary_candidate.pair_address,
                now,
                primary_candidate.hot_score,
                last_seen_metrics,
            )

            if newly_eligible:
                await increment_counter(db, "eligible_count", 1)
                await increment_daily_counter(db, "matches", 1, now)
                logger.info(
                    "eligible_discovered",
                    extra={
                        "token": token_address,
                        "pair": primary_candidate.pair_address,
                        "metrics": primary_result.metrics.__dict__,
                    },
                )

            if not eligible:
                continue

            if muted:
                logger.info("alert_suppressed_muted", extra={"token": token_address})
                continue

            if not config.allowed_chat_ids:
                logger.warning("allowlist_empty_skip_post")
                continue

            already_alerted = token_row["last_alerted_at"]
            if already_alerted:
                continue

            first_seen_ts = self._first_seen_ts(primary_candidate.pair, token_row)
            trigger_reason = build_trigger_reason(config.filters)
            wallet_analysis = self._get_cached_wallet_analysis(
                token_row, now, config.wallet_analysis_ttl_sec
            )
            intent_data = self._get_cached_intent(token_row, now, config.wallet_analysis_ttl_sec)
            text = format_alert_message(
                primary_candidate.pair,
                token_address,
                primary_result.metrics,
                first_seen_ts,
                config.display_timezone,
                config.chain_id,
                trigger_reason,
                config.alert_tagline,
                wallet_analysis,
                config.wallet_analysis_label,
                intent_data,
            )

            if config.dry_run:
                print(text)
                await db.update_last_alerted(token_address, now)
                await increment_counter(db, "alerted_count", 1)
                await add_lag_sample(db, now - first_seen_ts, config.metrics_sample_size)
                continue

            posted_refs = await self._post_alert(text, primary_candidate.pair, token_address)
            if posted_refs:
                await db.update_last_alerted(token_address, now)
                await increment_counter(db, "alerted_count", 1)
                await add_lag_sample(db, now - first_seen_ts, config.metrics_sample_size)
                await self._maybe_schedule_wallet_analysis(
                    token_address=token_address,
                    pair=primary_candidate.pair,
                    metrics=primary_result.metrics,
                    first_seen_ts=first_seen_ts,
                    trigger_reason=trigger_reason,
                    posted_refs=posted_refs,
                    cached=wallet_analysis is not None,
                )
                await self._maybe_schedule_intent_analysis(
                    token_address=token_address,
                    pair=primary_candidate.pair,
                    metrics=primary_result.metrics,
                    first_seen_ts=first_seen_ts,
                    trigger_reason=trigger_reason,
                    posted_refs=posted_refs,
                    cached=intent_data is not None,
                )

    async def backfill_called_prices(self) -> None:
        if self._backfill_lock.locked():
            return
        async with self._backfill_lock:
            if await self.ctx.db.get_state_bool("performance_backfill_done", False):
                return
            self.ctx.logger.info("performance_backfill_start")
            batch_size = 500
            while True:
                rows = await self.ctx.db.get_tokens_missing_called_price(batch_size)
                if not rows:
                    break
                updated = 0
                for row in rows:
                    token_address = row["token_address"]
                    called_price = _snapshot_price(row["eligible_first_metrics"])
                    if called_price is None:
                        called_price = _snapshot_price(row["last_seen_metrics"])
                    if called_price is None:
                        continue
                    max_price = row["max_price_usd"] or called_price
                    last_price = _snapshot_price(row["last_seen_metrics"])
                    if last_price is not None and last_price > max_price:
                        max_price = last_price
                    max_market_cap = row["max_market_cap"]
                    snapshot_mcap = _snapshot_mcap(row["eligible_first_metrics"])
                    if max_market_cap is None:
                        max_market_cap = snapshot_mcap
                    await self.ctx.db.update_called_prices(
                        token_address=token_address,
                        called_price_usd=called_price,
                        max_price_usd=max_price,
                        max_market_cap=max_market_cap,
                    )
                    updated += 1
                if updated == 0:
                    self.ctx.logger.info("performance_backfill_no_progress")
                    break
                await asyncio.sleep(0)
            await self.ctx.db.set_state("performance_backfill_done", "true")
            self.ctx.logger.info("performance_backfill_done")

    async def refresh_performance_batch(self) -> None:
        now = utc_now_ts()
        min_first_at = now - PERFORMANCE_LOOKBACK_DAYS * 86400
        rows = await self.ctx.db.get_called_for_refresh(PERFORMANCE_BATCH_SIZE, min_first_at)
        if not rows:
            return
        for row in rows:
            token_address = row["token_address"]
            pair_address = _snapshot_pair_address(row["last_seen_metrics"])
            if not pair_address:
                pair_address = _snapshot_pair_address(row["eligible_first_metrics"])
            if not pair_address:
                continue
            payload = await self.ctx.dex.get_pair(self.ctx.config.chain_id, pair_address)
            pair = _extract_pair(payload)
            if not pair:
                continue
            metrics = extract_metrics(pair, self.ctx.config.use_fdv_as_mc_proxy)
            last_seen_metrics = _metrics_snapshot(pair, metrics)
            price_usd = _to_float(pair.get("priceUsd"))
            called_price_usd = row["called_price_usd"]
            max_price_usd = row["max_price_usd"]
            if price_usd is not None:
                if max_price_usd is None or price_usd > max_price_usd:
                    max_price_usd = price_usd
            max_market_cap = row["max_market_cap"]
            if metrics.market_cap_value is not None:
                if max_market_cap is None or metrics.market_cap_value > max_market_cap:
                    max_market_cap = metrics.market_cap_value

            hit_2x_at = row["hit_2x_at"]
            hit_3x_at = row["hit_3x_at"]
            hit_5x_at = row["hit_5x_at"]
            if called_price_usd and price_usd and called_price_usd > 0:
                multiple = price_usd / called_price_usd
                if hit_2x_at is None and multiple >= 2.0:
                    hit_2x_at = now
                if hit_3x_at is None and multiple >= 3.0:
                    hit_3x_at = now
                if hit_5x_at is None and multiple >= 5.0:
                    hit_5x_at = now

            await self.ctx.db.update_performance_snapshot(
                token_address=token_address,
                last_seen_metrics=last_seen_metrics,
                last_checked_at=now,
                max_price_usd=max_price_usd,
                max_market_cap=max_market_cap,
                hit_2x_at=hit_2x_at,
                hit_3x_at=hit_3x_at,
                hit_5x_at=hit_5x_at,
            )

    def _dedup_candidates(self, candidates: List[PairCandidate], max_count: int) -> List[PairCandidate]:
        dedup: Dict[str, PairCandidate] = {}
        for candidate in sorted(candidates, key=lambda item: item.hot_score, reverse=True):
            if candidate.pair_address.lower() in dedup:
                continue
            dedup[candidate.pair_address.lower()] = candidate
            if len(dedup) >= max_count:
                break
        return list(dedup.values())

    def _group_pairs_by_token(self, candidates: List[PairCandidate]) -> Dict[str, List[PairCandidate]]:
        grouped: Dict[str, List[PairCandidate]] = {}
        for candidate in candidates:
            key = candidate.token_address.lower()
            grouped.setdefault(key, []).append(candidate)
        return grouped

    def _first_seen_ts(self, pair: Dict[str, Any], token_row) -> int:
        pair_created_at = _coerce_ts(pair.get("pairCreatedAt"))
        first_seen = token_row["first_seen"]
        return pair_created_at or first_seen

    def _get_cached_wallet_analysis(
        self, token_row, now: int, ttl_sec: int
    ) -> Optional[Dict[str, Any]]:
        analysis_at = token_row["wallet_analysis_at"]
        analysis_json = token_row["wallet_analysis_json"]
        if not analysis_at or not analysis_json:
            return None
        if now - analysis_at > ttl_sec:
            return None
        data = _parse_wallet_analysis(analysis_json)
        if not data:
            return None
        if "partial" not in data and token_row["wallet_analysis_partial"] is not None:
            data["partial"] = bool(token_row["wallet_analysis_partial"])
        return data

    def _get_cached_intent(
        self, token_row, now: int, ttl_sec: int
    ) -> Optional[Dict[str, Any]]:
        intent_at = token_row["intent_at"]
        intent_json = token_row["intent_json"]
        if not intent_at or not intent_json:
            return None
        if now - intent_at > ttl_sec:
            return None
        data = _parse_wallet_analysis(intent_json)
        return data if data else None

    async def _maybe_schedule_wallet_analysis(
        self,
        token_address: str,
        pair: Dict[str, Any],
        metrics,
        first_seen_ts: int,
        trigger_reason: str,
        posted_refs: List[AlertMessageRef],
        cached: bool,
    ) -> None:
        config = self.ctx.config
        if cached or not config.wallet_analysis_enabled:
            return
        if self.ctx.wallet_analyzer is None:
            return
        pair_address = pair.get("pairAddress")
        if not pair_address:
            return
        async with self._analysis_lock:
            if token_address in self._analysis_inflight:
                return
            self._analysis_inflight.add(token_address)
        asyncio.create_task(
            self._run_wallet_analysis(
                token_address=token_address,
                pair=pair,
                metrics=metrics,
                first_seen_ts=first_seen_ts,
                trigger_reason=trigger_reason,
                posted_refs=posted_refs,
            )
        )

    async def _run_wallet_analysis(
        self,
        token_address: str,
        pair: Dict[str, Any],
        metrics,
        first_seen_ts: int,
        trigger_reason: str,
        posted_refs: List[AlertMessageRef],
    ) -> None:
        try:
            await self.ctx.db.increment_state_int("metrics_wallet_runs", 1)
            async with self._analysis_sem:
                analyzer = self.ctx.wallet_analyzer
                if analyzer is None:
                    return
                pair_address = pair.get("pairAddress")
                if not pair_address:
                    return
                result = await analyzer.analyze(pair_address, token_address)
            if result is None:
                await self.ctx.db.increment_state_int("metrics_wallet_no_data", 1)
                return
            now = utc_now_ts()
            analysis_json = result.to_json()
            await self.ctx.db.update_wallet_analysis(
                token_address=token_address,
                analysis_json=analysis_json,
                analysis_at=now,
                partial=result.partial,
            )
            await self.ctx.db.increment_state_int("metrics_wallet_success", 1)
            await self.ctx.db.set_state("wallet_analysis_last_at", str(now))
            await self.ctx.db.set_state("wallet_analysis_last_token", token_address)
            analysis_data = result.to_dict()
            token_row = await self.ctx.db.get_token(token_address)
            intent_data = None
            if token_row is not None:
                intent_data = self._get_cached_intent(
                    token_row, now, self.ctx.config.wallet_analysis_ttl_sec
                )
            updated_text = format_alert_message(
                pair,
                token_address,
                metrics,
                first_seen_ts,
                self.ctx.config.display_timezone,
                self.ctx.config.chain_id,
                trigger_reason,
                self.ctx.config.alert_tagline,
                analysis_data,
                self.ctx.config.wallet_analysis_label,
                intent_data,
            )
            await self._edit_alerts(
                posted_refs,
                updated_text,
                pair,
                token_address,
                followup=self._post_wallet_analysis_followup,
                followup_payload=analysis_data,
            )
            self.ctx.logger.info(
                "wallet_analysis_ready",
                extra={"token": token_address, "buyers": result.unique_buyers},
            )
        except Exception:
            try:
                await self.ctx.db.increment_state_int("metrics_wallet_fail", 1)
                await self.ctx.db.set_state("wallet_analysis_last_error", "wallet_analysis_failed")
            except Exception:
                self.ctx.logger.exception("wallet_analysis_metrics_failed")
            self.ctx.logger.exception("wallet_analysis_failed", extra={"token": token_address})
        finally:
            async with self._analysis_lock:
                self._analysis_inflight.discard(token_address)

    async def _maybe_schedule_intent_analysis(
        self,
        token_address: str,
        pair: Dict[str, Any],
        metrics,
        first_seen_ts: int,
        trigger_reason: str,
        posted_refs: List[AlertMessageRef],
        cached: bool,
    ) -> None:
        if cached:
            return
        intent_analyzer = self.ctx.intent_analyzer
        if intent_analyzer is None or not intent_analyzer.enabled:
            return
        pair_address = pair.get("pairAddress")
        if not pair_address:
            return
        async with self._intent_lock:
            if token_address in self._intent_inflight:
                return
            self._intent_inflight.add(token_address)
        asyncio.create_task(
            self._run_intent_analysis(
                token_address=token_address,
                pair=pair,
                metrics=metrics,
                first_seen_ts=first_seen_ts,
                trigger_reason=trigger_reason,
                posted_refs=posted_refs,
            )
        )

    async def _run_intent_analysis(
        self,
        token_address: str,
        pair: Dict[str, Any],
        metrics,
        first_seen_ts: int,
        trigger_reason: str,
        posted_refs: List[AlertMessageRef],
    ) -> None:
        try:
            async with self._analysis_sem:
                analyzer = self.ctx.intent_analyzer
                if analyzer is None:
                    return
                pair_address = pair.get("pairAddress")
                if not pair_address:
                    return
                result = await analyzer.analyze(pair_address, token_address)
            if result is None:
                return
            now = utc_now_ts()
            await self.ctx.db.update_intent_analysis(
                token_address=token_address,
                score=result.score,
                label=result.label,
                intent_json=result.to_json(),
                intent_at=now,
            )
            token_row = await self.ctx.db.get_token(token_address)
            wallet_analysis = None
            if token_row is not None:
                wallet_analysis = self._get_cached_wallet_analysis(
                    token_row, now, self.ctx.config.wallet_analysis_ttl_sec
                )
            updated_text = format_alert_message(
                pair,
                token_address,
                metrics,
                first_seen_ts,
                self.ctx.config.display_timezone,
                self.ctx.config.chain_id,
                trigger_reason,
                self.ctx.config.alert_tagline,
                wallet_analysis,
                self.ctx.config.wallet_analysis_label,
                result.to_dict(),
            )
            await self._edit_alerts(
                posted_refs,
                updated_text,
                pair,
                token_address,
                followup=self._post_intent_followup,
                followup_payload=result.to_dict(),
            )
            self.ctx.logger.info(
                "intent_analysis_ready",
                extra={
                    "token": token_address,
                    "score": result.score,
                    "label": result.label,
                    "sample_swaps": result.sample_swaps,
                    "partial": result.partial,
                },
            )
        except Exception:
            self.ctx.logger.exception("intent_analysis_failed", extra={"token": token_address})
        finally:
            async with self._intent_lock:
                self._intent_inflight.discard(token_address)

    async def _edit_alerts(
        self,
        refs: List[AlertMessageRef],
        text: str,
        pair: Dict[str, Any],
        token_address: str,
        followup=None,
        followup_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        keyboard = build_alert_keyboard(pair, token_address, self.ctx.config.chain_id)
        for ref in refs:
            try:
                kwargs = {}
                if ref.thread_id is not None:
                    kwargs["message_thread_id"] = ref.thread_id
                await self.bot.edit_message_text(
                    chat_id=ref.chat_id,
                    message_id=ref.message_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=keyboard,
                    **kwargs,
                )
            except Exception:
                self.ctx.logger.exception(
                    "alert_edit_failed",
                    extra={"chat_id": ref.chat_id, "thread_id": ref.thread_id, "token": token_address},
                )
                if followup is not None:
                    await followup(
                        ref.chat_id,
                        ref.thread_id,
                        pair,
                        token_address,
                        followup_payload or {},
                    )

    async def _post_wallet_analysis_followup(
        self,
        chat_id: int,
        thread_id: Optional[int],
        pair: Dict[str, Any],
        token_address: str,
        analysis_data: Dict[str, Any],
    ) -> None:
        keyboard = build_alert_keyboard(pair, token_address, self.ctx.config.chain_id)
        text = format_wallet_analysis_update(
            pair,
            token_address,
            analysis_data,
            self.ctx.config.wallet_analysis_label,
            self.ctx.config.display_timezone,
            self.ctx.config.chain_id,
        )
        try:
            kwargs = {}
            if thread_id is not None:
                kwargs["message_thread_id"] = thread_id
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
                **kwargs,
            )
        except Exception:
            self.ctx.logger.exception(
                "wallet_analysis_send_failed",
                extra={"chat_id": chat_id, "thread_id": thread_id, "token": token_address},
            )

    async def _post_intent_followup(
        self,
        chat_id: int,
        thread_id: Optional[int],
        pair: Dict[str, Any],
        token_address: str,
        intent_data: Dict[str, Any],
    ) -> None:
        keyboard = build_alert_keyboard(pair, token_address, self.ctx.config.chain_id)
        text = format_intent_update(
            pair,
            token_address,
            intent_data,
            self.ctx.config.display_timezone,
            self.ctx.config.chain_id,
        )
        try:
            kwargs = {}
            if thread_id is not None:
                kwargs["message_thread_id"] = thread_id
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
                **kwargs,
            )
        except Exception:
            self.ctx.logger.exception(
                "intent_send_failed",
                extra={"chat_id": chat_id, "thread_id": thread_id, "token": token_address},
            )

    async def _post_alert(
        self, text: str, pair: Dict[str, Any], token_address: str
    ) -> List[AlertMessageRef]:
        config = self.ctx.config
        keyboard = build_alert_keyboard(pair, token_address, config.chain_id)
        posted_refs: List[AlertMessageRef] = []
        for chat_id in config.allowed_chat_ids:
            thread_ids = [None]
            if config.allowed_thread_ids and chat_id < 0:
                thread_ids = list(config.allowed_thread_ids)
            for thread_id in thread_ids:
                try:
                    kwargs = {}
                    if thread_id is not None:
                        kwargs["message_thread_id"] = thread_id
                    message = await self.bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=keyboard,
                        **kwargs,
                    )
                    posted_refs.append(AlertMessageRef(chat_id, message.message_id, thread_id))
                except Exception:
                    self.ctx.logger.exception(
                        "alert_send_failed",
                        extra={"chat_id": chat_id, "thread_id": thread_id, "token": token_address},
                    )
        return posted_refs
