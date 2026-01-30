from __future__ import annotations

import asyncio
import csv
import heapq
import io
import json
import statistics
from types import SimpleNamespace
from typing import Any, Dict, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
)

from .types import AppContext, FilterMetrics
from .utils import (
    escape_html,
    format_duration,
    format_pct,
    format_ts,
    format_ts_bold_if_past,
    format_usd,
    parse_duration,
    utc_now_ts,
)

WELCOME_HEADER = "+----------------------------+\n| Freedom Trench Bot         |\n| Solana Alerts              |\n+----------------------------+"
ALERT_HEADER = "+----------------------------+\n| Freedom Trench Bot         |\n| APED 🚀                    |\n+----------------------------+"
SELL_HEADER = "+----------------------------+\n| Freedom Trench Bot         |\n| SOLD ✅                    |\n+----------------------------+"

STARTUP_FRAMES = [
    "> initializing...",
    "> initializing...\n> loading solana modules...",
    "> initializing...\n> loading solana modules...\n> applying eligibility filters...",
    "> initializing...\n> loading solana modules...\n> applying eligibility filters...\n> starting market scanner...",
]

STARTUP_FINAL_FRAME = (
    "███████╗██████╗ ███████╗███████╗██████╗  ██████╗ ███╗   ███╗\n"
    "██╔════╝██╔══██╗██╔════╝██╔════╝██╔══██╗██╔═══██╗████╗ ████║\n"
    "█████╗  ██████╔╝█████╗  █████╗  ██║  ██║██║   ██║██╔████╔██║\n"
    "██╔══╝  ██╔══██╗██╔══╝  ██╔══╝  ██║  ██║██║   ██║██║╚██╔╝██║\n"
    "██║     ██║  ██║███████╗███████╗██████╔╝╚██████╔╝██║ ╚═╝ ██║\n"
    "╚═╝     ╚═╝  ╚═╝╚══════╝╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚═╝\n"
    "\n"
    "                Freedom Trench Bot\n"
    "────────────────────────────────────────\n"
    "Solana Eligibility Scanner • LIVE"
)

PERFORMANCE_LOOKBACK_DAYS = 7
PERFORMANCE_SUMMARY_LIMIT = 5000
PERFORMANCE_TOP_N = 5
PERFORMANCE_EXPORT_LIMIT = 50000

HELP_TEXT = (
    "/start - onboarding and status\n"
    "/status - monitoring status and filters\n"
    "/stats - account summary (since reset)\n"
    "/performance - simulation summary (since reset by default)\n"
    "/archive - archive summary before reset (all-time)\n"
    "/moonbag - list moonbag holdings (admin only)\n"
    "/filters - current filters\n"
    "/health - health summary (admin only)\n"
    "/pause - pause monitoring (admin only)\n"
    "/resume - resume monitoring (admin only)\n"
    "/mute <duration> - mute alerts, ex: 1h or 30m (admin only)\n"
    "/reset - reset simulation baseline (admin only)\n"
    "/help - this help"
)


def get_app_ctx(context: ContextTypes.DEFAULT_TYPE) -> Optional[AppContext]:
    return context.application.bot_data.get("app_ctx")


def build_dex_url(pair: dict, chain_id: str) -> str:
    url = pair.get("url") if isinstance(pair, dict) else None
    if url:
        return url
    pair_address = pair.get("pairAddress") if isinstance(pair, dict) else None
    if pair_address:
        return f"https://dexscreener.com/{chain_id}/{pair_address}"
    return f"https://dexscreener.com/{chain_id}"


def build_alert_keyboard(pair: dict, token_address: str, chain_id: str) -> InlineKeyboardMarkup:
    dex_url = build_dex_url(pair, chain_id)
    solscan_url = f"https://solscan.io/token/{token_address}"
    buttons = [
        [
            InlineKeyboardButton("Open Dexscreener", url=dex_url),
            InlineKeyboardButton("Solscan", url=solscan_url),
        ],
        [
            InlineKeyboardButton("Mute 1h", callback_data="mute:1h"),
            InlineKeyboardButton("Settings", callback_data="settings"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def build_status_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("Settings", callback_data="settings"),
        ]
    ]
    return InlineKeyboardMarkup(buttons)


async def get_sim_settings(ctx: AppContext):
    return SimpleNamespace(
        sim_start_balance=await ctx.db.get_state_float(
            "sim_start_balance", ctx.config.sim_start_balance
        ),
        sim_position_size=await ctx.db.get_state_float(
            "sim_position_size", ctx.config.sim_position_size
        ),
        sim_target_multiple=ctx.config.sim_target_multiple,
        sim_buy_fee_pct=ctx.config.sim_buy_fee_pct,
        sim_sell_fee_pct=ctx.config.sim_sell_fee_pct,
        sim_slippage_sample_sec=ctx.config.sim_slippage_sample_sec,
    )


def build_trigger_reason(filters) -> str:
    return (
        f"Trigger: MC<= {format_usd(filters.max_market_cap)}, "
        f"Vol1h>= {format_usd(filters.min_volume_1h)}, "
        f"Change1h/6h/24h>= {filters.min_change_1h:.2f}%/"
        f"{filters.min_change_6h:.2f}%/{filters.min_change_24h:.2f}%"
    )


def _parse_metrics_snapshot(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


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


def _format_sol(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f} SOL"


def _format_ratio(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_wallet_analysis_block(
    analysis: Dict[str, Any], label: str, tz_name: str
) -> list[str]:
    if not analysis:
        return []
    sample_size = int(analysis.get("sample_size") or 0)
    unique_buyers = int(analysis.get("unique_buyers") or 0)
    fresh_wallets = int(analysis.get("fresh_wallets") or 0)
    fresh_ratio = _to_float(analysis.get("fresh_ratio"))
    avg_sol = _to_float(analysis.get("avg_sol"))
    median_sol = _to_float(analysis.get("median_sol"))
    min_sol = _to_float(analysis.get("min_sol"))
    max_sol = _to_float(analysis.get("max_sol"))
    earliest_buy_ts = _to_int(analysis.get("earliest_buy_ts"))
    partial = bool(analysis.get("partial"))

    lines = [
        escape_html(label),
        f"First buyers: {unique_buyers}/{sample_size}",
        f"Fresh wallets: {fresh_wallets} ({_format_ratio(fresh_ratio)})",
        f"Avg SOL: {_format_sol(avg_sol)} | Median SOL: {_format_sol(median_sol)}",
        f"SOL range: {_format_sol(min_sol)} - {_format_sol(max_sol)}",
    ]
    if earliest_buy_ts:
        lines.append(f"Earliest buy: {format_ts(earliest_buy_ts, tz_name)}")
    if partial:
        lines.append("Analysis: partial (history cap)")
    return lines


def _format_mcap_from_snapshot(snapshot: Dict[str, Any]) -> str:
    value = _to_float(snapshot.get("marketCap"))
    label = snapshot.get("marketCapLabel") or "Market Cap"
    suffix = ""
    if label != "Market Cap":
        suffix = f" ({escape_html(str(label))})"
    return f"{format_usd(value)}{suffix}"


def _snapshot_holder_count(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    value = data.get("holderCount")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.8f}".rstrip("0").rstrip(".")


def format_called_stats(rows, tz_name: str, retention_sec: int, limit: int, sim_settings) -> str:
    hours = max(1, int(retention_sec / 3600))
    header = f"<pre>{WELCOME_HEADER}</pre>"
    if not rows:
        return f"{header}\nCalled last {hours}h: 0\nNo calls in the last {hours}h."

    lines = [header, f"Called last {hours}h: {len(rows)} (showing up to {limit})"]
    for idx, row in enumerate(rows, start=1):
        token_address = row["token_address"]
        name = escape_html(row["last_name"] or "Unknown")
        symbol = escape_html(row["last_symbol"] or "?")
        called_ts = row["eligible_first_at"]
        sim = _compute_sim_row(row, sim_settings)
        entry_price = sim["entry_price"]
        current_price = sim["current_price"]
        max_multiple = sim["max_multiple"]
        min_multiple = sim["min_multiple"]
        recouped = sim["recouped"]
        recouped_at = sim["recouped_at"]
        slippage_pct = sim["slippage_pct"]
        above_target_total_sec = sim["above_target_total_sec"]
        sim_taken = bool(row["sim_taken"]) if row["sim_taken"] is not None else False
        sim_position = row["sim_position_usd"]
        sim_ape_pct = row["sim_ape_pct"]
        sim_cash_before = row["sim_cash_before"]
        sim_cash_after = row["sim_cash_after"]

        lines.append(f"{idx}. {name} ({symbol})")
        lines.append(f"CA: <code>{escape_html(token_address)}</code>")
        lines.append(f"Called: {format_ts_bold_if_past(called_ts, tz_name)}")
        lines.append(
            f"Entry: {_format_price(entry_price)} | Now: {_format_price(current_price)}"
        )
        if sim_taken:
            ape_line = f"APED: {_format_usd2(sim_position)}"
            if sim_ape_pct is not None:
                ape_line += f" ({sim_ape_pct:.2f}% of cash)"
            if sim_cash_before is not None and sim_cash_after is not None:
                ape_line += f" | cash { _format_usd2(sim_cash_before)} → {_format_usd2(sim_cash_after)}"
            lines.append(ape_line)
        else:
            lines.append("APED: no (insufficient cash)")
        if max_multiple is not None:
            lines.append(f"Max multiple: {_format_multiple(max_multiple)}")
        if min_multiple is not None:
            drawdown_pct = (min_multiple - 1.0) * 100.0
            lines.append(f"Min multiple: {_format_multiple(min_multiple)} ({format_pct(drawdown_pct)})")
        if recouped:
            recoup_line = "Recoup: yes"
            if recouped_at:
                recoup_line += f" at {format_ts(recouped_at, tz_name)}"
            lines.append(recoup_line)
        else:
            lines.append("Recoup: no")
        if slippage_pct is not None:
            lines.append(f"Slippage (post-alert): {format_pct(slippage_pct)}")
        if above_target_total_sec:
            lines.append(
                f"Time above target: {format_duration(int(above_target_total_sec))}"
            )
        lines.append("")
    return "\n".join(lines).strip()


async def send_called_stats_message(message, ctx: AppContext) -> None:
    if message is None:
        return
    now = utc_now_ts()
    rows = await ctx.db.get_called_since(
        ctx.config.called_list_limit, now - ctx.config.eligible_retention_sec
    )
    sim_settings = await get_sim_settings(ctx)
    text = format_called_stats(
        rows,
        ctx.config.display_timezone,
        ctx.config.eligible_retention_sec,
        ctx.config.called_list_limit,
        sim_settings,
    )
    await message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def send_startup_animation(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ctx: AppContext,
    frame_delay: float = 0.3,
) -> None:
    chat = update.effective_chat
    if chat is None:
        return


async def send_startup_animation_to_chat(
    bot,
    chat_id: int,
    frame_delay: float = 0.3,
) -> None:
    def wrap_pre(text: str) -> str:
        return f"<pre>{escape_html(text)}</pre>"

    try:
        message = await bot.send_message(
            chat_id=chat_id, text=wrap_pre(STARTUP_FRAMES[0]), parse_mode=ParseMode.HTML
        )
    except Exception:
        return

    try:
        for frame in STARTUP_FRAMES[1:]:
            await asyncio.sleep(frame_delay)
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message.message_id,
                text=wrap_pre(frame),
                parse_mode=ParseMode.HTML,
            )
        await asyncio.sleep(frame_delay)
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message.message_id,
            text=wrap_pre(STARTUP_FINAL_FRAME),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=wrap_pre(STARTUP_FINAL_FRAME),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            return

    def wrap_pre(text: str) -> str:
        return f"<pre>{escape_html(text)}</pre>"

    try:
        message = await context.bot.send_message(
            chat_id=chat.id, text=wrap_pre(STARTUP_FRAMES[0]), parse_mode=ParseMode.HTML
        )
    except Exception:
        ctx.logger.exception("startup_animation_send_failed", extra={"chat_id": chat.id})
        return

    try:
        for frame in STARTUP_FRAMES[1:]:
            await asyncio.sleep(frame_delay)
            await context.bot.edit_message_text(
                chat_id=chat.id,
                message_id=message.message_id,
                text=wrap_pre(frame),
                parse_mode=ParseMode.HTML,
            )
        await asyncio.sleep(frame_delay)
        await context.bot.edit_message_text(
            chat_id=chat.id,
            message_id=message.message_id,
            text=wrap_pre(STARTUP_FINAL_FRAME),
            parse_mode=ParseMode.HTML,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        ctx.logger.warning("startup_animation_edit_failed", extra={"chat_id": chat.id})
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=wrap_pre(STARTUP_FINAL_FRAME),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            ctx.logger.exception("startup_animation_fallback_failed", extra={"chat_id": chat.id})
        return


def format_alert_message(
    pair: dict,
    token_address: str,
    metrics: FilterMetrics,
    first_seen_ts: int,
    tz_name: str,
    chain_id: str,
    trigger_reason: str,
    tagline: str,
    ape_amount_usd: Optional[float],
    ape_pct: Optional[float],
    cash_balance: Optional[float],
    entry_price: Optional[float],
    sim_taken: bool,
    wallet_analysis: Optional[Dict[str, Any]] = None,
    wallet_label: str = "",
) -> str:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    token_address_lc = token_address.lower()
    token_obj = base
    if isinstance(base, dict) and base.get("address") and base["address"].lower() == token_address_lc:
        token_obj = base
    elif (
        isinstance(quote, dict)
        and quote.get("address")
        and quote["address"].lower() == token_address_lc
    ):
        token_obj = quote
    name = escape_html(token_obj.get("name") or "Unknown")
    symbol = escape_html(token_obj.get("symbol") or "?")

    mcap_suffix = ""
    if metrics.market_cap_label != "Market Cap":
        mcap_suffix = f" ({escape_html(metrics.market_cap_label)})"

    header_block = f"<pre>{ALERT_HEADER}</pre>"
    ca_block = f"<pre>{escape_html(token_address)}</pre>"

    lines = [
        header_block,
        escape_html(tagline),
    ]
    lines.extend(
        [
            f"Token: {name} ({symbol})",
            "Chain: Solana",
            "CA:",
            ca_block,
            f"MCap (aped): {format_usd(metrics.market_cap_value)}{mcap_suffix}",
        ]
    )
    if entry_price is not None:
        lines.append(f"Ape price: {_format_price(entry_price)}")
    if sim_taken:
        if ape_amount_usd is not None:
            ape_line = f"APED: {_format_usd2(ape_amount_usd)}"
            if ape_pct is not None:
                ape_line += f" ({ape_pct:.2f}% of cash)"
            lines.append(ape_line)
        if cash_balance is not None:
            lines.append(f"Account balance (cash): {_format_usd2(cash_balance)}")
    else:
        lines.append("APED: no (insufficient cash)")
    if wallet_analysis:
        label = wallet_label or "Top Wallet Call"
        lines.extend(format_wallet_analysis_block(wallet_analysis, label, tz_name))
        lines.extend(
            [
                f"First seen: {format_ts_bold_if_past(first_seen_ts, tz_name)}",
                f"Dexscreener: <a href=\"{build_dex_url(pair, chain_id)}\">link</a>",
                f"Solscan: <a href=\"https://solscan.io/token/{token_address}\">link</a>",
            ]
        )
    return "\n".join(lines)


def format_sell_message(
    pair: dict,
    token_address: str,
    metrics: FilterMetrics,
    sold_price: Optional[float],
    tz_name: str,
    sold_at: int,
    cash_balance: Optional[float],
) -> str:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    token_address_lc = token_address.lower()
    token_obj = base
    if isinstance(base, dict) and base.get("address") and base["address"].lower() == token_address_lc:
        token_obj = base
    elif (
        isinstance(quote, dict)
        and quote.get("address")
        and quote["address"].lower() == token_address_lc
    ):
        token_obj = quote
    name = escape_html(token_obj.get("name") or "Unknown")
    symbol = escape_html(token_obj.get("symbol") or "?")
    mcap_suffix = ""
    if metrics.market_cap_label != "Market Cap":
        mcap_suffix = f" ({escape_html(metrics.market_cap_label)})"

    header_block = f"<pre>{SELL_HEADER}</pre>"
    ca_block = f"<pre>{escape_html(token_address)}</pre>"

    lines = [
        header_block,
        "Recouped initial position",
        f"Token: {name} ({symbol})",
        "Chain: Solana",
        "CA:",
        ca_block,
        f"Sold price: {_format_price(sold_price)}",
        f"MCap (sold): {format_usd(metrics.market_cap_value)}{mcap_suffix}",
        f"Sold at: {format_ts(sold_at, tz_name)}",
    ]
    if cash_balance is not None:
        lines.append(f"Account balance (cash): {_format_usd2(cash_balance)}")
    return "\n".join(lines)


def format_wallet_analysis_update(
    pair: dict,
    token_address: str,
    analysis: Dict[str, Any],
    label: str,
    tz_name: str,
    chain_id: str,
) -> str:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    token_address_lc = token_address.lower()
    token_obj = base
    if isinstance(base, dict) and base.get("address") and base["address"].lower() == token_address_lc:
        token_obj = base
    elif (
        isinstance(quote, dict)
        and quote.get("address")
        and quote["address"].lower() == token_address_lc
    ):
        token_obj = quote
    name = escape_html(token_obj.get("name") or "Unknown")
    symbol = escape_html(token_obj.get("symbol") or "?")

    header_block = f"<pre>{WELCOME_HEADER}</pre>"
    ca_block = f"<pre>{escape_html(token_address)}</pre>"
    lines = [
        header_block,
        "Wallet analysis update",
        f"Token: {name} ({symbol})",
        "CA:",
        ca_block,
    ]
    lines.extend(format_wallet_analysis_block(analysis, label or "Top Wallet Call", tz_name))
    lines.extend(
        [
            f"Dexscreener: <a href=\"{build_dex_url(pair, chain_id)}\">link</a>",
            f"Solscan: <a href=\"https://solscan.io/token/{token_address}\">link</a>",
        ]
    )
    return "\n".join(lines)


def _format_multiple(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}x"


def _snapshot_price(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    return _to_float(data.get("priceUsd"))


def _format_usd2(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.2f}"


def _entry_price_from_row(row) -> Optional[float]:
    called_price = row["called_price_usd"]
    if called_price and called_price > 0:
        return called_price
    fallback = _snapshot_price(row["eligible_first_metrics"])
    if fallback and fallback > 0:
        return fallback
    last_price = _snapshot_price(row["last_seen_metrics"])
    return last_price if last_price and last_price > 0 else None


def _compute_sim_row(row, config) -> Dict[str, Any]:
    entry_price = _entry_price_from_row(row)
    current_price = _snapshot_price(row["last_seen_metrics"])
    if current_price is None:
        current_price = entry_price
    max_price = row["max_price_usd"] or current_price or entry_price
    min_price = row["min_price_usd"] or current_price or entry_price
    target_price = entry_price * config.sim_target_multiple if entry_price else None
    recouped_at = row["recouped_at"]
    recouped = False
    if target_price and max_price and max_price >= target_price:
        recouped = True
    if recouped_at:
        recouped = True
    post_alert_price = row["post_alert_price_usd"]
    post_alert_at = row["post_alert_at"]
    slippage_pct = None
    if entry_price and post_alert_price and entry_price > 0:
        slippage_pct = ((post_alert_price / entry_price) - 1.0) * 100.0
    above_target_total_sec = row["above_target_total_sec"] or 0

    buy_fee = max(0.0, config.sim_buy_fee_pct) / 100.0
    sell_fee = max(0.0, config.sim_sell_fee_pct) / 100.0
    position_size = config.sim_position_size

    tokens_bought = None
    tokens_sold = None
    moonbag_tokens = None
    recoup_possible: Optional[bool] = None
    if entry_price and entry_price > 0:
        tokens_bought = position_size / (entry_price * (1.0 + buy_fee))
        if target_price and target_price > 0 and sell_fee < 1.0:
            tokens_sold = position_size / (target_price * (1.0 - sell_fee))
            recoup_possible = tokens_bought > tokens_sold
            if recoup_possible:
                moonbag_tokens = tokens_bought - tokens_sold

    net_exit = 1.0 - sell_fee
    if net_exit < 0:
        net_exit = 0.0

    moonbag_tokens = row["moonbag_tokens"]
    moonbag_sold_at = row["moonbag_sold_at"]
    tokens_remaining = None
    if tokens_bought is not None:
        if recouped:
            if moonbag_sold_at:
                tokens_remaining = 0.0
            elif moonbag_tokens is not None:
                tokens_remaining = moonbag_tokens
            elif recoup_possible and moonbag_tokens is not None:
                tokens_remaining = moonbag_tokens
            else:
                tokens_remaining = 0.0
        else:
            tokens_remaining = tokens_bought

    current_value = None
    if tokens_remaining is not None and current_price is not None:
        current_value = tokens_remaining * current_price * net_exit

    def _multiple(price: Optional[float]) -> Optional[float]:
        if entry_price and price and entry_price > 0:
            return price / entry_price
        return None

    return {
        "entry_price": entry_price,
        "current_price": current_price,
        "max_price": max_price,
        "min_price": min_price,
        "current_multiple": _multiple(current_price),
        "max_multiple": _multiple(max_price),
        "min_multiple": _multiple(min_price),
        "target_price": target_price,
        "recouped": recouped,
        "recouped_at": recouped_at,
        "post_alert_price": post_alert_price,
        "post_alert_at": post_alert_at,
        "slippage_pct": slippage_pct,
        "above_target_total_sec": above_target_total_sec,
        "recoup_possible": recoup_possible,
        "tokens_bought": tokens_bought,
        "moonbag_tokens": moonbag_tokens,
        "moonbag_sold_at": moonbag_sold_at,
        "current_value": current_value,
    }


def format_performance_summary(
    rows,
    tz_name: str,
    window_label: str,
    total_calls: int,
    limit: int,
    sim_settings,
    reset_at: int,
) -> str:
    header = f"<pre>{WELCOME_HEADER}</pre>"
    shown = len(rows)
    if total_calls == 0:
        return f"{header}\nSimulation ({window_label}): 0\nNo calls yet."

    now = utc_now_ts()
    rows_sorted = sorted(rows, key=lambda item: item["eligible_first_at"] or 0)
    effective_reset = reset_at or 0

    cash = sim_settings.sim_start_balance
    position_size = sim_settings.sim_position_size
    recoup_heap: list[int] = []
    taken_flags: Dict[str, bool] = {}
    taken_count = 0
    skipped_count = 0
    recouped_cash_count = 0

    for row in rows_sorted:
        first_at = row["eligible_first_at"] or 0
        if effective_reset and first_at < effective_reset:
            continue
        while recoup_heap and recoup_heap[0] <= first_at:
            heapq.heappop(recoup_heap)
            cash += position_size
            recouped_cash_count += 1
        sim_taken_value = row["sim_taken"]
        if sim_taken_value is not None:
            taken = bool(sim_taken_value)
            taken_flags[row["token_address"]] = taken
            if taken:
                cash -= position_size
                taken_count += 1
                if row["recouped_at"]:
                    heapq.heappush(recoup_heap, row["recouped_at"])
            else:
                skipped_count += 1
        else:
            if cash >= position_size:
                taken_flags[row["token_address"]] = True
                cash -= position_size
                taken_count += 1
                if row["recouped_at"]:
                    heapq.heappush(recoup_heap, row["recouped_at"])
            else:
                taken_flags[row["token_address"]] = False
                skipped_count += 1

    while recoup_heap and recoup_heap[0] <= now:
        heapq.heappop(recoup_heap)
        cash += position_size
        recouped_cash_count += 1

    open_positions = max(0, taken_count - recouped_cash_count)

    tracked = 0
    recouped = 0
    recoup_possible_misses = 0
    multiples: list[float] = []
    min_multiples: list[float] = []
    winners: list[tuple[float, Any]] = []
    recoup_times: list[int] = []
    above_target_times: list[int] = []
    slippage_samples: list[float] = []
    moonbag_10x = 0
    moonbag_100x = 0
    moonbag_1000x = 0
    equity = cash

    for row in rows:
        if effective_reset and (row["eligible_first_at"] or 0) < effective_reset:
            continue
        if taken_flags.get(row["token_address"]) is False:
            continue
        sim = _compute_sim_row(row, sim_settings)
        entry_price = sim["entry_price"]
        if entry_price:
            tracked += 1
        max_multiple = sim["max_multiple"]
        if max_multiple is not None:
            multiples.append(max_multiple)
            winners.append((max_multiple, row))
        min_multiple = sim["min_multiple"]
        if min_multiple is not None:
            min_multiples.append(min_multiple)
        if sim["recouped"]:
            recouped += 1
            if row["eligible_first_at"] and row["recouped_at"]:
                recoup_times.append(row["recouped_at"] - row["eligible_first_at"])
            if max_multiple is not None:
                if max_multiple >= 10.0:
                    moonbag_10x += 1
                if max_multiple >= 100.0:
                    moonbag_100x += 1
                if max_multiple >= 1000.0:
                    moonbag_1000x += 1
        else:
            if sim["recoup_possible"] is False:
                recoup_possible_misses += 1
        if sim["above_target_total_sec"]:
            above_target_times.append(int(sim["above_target_total_sec"]))
        if sim["slippage_pct"] is not None:
            slippage_samples.append(sim["slippage_pct"])

        if taken_flags.get(row["token_address"]):
            current_value = sim["current_value"]
            if current_value is not None:
                equity += current_value

    lines = [
        header,
        f"Simulation ({window_label})",
        f"Signals: {total_calls}, tracked: {tracked}",
        f"Sim start: {_format_usd2(sim_settings.sim_start_balance)} | position: {_format_usd2(position_size)}",
        f"Target: {sim_settings.sim_target_multiple:.2f}x | fees: buy {sim_settings.sim_buy_fee_pct:.2f}% / sell {sim_settings.sim_sell_fee_pct:.2f}%",
        f"Slippage sample: {sim_settings.sim_slippage_sample_sec}s post-alert",
        f"Taken: {taken_count}, skipped: {skipped_count}, open: {open_positions}",
        f"Recouped: {recouped} ({_format_ratio(recouped / tracked) if tracked else 'n/a'})",
        f"Cash: {_format_usd2(cash)} | Equity: {_format_usd2(equity)}",
    ]
    if effective_reset:
        lines.append(f"Reset: {format_ts(effective_reset, tz_name)}")

    if total_calls > shown:
        lines.append(f"Showing: {shown} most recent (sample)")
    if tracked > 0:
        median_multiple = statistics.median(multiples) if multiples else None
        median_min = statistics.median(min_multiples) if min_multiples else None
        median_recoup = statistics.median(recoup_times) if recoup_times else None
        median_above = statistics.median(above_target_times) if above_target_times else None
        median_slip = statistics.median(slippage_samples) if slippage_samples else None
        if median_multiple is not None:
            lines.append(f"Median max multiple: {_format_multiple(median_multiple)}")
        if median_min is not None:
            lines.append(f"Median min multiple: {_format_multiple(median_min)}")
        if median_recoup is not None:
            lines.append(f"Median time to recoup: {format_duration(int(median_recoup))}")
        if median_above is not None:
            lines.append(f"Median time above target: {format_duration(int(median_above))}")
        if median_slip is not None:
            lines.append(f"Median slippage (post-alert): {format_pct(median_slip)}")
        lines.append(
            f"Moonbags: 10x {moonbag_10x} | 100x {moonbag_100x} | 1000x {moonbag_1000x}"
        )
        if recoup_possible_misses:
            lines.append(f"Target unreachable (fees too high): {recoup_possible_misses}")
        winners.sort(key=lambda item: item[0], reverse=True)
        lines.append(f"Top {min(PERFORMANCE_TOP_N, len(winners))} winners:")
        for idx, (multiple, row) in enumerate(winners[:PERFORMANCE_TOP_N], start=1):
            name = escape_html(row["last_name"] or "Unknown")
            symbol = escape_html(row["last_symbol"] or "?")
            called_at = row["eligible_first_at"]
            recouped_at = row["recouped_at"]
            time_to_recoup = "n/a"
            if recouped_at and called_at:
                time_to_recoup = format_duration(recouped_at - called_at)
            lines.append(
                f"{idx}. {name} ({symbol}) {multiple:.2f}x | recoup: {time_to_recoup}"
            )
        sample_count = min(10, len(rows))
        lines.append(f"Recent sample (last {sample_count}):")
        for idx, row in enumerate(rows[:sample_count], start=1):
            sim = _compute_sim_row(row, sim_settings)
            name = escape_html(row["last_name"] or "Unknown")
            symbol = escape_html(row["last_symbol"] or "?")
            status = "recouped" if sim["recouped"] else "open"
            taken = taken_flags.get(row["token_address"])
            if taken is False:
                status = f"{status} (skipped)"
            current_multiple = sim["current_multiple"]
            max_multiple = sim["max_multiple"]
            min_multiple = sim["min_multiple"]
            line = f"{idx}. {name} ({symbol}) {status}"
            details = []
            if current_multiple is not None:
                details.append(f"now {_format_multiple(current_multiple)}")
            if max_multiple is not None:
                details.append(f"max {_format_multiple(max_multiple)}")
            if min_multiple is not None:
                details.append(f"min {_format_multiple(min_multiple)}")
            if details:
                line += " | " + " | ".join(details)
            lines.append(line)
    else:
        lines.append("Tracked: n/a (waiting for price updates)")

    lines.append("Note: best-effort based on tracked updates.")
    return "\n".join(lines)


def format_archive_summary(
    rows,
    tz_name: str,
    window_label: str,
    total_calls: int,
    limit: int,
) -> str:
    header = f"<pre>{WELCOME_HEADER}</pre>"
    shown = len(rows)
    if total_calls == 0:
        return f"{header}\nArchive ({window_label}): 0\nNo archived calls."

    tracked = 0
    multiples: list[float] = []
    winners: list[tuple[float, Any]] = []
    for row in rows:
        called_price = row["called_price_usd"]
        max_price = row["max_price_usd"]
        if called_price and max_price and called_price > 0:
            tracked += 1
            multiple = max_price / called_price
            multiples.append(multiple)
            winners.append((multiple, row))

    lines = [header, f"Archive ({window_label})", f"Calls: {total_calls}, tracked: {tracked}"]
    if total_calls > shown:
        lines.append(f"Showing: {shown} most recent (sample)")
    if tracked > 0:
        lines.append(f"Median max multiple: {_format_multiple(statistics.median(multiples))}")
        winners.sort(key=lambda item: item[0], reverse=True)
        lines.append(f"Top {min(PERFORMANCE_TOP_N, len(winners))} winners:")
        for idx, (multiple, row) in enumerate(winners[:PERFORMANCE_TOP_N], start=1):
            name = escape_html(row["last_name"] or "Unknown")
            symbol = escape_html(row["last_symbol"] or "?")
            lines.append(f"{idx}. {name} ({symbol}) {multiple:.2f}x")
    else:
        lines.append("Tracked: n/a (waiting for price updates)")

    lines.append("Note: archive is pre-reset data only.")
    return "\n".join(lines)


def build_performance_csv(rows, tz_name: str, sim_settings) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "token_address",
            "name",
            "symbol",
            "called_at",
            "entry_price_usd",
            "current_price_usd",
            "max_price_usd",
            "min_price_usd",
            "current_multiple",
            "max_multiple",
            "min_multiple",
            "recouped_at",
            "post_alert_price_usd",
            "post_alert_at",
            "slippage_pct",
            "above_target_total_sec",
        ]
    )
    for row in rows:
        sim = _compute_sim_row(row, sim_settings)
        entry_price = sim["entry_price"]
        current_price = sim["current_price"]
        max_price = row["max_price_usd"]
        min_price = row["min_price_usd"]
        current_multiple = sim["current_multiple"]
        max_multiple = sim["max_multiple"]
        min_multiple = sim["min_multiple"]
        slippage_pct = sim["slippage_pct"]
        above_target_total_sec = sim["above_target_total_sec"]
        writer.writerow(
            [
                row["token_address"],
                row["last_name"] or "Unknown",
                row["last_symbol"] or "?",
                format_ts(row["eligible_first_at"], tz_name),
                entry_price if entry_price is not None else "",
                current_price if current_price is not None else "",
                max_price if max_price is not None else "",
                min_price if min_price is not None else "",
                f"{current_multiple:.2f}" if current_multiple is not None else "",
                f"{max_multiple:.2f}" if max_multiple is not None else "",
                f"{min_multiple:.2f}" if min_multiple is not None else "",
                format_ts(row["recouped_at"], tz_name),
                row["post_alert_price_usd"] if row["post_alert_price_usd"] is not None else "",
                format_ts(row["post_alert_at"], tz_name),
                f"{slippage_pct:.2f}" if slippage_pct is not None else "",
                above_target_total_sec if above_target_total_sec else "",
            ]
        )
    return output.getvalue().encode("utf-8")


def format_filters(ctx: AppContext) -> str:
    filters = ctx.config.filters
    lines = [
        f"Chain: {ctx.config.chain_id}",
        f"Market cap max: {format_usd(filters.max_market_cap)}",
        f"FDV proxy: {'on' if ctx.config.use_fdv_as_mc_proxy else 'off'}",
        f"Profile required: {'yes' if filters.require_profile else 'no'}",
        f"Change 24h min: {filters.min_change_24h:.2f}%",
        f"Change 6h min: {filters.min_change_6h:.2f}%",
        f"Change 1h min: {filters.min_change_1h:.2f}%",
        f"Volume 1h min: {format_usd(filters.min_volume_1h)}",
    ]
    return "\n".join(lines)


def format_account_stats(rows, tz_name: str, sim_settings, reset_at: int, sim_cash: float) -> str:
    header = f"<pre>{WELCOME_HEADER}</pre>"
    equity = sim_cash
    taken = 0
    recouped = 0
    for row in rows:
        if reset_at and (row["eligible_first_at"] or 0) < reset_at:
            continue
        if row["sim_taken"] is None or not bool(row["sim_taken"]):
            continue
        taken += 1
        sim = _compute_sim_row(row, sim_settings)
        if sim["recouped"]:
            recouped += 1
        current_value = sim["current_value"]
        if current_value is not None:
            equity += current_value
    roi = None
    if sim_settings.sim_start_balance:
        roi = ((equity / sim_settings.sim_start_balance) - 1.0) * 100.0
    lines = [
        header,
        "Account Summary",
        f"Sim start: {_format_usd2(sim_settings.sim_start_balance)}",
        f"Cash: {_format_usd2(sim_cash)}",
        f"Equity: {_format_usd2(equity)}",
        f"ROI: {format_pct(roi)}",
        f"Taken: {taken} | Recouped: {recouped}",
    ]
    if reset_at:
        lines.append(f"Reset: {format_ts(reset_at, tz_name)}")
    return "\n".join(lines)


def format_status(
    ctx: AppContext,
    paused: bool,
    mute_until: int,
    last_scan: int,
    candidates_per_min: float,
    pairs_per_min: float,
    scanned_pairs: int,
    unique_tokens_checked: int,
    eligible_count: int,
    alerted_count: int,
    matches_per_day: int,
    api_requests: int,
    rate_limited: int,
    median_lag_sec: int,
    sim_cash: float,
    sim_settings,
) -> str:
    now = utc_now_ts()
    mute_active = mute_until and mute_until > now
    mute_line = "Muted: no"
    if mute_active:
        mute_line = f"Muted: yes until {format_ts(mute_until, ctx.config.display_timezone)}"

    lines = [
        f"Monitoring: {'paused' if paused else 'running'}",
        mute_line,
        f"Dry run: {'on' if ctx.config.dry_run else 'off'}",
        f"Discovery mode: {ctx.config.discovery_mode}",
        f"Last scan: {format_ts(last_scan, ctx.config.display_timezone)}",
        f"Counts: scanned_pairs {scanned_pairs}, tokens_checked {unique_tokens_checked}, eligible {eligible_count}, alerted {alerted_count}",
        f"Matches/day: {matches_per_day}",
        f"Rates: candidates/min {candidates_per_min:.2f}, pairs_fetched/min {pairs_per_min:.2f}",
        f"API: requests {api_requests}, rate_limited {rate_limited}",
        f"Median alert lag: {format_duration(median_lag_sec)}",
        (
            "Sim: start "
            f"{_format_usd2(sim_settings.sim_start_balance)}, "
            f"pos {_format_usd2(sim_settings.sim_position_size)}, "
            f"target {sim_settings.sim_target_multiple:.2f}x, "
            f"fees {sim_settings.sim_buy_fee_pct:.2f}%/{sim_settings.sim_sell_fee_pct:.2f}%, "
            f"slip {sim_settings.sim_slippage_sample_sec}s"
        ),
        f"Sim cash: {_format_usd2(sim_cash)}",
        "Filters:",
        format_filters(ctx),
    ]
    return "\n".join(lines)


def is_user_admin(
    user_id: int, chat_type: str, admin_user_ids: set[int], chat_admin_ids: Optional[set[int]]
) -> bool:
    if user_id in admin_user_ids:
        return True
    if chat_type == ChatType.PRIVATE:
        return False
    if chat_admin_ids is None:
        return False
    return user_id in chat_admin_ids


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, ctx: AppContext) -> bool:
    user = update.effective_user
    if user is None:
        return False
    if user.id in ctx.config.admin_user_ids:
        return True
    chat = update.effective_chat
    if chat is None or chat.type == ChatType.PRIVATE:
        return False
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = {admin.user.id for admin in admins}
        return is_user_admin(user.id, chat.type, ctx.config.admin_user_ids, admin_ids)
    except Exception:
        return False


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        if update.effective_message:
            await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return

    await send_startup_animation(update, context, ctx)

    sim_settings = await get_sim_settings(ctx)
    sim_cash = await ctx.db.get_state_float("sim_cash", sim_settings.sim_start_balance)
    balance_buttons = [
        InlineKeyboardButton("$25", callback_data="sim:balance:25"),
        InlineKeyboardButton("$50", callback_data="sim:balance:50"),
        InlineKeyboardButton("$100", callback_data="sim:balance:100"),
        InlineKeyboardButton("$200", callback_data="sim:balance:200"),
    ]
    position_buttons = [
        InlineKeyboardButton("$0.5", callback_data="sim:position:0.5"),
        InlineKeyboardButton("$1", callback_data="sim:position:1"),
        InlineKeyboardButton("$2", callback_data="sim:position:2"),
        InlineKeyboardButton("$5", callback_data="sim:position:5"),
    ]
    keyboard = InlineKeyboardMarkup([balance_buttons, position_buttons, [InlineKeyboardButton("Start Sim", callback_data="sim:confirm")]])
    sim_text = (
        "Simulation setup\n"
        f"• Balance: {_format_usd2(sim_settings.sim_start_balance)}\n"
        f"• Position: {_format_usd2(sim_settings.sim_position_size)}\n"
        f"• Target: {sim_settings.sim_target_multiple:.2f}x\n"
        f"• Fees: {sim_settings.sim_buy_fee_pct:.2f}% / {sim_settings.sim_sell_fee_pct:.2f}%\n"
        f"• Cash now: {_format_usd2(sim_cash)}"
    )

    status_lines = [
        "STATUS",
        "• Chain: Solana",
        "• Mode: Auto Ape Simulation",
        f"• Scan Interval: {ctx.config.scan_interval_sec}s",
        "• Alerts: ENABLED",
    ]
    status_text = "\n".join(status_lines)

    if update.effective_message:
        await update.effective_message.reply_text(sim_text, reply_markup=keyboard)
        await update.effective_message.reply_text(
            status_text, reply_markup=build_status_keyboard()
        )
    elif update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=sim_text,
            reply_markup=keyboard,
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=status_text,
            reply_markup=build_status_keyboard(),
        )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = [f"<pre>{WELCOME_HEADER}</pre>", HELP_TEXT]
    await update.effective_message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=build_status_keyboard(),
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    lines = [f"<pre>{WELCOME_HEADER}</pre>", format_filters(ctx)]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return

    paused = await ctx.db.get_state_bool("paused", False)
    mute_until = await ctx.db.get_state_int("mute_until", 0)
    last_scan = await ctx.db.get_state_int("last_scan_at", 0)
    candidates_per_min = await ctx.db.get_state_float("metrics_candidates_per_min", 0.0)
    pairs_per_min = await ctx.db.get_state_float("metrics_pairs_fetched_per_min", 0.0)
    scanned_pairs = await ctx.db.get_state_int("metrics_scanned_pairs", 0)
    unique_tokens_checked = await ctx.db.get_state_int("metrics_unique_tokens_checked", 0)
    eligible_count = await ctx.db.get_state_int("metrics_eligible_count", 0)
    alerted_count = await ctx.db.get_state_int("metrics_alerted_count", 0)
    matches_per_day = await ctx.db.get_state_int("metrics_matches_per_day", 0)
    api_requests = await ctx.db.get_state_int("metrics_api_requests", 0)
    rate_limited = await ctx.db.get_state_int("metrics_rate_limited_count", 0)
    median_lag_sec = await ctx.db.get_state_int("metrics_alert_lag_median_sec", 0)
    sim_cash = await ctx.db.get_state_float("sim_cash", ctx.config.sim_start_balance)
    sim_settings = await get_sim_settings(ctx)

    status = format_status(
        ctx,
        paused,
        mute_until,
        last_scan,
        candidates_per_min,
        pairs_per_min,
        scanned_pairs,
        unique_tokens_checked,
        eligible_count,
        alerted_count,
        matches_per_day,
        api_requests,
        rate_limited,
        median_lag_sec,
        sim_cash,
        sim_settings,
    )
    lines = [f"<pre>{WELCOME_HEADER}</pre>", status]
    await update.effective_message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=build_status_keyboard(),
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    reset_at = await ctx.db.get_state_int("sim_reset_at", 0)
    rows = await ctx.db.get_called_for_performance(PERFORMANCE_EXPORT_LIMIT, reset_at or None)
    sim_settings = await get_sim_settings(ctx)
    sim_cash = await ctx.db.get_state_float("sim_cash", sim_settings.sim_start_balance)
    text = format_account_stats(rows, ctx.config.display_timezone, sim_settings, reset_at, sim_cash)
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_performance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    now = utc_now_ts()
    min_first_at = None
    window_label = "all-time"
    export = False
    reset_at = await ctx.db.get_state_int("sim_reset_at", 0)
    if not context.args and reset_at:
        window_label = "since reset"
    if context.args:
        for raw in context.args:
            arg = raw.strip().lower()
            if arg in ("list", "export", "csv"):
                export = True
                continue
            if arg in ("all", "alltime", "all-time"):
                min_first_at = None
                window_label = "all-time"
                continue
            duration = parse_duration(arg)
            if duration is None:
                await update.effective_message.reply_text(
                    "Usage: /performance [7d|30d|all] [export]"
                )
                return
            min_first_at = now - duration
            if duration % 86400 == 0:
                window_label = f"last {duration // 86400}d"
            else:
                window_label = f"last {format_duration(duration)}"

    effective_min = min_first_at
    if reset_at and (effective_min is None or reset_at > effective_min):
        effective_min = reset_at
    total_calls = await ctx.db.count_called_since(effective_min)
    limit = PERFORMANCE_EXPORT_LIMIT if export else PERFORMANCE_SUMMARY_LIMIT
    rows = await ctx.db.get_called_for_performance(limit, effective_min)
    sim_settings = await get_sim_settings(ctx)
    text = format_performance_summary(
        rows,
        ctx.config.display_timezone,
        window_label,
        total_calls,
        limit,
        sim_settings,
        reset_at,
    )
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    if export and rows:
        csv_bytes = build_performance_csv(rows, ctx.config.display_timezone, sim_settings)
        filename = f"performance_{window_label.replace(' ', '_')}.csv"
        await update.effective_message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename=filename),
            caption=f"Simulation export ({window_label})",
        )


async def cmd_archive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    now = utc_now_ts()
    max_first_at = await ctx.db.get_state_int("sim_reset_at", 0)
    if max_first_at == 0:
        max_first_at = now
    window_label = "before reset"
    min_first_at: Optional[int] = None
    if context.args:
        for raw in context.args:
            arg = raw.strip().lower()
            if arg in ("all", "alltime", "all-time"):
                window_label = "before reset"
                continue
            duration = parse_duration(arg)
            if duration is None:
                await update.effective_message.reply_text(
                    "Usage: /archive [7d|30d|all]"
                )
                return
            window_label = (
                f"last {duration // 86400}d"
                if duration % 86400 == 0
                else f"last {format_duration(duration)}"
            )
            min_first_at = max(0, max_first_at - duration)

    if min_first_at is not None:
        total_calls = await ctx.db.count_called_between(min_first_at, max_first_at)
    else:
        total_calls = await ctx.db.count_called_before(max_first_at)
    limit = PERFORMANCE_SUMMARY_LIMIT
    rows = await ctx.db.get_called_before(limit, max_first_at)
    if min_first_at is not None:
        rows = [row for row in rows if (row["eligible_first_at"] or 0) >= min_first_at]
    text = format_archive_summary(
        rows,
        ctx.config.display_timezone,
        window_label,
        total_calls,
        limit,
    )
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_moonbag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return
    now = utc_now_ts()
    reset_at = await ctx.db.get_state_int("sim_reset_at", 0)
    rows = await ctx.db.get_called_for_performance(PERFORMANCE_EXPORT_LIMIT, reset_at or None)
    sim_settings = await get_sim_settings(ctx)
    lines = [f"<pre>{WELCOME_HEADER}</pre>", "Moonbag Holdings"]
    buttons = []
    total_value = 0.0
    any_holdings = False
    for row in rows:
        if reset_at and (row["eligible_first_at"] or 0) < reset_at:
            continue
        if not row["sim_taken"]:
            continue
        sim = _compute_sim_row(row, sim_settings)
        if not sim["recouped"]:
            continue
        if row["moonbag_sold_at"]:
            continue
        if not row["moonbag_tokens"]:
            continue
        any_holdings = True
        current_value = sim["current_value"]
        name = escape_html(row["last_name"] or "Unknown")
        symbol = escape_html(row["last_symbol"] or "?")
        lines.append(f"{name} ({symbol})")
        lines.append(f"CA: <code>{escape_html(row['token_address'])}</code>")
        if sim["current_multiple"] is not None:
            lines.append(f"Now: {_format_multiple(sim['current_multiple'])}")
        if current_value is not None:
            total_value += current_value
            lines.append(f"Value: {_format_usd2(current_value)}")
        lines.append("")
        buttons.append([InlineKeyboardButton(f"Sell {symbol}", callback_data=f"moonbag:sell:{row['token_address']}")])
    lines.append(f"Total value: {_format_usd2(total_value)}")
    if not any_holdings:
        lines.append("No moonbags yet.")
    if buttons:
        buttons.append([InlineKeyboardButton("Sell All", callback_data="moonbag:sell_all")])
    await update.effective_message.reply_text(
        "\n".join(lines).strip(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
    )


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return

    pool_size = await ctx.db.count_pair_pool()
    last_scan = await ctx.db.get_state_int("last_scan_at", 0)
    last_api_success = await ctx.db.get_state_int("last_api_success", 0)
    scan_overlap = await ctx.db.get_state_int("metrics_scan_overlap", 0)
    api_requests = await ctx.db.get_state_int("metrics_api_requests", 0)
    rate_limited = await ctx.db.get_state_int("metrics_rate_limited_count", 0)

    lines = [
        "HEALTH",
        f"Pool size: {pool_size}",
        f"Last scan: {format_ts(last_scan, ctx.config.display_timezone)}",
        f"Last API success: {format_ts(last_api_success, ctx.config.display_timezone)}",
        f"Scan overlap warnings: {scan_overlap}",
        f"API requests: {api_requests}",
        f"Rate limited: {rate_limited}",
        f"Holder count: {'on' if ctx.config.holder_count_enabled else 'off'} (min {ctx.config.holder_count_min})",
    ]
    await update.effective_message.reply_text("\n".join(lines))


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return
    await ctx.db.set_state("paused", "true")
    await update.effective_message.reply_text("Monitoring paused.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return
    await ctx.db.set_state("paused", "false")
    await update.effective_message.reply_text("Monitoring resumed.")


async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /mute 1h or /mute 30m")
        return

    duration = parse_duration(context.args[0])
    if not duration:
        await update.effective_message.reply_text("Invalid duration. Use 1h, 30m, 2d")
        return

    mute_until = utc_now_ts() + duration
    await ctx.db.set_state("mute_until", str(mute_until))
    await update.effective_message.reply_text(
        f"Alerts muted until {format_ts(mute_until, ctx.config.display_timezone)}"
    )


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return
    reset_at = utc_now_ts()
    await ctx.db.set_state("sim_reset_at", str(reset_at))
    await ctx.db.set_state("sim_cash", str(ctx.config.sim_start_balance))
    await update.effective_message.reply_text(
        f"Simulation reset at {format_ts(reset_at, ctx.config.display_timezone)}"
    )


async def cmd_setthresholds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    if not await is_admin(update, context, ctx):
        await update.effective_message.reply_text("Admin only.")
        return
    await update.effective_message.reply_text(
        "Thresholds are configured via environment variables. Update .env and restart the bot."
    )


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.my_chat_member is None:
        return
    old_status = update.my_chat_member.old_chat_member.status
    new_status = update.my_chat_member.new_chat_member.status
    if new_status in ("member", "administrator") and old_status in ("left", "kicked"):
        await cmd_start(update, context)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    ctx = get_app_ctx(context)
    if ctx is None:
        await query.message.reply_text("Bot is starting, try again in a moment.")
        return

    data = query.data or ""
    if data.startswith("mute:"):
        if not await is_admin(update, context, ctx):
            await query.answer("Admin only", show_alert=True)
            return
        duration_value = data.split(":", 1)[1]
        duration = parse_duration(duration_value)
        if not duration:
            await query.message.reply_text("Invalid duration")
            return
        mute_until = utc_now_ts() + duration
        await ctx.db.set_state("mute_until", str(mute_until))
        await query.message.reply_text(
            f"Alerts muted until {format_ts(mute_until, ctx.config.display_timezone)}"
        )
        return

    if data.startswith("sim:"):
        if not await is_admin(update, context, ctx):
            await query.answer("Admin only", show_alert=True)
            return
        parts = data.split(":")
        if len(parts) >= 3 and parts[1] in ("balance", "position"):
            try:
                value = float(parts[2])
            except ValueError:
                await query.message.reply_text("Invalid value.")
                return
            if parts[1] == "balance":
                await ctx.db.set_state("sim_start_balance", str(value))
                await ctx.db.set_state("sim_cash", str(value))
            else:
                await ctx.db.set_state("sim_position_size", str(value))
            await query.message.reply_text(
                f"Simulation {parts[1]} set to {_format_usd2(value)}."
            )
            return
        if len(parts) >= 2 and parts[1] == "confirm":
            reset_at = utc_now_ts()
            sim_settings = await get_sim_settings(ctx)
            await ctx.db.set_state("sim_reset_at", str(reset_at))
            await ctx.db.set_state("sim_cash", str(sim_settings.sim_start_balance))
            await query.message.reply_text(
                f"Simulation started at {format_ts(reset_at, ctx.config.display_timezone)}"
            )
            return

    if data.startswith("moonbag:"):
        if not await is_admin(update, context, ctx):
            await query.answer("Admin only", show_alert=True)
            return
        parts = data.split(":")
        if len(parts) >= 2 and parts[1] == "sell_all":
            sim_settings = await get_sim_settings(ctx)
            sim_cash = await ctx.db.get_state_float("sim_cash", sim_settings.sim_start_balance)
            reset_at = await ctx.db.get_state_int("sim_reset_at", 0)
            rows = await ctx.db.get_called_for_performance(PERFORMANCE_EXPORT_LIMIT, reset_at or None)
            sold_total = 0.0
            for row in rows:
                if reset_at and (row["eligible_first_at"] or 0) < reset_at:
                    continue
                if not row["sim_taken"]:
                    continue
                if not row["moonbag_tokens"] or row["moonbag_sold_at"]:
                    continue
                sim = _compute_sim_row(row, sim_settings)
                current_value = sim["current_value"]
                if current_value is None:
                    continue
                sim_cash += current_value
                sold_total += current_value
                await ctx.db.update_moonbag_state(
                    token_address=row["token_address"],
                    moonbag_tokens=row["moonbag_tokens"],
                    moonbag_sold_at=utc_now_ts(),
                    moonbag_sold_value=current_value,
                )
            await ctx.db.set_state("sim_cash", str(sim_cash))
            await query.message.reply_text(
                f"Sold all moonbags for {_format_usd2(sold_total)}. Cash: {_format_usd2(sim_cash)}"
            )
            return
        if len(parts) >= 3 and parts[1] == "sell":
            token_address = parts[2]
            sim_settings = await get_sim_settings(ctx)
            row = await ctx.db.get_token(token_address)
            if row is None:
                await query.message.reply_text("Token not found.")
                return
            if not row["moonbag_tokens"] or row["moonbag_sold_at"]:
                await query.message.reply_text("No moonbag to sell.")
                return
            sim = _compute_sim_row(row, sim_settings)
            current_value = sim["current_value"]
            if current_value is None:
                await query.message.reply_text("No price available.")
                return
            sim_cash = await ctx.db.get_state_float("sim_cash", sim_settings.sim_start_balance)
            sim_cash += current_value
            await ctx.db.set_state("sim_cash", str(sim_cash))
            await ctx.db.update_moonbag_state(
                token_address=token_address,
                moonbag_tokens=row["moonbag_tokens"],
                moonbag_sold_at=utc_now_ts(),
                moonbag_sold_value=current_value,
            )
            await query.message.reply_text(
                f"Sold moonbag for {_format_usd2(current_value)}. Cash: {_format_usd2(sim_cash)}"
            )
            return

    if data == "settings":
        await query.message.reply_text(
            f"<pre>{WELCOME_HEADER}</pre>\n{format_filters(ctx)}",
            parse_mode=ParseMode.HTML,
        )
        return


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = context.application.bot_data.get("app_ctx")
    if ctx:
        ctx.logger.exception("handler_error", exc_info=context.error)


def register_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("performance", cmd_performance))
    application.add_handler(CommandHandler("archive", cmd_archive))
    application.add_handler(CommandHandler("moonbag", cmd_moonbag))
    application.add_handler(CommandHandler("filters", cmd_filters))
    application.add_handler(CommandHandler("health", cmd_health))
    application.add_handler(CommandHandler("pause", cmd_pause))
    application.add_handler(CommandHandler("resume", cmd_resume))
    application.add_handler(CommandHandler("mute", cmd_mute))
    application.add_handler(CommandHandler("reset", cmd_reset))
    application.add_handler(CommandHandler("setthresholds", cmd_setthresholds))

    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    application.add_error_handler(on_error)






