from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
    format_usd,
    parse_duration,
    utc_now_ts,
)

WELCOME_HEADER = "+----------------------------+\n| Freedom Trench Bot         |\n| Solana Alerts              |\n+----------------------------+"
ALERT_HEADER = "+----------------------------+\n| Freedom Trench Bot         |\n| BECAME ELIGIBLE ✅         |\n+----------------------------+"

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

HELP_TEXT = (
    "/start - onboarding and status\n"
    "/status - monitoring status and filters\n"
    "/eligible - list currently eligible tokens\n"
    "/stats - list tokens called in the last 24h\n"
    "/filters - current filters\n"
    "/health - health summary (admin only)\n"
    "/pause - pause monitoring (admin only)\n"
    "/resume - resume monitoring (admin only)\n"
    "/mute <duration> - mute alerts, ex: 1h or 30m (admin only)\n"
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
            InlineKeyboardButton("Currently Eligible", callback_data="eligible:list"),
            InlineKeyboardButton("Settings", callback_data="settings"),
        ]
    ]
    return InlineKeyboardMarkup(buttons)


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


def format_eligible_list(rows, tz_name: str, retention_sec: int) -> str:
    hours = max(1, int(retention_sec / 3600))
    header = f"<pre>{WELCOME_HEADER}</pre>"
    if not rows:
        return f"{header}\nCurrently eligible (last {hours}h): 0\nNo tokens currently eligible."

    lines = [header, f"Currently eligible (last {hours}h): {len(rows)}"]
    for idx, row in enumerate(rows, start=1):
        token_address = row["token_address"]
        name = escape_html(row["last_name"] or "Unknown")
        symbol = escape_html(row["last_symbol"] or "?")
        found_ts = row["eligible_first_at"]
        found_snapshot = _parse_metrics_snapshot(row["eligible_first_metrics"])
        current_snapshot = _parse_metrics_snapshot(row["last_seen_metrics"])
        if not current_snapshot:
            current_snapshot = found_snapshot

        lines.append(f"{idx}. {name} ({symbol})")
        lines.append(f"CA: <code>{escape_html(token_address)}</code>")
        lines.append(f"Found: {format_ts(found_ts, tz_name)}")
        lines.append(f"MCap now: {_format_mcap_from_snapshot(current_snapshot)}")
        lines.append(f"MCap found: {_format_mcap_from_snapshot(found_snapshot)}")
        lines.append("")
    return "\n".join(lines).strip()


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.8f}".rstrip("0").rstrip(".")


def format_called_stats(rows, tz_name: str, retention_sec: int, limit: int) -> str:
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
        found_snapshot = _parse_metrics_snapshot(row["eligible_first_metrics"])
        current_snapshot = _parse_metrics_snapshot(row["last_seen_metrics"])
        if not current_snapshot:
            current_snapshot = found_snapshot

        called_price = row["called_price_usd"]
        max_price = row["max_price_usd"]
        roi = None
        if called_price and max_price and called_price > 0:
            roi = ((max_price / called_price) - 1.0) * 100.0

        ath_mcap = row["max_market_cap"]
        if ath_mcap is None:
            ath_mcap = _to_float(found_snapshot.get("marketCap"))

        lines.append(f"{idx}. {name} ({symbol})")
        lines.append(f"CA: <code>{escape_html(token_address)}</code>")
        lines.append(f"Called: {format_ts(called_ts, tz_name)}")
        lines.append(f"MCap called: {_format_mcap_from_snapshot(found_snapshot)}")
        lines.append(f"MCap now: {_format_mcap_from_snapshot(current_snapshot)}")
        lines.append(f"ATH MCap (since call): {format_usd(ath_mcap)}")
        lines.append(f"Max ROI (since call): {format_pct(roi)}")
        lines.append("")
    return "\n".join(lines).strip()


async def send_eligible_list_message(message, ctx: AppContext) -> None:
    if message is None:
        return
    now = utc_now_ts()
    rows = await ctx.db.get_currently_eligible(
        ctx.config.eligible_list_limit, now - ctx.config.eligible_retention_sec
    )
    text = format_eligible_list(rows, ctx.config.display_timezone, ctx.config.eligible_retention_sec)
    await message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def send_called_stats_message(message, ctx: AppContext) -> None:
    if message is None:
        return
    now = utc_now_ts()
    rows = await ctx.db.get_called_since(
        ctx.config.called_list_limit, now - ctx.config.eligible_retention_sec
    )
    text = format_called_stats(
        rows,
        ctx.config.display_timezone,
        ctx.config.eligible_retention_sec,
        ctx.config.called_list_limit,
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
        f"Token: {name} ({symbol})",
        "Chain: Solana",
        "CA:",
        ca_block,
        f"MCap: {format_usd(metrics.market_cap_value)}{mcap_suffix}",
    ]
    if wallet_analysis:
        label = wallet_label or "Top Wallet Call"
        lines.extend(format_wallet_analysis_block(wallet_analysis, label, tz_name))
    lines.extend(
        [
            f"First seen: {format_ts(first_seen_ts, tz_name)}",
            f"Dexscreener: <a href=\"{build_dex_url(pair, chain_id)}\">link</a>",
            f"Solscan: <a href=\"https://solscan.io/token/{token_address}\">link</a>",
        ]
    )
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

    status_lines = [
        "STATUS",
        "• Chain: Solana",
        "• Mode: Eligible List",
        f"• Scan Interval: {ctx.config.scan_interval_sec}s",
        "• Alerts: ENABLED",
    ]
    status_text = "\n".join(status_lines)

    if update.effective_message:
        await update.effective_message.reply_text(
            status_text, reply_markup=build_status_keyboard()
        )
    elif update.effective_chat:
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
    )
    lines = [f"<pre>{WELCOME_HEADER}</pre>", status]
    await update.effective_message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=build_status_keyboard(),
    )


async def cmd_eligible(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    await send_eligible_list_message(update.effective_message, ctx)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ctx = get_app_ctx(context)
    if ctx is None:
        await update.effective_message.reply_text("Bot is starting, try again in a moment.")
        return
    await send_called_stats_message(update.effective_message, ctx)


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
    wallet_runs = await ctx.db.get_state_int("metrics_wallet_runs", 0)
    wallet_success = await ctx.db.get_state_int("metrics_wallet_success", 0)
    wallet_fail = await ctx.db.get_state_int("metrics_wallet_fail", 0)
    wallet_no_data = await ctx.db.get_state_int("metrics_wallet_no_data", 0)
    wallet_api_requests = await ctx.db.get_state_int("metrics_wallet_api_requests", 0)
    wallet_rate_limited = await ctx.db.get_state_int("metrics_wallet_rate_limited_count", 0)
    wallet_last_at = await ctx.db.get_state_int("wallet_analysis_last_at", 0)
    wallet_last_token = await ctx.db.get_state("wallet_analysis_last_token") or "n/a"

    lines = [
        "HEALTH",
        f"Pool size: {pool_size}",
        f"Last scan: {format_ts(last_scan, ctx.config.display_timezone)}",
        f"Last API success: {format_ts(last_api_success, ctx.config.display_timezone)}",
        f"Scan overlap warnings: {scan_overlap}",
        f"API requests: {api_requests}",
        f"Rate limited: {rate_limited}",
        f"Wallet analysis: {'on' if ctx.config.wallet_analysis_enabled else 'off'} ({ctx.config.wallet_analysis_provider})",
        f"Wallet runs: {wallet_runs}, success: {wallet_success}, fail: {wallet_fail}, no_data: {wallet_no_data}",
        f"Wallet API: requests {wallet_api_requests}, rate_limited {wallet_rate_limited}",
        f"Wallet last: {format_ts(wallet_last_at, ctx.config.display_timezone)}",
        f"Wallet last token: {wallet_last_token}",
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

    if data == "eligible:list":
        await send_eligible_list_message(query.message, ctx)
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
    application.add_handler(CommandHandler("eligible", cmd_eligible))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("filters", cmd_filters))
    application.add_handler(CommandHandler("health", cmd_health))
    application.add_handler(CommandHandler("pause", cmd_pause))
    application.add_handler(CommandHandler("resume", cmd_resume))
    application.add_handler(CommandHandler("mute", cmd_mute))
    application.add_handler(CommandHandler("setthresholds", cmd_setthresholds))

    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    application.add_error_handler(on_error)






