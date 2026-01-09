from __future__ import annotations

import asyncio
from typing import Optional

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
ALERT_HEADER = "+----------------------------+\n| Freedom Trench Bot         |\n| BECAME ELIGIBLE ✅          |\n+----------------------------+"

STARTUP_FRAMES = [
    "> initializing...",
    "> initializing...\n> loading solana modules...",
    "> initializing...\n> loading solana modules...\n> applying eligibility filters...",
    "> initializing...\n> loading solana modules...\n> applying eligibility filters...\n> starting market scanner...",
]

STARTUP_FINAL_FRAME = (
    "#####  ####   #####  #####  ####   ####   #   #\n"
    "##     ##  #  ##     ##     ##  #  ##  #  ## ##\n"
    "####   ####   ####   ####   ##  #  ##  #  # # #\n"
    "##     ## #   ##     ##     ##  #  ##  #  #   #\n"
    "##     ##  #  #####  #####  ####   ####   #   #\n"
    "\n"
    "                Freedom Trench Bot\n"
    "----------------------------------------\n"
    "Solana Eligibility Scanner - LIVE"
)

HELP_TEXT = (
    "/start - onboarding and status\n"
    "/status - monitoring status and filters\n"
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


def build_trigger_reason(filters) -> str:
    return (
        f"Trigger: MC<= {format_usd(filters.max_market_cap)}, "
        f"Vol1h>= {format_usd(filters.min_volume_1h)}, "
        f"Change1h/6h/24h>= {filters.min_change_1h:.2f}%/"
        f"{filters.min_change_6h:.2f}%/{filters.min_change_24h:.2f}%"
    )


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
        f"Token: {name} ({symbol})",
        "Chain: Solana",
        "CA:",
        ca_block,
        f"MCap: {format_usd(metrics.market_cap_value)}{mcap_suffix}",
        f"Vol 1h: {format_usd(metrics.volume_1h)}",
        f"Change 1h: {format_pct(metrics.change_1h)} | 6h: {format_pct(metrics.change_6h)} | 24h: {format_pct(metrics.change_24h)}",
        trigger_reason,
        f"First seen: {format_ts(first_seen_ts, tz_name)}",
        f"Dexscreener: <a href=\"{build_dex_url(pair, chain_id)}\">link</a>",
        f"Solscan: <a href=\"https://solscan.io/token/{token_address}\">link</a>",
    ]
    return "\n".join(lines)


def format_filters(ctx: AppContext) -> str:
    filters = ctx.config.filters
    lines = [
        f"Chain: {ctx.config.chain_id}",
        f"Market cap max: {format_usd(filters.max_market_cap)}",
        f"FDV proxy: {'on' if ctx.config.use_fdv_as_mc_proxy else 'off'}",
        f"Change 24h min: {filters.min_change_24h:.2f}%",
        f"Change 6h min: {filters.min_change_6h:.2f}%",
        f"Change 1h min: {filters.min_change_1h:.2f}%",
        f"Volume 1h min: {format_usd(filters.min_volume_1h)}",
        f"Re-arm min ineligible: {format_duration(ctx.config.min_ineligible_duration_sec)}",
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
        "• Mode: Eligibility Transitions",
        f"• Scan Interval: {ctx.config.scan_interval_sec}s",
        "• Alerts: ENABLED",
    ]
    status_text = "\n".join(status_lines)

    if update.effective_message:
        await update.effective_message.reply_text(status_text)
    elif update.effective_chat:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=status_text)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = [f"<pre>{WELCOME_HEADER}</pre>", HELP_TEXT]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


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
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


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
    application.add_handler(CommandHandler("filters", cmd_filters))
    application.add_handler(CommandHandler("health", cmd_health))
    application.add_handler(CommandHandler("pause", cmd_pause))
    application.add_handler(CommandHandler("resume", cmd_resume))
    application.add_handler(CommandHandler("mute", cmd_mute))
    application.add_handler(CommandHandler("setthresholds", cmd_setthresholds))

    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    application.add_error_handler(on_error)
