from __future__ import annotations

import asyncio
import os
import signal

import aiohttp
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder

from .bot import register_handlers
from .config import load_config
from .db import Database
from .dexscreener import DexscreenerClient
from .discovery import DiscoveryEngine
from .logger import setup_logging
from .scheduler import Scanner, PERFORMANCE_REFRESH_INTERVAL_SEC
from .types import AppContext
from .wallet_analysis import WalletAnalyzer


def main() -> None:
    load_dotenv()
    config = load_config()
    logger = setup_logging(config.log_level)

    db_dir = os.path.dirname(config.sqlite_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    async def post_init(application):
        db = await Database.connect(config.sqlite_path)
        await db.init()

        timeout = aiohttp.ClientTimeout(total=config.dex_timeout_sec)
        session = aiohttp.ClientSession(timeout=timeout)

        dex = DexscreenerClient(session, config, logger, db=db)
        discovery = DiscoveryEngine(dex, config, logger)
        wallet_analyzer = None
        if config.wallet_analysis_enabled and config.wallet_analysis_provider == "helius":
            if not config.helius_api_key:
                logger.warning("wallet_analysis_disabled_missing_api_key")
            else:
                wallet_analyzer = WalletAnalyzer(session, config, logger, db=db)
        app_ctx = AppContext(
            config=config,
            logger=logger,
            db=db,
            session=session,
            dex=dex,
            discovery=discovery,
            wallet_analyzer=wallet_analyzer,
        )
        application.bot_data["app_ctx"] = app_ctx

        scanner = Scanner(app_ctx, application.bot)
        application.bot_data["scanner"] = scanner
        scan_job = application.job_queue.run_repeating(
            scanner.scan_job,
            interval=config.scan_interval_sec,
            first=3,
            name="scanner",
        )
        application.bot_data["scan_job"] = scan_job
        perf_job = application.job_queue.run_repeating(
            scanner.performance_job,
            interval=PERFORMANCE_REFRESH_INTERVAL_SEC,
            first=15,
            name="performance_tracker",
        )
        application.bot_data["perf_job"] = perf_job
        asyncio.create_task(scanner.backfill_called_prices())
        logger.info(
            "bot_ready",
            extra={
                "scan_interval_sec": config.scan_interval_sec,
                "discovery_mode": config.discovery_mode,
                "allowed_chats": len(config.allowed_chat_ids),
                "allowed_threads": len(config.allowed_thread_ids),
                "db_path": config.sqlite_path,
                "dry_run": config.dry_run,
                "wallet_analysis": config.wallet_analysis_enabled,
                "wallet_provider": config.wallet_analysis_provider,
            },
        )

    async def post_shutdown(application):
        scan_job = application.bot_data.get("scan_job")
        if scan_job:
            scan_job.schedule_removal()
        perf_job = application.bot_data.get("perf_job")
        if perf_job:
            perf_job.schedule_removal()
        app_ctx = application.bot_data.get("app_ctx")
        if app_ctx:
            await app_ctx.session.close()
            await app_ctx.db.close()
            logger.info("bot_shutdown")

    application = (
        ApplicationBuilder()
        .token(config.bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    register_handlers(application)
    application.run_polling(stop_signals=(signal.SIGINT, signal.SIGTERM))


if __name__ == "__main__":
    main()
