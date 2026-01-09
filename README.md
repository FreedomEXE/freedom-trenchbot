# Freedom Trench Bot

Freedom Trench Bot monitors Solana markets via the Dexscreener API and posts alerts to allowlisted Telegram group chats when tokens transition from ineligible to eligible. It does not trade or manage wallets.

## Architecture (brief)
- Telegram bot (python-telegram-bot) handles commands, admin checks, and alert delivery.
- Dexscreener client uses rate limiting, retries, and caching for API calls.
- Market sampler polls Solana pair data and maintains a rolling candidate pool; hot pairs are rechecked more frequently.
- Eligibility transition engine alerts only when a token moves from ineligible to eligible.
- SQLite stores seen tokens, eligibility state, candidate pool, last checked time, last alert, and bot state (pause, mute, counters).

## Discovery strategy
Discovery runs in market sampling mode (`DISCOVERY_MODE=market_sampler`):
- Each scan pulls a broad set of Solana pairs via `token-pairs` on base tokens (e.g., WSOL/USDC). The endpoint returns a full list per base token (no paging), so repeated polling keeps recall high for those bases.
- Pairs are added to a rolling pool (default retention: 6 hours) and scored by liquidity + volume.
- The hottest pairs are rechecked every scan to catch spikes from older tokens.
- The pool is refreshed every scan from the broader sampler to avoid blind spots; use `MARKET_BASE_TOKENS` to widen coverage beyond WSOL/USDC.
- Fallback mode (`fallback_search`) exists for resiliency, but the primary path is market-wide sampling.

### Endpoints used
- `GET https://api.dexscreener.com/token-pairs/v1/solana/{tokenAddress}` (market sampler + metrics)
- `GET https://api.dexscreener.com/latest/dex/pairs/{chainId}/{pairId}` (hot recheck)
- `GET https://api.dexscreener.com/latest/dex/search?q=...` (fallback mode only)

This avoids HTML scraping and stays within rate limits. Tune pool size, hot recheck count, and scan interval in `.env`.
Set `MARKET_BASE_TOKENS` to override the default WSOL/USDC sampler list.

## Setup
1) Use Python 3.12.x (see `.python-version`).
2) Create a Telegram bot and copy the token.
3) Add the bot to your group and make it admin (so admin checks work).
4) Create a `.env` file from `.env.example` and set at least:
   - `BOT_TOKEN`
   - `ALLOWED_CHAT_IDS` (comma separated, use the group chat id)
   - `ADMIN_USER_IDS` (optional override)
5) Install dependencies:
   - `pip install -r requirements.txt`
   - For tests: `pip install -r requirements-dev.txt`

## Run
Single entrypoint:
```
python -m freedom_trench_bot
```

### Dry run
Set `DRY_RUN=true` to log would-alert tokens without posting to Telegram.

### Key config knobs
- `SCAN_INTERVAL_SECONDS` (default 20)
- `CANDIDATE_POOL_MAX` and `HOT_RECHECK_TOP_N` (pool size + hot rechecks)
- `MIN_INELIGIBLE_MINUTES_TO_REARM` and `DEDUPE_WINDOW_HOURS` (anti-spam rearm rules)
- `USE_FDV_AS_MC_PROXY` (market cap fallback)
- `DB_PATH` (defaults to `./data/freedom_trench_bot.db`)

## Commands
- `/start` - onboarding and status
- `/status` - monitoring status, last scan, counters, filters
- `/filters` - current filters
- `/health` - health summary (admin only)
- `/pause` - pause monitoring (admin only)
- `/resume` - resume monitoring (admin only)
- `/mute <duration>` - mute alerts for a duration like `1h`, `30m` (admin only)
- `/help` - quick help

## Alert format
Alerts fire only when a token transitions from ineligible to eligible. The message includes token name/symbol, chain, CA, market cap (or FDV proxy), volume 1h, 1h/6h/24h changes, trigger reason, first seen, and links.

## Brand kit
SVG logo: `assets/freedom-trench-bot.svg`

## Notes
- If `marketCap` is missing, FDV is only used when `USE_FDV_AS_MC_PROXY=true` and is labeled as a proxy.
- Alerts only fire on eligibility transitions and require the token to be ineligible for at least `MIN_INELIGIBLE_MINUTES_TO_REARM` before re-alerting.
- Missing change or volume fields fail the filter by design.
- No trading, wallet creation, or automation is included.
