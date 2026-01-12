# Freedom Trench Bot

Freedom Trench Bot monitors Solana markets via the Dexscreener API and posts alerts to allowlisted Telegram group chats when tokens become eligible. It also keeps a 24h "currently eligible" list for quick review. It does not trade or manage wallets.

## Architecture (brief)
- Telegram bot (python-telegram-bot) handles commands, admin checks, and alert delivery.
- Dexscreener client uses rate limiting, retries, and caching for API calls.
- Market sampler polls Solana pair data and maintains a rolling candidate pool; hot pairs are rechecked more frequently.
- Eligibility evaluation computes per-scan eligibility and alerts only once when a token is first discovered eligible.
- SQLite stores seen tokens, eligibility state, candidate pool, eligible-first metrics, last checked time, last alert, and bot state (pause, mute, counters).

## Discovery strategy
Discovery runs in hybrid mode (`DISCOVERY_MODE=hybrid`) for best recall:
- Market sampler pulls Solana pairs via `token-pairs` on base tokens (WSOL/USDC/USDT/USD1 by default, plus any `MARKET_BASE_TOKENS` you add).
- Search queries add additional coverage for non-base pairs and fast-moving trends.
- Token profiles/boosts add newly profiled tokens that may not appear in the base sampler yet.
- Pairs are added to a rolling pool (default retention: 6 hours) and scored by liquidity + volume.
- The hottest pairs are rechecked every scan to catch spikes from older tokens.

### Endpoints used
- `GET https://api.dexscreener.com/token-pairs/v1/solana/{tokenAddress}` (market sampler + token lookups)
- `GET https://api.dexscreener.com/latest/dex/pairs/{chainId}/{pairId}` (hot recheck)
- `GET https://api.dexscreener.com/latest/dex/search?q=...` (hybrid search)
- `GET https://api.dexscreener.com/token-profiles/latest/v1` (hybrid profiles)
- `GET https://api.dexscreener.com/token-boosts/latest/v1` (hybrid boosts)

This avoids HTML scraping and stays within rate limits. Tune pool size, hot recheck count, and scan interval in `.env`.
Set `MARKET_BASE_TOKENS` to add additional base tokens to sample.

## Wallet analysis (optional)
Wallet analysis enriches alerts with a "first buyers" snapshot (default 20 buyers), fresh wallet ratio, and average SOL balance.
It runs asynchronously after an alert is posted and edits the original alert in place (with a follow-up message if edits fail).

This requires a Helius API key:
- `WALLET_ANALYSIS_ENABLED=true`
- `WALLET_ANALYSIS_PROVIDER=helius`
- `HELIUS_API_KEY=...`

Older pools are best-effort due to pagination caps; the alert will note when history is partial.

Flow scoring uses Dexscreener 5m + 1h txns/volume to add a one-line "Flow" label and does not require Helius.

## Setup
1) Use Python 3.12.x (see `.python-version`).
2) Create a Telegram bot and copy the token.
3) Add the bot to your group and make it admin (so admin checks work).
4) Create a `.env` file from `.env.example` and set at least:
   - `BOT_TOKEN`
   - `ALLOWED_CHAT_IDS` (comma separated, use the group chat id)
   - `ADMIN_USER_IDS` (optional override)
   - `ALLOWED_THREAD_IDS` (optional, restrict alerts to specific topics/threads)
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
- `USE_FDV_AS_MC_PROXY` (market cap fallback)
- `FILTER_REQUIRE_PROFILE` (require profile metadata in Dexscreener `info`)
- `DB_PATH` (defaults to `./data/freedom_trench_bot.db`)
- `DISCOVERY_MODE` (`hybrid`, `market_sampler`, or `fallback_search`)
- `SEARCH_QUERIES`, `HYBRID_SEARCH_REFRESH_SECONDS`, `HYBRID_REFRESH_SECONDS`, `HYBRID_MAX_TOKENS`
- `ALLOWED_THREAD_IDS` (restrict alerts to specific thread IDs in a group)
- `CALLED_LIST_LIMIT` (max items in `/stats`)
- `ALERT_TAGLINE` (custom line shown in alert messages)
- `FLOW_SCORE_MIN` (threshold for flow-filtered performance simulations; default 75)
- `WALLET_ANALYSIS_ENABLED` (enable first-buyer analysis)
- `WALLET_ANALYSIS_LABEL` (custom label shown in wallet analysis section)
- `WALLET_ANALYSIS_SAMPLE` (number of first buyers to sample)
- `WALLET_ANALYSIS_MAX_PAGES` (pagination cap for older pools)
- `WALLET_ANALYSIS_TTL_HOURS` (cache analysis per token)
- `FRESH_WALLET_MAX_AGE_DAYS`, `FRESH_WALLET_MAX_TX` (fresh wallet definition)

## Commands
- `/start` - onboarding and status
- `/status` - monitoring status, last scan, counters, filters
- `/eligible` - list currently eligible tokens (flow filtered)
- `/filters` - current filters
- `/performance [7d|30d|all] [export]` - performance summary (default all-time), optional CSV export
- `/health` - health summary (admin only)
- `/pause` - pause monitoring (admin only)
- `/resume` - resume monitoring (admin only)
- `/mute <duration>` - mute alerts for a duration like `1h`, `30m` (admin only)
- `/help` - quick help

## Alert format
Alerts fire only once when a token is first discovered eligible. The message includes token name/symbol, chain, CA, market cap (or FDV proxy), first seen, and links. A one-line "Flow" score is appended. When wallet analysis is enabled, a "Top Wallet Call" section is appended once the analysis completes.

## Brand kit
SVG logo: `assets/freedom-trench-bot.svg`

## Notes
- If `marketCap` is missing, FDV is only used when `USE_FDV_AS_MC_PROXY=true` and is labeled as a proxy.
- Alerts only fire once per token; check `/eligible` to see the currently eligible list.
- Missing change or volume fields fail the filter by design.
- Performance tracking is best-effort; inactive tokens may update less frequently.
- No trading, wallet creation, or automation is included.
