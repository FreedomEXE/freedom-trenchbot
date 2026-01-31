from __future__ import annotations

from typing import Optional, List

import aiosqlite


class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    @classmethod
    async def connect(cls, path: str) -> "Database":
        db = cls(path)
        db.conn = await aiosqlite.connect(path)
        db.conn.row_factory = aiosqlite.Row
        await db.conn.execute("PRAGMA journal_mode=WAL")
        await db.conn.execute("PRAGMA synchronous=NORMAL")
        await db.conn.execute("PRAGMA busy_timeout=5000")
        return db

    async def init(self) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tokens (
                token_address TEXT PRIMARY KEY,
                chain_id TEXT NOT NULL,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                last_checked_at INTEGER,
                last_alerted_at INTEGER,
                last_eligible INTEGER,
                last_eligible_at INTEGER,
                last_ineligible_at INTEGER,
                last_seen_metrics TEXT,
                eligible_first_at INTEGER,
                eligible_first_metrics TEXT,
                last_name TEXT,
                last_symbol TEXT,
                called_price_usd REAL,
                max_price_usd REAL,
                min_price_usd REAL,
                max_market_cap REAL,
                post_alert_price_usd REAL,
                post_alert_at INTEGER,
                above_target_started_at INTEGER,
                above_target_last_at INTEGER,
                above_target_total_sec INTEGER,
                sim_taken INTEGER,
                sim_cash_before REAL,
                sim_cash_after REAL,
                sim_position_usd REAL,
                sim_ape_pct REAL,
                moonbag_tokens REAL,
                moonbag_sold_at INTEGER,
                moonbag_sold_value REAL,
                wallet_analysis_at INTEGER,
                wallet_analysis_json TEXT,
                wallet_analysis_partial INTEGER,
                intent_score INTEGER,
                intent_label TEXT,
                intent_at INTEGER,
                intent_json TEXT,
                hit_2x_at INTEGER,
                hit_3x_at INTEGER,
                hit_5x_at INTEGER,
                recouped_at INTEGER,
                stoploss_at INTEGER,
                stoploss_price_usd REAL
            )
            """
        )
        await self._ensure_column("tokens", "last_checked", "INTEGER")
        await self._ensure_column("tokens", "last_alerted", "INTEGER")
        await self._ensure_column("tokens", "last_checked_at", "INTEGER")
        await self._ensure_column("tokens", "last_alerted_at", "INTEGER")
        await self._ensure_column("tokens", "last_eligible", "INTEGER")
        await self._ensure_column("tokens", "last_eligible_at", "INTEGER")
        await self._ensure_column("tokens", "last_ineligible_at", "INTEGER")
        await self._ensure_column("tokens", "last_seen_metrics", "TEXT")
        await self._ensure_column("tokens", "eligible_first_at", "INTEGER")
        await self._ensure_column("tokens", "eligible_first_metrics", "TEXT")
        await self._ensure_column("tokens", "last_name", "TEXT")
        await self._ensure_column("tokens", "last_symbol", "TEXT")
        await self._ensure_column("tokens", "called_price_usd", "REAL")
        await self._ensure_column("tokens", "max_price_usd", "REAL")
        await self._ensure_column("tokens", "min_price_usd", "REAL")
        await self._ensure_column("tokens", "max_market_cap", "REAL")
        await self._ensure_column("tokens", "post_alert_price_usd", "REAL")
        await self._ensure_column("tokens", "post_alert_at", "INTEGER")
        await self._ensure_column("tokens", "above_target_started_at", "INTEGER")
        await self._ensure_column("tokens", "above_target_last_at", "INTEGER")
        await self._ensure_column("tokens", "above_target_total_sec", "INTEGER")
        await self._ensure_column("tokens", "sim_taken", "INTEGER")
        await self._ensure_column("tokens", "sim_cash_before", "REAL")
        await self._ensure_column("tokens", "sim_cash_after", "REAL")
        await self._ensure_column("tokens", "sim_position_usd", "REAL")
        await self._ensure_column("tokens", "sim_ape_pct", "REAL")
        await self._ensure_column("tokens", "moonbag_tokens", "REAL")
        await self._ensure_column("tokens", "moonbag_sold_at", "INTEGER")
        await self._ensure_column("tokens", "moonbag_sold_value", "REAL")
        await self._ensure_column("tokens", "wallet_analysis_at", "INTEGER")
        await self._ensure_column("tokens", "wallet_analysis_json", "TEXT")
        await self._ensure_column("tokens", "wallet_analysis_partial", "INTEGER")
        await self._ensure_column("tokens", "intent_score", "INTEGER")
        await self._ensure_column("tokens", "intent_label", "TEXT")
        await self._ensure_column("tokens", "intent_at", "INTEGER")
        await self._ensure_column("tokens", "intent_json", "TEXT")
        await self._ensure_column("tokens", "hit_2x_at", "INTEGER")
        await self._ensure_column("tokens", "hit_3x_at", "INTEGER")
        await self._ensure_column("tokens", "hit_5x_at", "INTEGER")
        await self._ensure_column("tokens", "recouped_at", "INTEGER")
        await self._ensure_column("tokens", "stoploss_at", "INTEGER")
        await self._ensure_column("tokens", "stoploss_price_usd", "REAL")
        await self._migrate_token_timestamps()
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_pool (
                pair_address TEXT PRIMARY KEY,
                chain_id TEXT NOT NULL,
                token_address TEXT NOT NULL,
                last_seen_at INTEGER NOT NULL,
                last_checked_at INTEGER,
                last_hot_score REAL,
                last_metrics TEXT,
                source TEXT
            )
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_chain_id ON tokens(chain_id)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_last_checked ON tokens(last_checked_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_last_alerted ON tokens(last_alerted_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_last_eligible ON tokens(last_eligible)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_eligible_first ON tokens(eligible_first_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_called_price ON tokens(called_price_usd)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_wallet_analysis ON tokens(wallet_analysis_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pair_pool_last_seen ON pair_pool(last_seen_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pair_pool_hot_score ON pair_pool(last_hot_score)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pair_pool_last_checked ON pair_pool(last_checked_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pair_pool_token ON pair_pool(token_address)"
        )
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    async def get_token(self, token_address: str) -> Optional[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            "SELECT * FROM tokens WHERE token_address = ?",
            (token_address,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row

    async def upsert_token_seen(self, token_address: str, chain_id: str, ts: int) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            INSERT INTO tokens (token_address, chain_id, first_seen, last_seen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(token_address) DO UPDATE SET
                last_seen = excluded.last_seen
            """,
            (token_address, chain_id, ts, ts),
        )
        await self.conn.commit()

    async def update_last_checked(self, token_address: str, ts: int) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "UPDATE tokens SET last_checked_at = ? WHERE token_address = ?",
            (ts, token_address),
        )
        await self.conn.commit()

    async def update_last_alerted(self, token_address: str, ts: int) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "UPDATE tokens SET last_alerted_at = ? WHERE token_address = ?",
            (ts, token_address),
        )
        await self.conn.commit()

    async def update_token_state(
        self,
        token_address: str,
        last_checked_at: int,
        last_eligible: bool,
        last_eligible_at: Optional[int],
        last_ineligible_at: Optional[int],
        last_seen_metrics: Optional[str],
        eligible_first_at: Optional[int],
        eligible_first_metrics: Optional[str],
        last_name: Optional[str],
        last_symbol: Optional[str],
        called_price_usd: Optional[float],
        max_price_usd: Optional[float],
        min_price_usd: Optional[float],
        max_market_cap: Optional[float],
        recouped_at: Optional[int],
        stoploss_at: Optional[int],
        stoploss_price_usd: Optional[float],
        post_alert_price_usd: Optional[float],
        post_alert_at: Optional[int],
        above_target_started_at: Optional[int],
        above_target_last_at: Optional[int],
        above_target_total_sec: Optional[int],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET last_checked_at = ?,
                last_eligible = ?,
                last_eligible_at = ?,
                last_ineligible_at = ?,
                last_seen_metrics = ?,
                eligible_first_at = ?,
                eligible_first_metrics = ?,
                last_name = ?,
                last_symbol = ?,
                called_price_usd = ?,
                max_price_usd = ?,
                min_price_usd = ?,
                max_market_cap = ?,
                recouped_at = ?,
                stoploss_at = ?,
                stoploss_price_usd = ?,
                post_alert_price_usd = ?,
                post_alert_at = ?,
                above_target_started_at = ?,
                above_target_last_at = ?,
                above_target_total_sec = ?
            WHERE token_address = ?
            """,
            (
                last_checked_at,
                1 if last_eligible else 0,
                last_eligible_at,
                last_ineligible_at,
                last_seen_metrics,
                eligible_first_at,
                eligible_first_metrics,
                last_name,
                last_symbol,
                called_price_usd,
                max_price_usd,
                min_price_usd,
                max_market_cap,
                recouped_at,
                stoploss_at,
                stoploss_price_usd,
                post_alert_price_usd,
                post_alert_at,
                above_target_started_at,
                above_target_last_at,
                above_target_total_sec,
                token_address,
            ),
        )
        await self.conn.commit()

    async def update_sim_state(
        self,
        token_address: str,
        sim_taken: Optional[bool],
        sim_cash_before: Optional[float],
        sim_cash_after: Optional[float],
        sim_position_usd: Optional[float],
        sim_ape_pct: Optional[float],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET sim_taken = ?,
                sim_cash_before = ?,
                sim_cash_after = ?,
                sim_position_usd = ?,
                sim_ape_pct = ?
            WHERE token_address = ?
            """,
            (
                1 if sim_taken else 0 if sim_taken is not None else None,
                sim_cash_before,
                sim_cash_after,
                sim_position_usd,
                sim_ape_pct,
                token_address,
            ),
        )
        await self.conn.commit()

    async def update_moonbag_state(
        self,
        token_address: str,
        moonbag_tokens: Optional[float],
        moonbag_sold_at: Optional[int],
        moonbag_sold_value: Optional[float],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET moonbag_tokens = ?,
                moonbag_sold_at = ?,
                moonbag_sold_value = ?
            WHERE token_address = ?
            """,
            (moonbag_tokens, moonbag_sold_at, moonbag_sold_value, token_address),
        )
        await self.conn.commit()

    async def update_stoploss_state(
        self,
        token_address: str,
        stoploss_at: Optional[int],
        stoploss_price_usd: Optional[float],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET stoploss_at = ?,
                stoploss_price_usd = ?
            WHERE token_address = ?
            """,
            (stoploss_at, stoploss_price_usd, token_address),
        )
        await self.conn.commit()

    async def update_wallet_analysis(
        self,
        token_address: str,
        analysis_json: str,
        analysis_at: int,
        partial: bool,
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET wallet_analysis_at = ?,
                wallet_analysis_json = ?,
                wallet_analysis_partial = ?
            WHERE token_address = ?
            """,
            (analysis_at, analysis_json, 1 if partial else 0, token_address),
        )
        await self.conn.commit()

    async def update_intent_analysis(
        self,
        token_address: str,
        score: int,
        label: str,
        intent_json: str,
        intent_at: int,
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET intent_score = ?,
                intent_label = ?,
                intent_json = ?,
                intent_at = ?
            WHERE token_address = ?
            """,
            (score, label, intent_json, intent_at, token_address),
        )
        await self.conn.commit()

    async def get_tokens_missing_called_price(self, limit: int) -> List[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT token_address, eligible_first_at, eligible_first_metrics, last_seen_metrics,
                   called_price_usd, max_price_usd, min_price_usd, max_market_cap, recouped_at,
                   stoploss_at, stoploss_price_usd,
                   post_alert_price_usd, post_alert_at, above_target_started_at,
                   above_target_last_at, above_target_total_sec, sim_taken,
                   sim_position_usd, moonbag_tokens, moonbag_sold_at
            FROM tokens
            WHERE eligible_first_at IS NOT NULL
              AND called_price_usd IS NULL
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def update_called_prices(
        self,
        token_address: str,
        called_price_usd: Optional[float],
        max_price_usd: Optional[float],
        min_price_usd: Optional[float],
        max_market_cap: Optional[float],
        recouped_at: Optional[int],
        stoploss_at: Optional[int],
        stoploss_price_usd: Optional[float],
        post_alert_price_usd: Optional[float],
        post_alert_at: Optional[int],
        above_target_started_at: Optional[int],
        above_target_last_at: Optional[int],
        above_target_total_sec: Optional[int],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET called_price_usd = ?,
                max_price_usd = ?,
                min_price_usd = ?,
                max_market_cap = ?,
                recouped_at = ?,
                stoploss_at = ?,
                stoploss_price_usd = ?,
                post_alert_price_usd = ?,
                post_alert_at = ?,
                above_target_started_at = ?,
                above_target_last_at = ?,
                above_target_total_sec = ?
            WHERE token_address = ?
            """,
            (
                called_price_usd,
                max_price_usd,
                min_price_usd,
                max_market_cap,
                recouped_at,
                stoploss_at,
                stoploss_price_usd,
                post_alert_price_usd,
                post_alert_at,
                above_target_started_at,
                above_target_last_at,
                above_target_total_sec,
                token_address,
            ),
        )
        await self.conn.commit()

    async def get_called_for_performance(
        self, limit: int, min_first_at: Optional[int]
    ) -> List[aiosqlite.Row]:
        assert self.conn is not None
        if min_first_at is None:
            cur = await self.conn.execute(
                """
                SELECT token_address, eligible_first_at, last_name, last_symbol,
                       called_price_usd, max_price_usd, min_price_usd,
                       last_seen_metrics, eligible_first_metrics, recouped_at,
                       stoploss_at, stoploss_price_usd,
                       post_alert_price_usd, post_alert_at, above_target_total_sec,
                       sim_taken, sim_cash_before, sim_cash_after, sim_position_usd, sim_ape_pct,
                       moonbag_tokens, moonbag_sold_at, moonbag_sold_value
                FROM tokens
                WHERE eligible_first_at IS NOT NULL
                ORDER BY eligible_first_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        else:
            cur = await self.conn.execute(
                """
                SELECT token_address, eligible_first_at, last_name, last_symbol,
                       called_price_usd, max_price_usd, min_price_usd,
                       last_seen_metrics, eligible_first_metrics, recouped_at,
                       stoploss_at, stoploss_price_usd,
                       post_alert_price_usd, post_alert_at, above_target_total_sec,
                       sim_taken, sim_cash_before, sim_cash_after, sim_position_usd, sim_ape_pct,
                       moonbag_tokens, moonbag_sold_at, moonbag_sold_value
                FROM tokens
                WHERE eligible_first_at IS NOT NULL
                  AND eligible_first_at >= ?
                ORDER BY eligible_first_at DESC
                LIMIT ?
                """,
                (min_first_at, limit),
            )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def count_called_since(self, min_first_at: Optional[int]) -> int:
        assert self.conn is not None
        if min_first_at is None:
            cur = await self.conn.execute(
                "SELECT COUNT(*) as count FROM tokens WHERE eligible_first_at IS NOT NULL"
            )
        else:
            cur = await self.conn.execute(
                """
                SELECT COUNT(*) as count
                FROM tokens
                WHERE eligible_first_at IS NOT NULL
                  AND eligible_first_at >= ?
                """,
                (min_first_at,),
            )
        row = await cur.fetchone()
        await cur.close()
        return row["count"] if row else 0

    async def count_called_before(self, max_first_at: int) -> int:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT COUNT(*) as count
            FROM tokens
            WHERE eligible_first_at IS NOT NULL
              AND eligible_first_at < ?
            """,
            (max_first_at,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row["count"] if row else 0

    async def count_called_between(self, min_first_at: int, max_first_at: int) -> int:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT COUNT(*) as count
            FROM tokens
            WHERE eligible_first_at IS NOT NULL
              AND eligible_first_at >= ?
              AND eligible_first_at < ?
            """,
            (min_first_at, max_first_at),
        )
        row = await cur.fetchone()
        await cur.close()
        return row["count"] if row else 0

    async def get_called_before(self, limit: int, max_first_at: int) -> List[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT token_address, eligible_first_at, last_name, last_symbol,
                   called_price_usd, max_price_usd, min_price_usd,
                   last_seen_metrics, eligible_first_metrics, recouped_at,
                   stoploss_at, stoploss_price_usd,
                   post_alert_price_usd, post_alert_at, above_target_total_sec,
                   sim_taken, sim_cash_before, sim_cash_after, sim_position_usd, sim_ape_pct,
                   moonbag_tokens, moonbag_sold_at, moonbag_sold_value
            FROM tokens
            WHERE eligible_first_at IS NOT NULL
              AND eligible_first_at < ?
            ORDER BY eligible_first_at DESC
            LIMIT ?
            """,
            (max_first_at, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def get_called_for_refresh(
        self, limit: int, min_first_at: int
    ) -> List[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT token_address, eligible_first_at, eligible_first_metrics, last_seen_metrics,
                   called_price_usd, max_price_usd, min_price_usd, max_market_cap,
                   recouped_at, stoploss_at, stoploss_price_usd, post_alert_price_usd, post_alert_at,
                   above_target_started_at, above_target_last_at, above_target_total_sec, sim_taken,
                   sim_position_usd, moonbag_tokens, moonbag_sold_at
            FROM tokens
            WHERE eligible_first_at IS NOT NULL
              AND eligible_first_at >= ?
            ORDER BY COALESCE(last_checked_at, 0) ASC
            LIMIT ?
            """,
            (min_first_at, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def update_performance_snapshot(
        self,
        token_address: str,
        last_seen_metrics: Optional[str],
        last_checked_at: int,
        max_price_usd: Optional[float],
        min_price_usd: Optional[float],
        max_market_cap: Optional[float],
        recouped_at: Optional[int],
        stoploss_at: Optional[int],
        stoploss_price_usd: Optional[float],
        post_alert_price_usd: Optional[float],
        post_alert_at: Optional[int],
        above_target_started_at: Optional[int],
        above_target_last_at: Optional[int],
        above_target_total_sec: Optional[int],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE tokens
            SET last_seen_metrics = ?,
                last_checked_at = ?,
                max_price_usd = ?,
                min_price_usd = ?,
                max_market_cap = ?,
                recouped_at = ?,
                stoploss_at = ?,
                stoploss_price_usd = ?,
                post_alert_price_usd = ?,
                post_alert_at = ?,
                above_target_started_at = ?,
                above_target_last_at = ?,
                above_target_total_sec = ?
            WHERE token_address = ?
            """,
            (
                last_seen_metrics,
                last_checked_at,
                max_price_usd,
                min_price_usd,
                max_market_cap,
                recouped_at,
                stoploss_at,
                stoploss_price_usd,
                post_alert_price_usd,
                post_alert_at,
                above_target_started_at,
                above_target_last_at,
                above_target_total_sec,
                token_address,
            ),
        )
        await self.conn.commit()

    async def upsert_pair_pool(
        self,
        pair_address: str,
        chain_id: str,
        token_address: str,
        last_seen_at: int,
        last_hot_score: float,
        last_metrics: Optional[str],
        source: Optional[str],
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            INSERT INTO pair_pool (
                pair_address, chain_id, token_address, last_seen_at, last_hot_score, last_metrics, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pair_address) DO UPDATE SET
                token_address = excluded.token_address,
                last_seen_at = excluded.last_seen_at,
                last_hot_score = excluded.last_hot_score,
                last_metrics = excluded.last_metrics,
                source = excluded.source
            """,
            (
                pair_address,
                chain_id,
                token_address,
                last_seen_at,
                last_hot_score,
                last_metrics,
                source,
            ),
        )
        await self.conn.commit()

    async def update_pair_checked(
        self, pair_address: str, last_checked_at: int, last_hot_score: float, last_metrics: Optional[str]
    ) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            UPDATE pair_pool
            SET last_checked_at = ?, last_hot_score = ?, last_metrics = ?
            WHERE pair_address = ?
            """,
            (last_checked_at, last_hot_score, last_metrics, pair_address),
        )
        await self.conn.commit()

    async def trim_pair_pool(self, max_size: int) -> None:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT COUNT(*) as count FROM pair_pool")
        row = await cur.fetchone()
        await cur.close()
        count = row["count"] if row else 0
        if count <= max_size:
            return
        trim_count = count - max_size
        await self.conn.execute(
            """
            DELETE FROM pair_pool
            WHERE pair_address IN (
                SELECT pair_address FROM pair_pool
                ORDER BY last_seen_at ASC
                LIMIT ?
            )
            """,
            (trim_count,),
        )
        await self.conn.commit()

    async def purge_pair_pool(self, min_seen_at: int) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "DELETE FROM pair_pool WHERE last_seen_at < ?",
            (min_seen_at,),
        )
        await self.conn.commit()

    async def get_hot_pairs(self, limit: int, min_seen_at: int) -> List[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT pair_address, chain_id, token_address, last_hot_score, last_metrics
            FROM pair_pool
            WHERE last_seen_at >= ?
            ORDER BY last_hot_score DESC, last_checked_at ASC
            LIMIT ?
            """,
            (min_seen_at, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def count_pair_pool(self) -> int:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT COUNT(*) as count FROM pair_pool")
        row = await cur.fetchone()
        await cur.close()
        return row["count"] if row else 0

    async def get_currently_eligible(
        self, limit: int, min_first_at: int
    ) -> List[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT token_address, eligible_first_at, eligible_first_metrics,
                   last_seen_metrics, last_name, last_symbol
            FROM tokens
            WHERE last_eligible = 1
              AND eligible_first_at IS NOT NULL
              AND eligible_first_at >= ?
            ORDER BY eligible_first_at DESC
            LIMIT ?
            """,
            (min_first_at, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def get_called_since(self, limit: int, min_first_at: int) -> List[aiosqlite.Row]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT token_address, eligible_first_at, eligible_first_metrics,
                   last_seen_metrics, last_name, last_symbol,
                   called_price_usd, max_price_usd, min_price_usd, max_market_cap, recouped_at,
                   stoploss_at, stoploss_price_usd,
                   post_alert_price_usd, post_alert_at, above_target_total_sec,
                   sim_taken, sim_cash_before, sim_cash_after, sim_position_usd, sim_ape_pct,
                   moonbag_tokens, moonbag_sold_at, moonbag_sold_value
            FROM tokens
            WHERE eligible_first_at IS NOT NULL
              AND eligible_first_at >= ?
            ORDER BY eligible_first_at DESC
            LIMIT ?
            """,
            (min_first_at, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def set_state(self, key: str, value: str) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            INSERT INTO state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        await self.conn.commit()

    async def get_state(self, key: str) -> Optional[str]:
        assert self.conn is not None
        cur = await self.conn.execute(
            "SELECT value FROM state WHERE key = ?",
            (key,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row["value"] if row else None

    async def get_state_int(self, key: str, default: int = 0) -> int:
        value = await self.get_state(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    async def get_state_float(self, key: str, default: float = 0.0) -> float:
        value = await self.get_state(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default

    async def _ensure_column(self, table: str, column: str, col_type: str) -> None:
        assert self.conn is not None
        cur = await self.conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        await cur.close()
        columns = {row["name"] for row in rows}
        if column in columns:
            return
        await self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")

    async def _migrate_token_timestamps(self) -> None:
        assert self.conn is not None
        cur = await self.conn.execute("PRAGMA table_info(tokens)")
        rows = await cur.fetchall()
        await cur.close()
        columns = {row["name"] for row in rows}
        if "last_checked" in columns and "last_checked_at" in columns:
            await self.conn.execute(
                "UPDATE tokens SET last_checked_at = last_checked WHERE last_checked_at IS NULL"
            )
        if "last_alerted" in columns and "last_alerted_at" in columns:
            await self.conn.execute(
                "UPDATE tokens SET last_alerted_at = last_alerted WHERE last_alerted_at IS NULL"
            )

    async def get_state_bool(self, key: str, default: bool = False) -> bool:
        value = await self.get_state(key)
        if value is None:
            return default
        return value.lower() in ("1", "true", "yes", "on")

    async def increment_state_int(self, key: str, amount: int) -> None:
        current = await self.get_state_int(key, 0)
        await self.set_state(key, str(current + amount))
