from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from zy_quant.models import Bar


class HistoricalBarCache:
    """
    High-speed local SQLite WAL market bar cache (`data/cache/market_bars.db`):
    - Stores split-adjusted 15m (RTH) and 1d bars indexed by (symbol, timeframe, ts_epoch)
    - Enables multi-year (2020-2026) 100-stock walk-forward backtests to run offline in seconds
    - Supports incremental delta fetching so only uncached historical windows hit Alpaca REST API
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS market_bars (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    ts_epoch INTEGER NOT NULL,
                    ts_iso TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    PRIMARY KEY (symbol, timeframe, ts_epoch)
                ) WITHOUT ROWID;
                """
            )

    @staticmethod
    def _to_utc_epoch(dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.astimezone(timezone.utc).timestamp())

    def upsert_bars(self, timeframe: str, bars: Sequence[Bar]) -> int:
        if not bars:
            return 0
        rows = [
            (
                b.symbol.upper(),
                timeframe,
                self._to_utc_epoch(b.timestamp),
                (
                    b.timestamp.replace(tzinfo=timezone.utc)
                    if b.timestamp.tzinfo is None
                    else b.timestamp.astimezone(timezone.utc)
                ).isoformat(),
                float(b.open),
                float(b.high),
                float(b.low),
                float(b.close),
                float(b.volume),
            )
            for b in bars
        ]
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO market_bars(symbol, timeframe, ts_epoch, ts_iso, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe, ts_epoch) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume
                """,
                rows,
            )
        return len(rows)

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> List[Bar]:
        sym = symbol.upper()
        clauses = ["symbol = ?", "timeframe = ?"]
        params: list[object] = [sym, timeframe]
        if start_dt is not None:
            clauses.append("ts_epoch >= ?")
            params.append(self._to_utc_epoch(start_dt))
        if end_dt is not None:
            clauses.append("ts_epoch <= ?")
            params.append(self._to_utc_epoch(end_dt))

        sql = (
            f"SELECT symbol, ts_iso, open, high, low, close, volume "
            f"FROM market_bars WHERE {' AND '.join(clauses)} ORDER BY ts_epoch ASC"
        )
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [
            Bar(
                symbol=r["symbol"],
                timestamp=datetime.fromisoformat(r["ts_iso"]),
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=float(r["volume"]),
            )
            for r in rows
        ]

    def get_bars_batch(
        self,
        symbols: Sequence[str],
        timeframe: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> Dict[str, List[Bar]]:
        if not symbols:
            return {}
        clean_syms = [s.upper() for s in symbols]
        placeholders = ",".join("?" for _ in clean_syms)
        clauses = [f"symbol IN ({placeholders})", "timeframe = ?"]
        params: list[object] = [*clean_syms, timeframe]
        if start_dt is not None:
            clauses.append("ts_epoch >= ?")
            params.append(self._to_utc_epoch(start_dt))
        if end_dt is not None:
            clauses.append("ts_epoch <= ?")
            params.append(self._to_utc_epoch(end_dt))

        sql = (
            f"SELECT symbol, ts_iso, open, high, low, close, volume "
            f"FROM market_bars WHERE {' AND '.join(clauses)} ORDER BY symbol ASC, ts_epoch ASC"
        )
        out: Dict[str, List[Bar]] = {s: [] for s in clean_syms}
        with self._connect() as conn:
            for r in conn.execute(sql, params):
                out[r["symbol"]].append(
                    Bar(
                        symbol=r["symbol"],
                        timestamp=datetime.fromisoformat(r["ts_iso"]),
                        open=float(r["open"]),
                        high=float(r["high"]),
                        low=float(r["low"]),
                        close=float(r["close"]),
                        volume=float(r["volume"]),
                    )
                )
        return out

    def coverage_bounds(self, symbol: str, timeframe: str) -> Optional[tuple[datetime, datetime, int]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(ts_epoch) AS min_ts, MAX(ts_epoch) AS max_ts, COUNT(*) AS cnt
                FROM market_bars
                WHERE symbol = ? AND timeframe = ?
                """,
                (symbol.upper(), timeframe),
            ).fetchone()
        if not row or not row["cnt"] or row["min_ts"] is None or row["max_ts"] is None:
            return None
        return (
            datetime.fromtimestamp(int(row["min_ts"]), tz=timezone.utc),
            datetime.fromtimestamp(int(row["max_ts"]), tz=timezone.utc),
            int(row["cnt"]),
        )

    def stats(self) -> Dict[str, int]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total_bars, COUNT(DISTINCT symbol) AS distinct_symbols FROM market_bars"
            ).fetchone()
        return {
            "total_bars": int(row["total_bars"] or 0) if row else 0,
            "distinct_symbols": int(row["distinct_symbols"] or 0) if row else 0,
        }
