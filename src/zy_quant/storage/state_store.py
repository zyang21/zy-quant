from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from zy_quant.models import OpenPosition, OrderSide, OrderStatus, WatchlistCandidate


class StateStore:
    """
    Fail-safe SQLite WAL persistence layer:
    - Idempotent client_order_id journal (prevents duplicate buys on daemon restart)
    - Active positions & broker server-side stop order tracking
    - Persistent Kill-Switch (HALT) latch across process restarts
    - Watchlist scoring snapshots (supports 2-hour Red-Day Relative Strength tracking)
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
                CREATE TABLE IF NOT EXISTS system_flags (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    client_order_id TEXT PRIMARY KEY,
                    broker_order_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    shares REAL NOT NULL,
                    filled_shares REAL NOT NULL DEFAULT 0.0,
                    reconciled_shares REAL NOT NULL DEFAULT 0.0,
                    limit_price REAL NOT NULL,
                    fill_price REAL,
                    avg_fill_price REAL,
                    fee_usd REAL DEFAULT 0.0,
                    status TEXT NOT NULL,
                    reason TEXT,
                    plan_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    symbol TEXT PRIMARY KEY,
                    sector TEXT NOT NULL,
                    shares REAL NOT NULL,
                    initial_shares REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_time TEXT NOT NULL,
                    initial_stop REAL NOT NULL,
                    current_stop REAL NOT NULL,
                    highest_price REAL NOT NULL,
                    risk_per_share REAL NOT NULL,
                    bars_held_15m INTEGER NOT NULL DEFAULT 0,
                    days_held INTEGER NOT NULL DEFAULT 0,
                    entry_atr_1d REAL NOT NULL DEFAULT 0.0,
                    stop_order_type TEXT NOT NULL DEFAULT 'GTC_STOP',
                    earnings_cushion_applied_date TEXT,
                    breakeven_armed INTEGER NOT NULL DEFAULT 0,
                    scaled_out INTEGER NOT NULL DEFAULT 0,
                    broker_stop_order_id TEXT,
                    setup TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS closed_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    setup TEXT NOT NULL,
                    shares REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL NOT NULL,
                    gross_pnl_usd REAL NOT NULL,
                    fee_usd REAL NOT NULL,
                    net_pnl_usd REAL NOT NULL,
                    r_multiple REAL NOT NULL,
                    exit_reason TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    exit_time TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS watchlist_scores (
                    symbol TEXT PRIMARY KEY,
                    sector TEXT NOT NULL,
                    price REAL NOT NULL,
                    total_score REAL NOT NULL,
                    passed_hard_filters INTEGER NOT NULL,
                    filter_reason TEXT NOT NULL,
                    etf_score REAL NOT NULL,
                    inst_score REAL NOT NULL,
                    stability_rs_score REAL NOT NULL,
                    big_flow_score REAL NOT NULL,
                    insider_score REAL NOT NULL,
                    rs_vs_qqq REAL NOT NULL,
                    big_order_net_usd REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS daily_pipeline_runs (
                    trading_date TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    status TEXT NOT NULL,
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY (trading_date, phase)
                );

                CREATE TABLE IF NOT EXISTS daily_nav_snapshots (
                    trading_date TEXT PRIMARY KEY,
                    trd_env TEXT NOT NULL,
                    tier_key TEXT NOT NULL,
                    equity_usd REAL NOT NULL,
                    cash_usd REAL NOT NULL,
                    accrued_sweep_yield_usd REAL NOT NULL DEFAULT 0.0,
                    principal_heat_pct REAL NOT NULL DEFAULT 0.0,
                    open_drawdown_heat_pct REAL NOT NULL DEFAULT 0.0,
                    open_positions_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                """
            )
            # Idempotent schema migration for pre-existing SQLite DBs
            existing_cols = {
                r["name"]
                for r in conn.execute("PRAGMA table_info(orders)").fetchall()
            }
            migrations = [
                ("filled_shares", "REAL NOT NULL DEFAULT 0.0"),
                ("reconciled_shares", "REAL NOT NULL DEFAULT 0.0"),
                ("avg_fill_price", "REAL"),
                ("plan_json", "TEXT NOT NULL DEFAULT '{}'"),
            ]
            for col_name, col_def in migrations:
                if col_name not in existing_cols:
                    conn.execute(f"ALTER TABLE orders ADD COLUMN {col_name} {col_def}")

            existing_pos_cols = {
                r["name"]
                for r in conn.execute("PRAGMA table_info(positions)").fetchall()
            }
            pos_migrations = [
                ("days_held", "INTEGER NOT NULL DEFAULT 0"),
                ("entry_atr_1d", "REAL NOT NULL DEFAULT 0.0"),
                ("stop_order_type", "TEXT NOT NULL DEFAULT 'GTC_STOP'"),
                ("earnings_cushion_applied_date", "TEXT"),
            ]
            for col_name, col_def in pos_migrations:
                if col_name not in existing_pos_cols:
                    conn.execute(f"ALTER TABLE positions ADD COLUMN {col_name} {col_def}")

    # --- Kill-Switch / Flags ---
    def set_halted(self, halted: bool, reason: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps({"halted": halted, "reason": reason})
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO system_flags(key, value, updated_at)
                VALUES('kill_switch', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (payload, now),
            )

    def get_halt_state(self) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value, updated_at FROM system_flags WHERE key='kill_switch'"
            ).fetchone()
        if not row:
            return {"halted": False, "reason": "", "updated_at": None}
        data = json.loads(row["value"])
        data["updated_at"] = row["updated_at"]
        return data

    def set_flag(self, key: str, data: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO system_flags(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, json.dumps(data), now),
            )

    def get_flag(self, key: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM system_flags WHERE key=?", (key,)
            ).fetchone()
        return json.loads(row["value"]) if row else None

    # --- Idempotent Order Journal ---
    def record_order_intent(
        self,
        client_order_id: str,
        symbol: str,
        side: OrderSide,
        shares: float,
        limit_price: float,
        reason: str,
        plan_meta: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> bool:
        """Returns True if inserted cleanly, False if client_order_id already exists (idempotent guard)."""
        now = (created_at or datetime.now(timezone.utc)).isoformat()
        plan_str = json.dumps(plan_meta or {})
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO orders(
                        client_order_id, symbol, side, shares, filled_shares, reconciled_shares,
                        limit_price, status, reason, plan_json, created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, 0.0, 0.0, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        client_order_id,
                        symbol,
                        side.value,
                        shares,
                        limit_price,
                        OrderStatus.PENDING.value,
                        reason,
                        plan_str,
                        now,
                        now,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def update_order_status(
        self,
        client_order_id: str,
        status: OrderStatus,
        broker_order_id: Optional[str] = None,
        fill_price: Optional[float] = None,
        fee_usd: Optional[float] = None,
        filled_shares: Optional[float] = None,
        avg_fill_price: Optional[float] = None,
        reconciled_shares: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        eff_avg_px = avg_fill_price if avg_fill_price is not None else fill_price
        eff_fill_px = fill_price if fill_price is not None else avg_fill_price
        # Backward compatibility: direct FILLED calls without explicit filled_shares fill 100% of shares
        with self._connect() as conn:
            if filled_shares is None and status == OrderStatus.FILLED:
                row = conn.execute(
                    "SELECT shares, filled_shares FROM orders WHERE client_order_id=?",
                    (client_order_id,),
                ).fetchone()
                if row and float(row["filled_shares"] or 0.0) <= 0.0:
                    filled_shares = float(row["shares"])

            conn.execute(
                """
                UPDATE orders
                SET status=?,
                    broker_order_id=COALESCE(?, broker_order_id),
                    fill_price=COALESCE(?, fill_price),
                    avg_fill_price=COALESCE(?, avg_fill_price),
                    fee_usd=COALESCE(?, fee_usd),
                    filled_shares=COALESCE(?, filled_shares),
                    reconciled_shares=COALESCE(?, reconciled_shares),
                    reason=COALESCE(?, reason),
                    updated_at=?
                WHERE client_order_id=?
                """,
                (
                    status.value,
                    broker_order_id,
                    eff_fill_px,
                    eff_avg_px,
                    fee_usd,
                    filled_shares,
                    reconciled_shares,
                    reason,
                    now,
                    client_order_id,
                ),
            )

    def get_order(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)
            ).fetchone()
        return dict(row) if row else None

    def find_order_for_broker_update(
        self, broker_order_id: str, remark: str = ""
    ) -> Optional[Dict[str, Any]]:
        """
        Matches an incoming Futu push or poll record against local SQLite orders:
        1. Primary match by broker_order_id
        2. Fallback match by remark == client_order_id (handles race where Futu push arrives
           milliseconds before place_order() returns broker_order_id on main thread!)
        """
        with self._connect() as conn:
            if broker_order_id:
                row = conn.execute(
                    "SELECT * FROM orders WHERE broker_order_id=?", (str(broker_order_id),)
                ).fetchone()
                if row:
                    return dict(row)
            if remark:
                row = conn.execute(
                    "SELECT * FROM orders WHERE client_order_id=?", (remark.strip(),)
                ).fetchone()
                if row:
                    return dict(row)
        return None

    def get_active_orders(
        self, side: Optional[OrderSide] = None, symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Returns in-flight orders (PENDING, SUBMITTED, PARTIAL_FILLED) that lock slots/cash."""
        clauses = ["status IN ('PENDING', 'SUBMITTED', 'PARTIAL_FILLED')"]
        params: List[Any] = []
        if side is not None:
            clauses.append("side = ?")
            params.append(side.value)
        if symbol is not None:
            clauses.append("UPPER(symbol) = ?")
            params.append(symbol.upper())
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM orders WHERE {' AND '.join(clauses)} ORDER BY created_at ASC",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def count_orders_since(self, since_iso: str, side: Optional[OrderSide] = None) -> int:
        query = "SELECT COUNT(*) AS cnt FROM orders WHERE created_at >= ? AND status != 'REJECTED'"
        params: list[Any] = [since_iso]
        if side is not None:
            query += " AND side = ?"
            params.append(side.value)
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["cnt"]) if row else 0

    def count_trade_budget_orders_since(
        self, since_iso: str, side: Optional[OrderSide] = None
    ) -> int:
        """
        Counts orders toward daily/weekly turnover caps:
        Includes all active in-flight orders AND any terminal order that filled > 0 shares,
        while excluding 0-share unfilled orders that expired/cancelled ($0 fee churn).
        """
        query = """
            SELECT COUNT(*) AS cnt FROM orders
            WHERE created_at >= ?
              AND status NOT IN ('REJECTED', 'FAILED')
              AND NOT (status IN ('CANCELLED', 'EXPIRED') AND COALESCE(filled_shares, 0.0) <= 0.0)
        """
        params: list[Any] = [since_iso]
        if side is not None:
            query += " AND side = ?"
            params.append(side.value)
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["cnt"]) if row else 0

    def list_recent_orders(self, limit: int = 30) -> List[Dict[str, Any]]:
        return self.query_orders(limit=limit)

    def query_orders(
        self,
        start_iso: Optional[str] = None,
        end_iso: Optional[str] = None,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if start_iso:
            clauses.append("created_at >= ?")
            params.append(start_iso)
        if end_iso:
            clauses.append("created_at <= ?")
            params.append(end_iso)
        if symbol:
            clauses.append("UPPER(symbol) = ?")
            params.append(symbol.upper())
        if side:
            clauses.append("UPPER(side) = ?")
            params.append(side.upper())

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM orders {where_sql} ORDER BY created_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Positions & Broker Reconciliation ---
    def upsert_position(self, pos: OpenPosition) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO positions(
                    symbol, sector, shares, initial_shares, entry_price, entry_time,
                    initial_stop, current_stop, highest_price, risk_per_share,
                    bars_held_15m, days_held, entry_atr_1d, stop_order_type,
                    earnings_cushion_applied_date, breakeven_armed, scaled_out,
                    broker_stop_order_id, setup, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    shares=excluded.shares,
                    initial_shares=excluded.initial_shares,
                    entry_price=excluded.entry_price,
                    current_stop=excluded.current_stop,
                    highest_price=excluded.highest_price,
                    risk_per_share=excluded.risk_per_share,
                    bars_held_15m=excluded.bars_held_15m,
                    days_held=excluded.days_held,
                    entry_atr_1d=excluded.entry_atr_1d,
                    stop_order_type=excluded.stop_order_type,
                    earnings_cushion_applied_date=excluded.earnings_cushion_applied_date,
                    breakeven_armed=excluded.breakeven_armed,
                    scaled_out=excluded.scaled_out,
                    broker_stop_order_id=excluded.broker_stop_order_id,
                    metadata_json=excluded.metadata_json
                """,
                (
                    pos.symbol,
                    pos.sector,
                    pos.shares,
                    pos.initial_shares,
                    pos.entry_price,
                    pos.entry_time.isoformat(),
                    pos.initial_stop,
                    pos.current_stop,
                    pos.highest_price,
                    pos.risk_per_share,
                    pos.bars_held_15m,
                    pos.days_held,
                    pos.entry_atr_1d,
                    pos.stop_order_type,
                    pos.earnings_cushion_applied_date,
                    int(pos.breakeven_armed),
                    int(pos.scaled_out),
                    pos.broker_stop_order_id,
                    pos.setup,
                    json.dumps(pos.metadata),
                ),
            )

    def remove_position(self, symbol: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM positions WHERE symbol=?", (symbol,))

    def get_positions(self) -> Dict[str, OpenPosition]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM positions").fetchall()
        out: Dict[str, OpenPosition] = {}
        for r in rows:
            keys = r.keys()
            out[r["symbol"]] = OpenPosition(
                symbol=r["symbol"],
                sector=r["sector"],
                shares=float(r["shares"]),
                initial_shares=float(r["initial_shares"]),
                entry_price=float(r["entry_price"]),
                entry_time=datetime.fromisoformat(r["entry_time"]),
                initial_stop=float(r["initial_stop"]),
                current_stop=float(r["current_stop"]),
                highest_price=float(r["highest_price"]),
                risk_per_share=float(r["risk_per_share"]),
                bars_held_15m=int(r["bars_held_15m"]),
                days_held=int(r["days_held"] or 0) if "days_held" in keys else 0,
                entry_atr_1d=float(r["entry_atr_1d"] or 0.0) if "entry_atr_1d" in keys else 0.0,
                stop_order_type=str(r["stop_order_type"] or "GTC_STOP") if "stop_order_type" in keys else "GTC_STOP",
                earnings_cushion_applied_date=r["earnings_cushion_applied_date"] if "earnings_cushion_applied_date" in keys else None,
                breakeven_armed=bool(r["breakeven_armed"]),
                scaled_out=bool(r["scaled_out"]),
                broker_stop_order_id=r["broker_stop_order_id"],
                setup=r["setup"],
                metadata=json.loads(r["metadata_json"] or "{}"),
            )
        return out

    def reconcile_broker_positions(
        self,
        broker_holdings: Dict[str, Dict[str, float]],
        default_stop_pct: float = 0.025,
    ) -> Dict[str, List[str]]:
        """
        Startup crash-recovery reconciliation:
        - Adopts unknown live broker positions with a protective fallback stop so they are never orphaned
        - Updates share counts on partial fills
        - Cleans up local phantom positions already closed on broker side (e.g. server-side stop triggered while Mac was asleep)
        """
        local = self.get_positions()
        adopted, updated, closed_remotely = [], [], []
        now = datetime.now(timezone.utc)

        for sym, info in broker_holdings.items():
            qty = float(info.get("shares", 0.0))
            avg_px = float(info.get("avg_price", 0.0))
            if qty <= 0:
                continue
            if sym not in local:
                init_stop = round(avg_px * (1.0 - default_stop_pct), 2)
                self.upsert_position(
                    OpenPosition(
                        symbol=sym,
                        sector=str(info.get("sector", "Unknown")),
                        shares=qty,
                        initial_shares=qty,
                        entry_price=avg_px,
                        entry_time=now,
                        initial_stop=init_stop,
                        current_stop=init_stop,
                        highest_price=avg_px,
                        risk_per_share=max(0.01, avg_px - init_stop),
                        setup="RECONCILED_ADOPTED",
                    )
                )
                adopted.append(sym)
            elif abs(local[sym].shares - qty) > 1e-6:
                pos = local[sym]
                pos.shares = qty
                self.upsert_position(pos)
                updated.append(sym)

        for sym in list(local.keys()):
            if sym not in broker_holdings or float(broker_holdings[sym].get("shares", 0.0)) <= 0:
                self.remove_position(sym)
                closed_remotely.append(sym)

        return {
            "adopted": adopted,
            "updated": updated,
            "closed_remotely": closed_remotely,
        }

    # --- Trade Ledger & Daily Realized PnL ---
    def record_closed_trade(
        self,
        symbol: str,
        setup: str,
        shares: float,
        entry_price: float,
        exit_price: float,
        fee_usd: float,
        risk_per_share: float,
        exit_reason: str,
        entry_time: str,
        exit_time: Optional[str] = None,
    ) -> None:
        exit_ts = exit_time or datetime.now(timezone.utc).isoformat()
        gross = (exit_price - entry_price) * shares
        net = gross - fee_usd
        denom = max(0.01, risk_per_share * shares)
        r_mult = net / denom
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO closed_trades(
                    symbol, setup, shares, entry_price, exit_price,
                    gross_pnl_usd, fee_usd, net_pnl_usd, r_multiple, exit_reason, entry_time, exit_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    setup,
                    shares,
                    entry_price,
                    exit_price,
                    gross,
                    fee_usd,
                    net,
                    r_mult,
                    exit_reason,
                    entry_time,
                    exit_ts,
                ),
            )

    def realized_pnl_since(self, since_iso: str) -> float:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(net_pnl_usd), 0.0) AS s FROM closed_trades WHERE exit_time >= ?",
                (since_iso,),
            ).fetchone()
        return float(row["s"]) if row else 0.0

    def list_closed_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.query_closed_trades(limit=limit)

    def query_closed_trades(
        self,
        start_iso: Optional[str] = None,
        end_iso: Optional[str] = None,
        symbol: Optional[str] = None,
        exit_reason: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if start_iso:
            clauses.append("exit_time >= ?")
            params.append(start_iso)
        if end_iso:
            clauses.append("exit_time <= ?")
            params.append(end_iso)
        if symbol:
            clauses.append("UPPER(symbol) = ?")
            params.append(symbol.upper())
        if exit_reason:
            clauses.append("exit_reason = ?")
            params.append(exit_reason)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM closed_trades {where_sql} ORDER BY exit_time DESC, id DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Watchlist Persistence ---
    def save_watchlist_scores(
        self, items: List[WatchlistCandidate], prune_stale: bool = True
    ) -> None:
        with self._connect() as conn:
            if prune_stale and items:
                keep_syms = [item.symbol.upper() for item in items]
                placeholders = ",".join("?" for _ in keep_syms)
                conn.execute(
                    f"DELETE FROM watchlist_scores WHERE UPPER(symbol) NOT IN ({placeholders})",
                    keep_syms,
                )
            for item in items:
                conn.execute(
                    """
                    INSERT INTO watchlist_scores(
                        symbol, sector, price, total_score, passed_hard_filters, filter_reason,
                        etf_score, inst_score, stability_rs_score, big_flow_score, insider_score,
                        rs_vs_qqq, big_order_net_usd, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        sector=excluded.sector,
                        price=excluded.price,
                        total_score=excluded.total_score,
                        passed_hard_filters=excluded.passed_hard_filters,
                        filter_reason=excluded.filter_reason,
                        etf_score=excluded.etf_score,
                        inst_score=excluded.inst_score,
                        stability_rs_score=excluded.stability_rs_score,
                        big_flow_score=excluded.big_flow_score,
                        insider_score=excluded.insider_score,
                        rs_vs_qqq=excluded.rs_vs_qqq,
                        big_order_net_usd=excluded.big_order_net_usd,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.symbol,
                        item.sector,
                        item.price,
                        item.total_score,
                        int(item.passed_hard_filters),
                        item.filter_reason,
                        item.etf_component_score,
                        item.institutional_component_score,
                        item.stability_rs_component_score,
                        item.big_flow_component_score,
                        item.insider_component_score,
                        item.relative_strength_vs_qqq,
                        item.big_order_net_usd,
                        item.updated_at.isoformat(),
                    ),
                )

    def get_watchlist_scores(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM watchlist_scores ORDER BY passed_hard_filters DESC, total_score DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # --- 4-Phase Daily Swing Pipeline Checkpoints & NAV Snapshots ---
    def has_completed_pipeline_phase(self, trading_date_iso: str, phase: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM daily_pipeline_runs WHERE trading_date=? AND phase=?",
                (trading_date_iso, phase),
            ).fetchone()
        return bool(row and row["status"] == "COMPLETED")

    def record_pipeline_phase(
        self,
        trading_date_iso: str,
        phase: str,
        status: str,
        summary: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_pipeline_runs(trading_date, phase, status, summary_json, completed_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(trading_date, phase) DO UPDATE SET
                    status=excluded.status,
                    summary_json=excluded.summary_json,
                    completed_at=excluded.completed_at
                """,
                (trading_date_iso, phase, status, json.dumps(summary or {}), now),
            )

    def list_pipeline_runs(self, trading_date_iso: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            if trading_date_iso:
                rows = conn.execute(
                    "SELECT * FROM daily_pipeline_runs WHERE trading_date=? ORDER BY completed_at ASC",
                    (trading_date_iso,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM daily_pipeline_runs ORDER BY completed_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [dict(r) for r in rows]

    def record_daily_nav_snapshot(
        self,
        trading_date_iso: str,
        trd_env: str,
        tier_key: str,
        equity_usd: float,
        cash_usd: float,
        accrued_sweep_yield_usd: float = 0.0,
        principal_heat_pct: float = 0.0,
        open_drawdown_heat_pct: float = 0.0,
        open_positions_count: int = 0,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_nav_snapshots(
                    trading_date, trd_env, tier_key, equity_usd, cash_usd,
                    accrued_sweep_yield_usd, principal_heat_pct, open_drawdown_heat_pct,
                    open_positions_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trading_date) DO UPDATE SET
                    trd_env=excluded.trd_env,
                    tier_key=excluded.tier_key,
                    equity_usd=excluded.equity_usd,
                    cash_usd=excluded.cash_usd,
                    accrued_sweep_yield_usd=excluded.accrued_sweep_yield_usd,
                    principal_heat_pct=excluded.principal_heat_pct,
                    open_drawdown_heat_pct=excluded.open_drawdown_heat_pct,
                    open_positions_count=excluded.open_positions_count,
                    created_at=excluded.created_at
                """,
                (
                    trading_date_iso,
                    trd_env,
                    tier_key,
                    round(equity_usd, 2),
                    round(cash_usd, 2),
                    round(accrued_sweep_yield_usd, 2),
                    round(principal_heat_pct, 3),
                    round(open_drawdown_heat_pct, 3),
                    int(open_positions_count),
                    now,
                ),
            )

    # --- StoplossGuard Distinct Episode Query ---
    def list_stop_out_episodes_since(self, after_iso: str = "") -> List[Dict[str, Any]]:
        """
        Groups INITIAL_ATR_STOP trades by `(symbol, entry_time)` so partial stop-fill prints
        never multiply a single stopped-out position into multiple triggers, filtering strictly
        after `after_iso` (`cooldown_triggered_at` watermark).
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol,
                       entry_time,
                       MAX(exit_time) AS exit_time,
                       SUM(net_pnl_usd) AS net_pnl_usd,
                       MIN(r_multiple) AS min_r_multiple
                FROM closed_trades
                WHERE exit_reason = 'INITIAL_ATR_STOP'
                  AND exit_time > ?
                GROUP BY symbol, entry_time
                HAVING MIN(r_multiple) < -0.05
                ORDER BY exit_time ASC
                """,
                (after_iso,),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Stateful Virtual Cash Ledger & Arithmetic Reconciliation (SIMULATE) ---
    def _compute_open_cost_basis(self) -> float:
        positions = self.get_positions()
        return sum(
            p.shares * p.entry_price
            + float(p.metadata.get("buy_fee_usd") or 0.0)
            * (p.shares / max(1e-6, p.initial_shares))
            for p in positions.values()
        )

    def get_virtual_cash(self, tier_key: str, starting_equity_usd: float) -> float:
        flag_key = f"virtual_cash_{tier_key}"
        flag = self.get_flag(flag_key)
        if flag is not None and "cash_usd" in flag:
            return float(flag["cash_usd"])
        # Bootstrap cleanly from existing realized PnL and open cost basis on first read
        realized = self.realized_pnl_since("1970-01-01T00:00:00")
        open_cost = self._compute_open_cost_basis()
        init_cash = round(starting_equity_usd + realized - open_cost, 4)
        self.set_flag(
            flag_key,
            {"cash_usd": init_cash, "starting_equity_usd": starting_equity_usd},
        )
        return init_cash

    def adjust_virtual_cash(
        self, tier_key: str, delta_usd: float, starting_equity_usd: float
    ) -> float:
        curr = self.get_virtual_cash(tier_key, starting_equity_usd)
        updated = round(curr + delta_usd, 4)
        self.set_flag(
            f"virtual_cash_{tier_key}",
            {"cash_usd": updated, "starting_equity_usd": starting_equity_usd},
        )
        return updated

    def reconcile_stateful_ledger(
        self,
        tier_key: str,
        starting_equity_usd: float,
        tolerance_usd: float = 0.05,
    ) -> Dict[str, Any]:
        """
        Independent arithmetic identity check (non-tautological):
        Verifies `starting_equity + sum(realized_net_pnl) == stateful_virtual_cash + open_cost_basis`
        (including unamortized `buy_fee_usd` on partially scaled-out positions) within `$0.05`.
        """
        v_cash = self.get_virtual_cash(tier_key, starting_equity_usd)
        realized = self.realized_pnl_since("1970-01-01T00:00:00")
        open_cost = self._compute_open_cost_basis()
        expected_lhs = round(starting_equity_usd + realized, 4)
        actual_rhs = round(v_cash + open_cost, 4)
        drift = round(actual_rhs - expected_lhs, 4)
        return {
            "ok": abs(drift) <= tolerance_usd,
            "starting_equity_usd": round(starting_equity_usd, 2),
            "realized_net_pnl_usd": round(realized, 4),
            "virtual_cash_usd": round(v_cash, 4),
            "open_cost_basis_usd": round(open_cost, 4),
            "drift_usd": drift,
            "tolerance_usd": tolerance_usd,
        }
