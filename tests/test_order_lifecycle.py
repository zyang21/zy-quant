from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from zy_quant.config import AppSettings
from zy_quant.gateway.base import BrokerExecutionAdapter, MarketDataAdapter
from zy_quant.gateway.futu_adapter import (
    FutuGatewayAdapter,
    map_futu_order_status,
    parse_futu_order_row,
)
from zy_quant.models import (
    Bar,
    BrokerOrderUpdate,
    CapitalFlowSnapshot,
    OrderSide,
    OrderStatus,
    PositionPlan,
    QuoteSnapshot,
    SetupType,
    WatchlistCandidate,
)
from zy_quant.risk.mandate_gate import MandateRiskGate
from zy_quant.runtime.alerts import AlertNotifier
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.storage.state_store import StateStore


class FakeAsyncBrokerGateway(MarketDataAdapter, BrokerExecutionAdapter):
    """Deterministic test double for Futu SIMULATE/REAL async order push & polling."""

    def __init__(self, has_account: bool = True, trd_env: str = "SIMULATE") -> None:
        self._has_account = has_account
        self.trd_env_name = trd_env
        self._callbacks: List[Callable[[BrokerOrderUpdate], Any]] = []
        self.open_orders: Dict[str, BrokerOrderUpdate] = {}
        self.cancelled_order_ids: List[str] = []
        self.protective_stops: Dict[str, tuple[float, float, str]] = {}
        self.exact_fees: Dict[str, float] = {}
        self._next_oid = 1000
        self.bars_15m: Dict[str, List[Bar]] = {}
        self.bars_1d: Dict[str, List[Bar]] = {}
        self.push_synchronously_before_return = False

    def is_connected(self) -> bool:
        return True

    def connect(self) -> bool:
        return True

    def has_target_account(self) -> bool:
        return self._has_account

    def register_order_callback(self, callback: Callable[[BrokerOrderUpdate], Any]) -> None:
        self._callbacks.append(callback)

    def emit_push(self, update: BrokerOrderUpdate) -> None:
        self.open_orders[update.broker_order_id] = update
        for cb in list(self._callbacks):
            cb(update)

    def sync_open_orders(self, refresh_cache: bool = False) -> List[BrokerOrderUpdate]:
        return list(self.open_orders.values())

    def query_order_fee(self, order_id: str) -> Optional[float]:
        return self.exact_fees.get(order_id)

    def get_account_summary(self) -> Dict[str, float]:
        return {
            "equity_usd": 10000.0,
            "cash_usd": 10000.0,
            "market_val_usd": 0.0,
            "buying_power_usd": 10000.0,
            "connected": 1.0,
        }

    def get_open_positions(self) -> Dict[str, Dict[str, float]]:
        return {}

    def place_limit_order(
        self, symbol: str, side: OrderSide, shares: float, limit_price: float, remark: str = ""
    ) -> Optional[str]:
        self._next_oid += 1
        oid = f"FUTU-{self._next_oid}"
        upd = BrokerOrderUpdate(
            broker_order_id=oid,
            symbol=symbol,
            side=side,
            order_type="NORMAL",
            status=OrderStatus.SUBMITTED,
            raw_status="SUBMITTED",
            qty=shares,
            price=limit_price,
            filled_shares=0.0,
            avg_fill_price=None,
            remark=remark,
        )
        self.open_orders[oid] = upd
        if self.push_synchronously_before_return:
            # Simulate fast socket thread delivering FILLED_ALL before place_order returns on main thread!
            fast_fill = BrokerOrderUpdate(
                broker_order_id=oid,
                symbol=symbol,
                side=side,
                order_type="NORMAL",
                status=OrderStatus.FILLED,
                raw_status="FILLED_ALL",
                qty=shares,
                price=limit_price,
                filled_shares=shares,
                avg_fill_price=round(limit_price - 0.05, 2),
                remark=remark,
            )
            self.emit_push(fast_fill)
        return oid

    def place_or_update_protective_stop(
        self,
        symbol: str,
        shares: float,
        stop_price: float,
        existing_order_id: Optional[str] = None,
    ) -> Optional[str]:
        oid = existing_order_id or f"STOP-{symbol}-9001"
        self.protective_stops[symbol] = (shares, stop_price, oid)
        return oid

    def cancel_order(self, order_id: str) -> bool:
        self.cancelled_order_ids.append(order_id)
        existing = self.open_orders.get(order_id)
        if existing and existing.status.is_active:
            raw = "CANCELLED_PART" if existing.filled_shares > 0 else "CANCELLED_ALL"
            self.open_orders[order_id] = BrokerOrderUpdate(
                broker_order_id=existing.broker_order_id,
                symbol=existing.symbol,
                side=existing.side,
                order_type=existing.order_type,
                status=OrderStatus.CANCELLED,
                raw_status=raw,
                qty=existing.qty,
                price=existing.price,
                filled_shares=existing.filled_shares,
                avg_fill_price=existing.avg_fill_price,
                remark=existing.remark,
            )
        return True

    def get_bars(self, symbol: str, timeframe: str = "15m", days: int = 30) -> List[Bar]:
        if timeframe == "15m":
            return self.bars_15m.get(symbol, [])
        return self.bars_1d.get(symbol, [])

    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        return QuoteSnapshot(
            symbol=symbol,
            last_price=140.0,
            bid=139.98,
            ask=140.02,
            avg_daily_dollar_vol=2_000_000_000.0,
            timestamp=datetime(2026, 11, 25, 16, 0, tzinfo=timezone.utc),
        )

    def get_capital_flow(self, symbol: str) -> Optional[CapitalFlowSnapshot]:
        return CapitalFlowSnapshot(
            symbol=symbol,
            super_large_inflow=5_000_000.0,
            super_large_outflow=1_000_000.0,
            large_inflow=3_000_000.0,
            large_outflow=1_000_000.0,
        )

    def get_vix_level(self) -> float:
        return 16.5


def _build_orb_breakout_bars(symbol: str, day_utc: datetime) -> tuple[List[Bar], List[Bar]]:
    """Constructs clean daily uptrend + 15m 30m-ORB breakout bars inside prime window (10:15 ET)."""
    daily: List[Bar] = []
    for i in range(25):
        dt = day_utc - timedelta(days=26 - i)
        px = 125.0 + i * 0.6
        daily.append(Bar(symbol, dt, px - 0.5, px + 1.0, px - 1.0, px, 10_000_000.0))

    bars_15m: List[Bar] = []
    prev_day = day_utc - timedelta(days=1)
    # 22 prior-day 15m bars with ~1.25 ATR:
    # satisfies tier_1k expected_move >= 3.2% AND sizes 5 shares ($705 >= $450 min notional)
    for k in range(22):
        ts = prev_day.replace(hour=14, minute=30) + timedelta(minutes=15 * k)
        base = 138.5 + (k % 2) * 0.15
        bars_15m.append(Bar(symbol, ts, base, base + 0.65, base - 0.60, base + 0.05, 200_000.0))

    # Today's session (EST = UTC-5 in Nov):
    # Bar #1 (09:30 ET = 14:30 UTC), Bar #2 (09:45 ET = 14:45 UTC) -> ORB high = 140.00
    t0 = day_utc.replace(hour=14, minute=30, second=0, microsecond=0)
    bars_15m.append(Bar(symbol, t0, 139.0, 139.8, 138.8, 139.5, 220_000.0))
    bars_15m.append(Bar(symbol, t0 + timedelta(minutes=15), 139.5, 140.0, 139.2, 139.8, 230_000.0))
    # Bar #3 (10:00 ET = 15:00 UTC): fresh breakout above 140.00 on 2.5x RVOL!
    bars_15m.append(
        Bar(symbol, t0 + timedelta(minutes=30), 139.9, 141.2, 139.8, 141.0, 620_000.0)
    )
    return bars_15m, daily


def _seed_bullish_market_and_watchlist(
    gw: FakeAsyncBrokerGateway, store: StateStore, now_utc: datetime
) -> None:
    qqq_15m, qqq_1d = _build_orb_breakout_bars("QQQ", now_utc)
    _, spy_1d = _build_orb_breakout_bars("SPY", now_utc)
    nvda_15m, nvda_1d = _build_orb_breakout_bars("NVDA", now_utc)
    msft_15m, msft_1d = _build_orb_breakout_bars("MSFT", now_utc)

    gw.bars_15m["QQQ"] = qqq_15m
    gw.bars_1d["QQQ"] = qqq_1d
    gw.bars_1d["SPY"] = spy_1d
    gw.bars_15m["NVDA"] = nvda_15m
    gw.bars_1d["NVDA"] = nvda_1d
    gw.bars_15m["MSFT"] = msft_15m
    gw.bars_1d["MSFT"] = msft_1d

    store.save_watchlist_scores(
        [
            WatchlistCandidate(
                symbol="NVDA",
                sector="Semiconductors",
                price=141.0,
                total_score=86.0,
                passed_hard_filters=True,
                filter_reason="PASS",
                etf_component_score=29.0,
                institutional_component_score=18.0,
                stability_rs_component_score=13.0,
                big_flow_component_score=16.0,
                insider_component_score=10.0,
                relative_strength_vs_qqq=2.5,
                big_order_net_usd=6_000_000.0,
                updated_at=now_utc,
            ),
            WatchlistCandidate(
                symbol="MSFT",
                sector="Technology",
                price=141.0,
                total_score=82.0,
                passed_hard_filters=True,
                filter_reason="PASS",
                etf_component_score=29.0,
                institutional_component_score=18.0,
                stability_rs_component_score=12.0,
                big_flow_component_score=14.0,
                insider_component_score=9.0,
                relative_strength_vs_qqq=1.8,
                big_order_net_usd=4_000_000.0,
                updated_at=now_utc,
            ),
        ]
    )


def test_sqlite_legacy_orders_schema_migrates_idempotently(tmp_path: Path) -> None:
    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT PRIMARY KEY,
            broker_order_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            shares REAL NOT NULL,
            limit_price REAL NOT NULL,
            fill_price REAL,
            fee_usd REAL DEFAULT 0.0,
            status TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, broker_order_id, symbol, side, shares, limit_price, status, created_at, updated_at)
        VALUES ('OLD-1', 'B-1', 'AAPL', 'BUY', 5.0, 200.0, 'FILLED', '2026-11-01T15:00:00+00:00', '2026-11-01T15:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()

    store = StateStore(db_file)
    migrated = store.get_order("OLD-1")
    assert migrated is not None
    assert migrated["symbol"] == "AAPL"
    assert float(migrated["filled_shares"]) == 0.0
    assert migrated["plan_json"] == "{}"


def test_async_buy_submitted_partial_filled_and_idempotent_poll(tmp_path: Path) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "sim_async.db")
    gw = FakeAsyncBrokerGateway(has_account=True, trd_env="SIMULATE")
    now_utc = datetime(2026, 11, 25, 15, 5, tzinfo=timezone.utc)
    _seed_bullish_market_and_watchlist(gw, store, now_utc)

    daemon = TradingDaemon(settings, store=store, gateway=gw, notifier=AlertNotifier())
    daemon._last_watchlist_scan_at = now_utc

    # 1. Run cycle on tier_10k: orders are SUBMITTED, NOT immediately FILLED!
    res = daemon.run_cycle(tier_override="tier_10k", now_override=now_utc)
    assert len(res["entries"]) == 2
    assert res["next_sleep_minutes"] == 1  # Fast 1m wake-up while orders are in-flight

    # No OpenPosition or protective stop may exist while dealt_qty == 0!
    assert store.get_positions() == {}
    assert gw.protective_stops == {}

    active_orders = store.get_active_orders(side=OrderSide.BUY)
    assert len(active_orders) == 2
    nvda_ord = next(o for o in active_orders if o["symbol"] == "NVDA")
    assert nvda_ord["status"] == OrderStatus.SUBMITTED.value
    planned_qty = float(nvda_ord["shares"])
    assert planned_qty >= 2.0
    oid = nvda_ord["broker_order_id"]

    # 2. Futu push callback delivers PARTIAL_FILLED (4 shares @ $140.85)
    part_qty = 4.0 if planned_qty > 4.0 else 1.0
    gw.emit_push(
        BrokerOrderUpdate(
            broker_order_id=oid,
            symbol="NVDA",
            side=OrderSide.BUY,
            order_type="NORMAL",
            status=OrderStatus.PARTIAL_FILLED,
            raw_status="FILLED_PART",
            qty=planned_qty,
            price=float(nvda_ord["limit_price"]),
            filled_shares=part_qty,
            avg_fill_price=140.85,
            remark=nvda_ord["client_order_id"],
        )
    )

    pos_map = store.get_positions()
    assert "NVDA" in pos_map
    assert pos_map["NVDA"].shares == part_qty
    assert pos_map["NVDA"].entry_price == 140.85
    assert pos_map["NVDA"].setup == "ORB_30M"
    assert gw.protective_stops["NVDA"][0] == part_qty

    # 3. Futu push callback delivers FILLED_ALL for total planned_qty @ $140.92
    gw.emit_push(
        BrokerOrderUpdate(
            broker_order_id=oid,
            symbol="NVDA",
            side=OrderSide.BUY,
            order_type="NORMAL",
            status=OrderStatus.FILLED,
            raw_status="FILLED_ALL",
            qty=planned_qty,
            price=float(nvda_ord["limit_price"]),
            filled_shares=planned_qty,
            avg_fill_price=140.92,
            remark=nvda_ord["client_order_id"],
        )
    )

    pos_after = store.get_positions()["NVDA"]
    assert pos_after.shares == planned_qty  # Exact delta reconciled, zero double counting!
    assert pos_after.entry_price == 140.92
    assert gw.protective_stops["NVDA"][0] == planned_qty

    # 4. Subsequent polling via reconcile_open_orders() is 100% idempotent
    daemon.reconcile_open_orders(now=now_utc + timedelta(seconds=20))
    assert store.get_positions()["NVDA"].shares == planned_qty
    final_ord = store.get_order(nvda_ord["client_order_id"])
    assert final_ord is not None
    assert final_ord["status"] == OrderStatus.FILLED.value
    assert float(final_ord["fee_usd"]) > 0.0


def test_in_flight_slot_lock_and_unfilled_ttl_expiry_releases_budget(tmp_path: Path) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "ttl_slot.db")
    gw = FakeAsyncBrokerGateway(has_account=True, trd_env="SIMULATE")
    now_utc = datetime(2026, 11, 25, 15, 5, tzinfo=timezone.utc)
    _seed_bullish_market_and_watchlist(gw, store, now_utc)

    daemon = TradingDaemon(settings, store=store, gateway=gw, notifier=AlertNotifier())
    daemon._last_watchlist_scan_at = now_utc

    # tier_1k allows only max_concurrent_positions=1, max_trades_per_day=2
    res1 = daemon.run_cycle(tier_override="tier_1k", now_override=now_utc)
    assert len(res1["entries"]) == 1  # NVDA takes the single in-flight slot; MSFT is blocked!
    assert len(store.get_active_orders(side=OrderSide.BUY)) == 1

    # Verify MandateRiskGate explicitly blocks MSFT while NVDA order is SUBMITTED
    tier_1k = settings.tiers_cfg.get_active("tier_1k")
    gate = MandateRiskGate(store, settings.strategy_cfg.daemon)
    msft_plan = PositionPlan(
        symbol="MSFT",
        sector="Technology",
        shares=5.0,
        entry_price=141.0,
        limit_price=141.08,
        initial_stop=137.5,
        risk_per_share=3.5,
        total_risk_usd=17.5,
        notional_usd=705.0,
        estimated_round_trip_fee_usd=4.1,
        expected_reward_r=2.5,
        expectancy_r=0.55,
        setup=SetupType.ORB_30M,
    )
    decision = gate.check_entry(msft_plan, tier_1k, 1000.0, {}, now=now_utc + timedelta(seconds=30))
    assert decision.allowed is False
    assert "in-flight" in decision.reason.lower()

    # Advance past entry_order_ttl_seconds (90s -> 100s) with 0 fills: order cancels & expires!
    events = daemon.reconcile_open_orders(now=now_utc + timedelta(seconds=100))
    assert any("EXPIRED" in e for e in events)
    assert len(gw.cancelled_order_ids) == 1
    assert store.get_active_orders() == []
    assert store.get_positions() == {}

    # Because 0 shares filled ($0 fee), the expired order does NOT burn the trade budget,
    # and the slot is unlocked for MSFT!
    decision_after = gate.check_entry(
        msft_plan, tier_1k, 1000.0, {}, now=now_utc + timedelta(seconds=105)
    )
    assert decision_after.allowed is True


def test_partial_fill_ttl_timeout_cancels_remainder_and_locks_partial_position(
    tmp_path: Path,
) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "partial_ttl.db")
    gw = FakeAsyncBrokerGateway(has_account=True, trd_env="SIMULATE")
    now_utc = datetime(2026, 11, 25, 15, 5, tzinfo=timezone.utc)
    _seed_bullish_market_and_watchlist(gw, store, now_utc)

    daemon = TradingDaemon(settings, store=store, gateway=gw, notifier=AlertNotifier())
    daemon._last_watchlist_scan_at = now_utc
    daemon.run_cycle(tier_override="tier_10k", now_override=now_utc)

    nvda_ord = next(o for o in store.get_active_orders(side=OrderSide.BUY) if o["symbol"] == "NVDA")
    oid = nvda_ord["broker_order_id"]

    # Partial fill of 3 shares arrives at t+20s
    gw.emit_push(
        BrokerOrderUpdate(
            broker_order_id=oid,
            symbol="NVDA",
            side=OrderSide.BUY,
            order_type="NORMAL",
            status=OrderStatus.PARTIAL_FILLED,
            raw_status="FILLED_PART",
            qty=float(nvda_ord["shares"]),
            price=float(nvda_ord["limit_price"]),
            filled_shares=3.0,
            avg_fill_price=140.95,
            remark=nvda_ord["client_order_id"],
        )
    )

    # At t+95s, TTL cancels remaining unfilled shares and transitions order to terminal CANCELLED
    daemon.reconcile_open_orders(now=now_utc + timedelta(seconds=95))
    updated = store.get_order(nvda_ord["client_order_id"])
    assert updated is not None
    assert updated["status"] == OrderStatus.CANCELLED.value
    assert float(updated["filled_shares"]) == 3.0
    assert float(updated["fee_usd"]) > 0.0

    # OpenPosition retains exact 3.0 shares with matching 3.0-share protective stop!
    pos = store.get_positions()["NVDA"]
    assert pos.shares == 3.0
    assert gw.protective_stops["NVDA"][0] == 3.0


def test_fast_push_before_place_order_returns_and_server_side_stop_fill(tmp_path: Path) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "fast_push.db")
    gw = FakeAsyncBrokerGateway(has_account=True, trd_env="REAL")
    gw.push_synchronously_before_return = True
    now_utc = datetime(2026, 11, 25, 15, 5, tzinfo=timezone.utc)
    _seed_bullish_market_and_watchlist(gw, store, now_utc)

    daemon = TradingDaemon(settings, store=store, gateway=gw, notifier=AlertNotifier())
    daemon._last_watchlist_scan_at = now_utc
    daemon.run_cycle(tier_override="tier_1k", now_override=now_utc)

    # Even though push fired inside place_limit_order() before broker_oid returned,
    # remark == client_order_id matched it AND status stayed FILLED (not overwritten to SUBMITTED)!
    orders = store.query_orders(symbol="NVDA")
    assert len(orders) == 1
    assert orders[0]["status"] == OrderStatus.FILLED.value
    pos = store.get_positions()["NVDA"]
    stop_oid = pos.broker_stop_order_id
    assert stop_oid is not None

    # Now simulate Futu executing the server-side STOP_LIMIT order on a sharp drop with exact REAL fee
    gw.exact_fees[stop_oid] = 2.37
    gw.emit_push(
        BrokerOrderUpdate(
            broker_order_id=stop_oid,
            symbol="NVDA",
            side=OrderSide.SELL,
            order_type="STOP_LIMIT",
            status=OrderStatus.FILLED,
            raw_status="FILLED_ALL",
            qty=pos.shares,
            price=pos.current_stop,
            filled_shares=pos.shares,
            avg_fill_price=pos.current_stop - 0.05,
            remark="zy-quant protective stop",
        )
    )

    assert "NVDA" not in store.get_positions()
    closed = store.query_closed_trades(symbol="NVDA")
    assert len(closed) == 1
    assert closed[0]["exit_reason"] == "INITIAL_ATR_STOP"
    # Round-trip fee = buy_fee + exact REAL sell_fee ($2.37)
    assert float(closed[0]["fee_usd"]) > 2.37


def test_futu_order_fee_query_protocol_fallback_and_row_parser() -> None:
    assert map_futu_order_status("FILLED_ALL", 10) == OrderStatus.FILLED
    assert map_futu_order_status("FILLED_PART", 4) == OrderStatus.PARTIAL_FILLED
    assert map_futu_order_status("CANCELLED_PART", 4) == OrderStatus.CANCELLED
    assert map_futu_order_status("TIMEOUT", 0) == OrderStatus.EXPIRED
    assert map_futu_order_status("SUBMIT_FAILED", 0) == OrderStatus.FAILED

    df = pd.DataFrame(
        [
            {
                "order_id": "998877",
                "code": "US.NVDA",
                "trd_side": "BUY",
                "order_type": "NORMAL",
                "order_status": "FILLED_PART",
                "qty": 10.0,
                "price": 141.0,
                "dealt_qty": 6.0,
                "dealt_avg_price": 140.88,
                "remark": "BUY-NVDA-202611251500",
                "last_err_msg": "N/A",
                "updated_time": "2026-11-25 10:01:02",
            }
        ]
    )
    parsed = parse_futu_order_row(df.iloc[0])
    assert parsed.symbol == "NVDA"
    assert parsed.status == OrderStatus.PARTIAL_FILLED
    assert parsed.filled_shares == 6.0
    assert parsed.avg_fill_price == 140.88
    assert parsed.last_err_msg == ""

    # Verify Built-In OpenAPIHost 'Unknown protocol ID' gracefully disables order_fee_query
    adapter = FutuGatewayAdapter(trd_env="REAL")

    class DummyFutuMod:
        RET_OK = 0
        RET_ERROR = -1

        class TrdEnv:
            REAL = "REAL"
            SIMULATE = "SIMULATE"

    class DummyTradeCtx:
        def __init__(self) -> None:
            self.fee_calls = 0

        def get_acc_list(self) -> tuple[int, pd.DataFrame]:
            return 0, pd.DataFrame([{"acc_id": 12345, "trd_env": "REAL", "acc_type": "MARGIN"}])

        def order_fee_query(self, **kwargs: Any) -> tuple[int, Any]:
            self.fee_calls += 1
            return -1, "Unknown protocol ID: 2225"

    dummy_ctx = DummyTradeCtx()
    adapter._futu_mod = DummyFutuMod
    adapter._trade_ctx = dummy_ctx
    adapter.is_connected = lambda: True  # type: ignore[method-assign]

    assert adapter.query_order_fee("998877") is None
    assert adapter._order_fee_supported is False
    # Second call short-circuits without repeating unsupported RPC
    assert adapter.query_order_fee("998878") is None
    assert dummy_ctx.fee_calls == 1


def test_paper_local_mode_preserves_immediate_virtual_execution(tmp_path: Path) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "paper_local.db")
    gw = FakeAsyncBrokerGateway(has_account=False, trd_env="SIMULATE")
    now_utc = datetime(2026, 11, 25, 15, 5, tzinfo=timezone.utc)
    _seed_bullish_market_and_watchlist(gw, store, now_utc)

    daemon = TradingDaemon(settings, store=store, gateway=gw, notifier=AlertNotifier())
    daemon._last_watchlist_scan_at = now_utc
    res = daemon.run_cycle(tier_override="tier_1k", now_override=now_utc)

    assert len(res["entries"]) == 1
    assert "NVDA" in store.get_positions()
    orders = store.query_orders(symbol="NVDA")
    assert orders[0]["status"] == OrderStatus.FILLED.value
    assert orders[0]["broker_order_id"] == "PAPER-LOCAL"
