from __future__ import annotations

import json
import math
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx
import pandas as pd

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.cli import QuantCLI
from zy_quant.config import AlpacaDataConfig, AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.alpaca_adapter import (
    AlpacaMarketDataAdapter,
    build_causal_partial_1d_bar,
)
from zy_quant.gateway.futu_adapter import FutuGatewayAdapter
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.intel.earnings_calendar import normalize_nasdaq_earnings_timing
from zy_quant.intel.market_hours import (
    ET_TZ,
    add_trading_days,
    count_trading_days_between,
)
from zy_quant.models import (
    Bar,
    BrokerOrderUpdate,
    EarningsEvent,
    ExitReason,
    OpenPosition,
    OrderSide,
    OrderStatus,
    PipelinePhase,
    PositionPlan,
    SetupType,
)
from zy_quant.risk.correlation import compute_max_pairwise_correlation
from zy_quant.risk.mandate_gate import MandateRiskGate, compute_portfolio_heat
from zy_quant.runtime.alerts import AlertNotifier
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.runtime.pipeline import DailySwingPipeline, resolve_swing_phase
from zy_quant.storage.bar_cache import HistoricalBarCache
from zy_quant.storage.state_store import StateStore
from zy_quant.strategy.base import StrategyPipelineProtocol
from zy_quant.strategy.exit_models import ScientificExitAndSizingEngine
from zy_quant.strategy.signals import IntradaySignalEngine
from zy_quant.strategy.signals_swing import DailySwingSignalEngine


def _make_daily_series(
    symbol: str,
    n_days: int = 270,
    end_d: date = date(2026, 11, 25),
    start_px: float = 120.0,
    daily_drift: float = 0.0022,
    seed_offset: int = 0,
) -> List[Bar]:
    bars: List[Bar] = []
    cur = end_d - timedelta(days=int(n_days * 1.55))
    px = start_px
    idx = 0
    while cur <= end_d:
        if cur.weekday() < 5:
            idx += 1
            wave = math.sin((idx + seed_offset) * 0.37) * 0.008
            ret = daily_drift + wave
            o_px = px
            c_px = max(20.0, o_px * (1.0 + ret))
            h_px = max(o_px, c_px) * 1.008
            l_px = min(o_px, c_px) * 0.992
            ts = datetime(cur.year, cur.month, cur.day, 5, 0, tzinfo=timezone.utc)
            bars.append(Bar(symbol, ts, round(o_px, 2), round(h_px, 2), round(l_px, 2), round(c_px, 2), 8_000_000.0))
            px = c_px
        cur += timedelta(days=1)
    return bars[-n_days:]


def test_strategy_protocol_conformance_and_phase_clock_early_close() -> None:
    settings = AppSettings.load()
    swing_eng = DailySwingSignalEngine(
        settings.strategy_cfg.entry, settings.strategy_cfg.regime, settings.strategy_cfg.swing
    )
    intra_eng = IntradaySignalEngine(settings.strategy_cfg.entry, settings.strategy_cfg.regime)
    assert isinstance(swing_eng, StrategyPipelineProtocol)
    assert isinstance(intra_eng, StrategyPipelineProtocol)

    # Regular session Wednesday 2026-11-25
    assert resolve_swing_phase(datetime(2026, 11, 25, 9, 22, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.PRE_OPEN_SYNC
    assert resolve_swing_phase(datetime(2026, 11, 25, 9, 38, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.OPEN_GAP_GUARD
    assert resolve_swing_phase(datetime(2026, 11, 25, 12, 40, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.IDLE_GTC_ARMED
    assert resolve_swing_phase(datetime(2026, 11, 25, 15, 42, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.EOD_DECISION_WINDOW
    assert resolve_swing_phase(datetime(2026, 11, 25, 16, 5, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.POST_CLOSE_SETTLEMENT

    # Black Friday 2026-11-27 early close (13:00 ET): 12:40 ET MUST be EOD_DECISION_WINDOW!
    assert resolve_swing_phase(datetime(2026, 11, 27, 12, 40, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.EOD_DECISION_WINDOW
    assert resolve_swing_phase(datetime(2026, 11, 27, 13, 5, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.POST_CLOSE_SETTLEMENT
    # Thanksgiving 2026-11-26 holiday -> MARKET_CLOSED
    assert resolve_swing_phase(datetime(2026, 11, 26, 15, 40, tzinfo=ET_TZ).astimezone(timezone.utc)) == PipelinePhase.MARKET_CLOSED


def test_swing_poison_bar_zero_lookahead_and_dual_clock_backtester(tmp_path: Path) -> None:
    """
    Gate G1 Mandatory Poison-Bar Test:
    Mutating `session_day`'s 16:00 ET `1d` close (stamped at 05:00 UTC) to $0.01 (-99.99%)
    must produce 100% identical regime, correlation, signals, and `hybrid_1545` PnL!
    """
    settings = AppSettings.load()
    cache_clean = HistoricalBarCache(tmp_path / "swing_clean.db")
    cache_poison = HistoricalBarCache(tmp_path / "swing_poison.db")

    for idx, sym in enumerate(["QQQ", "SPY", "NVDA", "XOM", "JPM"]):
        b15 = YahooMarketDataAdapter.generate_synthetic_bars(sym, "15m", days=40)
        last_d = b15[-1].timestamp.astimezone(ET_TZ).date()
        b1d = _make_daily_series(sym, n_days=270, end_d=last_d - timedelta(days=1), seed_offset=idx * 5)
        day_ts = datetime(last_d.year, last_d.month, last_d.day, 5, 0, tzinfo=timezone.utc)

        clean_1d = [*b1d, Bar(sym, day_ts, 210.0, 215.0, 208.0, 214.0, 12_000_000.0)]
        poison_1d = [*b1d, Bar(sym, day_ts, 210.0, 215.0, 0.01, 0.01, 999_999_999.0)]

        cache_clean.upsert_bars("15m", b15)
        cache_clean.upsert_bars("1d", clean_1d)
        cache_poison.upsert_bars("15m", b15)
        cache_poison.upsert_bars("1d", poison_1d)

    ad_clean = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_clean)
    ad_poison = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_poison)

    bt_clean = BacktestEngine(settings, ad_clean).run(
        tier_key="tier_10k", symbols=["NVDA", "XOM", "JPM"], days=30, mode="swing_1d", clock_mode="hybrid_1545"
    )
    bt_poison = BacktestEngine(settings, ad_poison).run(
        tier_key="tier_10k", symbols=["NVDA", "XOM", "JPM"], days=30, mode="swing_1d", clock_mode="hybrid_1545"
    )
    assert bt_clean.clock_mode == "hybrid_1545"
    assert bt_clean.trades == bt_poison.trades
    assert bt_clean.net_pnl_usd == bt_poison.net_pnl_usd
    assert bt_clean.green_bars == bt_poison.green_bars

    # Also verify pure `1d` Primary Clock runs fast across 200+ days without needing 15m bars
    bt_1d = BacktestEngine(settings, ad_clean).run(
        tier_key="tier_10k", symbols=["NVDA", "XOM", "JPM"], days=180, mode="swing_1d", clock_mode="1d"
    )
    assert bt_1d.clock_mode == "1d"
    assert (bt_1d.green_bars + bt_1d.rest_bars) >= 150


def test_futu_gtc_stop_fractional_guard_and_cash_plus_fund_assets() -> None:
    adapter = FutuGatewayAdapter(trd_env="REAL")

    class DummyFutu:
        RET_OK = 0

        class TrdEnv:
            REAL = "REAL"
            SIMULATE = "SIMULATE"

        class TrdSide:
            BUY = "BUY"
            SELL = "SELL"

        class OrderType:
            NORMAL = "NORMAL"
            STOP = "STOP"
            STOP_LIMIT = "STOP_LIMIT"

        class TimeInForce:
            DAY = "DAY"
            GTC = "GTC"

        class ModifyOrderOp:
            NORMAL = "NORMAL"
            CANCEL = "CANCEL"

        class Currency:
            USD = "USD"

    class DummyTradeCtx:
        def __init__(self) -> None:
            self.placed: List[Dict[str, Any]] = []
            self.modified: List[Dict[str, Any]] = []

        def get_acc_list(self) -> tuple[int, pd.DataFrame]:
            return 0, pd.DataFrame([{"acc_id": 777, "trd_env": "REAL", "acc_type": "MARGIN"}])

        def accinfo_query(self, **kwargs: Any) -> tuple[int, pd.DataFrame]:
            # Simulate Futu HK Cash Plus where us_cash == 0 and idle USD sits in fund_assets!
            return 0, pd.DataFrame(
                [
                    {
                        "total_assets": 12500.0,
                        "us_cash": 0.0,
                        "fund_assets": 8400.0,
                        "market_val": 4100.0,
                        "power": 16000.0,
                    }
                ]
            )

        def place_order(self, **kwargs: Any) -> tuple[int, pd.DataFrame]:
            self.placed.append(kwargs)
            return 0, pd.DataFrame([{"order_id": f"GTC-{len(self.placed)}"}])

        def modify_order(self, **kwargs: Any) -> tuple[int, pd.DataFrame]:
            self.modified.append(kwargs)
            return 0, pd.DataFrame([{"order_id": kwargs.get("order_id")}])

    ctx = DummyTradeCtx()
    adapter._futu_mod = DummyFutu
    adapter._trade_ctx = ctx
    adapter.is_connected = lambda: True  # type: ignore[method-assign]

    # 1. Cash Plus `fund_assets` surfaces as available liquid cash (`$8,400`), never `$0`!
    acct = adapter.get_account_summary()
    assert acct["cash_usd"] == 8400.0
    assert acct["fund_assets_usd"] == 8400.0

    # 2. Fractional (< 1.0 sh) lot NEVER hits Futu `place_order(STOP/GTC)`!
    frac_oid = adapter.place_or_update_protective_stop("LLY", 0.72, 1280.0)
    assert frac_oid == "SIM-CLIENT-FRAC-STOP-LLY"
    assert len(ctx.placed) == 0

    # 3. Whole-share lot submits persistent `TimeInForce.GTC` + `OrderType.STOP` (`fill_outside_rth=False`)
    gtc_oid = adapter.place_or_update_protective_stop("NVDA", 12.0, 142.50, order_mode="STOP")
    assert gtc_oid == "GTC-1"
    assert ctx.placed[0]["time_in_force"] == "GTC"
    assert ctx.placed[0]["order_type"] == "STOP"
    assert ctx.placed[0]["fill_outside_rth"] is False
    assert ctx.placed[0]["qty"] == 12.0

    # 4. Partial scale-out modifies GTC stop quantity down to remaining 6 shares!
    adapter.place_or_update_protective_stop("NVDA", 6.0, 148.0, existing_order_id=gtc_oid)
    assert ctx.modified[-1]["qty"] == 6.0
    assert ctx.modified[-1]["aux_price"] == 148.0


def test_bmo_vs_amc_pre_earnings_3_tier_house_money_rule() -> None:
    assert normalize_nasdaq_earnings_timing("time-pre-market") == "BMO"
    assert normalize_nasdaq_earnings_timing("time-after-hours") == "AMC"
    assert normalize_nasdaq_earnings_timing("time-not-supplied") == "UNKNOWN"

    settings = AppSettings.load()
    exit_eng = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_10k = settings.tiers_cfg.get_active("tier_10k")
    wed = date(2026, 11, 18)  # T-1 before Thursday Nov 19
    thu = date(2026, 11, 19)  # T-0 report date

    # Case A: Thursday BMO earnings -> MUST act on Wednesday (T-1) close!
    # Sub-+1.0R (+0.4R) -> 100% exit!
    pos_bmo_weak = OpenPosition(
        "NVDA", "Semiconductors", 20.0, 20.0, 150.0,
        datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc), 140.0, 140.0, 155.0, 10.0,
    )
    ev_bmo = EarningsEvent("NVDA", thu, "BMO")
    wed_bar = Bar("NVDA", datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc), 153.0, 154.5, 152.0, 154.0, 1_000_000)
    act_bmo = exit_eng.step_daily_position(pos_bmo_weak, wed_bar, atr_1d=4.5, tier=tier_10k, upcoming_earnings=ev_bmo)
    assert act_bmo.should_exit is True
    assert act_bmo.reason == ExitReason.PRE_EARNINGS_CUSHION
    assert act_bmo.shares_to_sell == 20.0

    # Case B: Thursday AMC earnings -> holds through Wednesday (T-1), acts on Thursday (T-0) close!
    # +1.8R cushion -> scales out 50% (10 shares) and arms breakeven on remaining 10 shares!
    pos_amc_mid = OpenPosition(
        "AVGO", "Semiconductors", 20.0, 20.0, 200.0,
        datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc), 190.0, 190.0, 218.0, 10.0,
    )
    ev_amc = EarningsEvent("AVGO", thu, "AMC")
    wed_avgo = Bar("AVGO", datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc), 215.0, 218.0, 214.0, 217.0, 800_000)
    assert exit_eng.step_daily_position(pos_amc_mid, wed_avgo, atr_1d=5.0, tier=tier_10k, upcoming_earnings=ev_amc).should_exit is False

    thu_avgo = Bar("AVGO", datetime(2026, 11, 19, 20, 0, tzinfo=timezone.utc), 217.0, 219.0, 216.0, 218.0, 900_000)
    act_amc = exit_eng.step_daily_position(pos_amc_mid, thu_avgo, atr_1d=5.0, tier=tier_10k, upcoming_earnings=ev_amc)
    assert act_amc.should_exit is True
    assert act_amc.reason == ExitReason.PRE_EARNINGS_CUSHION
    assert act_amc.shares_to_sell == 10.0
    assert pos_amc_mid.breakeven_armed is True
    assert pos_amc_mid.current_stop > 200.0

    # Case C: Deep cushion (>= +2.5R) -> holds 100% into earnings on pure house money!
    pos_deep = OpenPosition(
        "META", "Communication", 10.0, 10.0, 500.0,
        datetime(2026, 11, 1, 15, 0, tzinfo=timezone.utc), 475.0, 530.0, 580.0, 25.0, breakeven_armed=True,
    )
    thu_meta = Bar("META", datetime(2026, 11, 19, 20, 0, tzinfo=timezone.utc), 570.0, 578.0, 568.0, 575.0, 700_000)
    assert exit_eng.step_daily_position(pos_deep, thu_meta, atr_1d=12.0, tier=tier_10k, upcoming_earnings=EarningsEvent("META", thu, "AMC")).should_exit is False


def test_portfolio_heat_60d_correlation_and_stoploss_guard_watermark(tmp_path: Path) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "risk_gates.db")
    gate = MandateRiskGate(store, settings.strategy_cfg.daemon)
    tier = settings.tiers_cfg.get_active("tier_10k").model_copy(
        update={"max_concurrent_positions": 4, "enforce_swing_caps": True, "max_portfolio_heat_pct": 0.045}
    )

    # 1. Principal Heat drops to $0 once winner ratchets stop >= entry_price!
    winner = OpenPosition(
        "XOM", "Energy", 20.0, 20.0, 110.0,
        datetime(2026, 11, 1, 15, 0, tzinfo=timezone.utc), 104.0, 114.0, 122.0, 6.0, breakeven_armed=True,
        metadata={"last_mark_price": 120.0},
    )
    unarmed = OpenPosition(
        "JPM", "Financials", 10.0, 10.0, 220.0,
        datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc), 200.0, 200.0, 221.0, 20.0,
        metadata={"last_mark_price": 218.0},
    )
    heat = compute_portfolio_heat({"XOM": winner, "JPM": unarmed}, [], 10000.0)
    # XOM contributes $0 principal heat (stop $114 > entry $110); JPM contributes $200 (2.0%)
    assert heat.principal_heat_usd == 200.0
    assert heat.principal_heat_pct == 2.0

    # 2. 60d Pairwise Correlation blocks same-batch `extra_pending_symbols` crowding!
    as_of = date(2026, 11, 25)
    base_nvda = _make_daily_series("NVDA", n_days=90, end_d=date(2026, 11, 24), seed_offset=1)
    # Clone identical return profile for AVGO (corr == 1.0 > 0.75) and orthogonal profile for LLY
    twin_avgo = [
        Bar("AVGO", b.timestamp, b.open * 2.0, b.high * 2.0, b.low * 2.0, b.close * 2.0, b.volume)
        for b in base_nvda
    ]
    indep_lly = _make_daily_series("LLY", n_days=90, end_d=date(2026, 11, 24), daily_drift=0.001, seed_offset=97)
    # Make LLY uncorrelated by alternating sign every 2nd bar
    for k in range(1, len(indep_lly), 3):
        prev_c = indep_lly[k - 1].close
        indep_lly[k] = Bar("LLY", indep_lly[k].timestamp, prev_c, prev_c * 1.002, prev_c * 0.985, prev_c * 0.99, 5_000_000)

    d_map = {"NVDA": base_nvda, "AVGO": twin_avgo, "LLY": indep_lly}
    corr_val, peer = compute_max_pairwise_correlation("AVGO", ["NVDA"], d_map, as_of_date=as_of)
    assert corr_val is not None and corr_val > 0.99 and peer == "NVDA"

    avgo_plan = PositionPlan(
        "AVGO", "Semiconductors", 8.0, 250.0, 250.1, 238.0, 12.0, 96.0, 2000.0, 4.1, 2.5, 0.55, SetupType.VCP_PIVOT_BREAKOUT
    )
    now_ref = datetime(2026, 11, 25, 20, 40, tzinfo=timezone.utc)
    # Blocked even when open_positions is empty if NVDA is already in `extra_pending_symbols`!
    dec_corr = gate.check_entry(
        avgo_plan, tier, 10000.0, {}, now=now_ref, daily_bars_map=d_map, as_of_date=as_of, extra_pending_symbols={"NVDA"}, enforce_swing_heat=True
    )
    assert dec_corr.allowed is False
    assert "correlation" in dec_corr.reason.lower()

    # 3. StoplossGuard: 3 partial stop prints of ONE position = 1 episode (must NOT trigger!),
    #    3 distinct episodes trigger 5-day cooldown, and Day 6 does NOT re-trigger!
    t_entry_1 = "2026-11-12T15:00:00+00:00"
    for part_i in range(3):
        store.record_closed_trade(
            "AMD", "VCP_PIVOT_BREAKOUT", 2.0, 150.0, 142.0, 1.5, 8.0, "INITIAL_ATR_STOP",
            entry_time=t_entry_1, exit_time=f"2026-11-16T15:0{part_i}:00+00:00",
        )
    ok_1ep, _, st_1 = gate.evaluate_stoploss_guard(datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc))
    assert ok_1ep is True
    assert st_1["recent_stop_episodes"] == 1  # Grouped cleanly to 1 episode!

    # Add 2 more distinct stopped-out positions -> total 3 distinct episodes -> triggers 5td cooldown!
    store.record_closed_trade(
        "MU", "VCP_PIVOT_BREAKOUT", 5.0, 100.0, 94.0, 4.1, 6.0, "INITIAL_ATR_STOP",
        entry_time="2026-11-13T15:00:00+00:00", exit_time="2026-11-17T18:00:00+00:00",
    )
    store.record_closed_trade(
        "QCOM", "DAILY_PULLBACK_EMA21", 5.0, 160.0, 152.0, 4.1, 8.0, "INITIAL_ATR_STOP",
        entry_time="2026-11-16T15:00:00+00:00", exit_time="2026-11-18T19:00:00+00:00",
    )

    trigger_dt = datetime(2026, 11, 18, 20, 30, tzinfo=timezone.utc)
    ok_trig, reason_trig, st_trig = gate.evaluate_stoploss_guard(trigger_dt)
    assert ok_trig is False
    assert "cooldown" in reason_trig.lower()
    until_d = date.fromisoformat(st_trig["cooldown_until_date"])
    assert count_trading_days_between(date(2026, 11, 18), until_d) == 5

    # Advance to Day 6 (1 trading day after `until_d`, still within 10td of Nov 18!):
    # Watermark MUST prevent stateless re-trigger!
    day_6 = add_trading_days(until_d, 1)
    day_6_utc = datetime.combine(day_6, time(15, 40), tzinfo=ET_TZ).astimezone(timezone.utc)
    ok_day6, _, _ = gate.evaluate_stoploss_guard(day_6_utc)
    assert ok_day6 is True


def test_stateful_virtual_cash_reconciliation_gtc_healer_and_4phase_idempotency(
    tmp_path: Path,
) -> None:
    settings = AppSettings.load()
    store = StateStore(tmp_path / "pipeline_ledger.db")
    daemon = TradingDaemon(settings, store=store, notifier=AlertNotifier())
    pipe = DailySwingPipeline(daemon)

    # 1. Verify non-tautological stateful `virtual_cash` ledger across BUY -> 50% scale-out -> final SELL
    tier_key = "tier_10k"
    tier = settings.tiers_cfg.get_active(tier_key)
    now_t = datetime(2026, 11, 25, 20, 40, tzinfo=timezone.utc)

    plan = PositionPlan(
        "CAT", "Industrials", 10.0, 350.0, 350.15, 335.0, 15.0, 150.0, 3500.0, 4.10, 2.5, 0.55, SetupType.DAILY_PULLBACK_EMA21
    )
    from zy_quant.models import EntrySignal

    sig = EntrySignal("CAT", "Industrials", SetupType.DAILY_PULLBACK_EMA21, now_t, 350.0, 6.8, 348.0, 1.4, 84.0, "test")
    buy_fee = daemon.fee_model.estimate_leg(10.0, 350.0, OrderSide.BUY).total_friction_usd
    store.record_order_intent("BUY-CAT-1", "CAT", OrderSide.BUY, 10.0, 350.15, "test", created_at=now_t)
    daemon._settle_paper_local_buy("BUY-CAT-1", "CAT", plan, sig, buy_fee, now_t, tier_key, tier)

    rec1 = store.reconcile_stateful_ledger(tier_key, tier.reference_equity_usd)
    assert rec1["ok"] is True
    assert abs(rec1["drift_usd"]) <= 0.001

    # Partial scale-out of 5 shares @ $375
    pos = store.get_positions()["CAT"]
    store.record_order_intent("SELL-CAT-HALF", "CAT", OrderSide.SELL, 5.0, 375.0, "scale", created_at=now_t)
    daemon._settle_paper_local_sell("SELL-CAT-HALF", "CAT", pos, 5.0, 375.0, "PRE_EARNINGS_CUSHION", tier_key, tier)
    rec2 = store.reconcile_stateful_ledger(tier_key, tier.reference_equity_usd)
    assert rec2["ok"] is True
    assert abs(rec2["drift_usd"]) <= 0.01

    # Deliberately corrupt virtual_cash by +$10 -> reconciliation MUST catch the drift (not a 0==0 tautology!)
    store.adjust_virtual_cash(tier_key, 10.0, tier.reference_equity_usd)
    rec_corrupt = store.reconcile_stateful_ledger(tier_key, tier.reference_equity_usd)
    assert rec_corrupt["ok"] is False
    assert abs(rec_corrupt["drift_usd"] - 10.0) < 0.02
    store.adjust_virtual_cash(tier_key, -10.0, tier.reference_equity_usd)

    # 2. Verify GTC Stop-Audit Healer re-arms missing / ex-dividend-cancelled 0-fill GTC stop
    pos_rem = store.get_positions()["CAT"]
    pos_rem.broker_stop_order_id = None
    store.upsert_position(pos_rem)
    healed = pipe.heal_overnight_gtc_stops(now_t)
    assert len(healed) == 1
    assert store.get_positions()["CAT"].broker_stop_order_id is not None

    # 3. Verify 4-phase checkpoint idempotency (`already_completed == True` on second call)
    store.record_pipeline_phase("2026-11-25", PipelinePhase.PRE_OPEN_SYNC.value, "COMPLETED", {"ok": True})
    dup_res = pipe.run_phase(PipelinePhase.PRE_OPEN_SYNC, now_utc=datetime(2026, 11, 25, 14, 25, tzinfo=timezone.utc))
    assert dup_res["already_completed"] is True


def test_consolidate_local_preserves_alpaca_keys_and_split_cliff_resync(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "config"
    shutil.copytree(AppSettings.load().root_dir / "config", cfg_dir)
    local_file = cfg_dir / "settings.local.json"
    local_file.write_text(
        json.dumps(
            {
                "futu": {"trd_env": "SIMULATE", "api_token": "FUTU_TOK_123456", "trade_pwd_md5": ""},
                "alpaca": {"api_key": "PK_KEEP_ME_99", "secret_key": "SK_KEEP_ME_99"},
                "web": {"host": "100.88.1.2"},
            }
        ),
        encoding="utf-8",
    )
    cli = QuantCLI(AppSettings.load(tmp_path))
    cli.run_config_show(consolidate_local=True)
    saved = json.loads(local_file.read_text(encoding="utf-8"))
    assert saved["alpaca"]["api_key"] == "PK_KEEP_ME_99"
    assert saved["web"]["host"] == "100.88.1.2"

    # Verify split-cliff detection purges pre-split cached $400 bar and re-syncs adjusted $40 series!
    cache = HistoricalBarCache(tmp_path / "split_test.db")
    anchor_dt = datetime(2026, 11, 20, 5, 0, tzinfo=timezone.utc)
    cache.upsert_bars("1d", [
        Bar("NVDA", datetime(2026, 11, 10, 5, 0, tzinfo=timezone.utc), 380.0, 390.0, 375.0, 385.0, 1_000_000),
        Bar("NVDA", anchor_dt, 395.0, 405.0, 390.0, 400.0, 1_000_000),  # Unsplit $400 close!
    ])

    calls = 0

    def split_handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        # After 10:1 split, Nov 20 bar now reports $40.00 (a -90% retroactive adjustment!)
        return httpx.Response(
            200,
            json={
                "bars": {
                    "NVDA": [
                        {"t": "2026-11-10T05:00:00Z", "o": 38.0, "h": 39.0, "l": 37.5, "c": 38.5, "v": 10_000_000},
                        {"t": "2026-11-20T05:00:00Z", "o": 39.5, "h": 40.5, "l": 39.0, "c": 40.0, "v": 10_000_000},
                        {"t": "2026-11-25T05:00:00Z", "o": 41.0, "h": 42.5, "l": 40.8, "c": 42.0, "v": 11_000_000},
                    ]
                },
                "next_page_token": None,
            },
        )

    apca = AlpacaMarketDataAdapter(
        AlpacaDataConfig(api_key="PK1", secret_key="SK1"),
        bar_cache=cache,
        http_client=httpx.Client(transport=httpx.MockTransport(split_handler)),
    )
    synced = apca.get_bars_batch(
        ["NVDA"],
        timeframe="1d",
        start_dt=datetime(2026, 11, 9, 0, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc),
    )["NVDA"]
    assert calls >= 2  # Tail delta detected cliff -> purged -> full re-sync!
    assert synced[0].close == 38.5  # Pre-split $385 bar replaced with split-adjusted $38.50!
