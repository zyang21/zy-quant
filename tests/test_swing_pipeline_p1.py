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
from zy_quant.intel.earnings_calendar import (
    EarningsCalendarFeed,
    normalize_nasdaq_earnings_timing,
)
from zy_quant.intel.market_hours import (
    ET_TZ,
    add_trading_days,
    count_trading_days_between,
    daily_bar_session_date,
    is_trading_day,
)
from zy_quant.intel.watchlist_scorer import SwingWatchlistScorer
from zy_quant.models import (
    Bar,
    BrokerOrderUpdate,
    EarningsEvent,
    EntrySignal,
    ExitReason,
    OpenPosition,
    OrderSide,
    OrderStatus,
    PipelinePhase,
    PositionPlan,
    QuoteSnapshot,
    RegimeLight,
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


def _make_consistent_1d_and_15m_series(
    symbol: str,
    n_days: int = 270,
    intraday_days: int = 45,
    end_d: date = date(2026, 11, 25),
    start_px: float = 140.0,
    daily_drift: float = 0.0024,
    seed_offset: int = 0,
    breakout_on_last_offset: int = -1,
) -> tuple[List[Bar], List[Bar]]:
    """
    Builds coherent 1d + 15m RTH bar series where every recent trading day's 26 x 15m candles
    (09:30..15:45 ET) aggregate consistently to that day's 1d OHLCV and include clean VCP/Donchian breakouts.
    """
    trading_dates: List[date] = []
    cur = end_d - timedelta(days=int(n_days * 1.65))
    while cur <= end_d:
        if is_trading_day(cur):
            trading_dates.append(cur)
        cur += timedelta(days=1)
    trading_dates = trading_dates[-n_days:]

    bars_1d: List[Bar] = []
    bars_15m: List[Bar] = []
    px = start_px
    first_15m_idx = max(0, len(trading_dates) - intraday_days)

    for idx, d_cur in enumerate(trading_dates):
        # Engineer periodic tight contraction -> high-volume breakout cycles so swing setups fire deterministically
        cycle_pos = (idx + seed_offset) % 32
        is_breakout_day = (cycle_pos == 26) or (
            breakout_on_last_offset >= 0 and idx == len(trading_dates) - 1 - breakout_on_last_offset
        )
        if 20 <= cycle_pos <= 25 and not is_breakout_day:
            # Tight 6-day VCP consolidation
            ret = 0.0008 if (cycle_pos % 2 == 0) else -0.0005
            span_pct = 0.0045
            day_vol = 6_500_000.0
        elif is_breakout_day:
            # Institutional breakout day (> prior 20d high on 1.85x RVOL, strong close near high)
            ret = 0.032
            span_pct = 0.006
            day_vol = 15_500_000.0
        else:
            wave = math.sin((idx + seed_offset) * 0.29) * 0.004
            ret = daily_drift + wave
            span_pct = 0.010
            day_vol = 8_200_000.0

        o_px = round(px, 2)
        c_px = round(max(25.0, o_px * (1.0 + ret)), 2)
        h_px = round(max(o_px, c_px) * (1.0 + span_pct * 0.35), 2)
        l_px = round(min(o_px, c_px) * (1.0 - span_pct * 0.45), 2)
        if is_breakout_day:
            # Close in upper 85% of day's range so both 15:45 ET and 16:00 ET confirm breakout cleanly
            h_px = round(c_px * 1.0015, 2)
            l_px = round(min(o_px, c_px) * 0.996, 2)

        # Stamp 1d bars at 00:00 UTC on half the symbols and 05:00 UTC on the rest to exercise Finding #1!
        hr_utc = 0 if (idx % 2 == 0) else 5
        ts_1d = datetime(d_cur.year, d_cur.month, d_cur.day, hr_utc, 0, tzinfo=timezone.utc)
        bars_1d.append(Bar(symbol, ts_1d, o_px, h_px, l_px, c_px, day_vol))

        if idx >= first_15m_idx:
            close_t = time(13, 0) if (d_cur == date(2026, 11, 27)) else time(16, 0)
            n_15m = max(4, ((close_t.hour * 60 + close_t.minute) - (9 * 60 + 30)) // 15)
            per_bar_vol = round(day_vol / float(n_15m), 2)
            for b_i in range(n_15m):
                frac_0 = b_i / float(n_15m)
                # Reach 99.5% of daily move by 15:45 ET so 15:45 ET and T+1 Open have realistic institutional parity
                frac_1 = min(1.0, (b_i + 1) / float(max(1, n_15m - 1))) if b_i < n_15m - 1 else 1.0
                bo = o_px if b_i == 0 else round(o_px + (c_px - o_px) * min(1.0, (b_i) / float(max(1, n_15m - 1))), 2)
                bc = c_px if b_i >= n_15m - 2 else round(o_px + (c_px - o_px) * frac_1, 2)
                bh = h_px if b_i == n_15m // 2 else max(bo, bc)
                bl = l_px if b_i == 1 else min(bo, bc)
                ts_et = datetime.combine(d_cur, time(9, 30), tzinfo=ET_TZ) + timedelta(minutes=15 * b_i)
                bars_15m.append(Bar(symbol, ts_et.astimezone(timezone.utc), bo, bh, bl, bc, per_bar_vol))

        px = c_px

    return bars_1d, bars_15m


def test_swing_poison_bar_zero_lookahead_and_dual_clock_backtester(tmp_path: Path) -> None:
    """
    Gate G1 & G7 Mandatory Poison-Bar + Dual-Clock Parity (< 0.3% drift) Test:
      1. Mutating `session_day`'s 16:00 ET `1d` bar (both `00:00Z` & `05:00Z`) AND `15:45-16:00 ET`
         closing `15m` bar to $0.01 (-99.99%) produces 100% identical regime, watchlist scores,
         60d correlation, entry signals, non-empty `hybrid_1545` trades (`len > 0`), and PnL!
      2. `clock_mode="1d"` vs `clock_mode="hybrid_1545"` over a shared active trading window
         achieves `< 0.3%` net return parity (`abs(ret_1d - ret_hybrid) < 0.30`)!
    """
    settings = AppSettings.load()
    cache_clean = HistoricalBarCache(tmp_path / "swing_clean.db")
    cache_poison = HistoricalBarCache(tmp_path / "swing_poison.db")

    syms = ["QQQ", "SPY", "NVDA", "XOM", "JPM"]
    clean_map_1d: Dict[str, List[Bar]] = {}
    poison_map_1d: Dict[str, List[Bar]] = {}
    partial_1545_map: Dict[str, Bar] = {}
    target_d = date(2026, 11, 25)
    dec_1545_utc = datetime(2026, 11, 25, 15, 45, tzinfo=ET_TZ).astimezone(timezone.utc)

    for idx, sym in enumerate(syms):
        drift = 0.0018 if sym in ("QQQ", "SPY") else 0.0028
        b1d, b15 = _make_consistent_1d_and_15m_series(
            sym,
            n_days=270,
            intraday_days=45,
            end_d=target_d,
            start_px=150.0 + idx * 20.0,
            daily_drift=drift,
            seed_offset=idx * 7,
            breakout_on_last_offset=0 if sym == "NVDA" else -1,
        )
        clean_map_1d[sym] = list(b1d)
        # Poison BOTH `target_d`'s 00:00Z 1d bar AND `target_d`'s 15:45-16:00 ET closing 15m bar!
        poison_1d = [
            Bar(sym, datetime(target_d.year, target_d.month, target_d.day, 0, 0, tzinfo=timezone.utc), 200.0, 210.0, 0.01, 0.01, 999_999_999.0)
            if daily_bar_session_date(x.timestamp) == target_d
            else x
            for x in b1d
        ]
        poison_15m = [
            Bar(sym, x.timestamp, x.open, x.high, 0.01, 0.01, 999_999_999.0)
            if (
                x.timestamp.astimezone(ET_TZ).date() == target_d
                and x.timestamp.astimezone(ET_TZ).time() >= time(15, 45)
            )
            else x
            for x in b15
        ]
        poison_map_1d[sym] = poison_1d
        p_bar = build_causal_partial_1d_bar(sym, [x for x in b15 if x.timestamp.astimezone(ET_TZ).date() == target_d], decision_ts_utc=dec_1545_utc)
        assert p_bar is not None
        partial_1545_map[sym] = p_bar

        cache_clean.upsert_bars("15m", b15)
        cache_clean.upsert_bars("1d", b1d)
        cache_poison.upsert_bars("15m", poison_15m)
        cache_poison.upsert_bars("1d", poison_1d)

    # 1. Direct Component Poison-Bar Invariance (Regime, Scorer, Correlation, Signal Engine)
    swing_eng = DailySwingSignalEngine(
        settings.strategy_cfg.entry, settings.strategy_cfg.regime, settings.strategy_cfg.swing
    )
    scorer = SwingWatchlistScorer(settings.watchlist_cfg, settings.strategy_cfg.swing)
    reg_clean = swing_eng.evaluate_regime(
        clean_map_1d["QQQ"], clean_map_1d["SPY"], clean_map_1d, as_of_date=target_d,
        qqq_partial_bar=partial_1545_map["QQQ"], spy_partial_bar=partial_1545_map["SPY"], universe_partial_bars=partial_1545_map,
    )
    reg_poison = swing_eng.evaluate_regime(
        poison_map_1d["QQQ"], poison_map_1d["SPY"], poison_map_1d, as_of_date=target_d,
        qqq_partial_bar=partial_1545_map["QQQ"], spy_partial_bar=partial_1545_map["SPY"], universe_partial_bars=partial_1545_map,
    )
    assert reg_clean.light == reg_poison.light
    assert reg_clean.qqq_sma50 == reg_poison.qqq_sma50
    assert reg_clean.breadth_above_sma50_pct == reg_poison.breadth_above_sma50_pct

    wl_clean = scorer.rank_swing_universe(
        {}, clean_map_1d, clean_map_1d["QQQ"], reg_clean.light, target_d,
        qqq_partial_bar=partial_1545_map["QQQ"], partial_bars_map=partial_1545_map,
    )
    wl_poison = scorer.rank_swing_universe(
        {}, poison_map_1d, poison_map_1d["QQQ"], reg_poison.light, target_d,
        qqq_partial_bar=partial_1545_map["QQQ"], partial_bars_map=partial_1545_map,
    )
    assert [(c.symbol, c.total_score) for c in wl_clean] == [(c.symbol, c.total_score) for c in wl_poison]

    corr_clean, _ = compute_max_pairwise_correlation("NVDA", ["XOM", "JPM"], clean_map_1d, as_of_date=target_d)
    corr_poison, _ = compute_max_pairwise_correlation("NVDA", ["XOM", "JPM"], poison_map_1d, as_of_date=target_d)
    assert corr_clean == corr_poison

    sig_clean = swing_eng.evaluate_entry(
        "NVDA", "Semiconductors", reg_clean.light, 82.0, clean_map_1d["NVDA"], clean_map_1d["QQQ"],
        as_of_date=target_d, partial_today_bar=partial_1545_map["NVDA"], qqq_partial_bar=partial_1545_map["QQQ"],
    )
    sig_poison = swing_eng.evaluate_entry(
        "NVDA", "Semiconductors", reg_poison.light, 82.0, poison_map_1d["NVDA"], poison_map_1d["QQQ"],
        as_of_date=target_d, partial_today_bar=partial_1545_map["NVDA"], qqq_partial_bar=partial_1545_map["QQQ"],
    )
    assert sig_clean is not None and sig_poison is not None
    assert sig_clean.entry_price == sig_poison.entry_price and sig_clean.setup == sig_poison.setup

    # 2. End-to-End Backtest Poison-Bar Test with non-empty closed trades (`len > 0`)!
    ad_clean = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_clean)
    ad_poison = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_poison)
    earn_close_d = daily_bar_session_date(clean_map_1d["XOM"][-3].timestamp)
    bt_earn = {
        "XOM": EarningsEvent("XOM", earn_close_d, "AMC"),
        "JPM": EarningsEvent("JPM", earn_close_d, "AMC"),
    }

    bt_clean = BacktestEngine(settings, ad_clean).run(
        tier_key="tier_10k", symbols=["NVDA", "XOM", "JPM"], days=40, mode="swing_1d", clock_mode="hybrid_1545", earnings_events=bt_earn
    )
    bt_poison = BacktestEngine(settings, ad_poison).run(
        tier_key="tier_10k", symbols=["NVDA", "XOM", "JPM"], days=40, mode="swing_1d", clock_mode="hybrid_1545", earnings_events=bt_earn
    )
    assert bt_clean.clock_mode == "hybrid_1545"
    assert len(bt_clean.trades) >= 1  # Non-vacuous: real swing trades executed & closed!
    assert bt_clean.trades == bt_poison.trades
    assert bt_clean.net_pnl_usd == bt_poison.net_pnl_usd
    assert bt_clean.final_equity_usd == bt_poison.final_equity_usd
    assert bt_clean.green_bars == bt_poison.green_bars

    # 3. Clock-Parity Verification Gate (`< 0.3%` return drift between `1d` and `hybrid_1545`)!
    bt_1d_parity = BacktestEngine(settings, ad_clean).run(
        tier_key="tier_10k", symbols=["NVDA", "XOM", "JPM"], days=40, mode="swing_1d", clock_mode="1d", earnings_events=bt_earn
    )
    assert bt_1d_parity.clock_mode == "1d"
    assert len(bt_1d_parity.trades) >= 1
    assert abs(bt_1d_parity.net_return_pct - bt_clean.net_return_pct) < 0.30

    # 4. Multi-month pure `1d` Primary Clock speed & coverage check
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


def test_midnight_utc_daily_bar_and_completed_15m_partial_bar_causality(tmp_path: Path) -> None:
    """
    Regression tests for Review Findings #1 & #2:
      - `00:00Z` daily bars resolve to `YYYY-MM-DD` (never `YYYY-MM-DD - 1` under `ET_TZ`)
      - `build_causal_partial_1d_bar` requires full 15m bar completion (`ts + 15m <= decision_ts`)
        and strictly excludes the `15:45-16:00 ET` closing bar even if evaluated at `15:52 ET`
      - `get_effective_15m_volume_scale()` returns `1.0` for cached SIP / fallback bars and empirical
        `SIP/IEX` median ratio only when live IEX bars were fetched
    """
    assert daily_bar_session_date(datetime(2026, 11, 25, 0, 0, tzinfo=timezone.utc)) == date(2026, 11, 25)
    assert daily_bar_session_date(datetime(2026, 11, 25, 5, 0, tzinfo=timezone.utc)) == date(2026, 11, 25)

    # Build 26 x 15m bars for 2026-11-25 where 15:30-15:45 closes at $155 and 15:45-16:00 closes at $0.01 (poison!)
    d_sess = date(2026, 11, 25)
    session_15m: List[Bar] = []
    for k in range(26):
        ts_et = datetime.combine(d_sess, time(9, 30), tzinfo=ET_TZ) + timedelta(minutes=15 * k)
        c_val = 0.01 if k == 25 else (155.0 if k == 24 else 150.0 + k * 0.1)
        session_15m.append(Bar("NVDA", ts_et.astimezone(timezone.utc), 150.0, max(151.0, c_val), min(149.0, c_val), c_val, 400_000.0))

    # At 15:40 ET: 15:30-15:45 bar (k=24) is NOT yet complete -> close must come from k=23 (15:15-15:30)!
    p_1540 = build_causal_partial_1d_bar("NVDA", session_15m, decision_ts_utc=datetime(2026, 11, 25, 15, 40, tzinfo=ET_TZ).astimezone(timezone.utc))
    assert p_1540 is not None and p_1540.close == round(150.0 + 23 * 0.1, 2)

    # At 15:45 ET: k=24 (15:30-15:45) IS complete ($155.0), k=25 (15:45-16:00) is excluded!
    p_1545 = build_causal_partial_1d_bar("NVDA", session_15m, decision_ts_utc=datetime(2026, 11, 25, 15, 45, tzinfo=ET_TZ).astimezone(timezone.utc))
    assert p_1545 is not None and p_1545.close == 155.0

    # At 15:52 ET: `close - 15m` cap still strictly excludes k=25 ($0.01 closing bar)!
    p_1552 = build_causal_partial_1d_bar("NVDA", session_15m, decision_ts_utc=datetime(2026, 11, 25, 15, 52, tzinfo=ET_TZ).astimezone(timezone.utc))
    assert p_1552 is not None and p_1552.close == 155.0

    # Verify SIP/IEX empirical calibration & source-aware `get_effective_15m_volume_scale`
    cache = HistoricalBarCache(tmp_path / "vol_cal.db")
    sip_bar_dt = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=3)
    d_str = sip_bar_dt.strftime("%Y-%m-%d")
    cache.upsert_bars("1d", [Bar("NVDA", sip_bar_dt, 140.0, 145.0, 139.0, 143.0, 44_000_000.0)])

    def iex_handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"bars": {"NVDA": [{"t": f"{d_str}T05:00:00Z", "o": 140.0, "h": 145.0, "l": 139.0, "c": 143.0, "v": 2_000_000.0}]}, "next_page_token": None},
        )

    apca = AlpacaMarketDataAdapter(
        AlpacaDataConfig(api_key="PK", secret_key="SK", feed="iex"),
        bar_cache=cache,
        http_client=httpx.Client(transport=httpx.MockTransport(iex_handler)),
    )
    ratios = apca.calibrate_sip_iex_volume_ratios(["NVDA"])
    assert ratios["NVDA"] == 22.0  # Empirical 44M / 2M = 22.0x (NOT hardcoded 35x!)
    apca.last_bars_feed[("NVDA", "15m")] = "SIP"
    assert apca.get_effective_15m_volume_scale("NVDA") == 1.0  # Cached SIP -> 1.0x!
    apca.last_bars_feed[("NVDA", "15m")] = "IEX"
    assert apca.get_effective_15m_volume_scale("NVDA") == 22.0  # Live IEX -> 22.0x!


def test_gtc_modify_failure_short_flip_guard_fractional_gap_and_multifill_ledger(tmp_path: Path) -> None:
    """
    Regression tests for Review Findings #3 & #4:
      - `place_or_update_protective_stop` NEVER submits a duplicate `GTC STOP` if `modify_order`
        AND `cancel_order` fail (zero short-flip hazard), and cancels before re-placing when needed
      - Fractional `<1 sh` stop ignores pre-entry yesterday bars and prices marketable SELL below live bid on crash
      - `_mark_order_submitted_safely` preserves fast push `FILLED` under `_order_lock`
      - Multi-fill cumulative `avg_fill_price` (`5 @ $200` + `5 @ $202`) keeps stateful `virtual_cash` drift `$0.00`
    """
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
            STOP = "STOP"
            STOP_LIMIT = "STOP_LIMIT"

        class TimeInForce:
            GTC = "GTC"

        class ModifyOrderOp:
            NORMAL = "NORMAL"
            CANCEL = "CANCEL"

    class GuardedTradeCtx:
        def __init__(self) -> None:
            self.allow_cancel = False
            self.placed: List[Dict[str, Any]] = []
            self.cancelled: List[str] = []

        def get_acc_list(self) -> tuple[int, pd.DataFrame]:
            return 0, pd.DataFrame([{"acc_id": 888, "trd_env": "REAL", "acc_type": "MARGIN"}])

        def modify_order(self, **kwargs: Any) -> tuple[int, Any]:
            if kwargs.get("modify_order_op") == "CANCEL":
                if self.allow_cancel:
                    self.cancelled.append(str(kwargs["order_id"]))
                    return 0, pd.DataFrame([{"order_id": kwargs["order_id"]}])
                return -1, "cancel timeout"
            return -1, "modify rejected"

        def place_order(self, **kwargs: Any) -> tuple[int, pd.DataFrame]:
            self.placed.append(kwargs)
            return 0, pd.DataFrame([{"order_id": f"NEW-GTC-{len(self.placed)}"}])

    ctx = GuardedTradeCtx()
    adapter._futu_mod = DummyFutu
    adapter._trade_ctx = ctx
    adapter.is_connected = lambda: True  # type: ignore[method-assign]
    adapter.sync_open_orders = lambda refresh_cache=False: [  # type: ignore[method-assign]
        BrokerOrderUpdate(
            broker_order_id="OLD-GTC-12",
            symbol="NVDA",
            side=OrderSide.SELL,
            order_type="STOP",
            status=OrderStatus.SUBMITTED,
            raw_status="SUBMITTED",
            qty=12.0,
            price=142.0,
            filled_shares=0.0,
            avg_fill_price=None,
            remark="stop",
        )
    ]

    # 1. Both modify AND cancel fail -> MUST keep OLD-GTC-12 and place ZERO duplicate GTC stops!
    kept_oid = adapter.place_or_update_protective_stop("NVDA", 6.0, 148.0, existing_order_id="OLD-GTC-12")
    assert kept_oid == "OLD-GTC-12"
    assert len(ctx.placed) == 0

    # 2. Modify fails, cancel succeeds -> cancels OLD-GTC-12 first, then places single 6-sh replacement GTC stop!
    ctx.allow_cancel = True
    replaced_oid = adapter.place_or_update_protective_stop("NVDA", 6.0, 148.0, existing_order_id="OLD-GTC-12")
    assert replaced_oid == "NEW-GTC-1"
    assert ctx.cancelled == ["OLD-GTC-12"]
    assert len(ctx.placed) == 1 and ctx.placed[0]["qty"] == 6.0

    # 3. Fractional stop: ignores yesterday's pre-entry low, and prices marketable sell near live $840 bid on crash!
    settings = AppSettings.load()
    store = StateStore(tmp_path / "frac_multifill.db")
    daemon = TradingDaemon(settings, store=store, notifier=AlertNotifier())
    pipe = DailySwingPipeline(daemon)

    entry_t = datetime(2026, 11, 24, 20, 45, tzinfo=timezone.utc)
    store.upsert_position(
        OpenPosition("LLY", "Healthcare", 0.50, 0.50, 920.0, entry_t, 900.0, 900.0, 925.0, 20.0, stop_order_type="CLIENT_FRAC_RTH")
    )
    # Yesterday 14:00 ET bar (before entry_t!) had low $880, today quote is healthy $930 -> NO false stop!
    daemon.market_data.get_bars = lambda sym, timeframe="15m", days=2: [  # type: ignore[method-assign]
        Bar("LLY", datetime(2026, 11, 24, 19, 0, tzinfo=timezone.utc), 890.0, 895.0, 880.0, 892.0, 100_000.0)
    ]
    daemon.market_data.get_quote = lambda sym: QuoteSnapshot("LLY", 930.0, 929.8, 930.2, 900_000_000.0, datetime(2026, 11, 25, 14, 35, tzinfo=timezone.utc))  # type: ignore[method-assign]
    assert pipe.execute_fractional_client_stops(datetime(2026, 11, 25, 14, 35, tzinfo=timezone.utc)) == []

    # Now simulate live gap-down crash to $840 (well below $900 stop) with real broker account active:
    submitted_limits: List[float] = []
    daemon.gateway.has_target_account = lambda: True  # type: ignore[method-assign]
    daemon.gateway.place_limit_order = lambda sym, side, qty, px, remark="": (submitted_limits.append(px) or "BRK-FRAC-1")  # type: ignore[method-assign]
    daemon.market_data.get_quote = lambda sym: QuoteSnapshot("LLY", 840.0, 839.5, 840.5, 900_000_000.0, datetime(2026, 11, 25, 14, 36, tzinfo=timezone.utc))  # type: ignore[method-assign]
    trig = pipe.execute_fractional_client_stops(datetime(2026, 11, 25, 14, 36, tzinfo=timezone.utc))
    assert len(trig) == 1
    assert submitted_limits[0] < 840.0  # Anchored below crashing $839.50 bid, NEVER hung at $900!

    # 4. Fast push FILLED race on `_mark_order_submitted_safely` + Multi-fill `virtual_cash` exactness!
    store_mf = StateStore(tmp_path / "multifill_exact.db")
    daemon_mf = TradingDaemon(settings, store=store_mf, notifier=AlertNotifier())
    tier_key = "tier_10k"
    tier = settings.tiers_cfg.get_active(tier_key)
    t0 = datetime(2026, 11, 25, 20, 40, tzinfo=timezone.utc)

    buy_meta = {
        "symbol": "MSFT", "sector": "Technology", "planned_shares": 10.0, "planned_entry_price": 200.0,
        "limit_price": 202.5, "initial_stop": 188.0, "risk_per_share": 12.0, "entry_atr_1d": 5.5,
        "setup": "DAILY_PULLBACK_EMA21", "tier_key": tier_key,
    }
    store_mf.record_order_intent("MF-BUY-1", "MSFT", OrderSide.BUY, 10.0, 202.5, "test", plan_meta=buy_meta, created_at=t0)
    store_mf.update_order_status("MF-BUY-1", OrderStatus.SUBMITTED, broker_order_id="B-MF-1")

    # Print 1: 5 sh @ $200.00 -> Print 2: remaining 5 sh @ $202.00 (cumulative avg_fill_price = $201.00!)
    daemon_mf.on_broker_order_update(
        BrokerOrderUpdate(
            broker_order_id="B-MF-1", symbol="MSFT", side=OrderSide.BUY, order_type="NORMAL", status=OrderStatus.PARTIAL_FILLED,
            raw_status="FILLED_PART", qty=10.0, price=202.5, filled_shares=5.0, avg_fill_price=200.0, remark="MF-BUY-1",
        )
    )
    daemon_mf.on_broker_order_update(
        BrokerOrderUpdate(
            broker_order_id="B-MF-1", symbol="MSFT", side=OrderSide.BUY, order_type="NORMAL", status=OrderStatus.FILLED,
            raw_status="FILLED_ALL", qty=10.0, price=202.5, filled_shares=10.0, avg_fill_price=201.0, remark="MF-BUY-1",
        )
    )
    # Even if main thread calls `_mark_order_submitted_safely` AFTER fast FILLED push, status stays FILLED!
    assert daemon_mf._mark_order_submitted_safely("MF-BUY-1", "B-MF-1", "fail") is True
    ord_row = store_mf.get_order("MF-BUY-1")
    assert ord_row is not None and ord_row["status"] == "FILLED"

    rec_after_buy = store_mf.reconcile_stateful_ledger(tier_key, tier.reference_equity_usd)
    assert rec_after_buy["ok"] is True
    assert abs(rec_after_buy["drift_usd"]) <= 0.01  # $0.00 drift (NOT $5.00)!

    # Multi-print SELL: 4 sh @ $215.00 -> 6 sh @ $220.00 (cumulative avg = $218.00)
    sell_meta = {
        "symbol": "MSFT", "entry_price": 201.0, "risk_per_share": 13.0, "setup": "DAILY_PULLBACK_EMA21",
        "entry_time": t0.isoformat(), "exit_reason": "CHANDELIER_TRAIL", "is_scale_out": False, "tier_key": tier_key,
    }
    store_mf.record_order_intent("MF-SELL-1", "MSFT", OrderSide.SELL, 10.0, 215.0, "exit", plan_meta=sell_meta, created_at=t0)
    store_mf.update_order_status("MF-SELL-1", OrderStatus.SUBMITTED, broker_order_id="S-MF-1")
    daemon_mf.on_broker_order_update(
        BrokerOrderUpdate(
            broker_order_id="S-MF-1", symbol="MSFT", side=OrderSide.SELL, order_type="NORMAL", status=OrderStatus.PARTIAL_FILLED,
            raw_status="FILLED_PART", qty=10.0, price=215.0, filled_shares=4.0, avg_fill_price=215.0, remark="MF-SELL-1",
        )
    )
    daemon_mf.on_broker_order_update(
        BrokerOrderUpdate(
            broker_order_id="S-MF-1", symbol="MSFT", side=OrderSide.SELL, order_type="NORMAL", status=OrderStatus.FILLED,
            raw_status="FILLED_ALL", qty=10.0, price=215.0, filled_shares=10.0, avg_fill_price=218.0, remark="MF-SELL-1",
        )
    )

    rec_after_sell = store_mf.reconcile_stateful_ledger(tier_key, tier.reference_equity_usd)
    assert rec_after_sell["ok"] is True
    assert abs(rec_after_sell["drift_usd"]) <= 0.01


def test_earnings_cache_resilience_amc_pead_yellow_sizing_and_web_swing_mode(tmp_path: Path) -> None:
    """
    Regression tests for Review Findings #5, #6, & #8:
      - `EarningsCalendarFeed` preserves valid disk cache when Nasdaq HTTP fails (never overwrites with `{}`)
      - `PEAD_CONTINUATION` evaluates gap reaction bar on `T+1` for `AMC` earnings
      - Un-splittable 1-share lot at `+1.8R` (`< +2.5R`) exits 100% before earnings
      - `YELLOW` (`0.65x`) & `tier_10k` `6%+` daily stops size cleanly without floor starvation
      - `ControlCenterApplication.get_strategy_summary()` exposes `swing_1d` blueprint
    """
    cache_file = tmp_path / "earnings_calendar.json"
    cache_file.write_text(
        json.dumps({"updated_at": "2020-01-01T00:00:00+00:00", "events": {"NVDA": {"date": "2026-12-03", "timing": "AMC"}}}),
        encoding="utf-8",
    )

    def broken_nasdaq(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    feed = EarningsCalendarFeed(cache_path=cache_file, http_client=httpx.Client(transport=httpx.MockTransport(broken_nasdaq)))
    ev_out = feed.get_upcoming_events(symbols=["NVDA"], ref_day=date(2026, 11, 25), allow_network=True)
    assert "NVDA" in ev_out and ev_out["NVDA"].timing == "AMC"
    disk_after = json.loads(cache_file.read_text(encoding="utf-8"))
    assert "NVDA" in disk_after["events"]  # Disk cache NOT wiped on HTTP 503!

    # AMC PEAD Continuation: Wednesday AMC report (Nov 18) gaps up Thursday (Nov 19 = reaction_date), breaks out Monday Nov 23 (T+2)!
    settings = AppSettings.load()
    swing_eng = DailySwingSignalEngine(settings.strategy_cfg.entry, settings.strategy_cfg.regime, settings.strategy_cfg.swing)
    qqq_d = _make_daily_series("QQQ", n_days=120, end_d=date(2026, 11, 23), start_px=480.0, daily_drift=0.001)
    nvda_d = _make_daily_series("NVDA", n_days=120, end_d=date(2026, 11, 18), start_px=130.0, daily_drift=0.0015)
    pre_c = nvda_d[-1].close
    nvda_d.extend([
        # Thu Nov 19 (AMC reaction gap-up +6.5%!)
        Bar("NVDA", datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc), round(pre_c * 1.05, 2), round(pre_c * 1.07, 2), round(pre_c * 1.045, 2), round(pre_c * 1.065, 2), 18_000_000.0),
        # Fri Nov 20 (tight inside consolidation holding above gap low)
        Bar("NVDA", datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc), round(pre_c * 1.06, 2), round(pre_c * 1.068, 2), round(pre_c * 1.052, 2), round(pre_c * 1.064, 2), 9_000_000.0),
        # Mon Nov 23 (T+2 PEAD continuation breakout above post-earnings high on 1.5x RVOL!)
        Bar("NVDA", datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc), round(pre_c * 1.066, 2), round(pre_c * 1.09, 2), round(pre_c * 1.062, 2), round(pre_c * 1.085, 2), 16_000_000.0),
    ])
    pead_sig = swing_eng.evaluate_entry(
        "NVDA", "Semiconductors", RegimeLight.GREEN, 85.0, nvda_d, qqq_d,
        as_of_date=date(2026, 11, 23), recent_earnings=EarningsEvent("NVDA", date(2026, 11, 18), "AMC"), include_completed_as_of=True,
    )
    assert pead_sig is not None and pead_sig.setup == SetupType.PEAD_CONTINUATION

    # Un-splittable 1-share lot at +1.8R (< +2.5R) exits 100% before earnings!
    exit_eng = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_10k = settings.tiers_cfg.get_active("tier_10k")
    pos_1sh = OpenPosition("COST", "Consumer", 1.0, 1.0, 900.0, datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc), 850.0, 850.0, 995.0, 50.0)
    act_1sh = exit_eng.step_daily_position(
        pos_1sh,
        Bar("COST", datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc), 985.0, 995.0, 980.0, 990.0, 1_500_000.0),
        atr_1d=18.0,
        tier=tier_10k,
        upcoming_earnings=EarningsEvent("COST", date(2026, 11, 19), "AMC"),
    )
    assert act_1sh.should_exit is True and act_1sh.shares_to_sell == 1.0

    # YELLOW regime (`0.65x` scale) & `tier_10k` `6.05%` 1d stop approve whole-share swing sizing!
    sig_200 = EntrySignal("META", "Communication", SetupType.VCP_PIVOT_BREAKOUT, datetime(2026, 11, 23, 20, 45, tzinfo=timezone.utc), 200.0, 5.5, 195.0, 1.6, 84.0, "test")
    plan_green, st_g = exit_eng.build_swing_position_plan(sig_200, 10000.0, 10000.0, tier_10k, position_scale=1.0)
    plan_yellow, st_y = exit_eng.build_swing_position_plan(sig_200, 10000.0, 10000.0, tier_10k, position_scale=0.65)
    assert st_g == "APPROVED" and plan_green is not None and plan_green.shares == 8.0
    assert st_y == "APPROVED" and plan_yellow is not None and plan_yellow.shares == 5.0

    # Web strategy blueprint reflects `swing_1d`
    from zy_quant.web.app import ctx as web_ctx

    bp = web_ctx.get_strategy_summary(mode_override="swing_1d")
    assert bp["mode"] == "swing_1d"
    assert "GTC STOP" in bp["initial_stop_rule"]


def test_round2_rpc_failure_safety_phase_sleep_tier1k_and_hkd_isolation(tmp_path: Path) -> None:
    """
    Regression tests for all Round-2 Review Findings (#1..#6):
      1. Futu RPC failure (`last_order_sync_ok=False` / `last_position_sync_ok=False`) never places
         duplicate `GTC STOP` orders, never wipes `broker_stop_order_id`, and never deletes SQLite positions
      2. Phase-boundary-aware `next_sleep_minutes` wakes right at `15:35 ET` / `12:35 ET` (never overshoots EOD)
      3. `tier_1k` (`$1,000`) executes swing trades without `25%` single-asset cap deadlock
      4. `get_account_summary()` isolates `HKD` fallback rows and caps Cash Plus at `equity - market_val`
      5. `EarningsCalendarFeed` preserves forward disk cache when mid-loop HTTP returns `429`
      6. Multi-fill `_handle_protective_stop_update` maintains `$0.00` stateful `virtual_cash` drift
    """
    from zy_quant.runtime.pipeline import _compute_phase_aware_sleep_minutes

    settings = AppSettings.load()

    # 1. Futu RPC failure safety (`order_list_query` & `position_list_query` transient errors)
    store_rpc = StateStore(tmp_path / "rpc_guard.db")
    daemon_rpc = TradingDaemon(settings, store=store_rpc, notifier=AlertNotifier())
    pipe_rpc = DailySwingPipeline(daemon_rpc)

    now_pre = datetime(2026, 11, 25, 9, 25, tzinfo=ET_TZ).astimezone(timezone.utc)
    live_pos = OpenPosition(
        "NVDA", "Semiconductors", 10.0, 10.0, 150.0, now_pre, 140.0, 140.0, 152.0, 10.0,
        stop_order_type="GTC_STOP", broker_stop_order_id="GTC-LIVE-99",
    )
    store_rpc.upsert_position(live_pos)

    placed_stops: List[str] = []
    daemon_rpc.gateway.trd_env_name = "REAL"
    daemon_rpc.gateway.has_target_account = lambda: True  # type: ignore[method-assign]
    daemon_rpc.gateway.is_connected = lambda: True  # type: ignore[method-assign]
    daemon_rpc.gateway.last_order_sync_ok = False
    daemon_rpc.gateway.last_position_sync_ok = False
    daemon_rpc.gateway.sync_open_orders = lambda refresh_cache=False: []  # type: ignore[method-assign]
    daemon_rpc.gateway.get_open_positions = lambda: {}  # type: ignore[method-assign]
    daemon_rpc._place_stop_order = lambda sym, sh, px, oid=None: (placed_stops.append(sym) or "DUP-STOP")  # type: ignore[method-assign]

    # Neither startup_reconcile nor heal_overnight_gtc_stops may delete NVDA or place a duplicate stop!
    daemon_rpc.startup_reconcile()
    assert "NVDA" in store_rpc.get_positions()
    healed_on_err = pipe_rpc.heal_overnight_gtc_stops(now_pre, already_reconciled=True)
    assert healed_on_err == []
    assert placed_stops == []
    assert store_rpc.get_positions()["NVDA"].broker_stop_order_id == "GTC-LIVE-99"

    # 2. Phase-boundary-aware `next_sleep_minutes` (never overshoots 25-min EOD window!)
    assert _compute_phase_aware_sleep_minutes(datetime(2026, 11, 25, 15, 25, tzinfo=ET_TZ), PipelinePhase.IDLE_GTC_ARMED, 0, False) == 10
    assert _compute_phase_aware_sleep_minutes(datetime(2026, 11, 27, 12, 34, tzinfo=ET_TZ), PipelinePhase.IDLE_GTC_ARMED, 0, False) == 1
    assert _compute_phase_aware_sleep_minutes(datetime(2026, 11, 25, 9, 27, tzinfo=ET_TZ), PipelinePhase.PRE_OPEN_SYNC, 0, False) == 3

    # 3. `tier_1k` (`$1,000`) sizing + MandateRiskGate + BacktestEngine non-deadlock verification!
    exit_eng = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_1k = settings.tiers_cfg.get_active("tier_1k")
    sig_1k = EntrySignal("COST", "Consumer", SetupType.VCP_PIVOT_BREAKOUT, now_pre, 520.0, 8.5, 512.0, 1.7, 84.0, "test")
    plan_1k, st_1k = exit_eng.build_swing_position_plan(sig_1k, 1000.0, 1000.0, tier_1k, position_scale=1.0)
    assert st_1k == "APPROVED" and plan_1k is not None and plan_1k.notional_usd > 250.0

    gate_1k = MandateRiskGate(StateStore(tmp_path / "gate_1k.db"), settings.strategy_cfg.daemon)
    dec_1k = gate_1k.check_entry(plan_1k, tier_1k, 1000.0, {}, now=now_pre, enforce_swing_heat=True)
    assert dec_1k.allowed is True, dec_1k.reason

    cache_1k = HistoricalBarCache(tmp_path / "bt_1k.db")
    for idx, sym in enumerate(["QQQ", "SPY", "NVDA", "XOM"]):
        b1d, _ = _make_consistent_1d_and_15m_series(
            sym, n_days=270, intraday_days=5, end_d=date(2026, 11, 25),
            start_px=95.0 + idx * 10.0, daily_drift=0.0028 if sym not in ("QQQ", "SPY") else 0.0016, seed_offset=idx * 5,
        )
        cache_1k.upsert_bars("1d", b1d)
    ad_1k = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_1k)
    bt_1k = BacktestEngine(settings, ad_1k).run(
        tier_key="tier_1k", symbols=["NVDA", "XOM"], days=90, mode="swing_1d", clock_mode="1d",
        earnings_events={"NVDA": EarningsEvent("NVDA", date(2026, 11, 20), "AMC"), "XOM": EarningsEvent("XOM", date(2026, 11, 20), "AMC")},
    )
    assert bt_1k.total_fees_usd > 0.0  # Real swing orders executed on tier_1k!

    # 4. Futu HK `HKD` fallback isolation & non-leveraged Cash Plus `equity - market_val` cap!
    futu_ad = FutuGatewayAdapter(trd_env="REAL")

    class DummyFutuCcy:
        RET_OK = 0

        class TrdEnv:
            REAL = "REAL"

        class Currency:
            USD = "USD"

    class CcyCtx:
        def __init__(self) -> None:
            self.fail_usd_query = True

        def get_acc_list(self) -> tuple[int, pd.DataFrame]:
            return 0, pd.DataFrame([{"acc_id": 999, "trd_env": "REAL", "acc_type": "MARGIN"}])

        def accinfo_query(self, **kwargs: Any) -> tuple[int, Any]:
            if kwargs.get("currency") == "USD" and self.fail_usd_query:
                return -1, "USD query unsupported"
            if kwargs.get("currency") == "USD":
                # MARGIN account where intraday stock purchase raised market_val=$7,100 while $8,400 MMF redeems T+1
                return 0, pd.DataFrame([{
                    "total_assets": 12500.0, "us_cash": 0.0, "fund_assets": 8400.0, "market_val": 7100.0, "power": 16000.0,
                }])
            # HKD fallback row (`HK$78,000` == `$10,000 USD`!)
            return 0, pd.DataFrame([{
                "currency": "HKD", "total_assets": 78000.0, "cash": 32700.0, "fund_assets": 45000.0,
                "usd_assets": 10000.0, "us_cash": 4200.0, "us_market_val": 5800.0, "power": 120000.0,
            }])

    ccy_ctx = CcyCtx()
    futu_ad._futu_mod = DummyFutuCcy
    futu_ad._trade_ctx = ccy_ctx
    futu_ad.is_connected = lambda: True  # type: ignore[method-assign]

    hkd_fb_acct = futu_ad.get_account_summary()
    assert hkd_fb_acct["equity_usd"] == 10000.0  # Strictly USD, NEVER HK$78,000!
    assert hkd_fb_acct["cash_usd"] == 4200.0

    ccy_ctx.fail_usd_query = False
    margin_mmf_acct = futu_ad.get_account_summary()
    assert margin_mmf_acct["cash_usd"] == 5400.0  # Capped at `12500 - 7100`, NEVER $8,400 or $16,000 leverage!

    # 5. `EarningsCalendarFeed` partial HTTP 429 mid-loop resilience
    partial_cache = tmp_path / "partial_earn.json"
    partial_cache.write_text(
        json.dumps({"updated_at": "2020-01-01T00:00:00+00:00", "events": {"NVDA": {"date": "2026-12-03", "timing": "AMC"}}}),
        encoding="utf-8",
    )

    def mid_loop_429(req: httpx.Request) -> httpx.Response:
        q_d = req.url.params.get("date", "")
        if q_d < "2026-11-25":
            return httpx.Response(200, json={"data": {"rows": [{"symbol": "CRM", "time": "time-after-hours"}]}})
        return httpx.Response(429, text="Rate limited")

    p_feed = EarningsCalendarFeed(cache_path=partial_cache, http_client=httpx.Client(transport=httpx.MockTransport(mid_loop_429)))
    ev_partial = p_feed.get_upcoming_events(symbols=["NVDA", "CRM"], ref_day=date(2026, 11, 25), allow_network=True)
    assert "NVDA" in ev_partial  # Forward cached earnings NOT wiped when forward HTTP hit 429!
    assert "NVDA" in json.loads(partial_cache.read_text(encoding="utf-8"))["events"]

    # 6. Multi-fill `_handle_protective_stop_update` exact delta accounting
    store_st = StateStore(tmp_path / "stop_multifill.db")
    daemon_st = TradingDaemon(settings, store=store_st, notifier=AlertNotifier())
    t_10k = settings.tiers_cfg.get_active("tier_10k")
    buy_fee_st = daemon_st.fee_model.estimate_leg(10.0, 150.0, OrderSide.BUY).total_friction_usd
    plan_st = PositionPlan("AVGO", "Semiconductors", 10.0, 150.0, 150.1, 140.0, 10.0, 100.0, 1500.0, 4.1, 2.5, 0.5, SetupType.VCP_PIVOT_BREAKOUT)
    sig_st = EntrySignal("AVGO", "Semiconductors", SetupType.VCP_PIVOT_BREAKOUT, now_pre, 150.0, 4.5, 148.0, 1.5, 84.0, "test")
    store_st.record_order_intent("BUY-AVGO-ST", "AVGO", OrderSide.BUY, 10.0, 150.1, "test", created_at=now_pre)
    daemon_st._settle_paper_local_buy("BUY-AVGO-ST", "AVGO", plan_st, sig_st, buy_fee_st, now_pre, "tier_10k", t_10k)

    pos_avgo = store_st.get_positions()["AVGO"]
    pos_avgo.broker_stop_order_id = "GTC-STOP-AVGO"
    store_st.upsert_position(pos_avgo)
    daemon_st.gateway.query_order_fee = lambda oid: 2.50  # type: ignore[method-assign]

    # Stop fills in 2 prints: 4 sh @ $140.00 -> 6 sh @ $138.00 (cumulative avg = $138.80, total fee = $2.50)
    daemon_st.on_broker_order_update(
        BrokerOrderUpdate("GTC-STOP-AVGO", "AVGO", OrderSide.SELL, "STOP", OrderStatus.PARTIAL_FILLED, "FILLED_PART", 10.0, 140.0, 4.0, 140.0)
    )
    daemon_st.on_broker_order_update(
        BrokerOrderUpdate("GTC-STOP-AVGO", "AVGO", OrderSide.SELL, "STOP", OrderStatus.FILLED, "FILLED_ALL", 10.0, 140.0, 10.0, 138.8)
    )
    rec_st = store_st.reconcile_stateful_ledger("tier_10k", t_10k.reference_equity_usd)
    assert rec_st["ok"] is True
    assert abs(rec_st["drift_usd"]) <= 0.01


def test_round3_filled_gtc_stop_guard_cancel_safety_and_phantom_position_guard(tmp_path: Path) -> None:
    """
    Regression tests for all Round-3 Review Findings (#1..#3):
      1. `place_or_update_protective_stop` NEVER submits a replacement `SELL STOP` when `existing_order_id`
         already `FILLED` (or partially filled), dispatching the fill instead (zero short-flip!)
      2. `cancel_order()` returns `False` for real broker order IDs when disconnected / `acc_id == 0`,
         full `SELL` exit aborts if `cancel_order` fails, and re-arms `GTC STOP` if `place_limit_order` fails!
      3. Concurrent `ZyTradeOrderHandler` stop-fill deletion during `_execute_eod_decision_dag` I/O
         cannot be resurrected as a phantom SQLite position by Step (b)!
    """
    adapter = FutuGatewayAdapter(trd_env="REAL")
    # 2a. Disconnected `cancel_order` MUST return `False` for real broker IDs, `True` for SIM-* IDs!
    assert adapter.cancel_order("SIM-LOCAL-STOP-NVDA") is True
    assert adapter.cancel_order("GTC-REAL-9001") is False

    class DummyFutuR3:
        RET_OK = 0

        class TrdEnv:
            REAL = "REAL"

        class TrdSide:
            SELL = "SELL"

        class OrderType:
            STOP = "STOP"

        class TimeInForce:
            GTC = "GTC"

        class ModifyOrderOp:
            NORMAL = "NORMAL"
            CANCEL = "CANCEL"

    class FilledStopCtx:
        def __init__(self) -> None:
            self.placed: List[Dict[str, Any]] = []

        def get_acc_list(self) -> tuple[int, pd.DataFrame]:
            return 0, pd.DataFrame([{"acc_id": 555, "trd_env": "REAL", "acc_type": "MARGIN"}])

        def modify_order(self, **kwargs: Any) -> tuple[int, Any]:
            return -1, "Order already filled"

        def place_order(self, **kwargs: Any) -> tuple[int, pd.DataFrame]:
            self.placed.append(kwargs)
            return 0, pd.DataFrame([{"order_id": "BAD-DUP-STOP"}])

    f_ctx = FilledStopCtx()
    adapter._futu_mod = DummyFutuR3
    adapter._trade_ctx = f_ctx
    adapter.is_connected = lambda: True  # type: ignore[method-assign]
    dispatched: List[BrokerOrderUpdate] = []
    adapter.register_order_callback(dispatched.append)

    def _sync_filled(refresh_cache: bool = False) -> List[BrokerOrderUpdate]:
        adapter.last_order_sync_ok = True
        return [
            BrokerOrderUpdate(
                "GTC-10", "NVDA", OrderSide.SELL, "STOP", OrderStatus.FILLED, "FILLED_ALL", 10.0, 140.0, 10.0, 140.0
            )
        ]

    adapter.sync_open_orders = _sync_filled  # type: ignore[method-assign]
    out_oid = adapter.place_or_update_protective_stop("NVDA", 10.0, 145.0, existing_order_id="GTC-10")
    assert out_oid == "GTC-10"
    assert len(f_ctx.placed) == 0  # ZERO duplicate GTC STOP placed on already-filled stop!
    assert len(dispatched) == 1 and dispatched[0].status == OrderStatus.FILLED

    # 2b & 3. Full SELL exit cancel-failure guard, naked-stop re-arm on rejected SELL, & phantom resurrection guard
    settings = AppSettings.load()
    store = StateStore(tmp_path / "r3_eod.db")
    daemon = TradingDaemon(settings, store=store, notifier=AlertNotifier())
    pipe = DailySwingPipeline(daemon)

    target_d = date(2026, 11, 25)
    now_eod = datetime(2026, 11, 25, 15, 45, tzinfo=ET_TZ).astimezone(timezone.utc)
    d_map = {
        sym: _make_daily_series(sym, n_days=120, end_d=date(2026, 11, 24), start_px=150.0, seed_offset=i * 3)
        for i, sym in enumerate(["QQQ", "SPY", "XOM", "JPM"])
    }
    pipe._fetch_260d_universe = lambda syms: d_map  # type: ignore[method-assign]
    daemon.earnings_feed.get_upcoming_events = lambda **kw: {  # type: ignore[method-assign]
        "XOM": EarningsEvent("XOM", target_d, "AMC"),  # Triggers 100% full exit (< +1.0R)!
    }
    daemon.market_data.has_credentials = lambda: False  # type: ignore[method-assign]
    daemon.market_data.get_bars = lambda sym, timeframe="15m", days=2: []  # type: ignore[method-assign]
    daemon.market_data.get_quote = lambda sym: QuoteSnapshot(sym, 152.0, 151.9, 152.1, 900_000_000.0, now_eod)  # type: ignore[method-assign]
    daemon.gateway.trd_env_name = "REAL"
    daemon.gateway.has_target_account = lambda: True  # type: ignore[method-assign]
    daemon.gateway.is_connected = lambda: True  # type: ignore[method-assign]
    daemon.gateway.get_account_summary = lambda: {"equity_usd": 10000.0, "cash_usd": 0.0}  # type: ignore[method-assign]

    # XOM will trigger full pre-earnings exit; JPM will evaluate non-exit ratchet!
    store.upsert_position(
        OpenPosition("XOM", "Energy", 10.0, 10.0, 150.0, now_eod - timedelta(days=3), 140.0, 140.0, 153.0, 10.0, broker_stop_order_id="STOP-XOM-1")
    )
    store.upsert_position(
        OpenPosition("JPM", "Financials", 10.0, 10.0, 150.0, now_eod - timedelta(days=3), 140.0, 140.0, 153.0, 10.0, broker_stop_order_id="STOP-JPM-1")
    )

    sell_attempts: List[str] = []
    rearmed_stops: List[str] = []
    cancel_should_succeed = False
    daemon.gateway.cancel_order = lambda oid: cancel_should_succeed  # type: ignore[method-assign]
    daemon.gateway.place_limit_order = lambda sym, side, qty, px, remark="": (sell_attempts.append(sym) or None)  # type: ignore[method-assign]
    daemon._place_stop_order = lambda sym, sh, px, oid=None: (rearmed_stops.append(sym) or f"REARMED-{sym}")  # type: ignore[method-assign]

    # Case A: `cancel_order` returns False -> full SELL must abort (0 `place_limit_order` calls, STOP-XOM-1 kept!)
    tier_10k = settings.tiers_cfg.get_active("tier_10k")
    pipe._execute_eod_decision_dag(now_eod, target_d, "tier_10k", tier_10k, ["XOM"], check_edgar=False)
    assert sell_attempts == []
    assert store.get_positions()["XOM"].broker_stop_order_id == "STOP-XOM-1"

    # Case B: Reset XOM earnings flag, `cancel_order` succeeds, `place_limit_order` fails (`None`) ->
    # MUST immediately re-arm XOM's GTC stop so position is NEVER left naked!
    # ALSO simulate concurrent push-thread stop fill deleting JPM during `get_upcoming_events` I/O ->
    # JPM MUST NOT be resurrected as a phantom open position!
    p_xom = store.get_positions()["XOM"]
    p_xom.earnings_cushion_applied_date = None
    store.upsert_position(p_xom)
    cancel_should_succeed = True

    orig_step = daemon.exit_engine.step_daily_position

    def _step_with_concurrent_jpm_close(pos: OpenPosition, *a: Any, **kw: Any) -> Any:
        res = orig_step(pos, *a, **kw)
        if pos.symbol == "JPM":
            store.remove_position("JPM")  # Concurrent ZyTradeOrderHandler closed JPM mid-Step (b)!
        return res

    daemon.exit_engine.step_daily_position = _step_with_concurrent_jpm_close  # type: ignore[method-assign]
    pipe._execute_eod_decision_dag(now_eod + timedelta(minutes=1), target_d, "tier_10k", tier_10k, ["XOM"], check_edgar=False)

    final_positions = store.get_positions()
    assert "JPM" not in final_positions  # Closed JPM was NOT resurrected!
    assert final_positions["XOM"].broker_stop_order_id == "REARMED-XOM"  # XOM re-armed after rejected SELL!
