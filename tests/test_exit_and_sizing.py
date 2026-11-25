from datetime import datetime, timezone

from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.models import Bar, EntrySignal, ExitReason, OpenPosition, SetupType
from zy_quant.strategy.exit_models import ScientificExitAndSizingEngine


def test_tier_1k_blocks_tiny_edge_and_approves_wide_swing() -> None:
    settings = AppSettings.load()
    engine = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_1k = settings.tiers_cfg.get_active("tier_1k")

    # Tiny ATR ($0.50 on $200 stock -> expected move ~1.05% < 3.2% Tier 1k floor) -> REJECTED
    weak_sig = EntrySignal(
        symbol="AAPL",
        sector="Technology",
        setup=SetupType.VWAP_PULLBACK,
        timestamp=datetime.now(timezone.utc),
        entry_price=200.0,
        atr_14=0.50,
        vwap=199.5,
        rvol=1.6,
        watchlist_score=82.0,
        rationale="test",
    )
    plan_weak, reason_weak = engine.build_position_plan(weak_sig, 1000.0, 1000.0, tier_1k)
    assert plan_weak is None
    assert "Expected move" in reason_weak

    # Strong swing ATR ($2.20 on $200 stock -> expected move 4.62% >= 3.2%) -> APPROVED
    strong_sig = EntrySignal(
        symbol="NVDA",
        sector="Semiconductors",
        setup=SetupType.VWAP_PULLBACK,
        timestamp=datetime.now(timezone.utc),
        entry_price=200.0,
        atr_14=2.20,
        vwap=199.0,
        rvol=1.8,
        watchlist_score=88.0,
        rationale="test",
    )
    plan_ok, status = engine.build_position_plan(strong_sig, 1000.0, 1000.0, tier_1k)
    assert status == "APPROVED"
    assert plan_ok is not None
    assert plan_ok.expectancy_r >= 0.25


def test_chandelier_ratchet_and_tier_100k_scale_out() -> None:
    settings = AppSettings.load()
    engine = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_100k = settings.tiers_cfg.get_active("tier_100k")
    now = datetime.now(timezone.utc)

    pos = OpenPosition(
        symbol="MSFT",
        sector="Technology",
        shares=40.0,
        initial_shares=40.0,
        entry_price=400.0,
        entry_time=now,
        initial_stop=394.0,  # 1R = $6/sh
        current_stop=394.0,
        highest_price=400.0,
        risk_per_share=6.0,
    )

    # Price rallies to $413 (+2.16R) -> triggers 50% scale-out (20 shares) & lifts stop above breakeven!
    bar_rally = Bar("MSFT", now, open=405.0, high=413.0, low=404.5, close=412.5, volume=500_000)
    act1 = engine.step_position(pos, bar_rally, atr_14=2.5, tier=tier_100k)
    assert act1.should_exit is True
    assert act1.reason == ExitReason.SCALE_OUT_2R
    assert act1.shares_to_sell == 20.0
    assert pos.current_stop > 400.0
    pos.shares -= act1.shares_to_sell

    # Price continues to $425 -> Chandelier ratchets stop strictly upward
    stop_before = pos.current_stop
    bar_higher = Bar("MSFT", now, open=413.0, high=425.0, low=412.5, close=424.0, volume=500_000)
    act2 = engine.step_position(pos, bar_higher, atr_14=2.5, tier=tier_100k)
    assert act2.should_exit is False
    assert pos.current_stop > stop_before


def test_triple_barrier_time_stop_exits_dead_trade() -> None:
    settings = AppSettings.load()
    engine = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_10k = settings.tiers_cfg.get_active("tier_10k")
    now = datetime.now(timezone.utc)

    max_bars = settings.strategy_cfg.exit_models.time_stop_bars_15m
    pos = OpenPosition(
        symbol="AAPL",
        sector="Technology",
        shares=15.0,
        initial_shares=15.0,
        entry_price=220.0,
        entry_time=now,
        initial_stop=216.0,
        current_stop=216.0,
        highest_price=220.5,
        risk_per_share=4.0,
        bars_held_15m=max_bars - 1,
    )
    # Final allowed bar closes flat at $220.20 (+0.05R < min_progress_r) -> Time Barrier exits!
    flat_bar = Bar("AAPL", now, open=220.1, high=220.4, low=219.8, close=220.2, volume=200_000)
    act = engine.step_position(pos, flat_bar, atr_14=1.8, tier=tier_10k)
    assert act.should_exit is True
    assert act.reason == ExitReason.TIME_BARRIER


def test_futu_hk_tier_10k_scale_out_progressive_floor_and_t0_eod_exit() -> None:
    settings = AppSettings.load()
    engine = ScientificExitAndSizingEngine(settings.strategy_cfg.exit_models, FutuUSFeeModel())
    tier_1k = settings.tiers_cfg.get_active("tier_1k")
    tier_10k = settings.tiers_cfg.get_active("tier_10k")
    # Wednesday 2026-11-25
    wed_entry = datetime(2026, 11, 25, 11, 0, tzinfo=timezone.utc)
    wed_close = datetime(2026, 11, 25, 15, 45, tzinfo=timezone.utc)

    # 1. Tier 2 ($10k) unlocks 50% scale-out at +1.8R when half-lot >= $1,500
    pos_10k = OpenPosition(
        symbol="NVDA",
        sector="Semiconductors",
        shares=24.0,
        initial_shares=24.0,
        entry_price=180.0,
        entry_time=wed_entry,
        initial_stop=175.0,  # 1R = $5
        current_stop=175.0,
        highest_price=180.0,
        risk_per_share=5.0,
    )
    rally_bar = Bar("NVDA", wed_close, open=186.0, high=190.0, low=185.5, close=189.5, volume=400_000)
    act_scale = engine.step_position(pos_10k, rally_bar, atr_14=1.8, tier=tier_10k)
    assert act_scale.should_exit is True
    assert act_scale.reason == ExitReason.SCALE_OUT_2R
    assert act_scale.shares_to_sell == 12.0

    # 2. Progressive profit-floor locks >= 55% of +2.0R gain on Tier 1k single-leg position
    pos_1k = OpenPosition(
        symbol="META",
        sector="Communication",
        shares=1.0,
        initial_shares=1.0,
        entry_price=600.0,
        entry_time=wed_entry,
        initial_stop=590.0,  # 1R = $10
        current_stop=590.0,
        highest_price=600.0,
        risk_per_share=10.0,
    )
    surge_bar = Bar("META", wed_close, open=612.0, high=620.0, low=611.0, close=619.0, volume=300_000)
    act_lock = engine.step_position(pos_1k, surge_bar, atr_14=4.5, tier=tier_1k)
    assert act_lock.should_exit is False
    # 55% of $20 gain = $611.00 floor (much higher than old 620 - 4.2*4.5 = $601.10!)
    assert pos_1k.current_stop >= 611.0

    # 3. Futu HK T+0 EOD exit flattens broken day-0 trade (< -0.65R) on Tier 10k before overnight gap,
    #    while sparing flat (+0.05R) consolidations from fee churn!
    flat_pos = OpenPosition(
        symbol="MSFT",
        sector="Technology",
        shares=10.0,
        initial_shares=10.0,
        entry_price=400.0,
        entry_time=wed_entry,
        initial_stop=394.0,
        current_stop=394.0,
        highest_price=401.0,
        risk_per_share=6.0,
    )
    flat_eod_bar = Bar("MSFT", wed_close, open=400.3, high=400.6, low=399.9, close=400.3, volume=200_000)
    assert engine.step_position(flat_pos, flat_eod_bar, atr_14=2.0, tier=tier_10k, is_eod_window=True).should_exit is False

    broken_pos = OpenPosition(
        symbol="MSFT",
        sector="Technology",
        shares=10.0,
        initial_shares=10.0,
        entry_price=400.0,
        entry_time=wed_entry,
        initial_stop=394.0,
        current_stop=394.0,
        highest_price=400.5,
        risk_per_share=6.0,
    )
    # Close at $395.50 = -0.75R (< -0.65R broken threshold, above $394 hard stop) -> cut T+0 before overnight gap!
    broken_eod_bar = Bar("MSFT", wed_close, open=396.5, high=396.8, low=395.2, close=395.5, volume=250_000)
    act_eod = engine.step_position(
        broken_pos, broken_eod_bar, atr_14=2.0, tier=tier_10k, is_eod_window=True
    )
    assert act_eod.should_exit is True
    assert act_eod.reason == ExitReason.EOD_T0_FLATTEN
