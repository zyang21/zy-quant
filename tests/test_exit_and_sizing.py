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
