from datetime import datetime, timedelta, timezone

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.models import Bar, RegimeLight, SetupType
from zy_quant.strategy.signals import IntradaySignalEngine


def test_30m_orb_signal_triggers_only_in_green_regime() -> None:
    settings = AppSettings.load()
    eng = IntradaySignalEngine(settings.strategy_cfg.entry)

    day0 = datetime(2026, 11, 20, 14, 30, tzinfo=timezone.utc)
    bars = []
    # 22 prior day warmup bars around $200
    for i in range(22):
        bars.append(
            Bar("MSFT", day0 + timedelta(minutes=15 * i), 200.0, 200.8, 199.4, 200.2, 400_000)
        )

    # Today's session: 2 ORB bars (09:30-10:00) capped at $201.50, then high-RVOL breakout to $203.00
    day1 = datetime(2026, 11, 23, 14, 30, tzinfo=timezone.utc)
    bars.append(Bar("MSFT", day1, 200.3, 201.4, 200.0, 201.0, 500_000))
    bars.append(Bar("MSFT", day1 + timedelta(minutes=15), 201.0, 201.5, 200.6, 201.2, 480_000))
    bars.append(Bar("MSFT", day1 + timedelta(minutes=30), 201.3, 203.2, 201.2, 203.0, 1_300_000))

    # Blocked in RED regime
    assert eng.evaluate_15m("MSFT", "Technology", bars, RegimeLight.RED, 85.0) is None

    # Triggered in GREEN regime
    sig = eng.evaluate_15m("MSFT", "Technology", bars, RegimeLight.GREEN, 85.0)
    assert sig is not None
    assert sig.setup == SetupType.ORB_30M


def test_backtest_runs_cleanly_across_all_three_tiers() -> None:
    settings = AppSettings.load()
    adapter = YahooMarketDataAdapter(allow_synthetic_fallback=True)
    # Pre-seed synthetic cache for fast deterministic unit test
    for sym in ["QQQ", "SPY", "NVDA", "MSFT", "META", "AAPL"]:
        adapter._bar_cache[(sym, "15m", 20)] = adapter.generate_synthetic_bars(sym, "15m", 20)
        adapter._bar_cache[(sym, "1d", 60)] = adapter.generate_synthetic_bars(sym, "1d", 60)

    bt = BacktestEngine(settings, adapter)
    for tier_key in ("tier_1k", "tier_10k", "tier_100k"):
        res = bt.run(tier_key=tier_key, symbols=["NVDA", "MSFT", "META", "AAPL"], days=20)
        assert res.initial_equity_usd == settings.tiers_cfg.tiers[tier_key].reference_equity_usd
        assert res.final_equity_usd > 0
        assert res.green_bars + res.rest_bars > 0
