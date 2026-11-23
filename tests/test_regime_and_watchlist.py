from datetime import date, datetime, timedelta, timezone

from zy_quant.config import AppSettings
from zy_quant.intel.regime import MarketRegimeDetector
from zy_quant.intel.watchlist_scorer import ConservativeWatchlistScorer
from zy_quant.models import Bar, CapitalFlowSnapshot, QuoteSnapshot, RegimeLight


def test_regime_traffic_light_and_2h_rest_cadence() -> None:
    settings = AppSettings.load()
    det = MarketRegimeDetector(settings.strategy_cfg.regime)
    base = datetime(2026, 11, 23, 15, 0, tzinfo=timezone.utc)

    daily_up = [
        Bar("QQQ", base - timedelta(days=25 - i), 490 + i, 492 + i, 489 + i, 491 + i, 40_000_000)
        for i in range(25)
    ]
    intraday_up = [
        Bar("QQQ", base + timedelta(minutes=15 * i), 515 + i * 0.5, 516 + i * 0.5, 514.8 + i * 0.5, 515.5 + i * 0.5, 1_000_000)
        for i in range(6)
    ]

    snap_green = det.evaluate(intraday_up, daily_up, daily_up, vix_level=16.5)
    assert snap_green.light == RegimeLight.GREEN
    assert snap_green.recommended_scan_minutes == 15

    # Sharp intraday drop or VIX panic -> RED light & 120-minute (2h) watchlist RS refresh cadence!
    snap_red = det.evaluate(intraday_up, daily_up, daily_up, vix_level=33.0)
    assert snap_red.light == RegimeLight.RED
    assert snap_red.recommended_scan_minutes == 120


def test_conservative_watchlist_veto_and_red_day_rs_boost() -> None:
    settings = AppSettings.load()
    scorer = ConservativeWatchlistScorer(settings.watchlist_cfg)
    nvda = next(u for u in settings.watchlist_cfg.universe if u.symbol == "NVDA")
    now = datetime.now(timezone.utc)

    good_quote = QuoteSnapshot("NVDA", 145.0, 144.98, 145.02, 12_000_000_000.0, now)
    flow = CapitalFlowSnapshot("NVDA", 80_000_000, 30_000_000, 60_000_000, 40_000_000)

    # On a RED market day (QQQ -2.0%), NVDA holding +0.8% green gets strong RS boost
    res_strong = scorer.score_security(
        sec=nvda,
        quote=good_quote,
        stock_intraday_return=0.008,
        qqq_intraday_return=-0.020,
        regime_light=RegimeLight.RED,
        capital_flow=flow,
    )
    assert res_strong.passed_hard_filters is True
    assert res_strong.total_score >= 75.0

    # Earnings tomorrow -> Hard filter veto!
    res_earnings = scorer.score_security(
        sec=nvda,
        quote=good_quote,
        stock_intraday_return=0.008,
        qqq_intraday_return=-0.020,
        regime_light=RegimeLight.GREEN,
        next_earnings_date=now.date() + timedelta(days=1),
        today=now.date(),
    )
    assert res_earnings.passed_hard_filters is False
    assert "earnings blackout" in res_earnings.filter_reason

    # Sub-$50 stock -> Hard filter veto!
    low_px_quote = QuoteSnapshot("NVDA", 32.0, 31.99, 32.01, 2_000_000_000.0, now)
    res_low = scorer.score_security(
        sec=nvda,
        quote=low_px_quote,
        stock_intraday_return=0.01,
        qqq_intraday_return=0.0,
        regime_light=RegimeLight.GREEN,
    )
    assert res_low.passed_hard_filters is False
    assert "price" in res_low.filter_reason
