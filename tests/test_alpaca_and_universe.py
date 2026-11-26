from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import httpx
import pytest

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AlpacaDataConfig, AppSettings
from zy_quant.gateway.alpaca_adapter import (
    AlpacaMarketDataAdapter,
    is_us_regular_trading_bar,
)
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.intel.earnings_calendar import EarningsCalendarFeed
from zy_quant.intel.universe_manager import (
    PointInTimeUniverseFilter,
    rebalance_universe_with_buffer,
)
from zy_quant.intel.watchlist_scorer import ConservativeWatchlistScorer
from zy_quant.storage.bar_cache import HistoricalBarCache


def test_universe_100_loaded_with_cross_sector_balance_and_gics_cap() -> None:
    settings = AppSettings.load()
    uni = settings.watchlist_cfg.universe
    symbols = [u.symbol for u in uni]
    # Exact 100 unique equities + 10 sector ETFs + 2 broad ETFs = 112 unique tickers
    assert len(uni) == 112
    assert len(set(symbols)) == 112

    equities = [u for u in uni if not u.sector.endswith("_ETF")]
    assert len(equities) == 100
    sectors = {u.sector for u in equities}
    assert {"Technology", "Semiconductors", "Financials", "Healthcare", "Industrials", "Energy", "Consumer", "Staples"} <= sectors

    # Verify GICS Sector Cap (max 3 per sector in Top 8) prevents Semiconductor crowding
    scorer = ConservativeWatchlistScorer(settings.watchlist_cfg)
    mock_rows = [
        {"symbol": f"SEMI{i}", "sector": "Semiconductors", "total_score": 90.0 - i, "passed_hard_filters": 1}
        for i in range(6)
    ] + [
        {"symbol": f"FIN{i}", "sector": "Financials", "total_score": 82.0 - i, "passed_hard_filters": 1}
        for i in range(4)
    ] + [
        {"symbol": f"ENG{i}", "sector": "Energy", "total_score": 78.0 - i, "passed_hard_filters": 1}
        for i in range(3)
    ]
    top_active = scorer.select_active_top_n(mock_rows, top_n=8, min_score=62.0, max_per_sector=3)
    assert len(top_active) == 8
    semi_picked = [r for r in top_active if r["sector"] == "Semiconductors"]
    assert len(semi_picked) == 3  # Strictly capped at 3 despite having top 6 raw scores!


def test_alpaca_secret_leak_in_tracked_settings_json_is_blocked(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "config"
    shutil.copytree(AppSettings.load().root_dir / "config", cfg_dir)
    leaked = {
        "account_tier": "tier_1k",
        "futu": {"host": "127.0.0.1", "port": 11111},
        "alpaca": {"enabled": True, "api_key": "PK_LEAKED_SECRET"},
    }
    (cfg_dir / "settings.json").write_text(json.dumps(leaked), encoding="utf-8")
    with pytest.raises(ValueError, match="SECURITY BLOCK"):
        AppSettings.load(tmp_path)


def test_rth_bar_filter_strips_extended_hours_and_respects_early_close() -> None:
    # Pre-market 08:00 ET (13:00 UTC in EST) -> False
    assert is_us_regular_trading_bar(datetime(2026, 11, 25, 13, 0, tzinfo=timezone.utc)) is False
    # Regular open 09:30 ET (14:30 UTC in EST) -> True
    assert is_us_regular_trading_bar(datetime(2026, 11, 25, 14, 30, tzinfo=timezone.utc)) is True
    # Regular last bar 15:45 ET (20:45 UTC in EST) -> True
    assert is_us_regular_trading_bar(datetime(2026, 11, 25, 20, 45, tzinfo=timezone.utc)) is True
    # After-hours 16:00 ET (21:00 UTC in EST) -> False
    assert is_us_regular_trading_bar(datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)) is False
    # Black Friday 2026-11-27 early close at 13:00 ET (18:00 UTC): 13:15 ET bar -> False!
    assert is_us_regular_trading_bar(datetime(2026, 11, 27, 18, 15, tzinfo=timezone.utc)) is False


def test_alpaca_adapter_batch_snapshots_pagination_sip_fallback_and_sqlite_cache(
    tmp_path: Path,
) -> None:
    call_counter: Dict[str, int] = {"snapshots": 0, "bars": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["APCA-API-KEY-ID"] == "PKTEST1234"
        assert request.headers["APCA-API-SECRET-KEY"] == "SKTEST5678"

        if request.url.path == "/v2/stocks/snapshots":
            call_counter["snapshots"] += 1
            return httpx.Response(
                200,
                json={
                    "NVDA": {
                        "latestTrade": {"p": 152.40, "t": "2026-11-25T20:59:00Z"},
                        "latestQuote": {"bp": 152.38, "ap": 152.42},
                        "dailyBar": {"o": 149.0, "h": 153.0, "l": 148.8, "c": 152.40, "v": 1_200_000},
                        "prevDailyBar": {"o": 147.0, "h": 150.0, "l": 146.5, "c": 148.90, "v": 1_100_000},
                    },
                    "XOM": {
                        "latestTrade": {"p": 118.50, "t": "2026-11-25T20:59:00Z"},
                        "latestQuote": {"bp": 118.48, "ap": 118.51},
                        "dailyBar": {"o": 117.0, "h": 119.0, "l": 116.8, "c": 118.50, "v": 600_000},
                        "prevDailyBar": {"o": 116.5, "h": 117.5, "l": 116.0, "c": 117.10, "v": 580_000},
                    },
                },
            )

        if request.url.path == "/v2/stocks/bars":
            # Simulate Free Tier 403 on real-time SIP -> automatic retry on IEX!
            if request.url.params.get("feed") == "sip":
                return httpx.Response(403, text="subscription does not permit querying recent SIP data")

            call_counter["bars"] += 1
            tok = request.url.params.get("page_token")
            if not tok:
                return httpx.Response(
                    200,
                    json={
                        "bars": {
                            "QQQ": [
                                # Pre-market bar (should be stripped!)
                                {"t": "2026-11-24T12:00:00Z", "o": 500, "h": 501, "l": 499, "c": 500.5, "v": 1000},
                                # Valid RTH bar #1
                                {"t": "2026-11-24T14:30:00Z", "o": 501, "h": 503, "l": 500.8, "c": 502.5, "v": 95000},
                            ]
                        },
                        "next_page_token": "PAGE_2",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "bars": {
                        "QQQ": [
                            {"t": "2026-11-25T20:45:00Z", "o": 504, "h": 506, "l": 503.5, "c": 505.8, "v": 110000},
                        ]
                    },
                    "next_page_token": None,
                },
            )

        return httpx.Response(404)

    cache = HistoricalBarCache(tmp_path / "bars.db")
    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    adapter = AlpacaMarketDataAdapter(
        alpaca_cfg=AlpacaDataConfig(api_key="PKTEST1234", secret_key="SKTEST5678", feed="iex"),
        bar_cache=cache,
        http_client=mock_client,
    )

    snaps = adapter.get_snapshots_batch(["NVDA", "XOM"])
    assert len(snaps) == 2
    assert snaps["NVDA"].last_price == 152.40
    assert snaps["NVDA"].avg_daily_dollar_vol > 500_000_000.0  # IEX scaled to consolidated ADV

    start_w = datetime(2026, 11, 23, 0, 0, tzinfo=timezone.utc)
    end_w = datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)
    bars_map = adapter.get_bars_batch(["QQQ"], timeframe="15m", start_dt=start_w, end_dt=end_w)
    # 2 RTH bars kept across 2 paginated responses; pre-market 12:00 UTC bar stripped!
    assert len(bars_map["QQQ"]) == 2
    assert call_counter["bars"] == 2

    # Second query over covered range hits SQLite BarCache directly with ZERO HTTP calls!
    bars_cached = adapter.get_bars_batch(["QQQ"], timeframe="15m", start_dt=start_w, end_dt=end_w)
    assert len(bars_cached["QQQ"]) == 2
    assert call_counter["bars"] == 2


def test_multi_month_backtest_above_58d_with_bar_cache_and_pit_filter(tmp_path: Path) -> None:
    settings = AppSettings.load()
    cache = HistoricalBarCache(tmp_path / "multi_month.db")

    # Seed 120 trading days (> 58d Yahoo ceiling!) into HistoricalBarCache
    for sym in ["QQQ", "SPY", "NVDA", "JPM", "XOM"]:
        cache.upsert_bars("15m", YahooMarketDataAdapter.generate_synthetic_bars(sym, "15m", days=120))
        cache.upsert_bars("1d", YahooMarketDataAdapter.generate_synthetic_bars(sym, "1d", days=160))

    # Offline cache-backed Alpaca adapter (no network calls required)
    adapter = AlpacaMarketDataAdapter(alpaca_cfg=AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache)

    # Point-in-Time CSV excluding XOM after 2020-01-01
    pit_csv = tmp_path / "pit.csv"
    pit_csv.write_text(
        "symbol,start_date,end_date\n"
        "NVDA,2018-01-01,\n"
        "JPM,2018-01-01,\n"
        "XOM,2018-01-01,2020-01-01\n",
        encoding="utf-8",
    )
    pit = PointInTimeUniverseFilter.from_csv(pit_csv)

    bt = BacktestEngine(settings, adapter)
    res = bt.run(tier_key="tier_10k", symbols=["NVDA", "JPM", "XOM"], days=110, pit_filter=pit)
    assert (res.green_bars + res.rest_bars) > 1500  # > 60 trading days simulated cleanly!
    # XOM must never be traded because PIT membership ended in 2020
    assert all(t["symbol"] != "XOM" for t in res.trades)


def test_universe_hysteresis_buffer_rebalance_and_earnings_disk_cache(tmp_path: Path) -> None:
    settings = AppSettings.load()
    cur_uni = settings.watchlist_cfg.universe

    # Build 125 synthetic candidate metrics where an incumbent sits at rank 105 (inside <=120 buffer)
    metrics: list[Dict[str, Any]] = []
    incumbent_syms = [u.symbol for u in cur_uni if not u.sector.endswith("_ETF")]
    kept_borderline = incumbent_syms[-1]

    for i in range(95):
        sym = incumbent_syms[i] if i < len(incumbent_syms) - 1 else f"NEW{i}"
        metrics.append(
            {
                "symbol": sym,
                "sector": f"Sector_{i % 6}",
                "price": 120.0,
                "adv_60d_usd": 2_000_000_000.0 - i * 10_000_000.0,
            }
        )
    # Place non-incumbent at rank 96..104 (outside <=85 entry rank) and incumbent at rank 105 (<=120 buffer!)
    for j in range(9):
        metrics.append(
            {"symbol": f"OUTSIDER{j}", "sector": "Financials", "price": 90.0, "adv_60d_usd": 900_000_000.0 - j * 1_000_000.0}
        )
    metrics.append(
        {"symbol": kept_borderline, "sector": "Industrials", "price": 110.0, "adv_60d_usd": 850_000_000.0}
    )

    rebalanced, summary = rebalance_universe_with_buffer(cur_uni, metrics, target_equities_count=96)
    reb_syms = {u.symbol for u in rebalanced}
    assert kept_borderline in reb_syms  # Protected by 85/120 hysteresis buffer!
    assert "OUTSIDER0" not in summary.added[:10] or summary.total_count == 96

    # Verify EarningsCalendarFeed reads fresh disk cache without hitting network
    earn_file = tmp_path / "earnings.json"
    tomorrow = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
    earn_file.write_text(
        json.dumps({"updated_at": datetime.now(timezone.utc).isoformat(), "dates": {"NVDA": tomorrow}}),
        encoding="utf-8",
    )
    feed = EarningsCalendarFeed(cache_path=earn_file)
    upcoming = feed.get_upcoming_earnings(symbols=["NVDA", "MSFT"], allow_network=False)
    assert "NVDA" in upcoming
    assert "MSFT" not in upcoming


def test_zero_lookahead_leak_from_same_day_daily_close(tmp_path: Path) -> None:
    """
    Regression test for [CRITICAL] bisect_left fix:
    Mutating the final day's 1d close (16:00 ET future price stamped at 05:00 UTC)
    must have ZERO effect on intraday 10:00 ET regime, RS ranking, or entry signals!
    """
    from zy_quant.models import Bar

    settings = AppSettings.load()
    cache_clean = HistoricalBarCache(tmp_path / "clean.db")
    cache_poisoned = HistoricalBarCache(tmp_path / "poisoned.db")

    for sym in ["QQQ", "SPY", "NVDA", "MSFT"]:
        b15 = YahooMarketDataAdapter.generate_synthetic_bars(sym, "15m", days=35)
        b1d = YahooMarketDataAdapter.generate_synthetic_bars(sym, "1d", days=60)
        last_day = b15[-1].timestamp.date()
        # Append a same-day 05:00 UTC 1d bar matching Alpaca's timestamp convention
        day_ts = datetime(last_day.year, last_day.month, last_day.day, 5, 0, tzinfo=timezone.utc)
        clean_1d = [*b1d, Bar(sym, day_ts, 200.0, 205.0, 198.0, 202.0, 10_000_000.0)]
        # Poison same-day 16:00 ET close to $0.01 (-99.99% crash at end of day!)
        poison_1d = [*b1d, Bar(sym, day_ts, 200.0, 205.0, 0.01, 0.01, 999_000_000.0)]

        cache_clean.upsert_bars("15m", b15)
        cache_clean.upsert_bars("1d", clean_1d)
        cache_poisoned.upsert_bars("15m", b15)
        cache_poisoned.upsert_bars("1d", poison_1d)

    ad_clean = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_clean)
    ad_poison = AlpacaMarketDataAdapter(AlpacaDataConfig(api_key="", secret_key=""), bar_cache=cache_poisoned)

    r_clean = BacktestEngine(settings, ad_clean).run(tier_key="tier_10k", symbols=["NVDA", "MSFT"], days=30)
    r_poison = BacktestEngine(settings, ad_poison).run(tier_key="tier_10k", symbols=["NVDA", "MSFT"], days=30)

    assert r_clean.trades == r_poison.trades
    assert r_clean.green_bars == r_poison.green_bars
    assert r_clean.net_pnl_usd == r_poison.net_pnl_usd


def test_surgical_head_delta_fetch_preserves_existing_cached_body(tmp_path: Path) -> None:
    """
    Regression test for [MEDIUM] 12-day warmup offset fix:
    When BarCache holds 2026-11-15..2026-11-25 and caller requests 2026-11-01..2026-11-25,
    AlpacaMarketDataAdapter must ONLY request the missing head delta (<= 2026-11-15)!
    """
    from zy_quant.models import Bar

    cache = HistoricalBarCache(tmp_path / "delta.db")
    cached_bars = [
        Bar("QQQ", datetime(2026, 11, 16, 14, 30, tzinfo=timezone.utc), 500, 502, 499, 501, 100_000),
        Bar("QQQ", datetime(2026, 11, 25, 20, 45, tzinfo=timezone.utc), 505, 507, 504, 506, 120_000),
    ]
    cache.upsert_bars("15m", cached_bars)

    requested_windows: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        s_p = str(request.url.params.get("start"))
        e_p = str(request.url.params.get("end"))
        requested_windows.append((s_p, e_p))
        return httpx.Response(
            200,
            json={
                "bars": {
                    "QQQ": [
                        {"t": "2026-11-03T14:30:00Z", "o": 495, "h": 497, "l": 494, "c": 496, "v": 90000}
                    ]
                },
                "next_page_token": None,
            },
        )

    adapter = AlpacaMarketDataAdapter(
        alpaca_cfg=AlpacaDataConfig(api_key="PK1", secret_key="SK1", feed="iex"),
        bar_cache=cache,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    out = adapter.get_bars_batch(
        ["QQQ"],
        timeframe="15m",
        start_dt=datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc),
    )
    assert len(requested_windows) == 1
    # Must stop at min_cached (2026-11-16), NOT re-download through 2026-11-25!
    assert requested_windows[0][1].startswith("2026-11-16")
    assert len(out["QQQ"]) == 3
