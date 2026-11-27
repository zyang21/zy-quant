from __future__ import annotations

import json
import math
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional

from zy_quant.config import AppSettings, UniverseSecurity
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.alpaca_adapter import build_causal_partial_1d_bar
from zy_quant.gateway.base import MarketDataAdapter
from zy_quant.intel.market_hours import (
    ET_TZ,
    add_trading_days,
    count_trading_days_between,
    daily_bar_session_date,
    session_close_time,
)
from zy_quant.intel.regime import MarketRegimeDetector
from zy_quant.intel.universe_manager import PointInTimeUniverseFilter
from zy_quant.intel.watchlist_scorer import SwingWatchlistScorer
from zy_quant.models import Bar, EarningsEvent, OpenPosition, OrderSide, RegimeLight
from zy_quant.risk.correlation import compute_max_pairwise_correlation
from zy_quant.risk.mandate_gate import compute_portfolio_heat
from zy_quant.strategy.exit_models import ScientificExitAndSizingEngine
from zy_quant.strategy.indicators import compute_wilder_atr_series
from zy_quant.strategy.signals import IntradaySignalEngine
from zy_quant.strategy.signals_swing import (
    DailySwingSignalEngine,
    build_strictly_causal_daily_series,
)


@dataclass
class BacktestResult:
    tier_key: str
    tier_label: str
    initial_equity_usd: float
    final_equity_usd: float
    net_return_pct: float
    gross_pnl_usd: float
    total_fees_usd: float
    net_pnl_usd: float
    fee_drag_of_gross_pct: float
    total_closed_trades: int
    win_rate_pct: float
    avg_r_multiple: float
    profit_factor: float
    max_drawdown_pct: float
    green_bars: int
    rest_bars: int
    period_start: str = ""
    period_end: str = ""
    clock_mode: str = "intraday_15m"
    data_source: str = "FUTU_REALTIME"
    is_synthetic_data: bool = False
    data_warnings: List[str] = field(default_factory=list)
    equity_curve: List[Dict[str, Any]] = field(default_factory=list)
    trades: List[Dict[str, Any]] = field(default_factory=list)


class BacktestEngine:
    """
    Event-driven 15-minute multi-symbol US Equities Backtester:
    Uses the EXACT same Regime Filter + 15m Signal Engine + Scientific Exit Models + 2026 Futu HK Fee Schedule
    as live/paper execution — zero look-ahead bias and full transaction-cost accounting.
    """

    def __init__(self, settings: AppSettings, data_adapter: MarketDataAdapter) -> None:
        self.settings = settings
        self.data = data_adapter
        self.fee_model = FutuUSFeeModel()
        self.regime_detector = MarketRegimeDetector(settings.strategy_cfg.regime)
        self.signal_engine = IntradaySignalEngine(settings.strategy_cfg.entry, settings.strategy_cfg.regime)
        self.swing_engine = DailySwingSignalEngine(
            settings.strategy_cfg.entry,
            settings.strategy_cfg.regime,
            settings.strategy_cfg.swing,
        )
        self.swing_scorer = SwingWatchlistScorer(
            settings.watchlist_cfg,
            settings.strategy_cfg.swing,
        )
        self.exit_engine = ScientificExitAndSizingEngine(
            settings.strategy_cfg.exit_models, self.fee_model
        )

    def _default_backtest_symbols(self, wide_data_available: bool) -> List[str]:
        non_broad = [
            u for u in self.settings.watchlist_cfg.universe if u.sector != "Broad_ETF"
        ]
        if wide_data_available:
            return [u.symbol for u in non_broad]
        # Quota-safe cross-sector Core 12 when running on Futu/Yahoo without Alpaca keys
        ordered = sorted(non_broad, key=lambda u: u.etf_core_score, reverse=True)
        picked: List[str] = []
        sec_counts: Dict[str, int] = {}
        for u in ordered:
            if sec_counts.get(u.sector, 0) >= 2:
                continue
            picked.append(u.symbol)
            sec_counts[u.sector] = sec_counts.get(u.sector, 0) + 1
            if len(picked) >= 12:
                break
        return picked

    def run(
        self,
        tier_key: Optional[str] = None,
        symbols: List[str] | None = None,
        days: int = 30,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        pit_filter: Optional[PointInTimeUniverseFilter] = None,
        mode: Optional[str] = None,
        clock_mode: str = "auto",
        earnings_events: Optional[Dict[str, EarningsEvent]] = None,
    ) -> BacktestResult:
        eff_mode = (mode or self.settings.strategy_cfg.mode or "swing_1d").strip().lower()
        if clock_mode == "intraday_15m":
            eff_mode = "intraday_15m"
        elif clock_mode in ("1d", "hybrid_1545"):
            eff_mode = "swing_1d"

        if eff_mode == "swing_1d" and clock_mode in ("1d", "hybrid_1545"):
            return self._run_swing_backtest(
                tier_key=tier_key,
                symbols=symbols,
                days=days,
                start_date=start_date,
                end_date=end_date,
                pit_filter=pit_filter,
                clock_mode=clock_mode,
                earnings_events=earnings_events,
            )

        # When clock_mode == "auto" and eff_mode == "swing_1d":
        # Use fast `1d` Primary Clock on multi-year (> 90d) sweeps unless `hybrid_1545` requested,
        # and `hybrid_1545` when `15m` bars exist for short windows (falling back to `1d` if absent).
        if eff_mode == "swing_1d" and clock_mode == "auto":
            chosen_clock = "1d" if days > 90 or start_date else "hybrid_1545"
            res_swing = self._run_swing_backtest(
                tier_key=tier_key,
                symbols=symbols,
                days=days,
                start_date=start_date,
                end_date=end_date,
                pit_filter=pit_filter,
                clock_mode=chosen_clock,
                earnings_events=earnings_events,
            )
            if res_swing.green_bars + res_swing.rest_bars > 0 or chosen_clock == "1d":
                return res_swing
            return self._run_swing_backtest(
                tier_key=tier_key,
                symbols=symbols,
                days=days,
                start_date=start_date,
                end_date=end_date,
                pit_filter=pit_filter,
                clock_mode="1d",
                earnings_events=earnings_events,
            )

        resolved_tier_key = tier_key or self.settings.active_tier_key
        tier = self.settings.tiers_cfg.get_active(resolved_tier_key)
        initial_eq = tier.reference_equity_usd
        cash = initial_eq

        # Compute full multi-year lookback window (no 58-day ceiling!)
        now_utc = datetime.now(timezone.utc)
        start_d = datetime.fromisoformat(start_date[:10]).date() if start_date else None
        end_d = datetime.fromisoformat(end_date[:10]).date() if end_date else None
        fetch_days = days
        if start_d is not None:
            fetch_days = max(days, (now_utc.date() - start_d).days + 10)

        has_batch_api = hasattr(self.data, "get_bars_batch") and bool(
            getattr(self.data, "has_credentials", lambda: False)()
        )
        bar_cache_obj = getattr(self.data, "bar_cache", None)
        wide_cached = (
            bar_cache_obj is not None and bar_cache_obj.stats().get("distinct_symbols", 0) >= 25
        )

        universe_map = {u.symbol: u for u in self.settings.watchlist_cfg.universe}
        target_symbols = symbols or self._default_backtest_symbols(
            wide_data_available=(has_batch_api or wide_cached)
        )

        # Load 15m & daily series (batch mode via Alpaca/BarCache or single-symbol fallback)
        symbol_bars: Dict[str, List[Bar]] = {}
        symbol_daily: Dict[str, List[Bar]] = {}
        symbol_atrs: Dict[str, List[float]] = {}
        missing_symbols: List[str] = []

        if hasattr(self.data, "get_bars_batch") and (has_batch_api or wide_cached):
            end_dt = (
                datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc)
                if end_d
                else now_utc
            )
            start_15m_dt = (
                datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc)
                - timedelta(days=12)
                if start_d
                else None
            )
            start_1d_dt = (
                (start_15m_dt - timedelta(days=45))
                if start_15m_dt is not None
                else None
            )
            all_syms = list(dict.fromkeys(["QQQ", "SPY", *target_symbols]))
            batch_15m = self.data.get_bars_batch(  # type: ignore[attr-defined]
                [s for s in all_syms if s != "SPY"],
                timeframe="15m",
                days=fetch_days,
                start_dt=start_15m_dt,
                end_dt=end_dt,
            )
            batch_1d = self.data.get_bars_batch(  # type: ignore[attr-defined]
                all_syms,
                timeframe="1d",
                days=max(270, fetch_days + 220),
                start_dt=(start_15m_dt - timedelta(days=390)) if start_15m_dt is not None else None,
                end_dt=end_dt,
            )
            qqq_15m = batch_15m.get("QQQ", [])
            qqq_1d = batch_1d.get("QQQ", [])
            spy_1d = batch_1d.get("SPY", [])
            for sym in target_symbols:
                b15 = batch_15m.get(sym, [])
                b1d = batch_1d.get(sym, [])
                if len(b15) >= 30:
                    symbol_bars[sym] = b15
                    symbol_daily[sym] = b1d
                    symbol_atrs[sym] = compute_wilder_atr_series(
                        b15, self.settings.strategy_cfg.entry.atr_period
                    )
                else:
                    missing_symbols.append(sym)
        else:
            qqq_15m = self.data.get_bars("QQQ", timeframe="15m", days=fetch_days)
            qqq_1d = self.data.get_bars("QQQ", timeframe="1d", days=max(60, fetch_days + 30))
            spy_1d = self.data.get_bars("SPY", timeframe="1d", days=max(60, fetch_days + 30))
            for sym in target_symbols:
                b15 = self.data.get_bars(sym, timeframe="15m", days=fetch_days)
                b1d = self.data.get_bars(sym, timeframe="1d", days=max(60, fetch_days + 30))
                if len(b15) >= 30:
                    symbol_bars[sym] = b15
                    symbol_daily[sym] = b1d
                    symbol_atrs[sym] = compute_wilder_atr_series(
                        b15, self.settings.strategy_cfg.entry.atr_period
                    )
                else:
                    missing_symbols.append(sym)

        fb = getattr(self.data, "fallback", self.data)
        sub_fb = getattr(fb, "fallback", fb)
        used_alpaca = bool(getattr(self.data, "used_alpaca", False))
        used_synth = bool(getattr(sub_fb, "used_synthetic", False))
        used_yahoo = bool(getattr(sub_fb, "used_yahoo_fallback", False))
        is_futu_live = bool(getattr(fb, "is_connected", lambda: False)())
        data_source = (
            "SYNTHETIC_FAKE"
            if used_synth
            else (
                "ALPACA_MARKET_DATA"
                if used_alpaca
                else (
                    "LOCAL_BAR_CACHE"
                    if wide_cached and qqq_15m
                    else ("FUTU_REALTIME" if is_futu_live else ("YAHOO_FALLBACK" if used_yahoo else "MISSING_DATA"))
                )
            )
        )
        warnings_list: List[str] = list(getattr(sub_fb, "fallback_warnings", []))[-5:]
        if missing_symbols:
            warnings_list.append(
                f"Skipped symbols with insufficient real market bars: {', '.join(missing_symbols)}"
            )

        if not qqq_15m or not symbol_bars:
            return BacktestResult(
                tier_key=resolved_tier_key,
                tier_label=tier.label,
                initial_equity_usd=initial_eq,
                final_equity_usd=initial_eq,
                net_return_pct=0.0,
                gross_pnl_usd=0.0,
                total_fees_usd=0.0,
                net_pnl_usd=0.0,
                fee_drag_of_gross_pct=0.0,
                total_closed_trades=0,
                win_rate_pct=0.0,
                avg_r_multiple=0.0,
                profit_factor=0.0,
                max_drawdown_pct=0.0,
                green_bars=0,
                rest_bars=0,
                data_source=data_source,
                is_synthetic_data=used_synth,
                data_warnings=warnings_list or ["No real market data available — connect FutuOpenD / 富途牛牛."],
            )

        # Index bars by timestamp + pre-index daily dates for O(log N) multi-year simulation
        bar_lookup: Dict[str, Dict[Any, tuple[int, Bar]]] = {}
        for sym, blist in symbol_bars.items():
            bar_lookup[sym] = {b.timestamp: (i, b) for i, b in enumerate(blist)}

        qqq_1d_dates = [daily_bar_session_date(d.timestamp) for d in qqq_1d]
        spy_1d_dates = [daily_bar_session_date(d.timestamp) for d in spy_1d]
        sym_1d_dates: Dict[str, List[Any]] = {
            sym: [daily_bar_session_date(d.timestamp) for d in dlist]
            for sym, dlist in symbol_daily.items()
        }

        positions: Dict[str, OpenPosition] = {}
        last_exit_time: Dict[str, datetime] = {}
        closed_trades: List[Dict[str, Any]] = []
        buy_timestamps: List[Any] = []
        total_fees = 0.0
        gross_pnl_total = 0.0
        peak_equity = initial_eq
        max_dd_pct = 0.0
        green_bars = 0
        rest_bars = 0
        equity_curve: List[Dict[str, Any]] = []

        warmup = 25
        eval_timestamps: List[datetime] = []
        for q_idx in range(warmup, len(qqq_15m)):
            q_bar = qqq_15m[q_idx]
            ts = q_bar.timestamp
            if start_d and ts.date() < start_d:
                continue
            if end_d and ts.date() > end_d:
                continue
            eval_timestamps.append(ts)

            # Slice daily bars strictly BEFORE current date (`bisect_left` -> d.date < cur_d)
            # Eliminates any look-ahead leak of today's 16:00 ET daily close during intraday bars!
            cur_d = ts.date()
            q_cut = bisect_left(qqq_1d_dates, cur_d)
            s_cut = bisect_left(spy_1d_dates, cur_d)
            q_d_slice = qqq_1d[max(0, q_cut - 35) : q_cut]
            s_d_slice = spy_1d[max(0, s_cut - 35) : s_cut]

            # Compute dynamic point-in-time 10-day realized QQQ volatility (replaces hardcoded 17.5)
            vix_est = 18.0
            if len(q_d_slice) >= 11:
                rets = [
                    math.log(q_d_slice[k].close / q_d_slice[k - 1].close)
                    for k in range(len(q_d_slice) - 10, len(q_d_slice))
                    if q_d_slice[k - 1].close > 0
                ]
                if len(rets) >= 5:
                    m_r = sum(rets) / len(rets)
                    v_r = sum((r - m_r) ** 2 for r in rets) / max(1, len(rets) - 1)
                    vix_est = max(11.0, min(80.0, math.sqrt(v_r * 252.0) * 100.0 * 1.10))

            regime = self.regime_detector.evaluate(
                qqq_intraday_15m=qqq_15m[max(0, q_idx - 40) : q_idx + 1],
                qqq_daily=q_d_slice,
                spy_daily=s_d_slice,
                vix_level=vix_est,
            )
            if regime.light == RegimeLight.GREEN:
                green_bars += 1
            else:
                rest_bars += 1

            # Detect final 30m of regular or early-close US session (e.g. bar >= 25 normal, >= 13 Black Friday)
            session_bar_num = sum(
                1
                for k in range(max(0, q_idx - 30), q_idx + 1)
                if qqq_15m[k].timestamp.date() == ts.date()
            )
            close_t = session_close_time(ts.date())
            eod_threshold = max(1, (((close_t.hour * 60 + close_t.minute) - (9 * 60 + 30)) // 15) - 1)
            is_eod_window = session_bar_num >= eod_threshold or (
                q_idx + 1 < len(qqq_15m) and qqq_15m[q_idx + 1].timestamp.date() != ts.date()
            )

            # 1. Step open positions first (stops, scale-outs, Chandelier ratchets, T+0 EOD, time barriers)
            for sym in list(positions.keys()):
                entry_pair = bar_lookup[sym].get(ts)
                if entry_pair is None:
                    continue
                s_idx, s_bar = entry_pair
                pos = positions[sym]
                atr_val = symbol_atrs[sym][s_idx]
                action = self.exit_engine.step_position(
                    pos, s_bar, atr_val, tier, is_eod_window=is_eod_window
                )

                if action.should_exit and action.shares_to_sell > 0:
                    sell_fee = self.fee_model.estimate_leg(
                        action.shares_to_sell, action.exit_price, OrderSide.SELL
                    ).total_friction_usd
                    proceeds = action.shares_to_sell * action.exit_price - sell_fee
                    cash += proceeds
                    total_fees += sell_fee

                    alloc_buy_fee = (
                        pos.metadata.get("buy_fee_usd", 0.0)
                        * (action.shares_to_sell / max(1e-6, pos.initial_shares))
                    )
                    leg_gross = (action.exit_price - pos.entry_price) * action.shares_to_sell
                    leg_fee = alloc_buy_fee + sell_fee
                    leg_net = leg_gross - leg_fee
                    gross_pnl_total += leg_gross
                    r_mult = leg_net / max(0.01, pos.risk_per_share * action.shares_to_sell)

                    closed_trades.append(
                        {
                            "symbol": sym,
                            "setup": pos.setup,
                            "shares": round(action.shares_to_sell, 4),
                            "entry_price": round(pos.entry_price, 2),
                            "exit_price": round(action.exit_price, 2),
                            "gross_pnl_usd": round(leg_gross, 2),
                            "fee_usd": round(leg_fee, 2),
                            "net_pnl_usd": round(leg_net, 2),
                            "r_multiple": round(r_mult, 2),
                            "exit_reason": action.reason.value if action.reason else "EXIT",
                            "entry_time": pos.entry_time.isoformat(),
                            "exit_time": ts.isoformat(),
                        }
                    )

                    pos.shares = round(pos.shares - action.shares_to_sell, 6)
                    if pos.shares <= 1e-5:
                        last_exit_time[sym] = ts
                        del positions[sym]

            # Mark-to-market current portfolio equity
            pos_market_val = 0.0
            for sym, pos in positions.items():
                pair = bar_lookup[sym].get(ts)
                mark_px = pair[1].close if pair else pos.entry_price
                pos_market_val += pos.shares * mark_px
            current_equity = cash + pos_market_val
            peak_equity = max(peak_equity, current_equity)
            dd = ((peak_equity - current_equity) / peak_equity) * 100.0 if peak_equity > 0 else 0.0
            max_dd_pct = max(max_dd_pct, dd)

            if q_idx % 4 == 0 or q_idx == len(qqq_15m) - 1:
                equity_curve.append(
                    {
                        "timestamp": ts.isoformat(),
                        "equity_usd": round(current_equity, 2),
                        "regime": regime.light.value,
                    }
                )

            # 2. Entry scan when Regime is GREEN (100% scale) or YELLOW (Selective Alpha Leaders @ 65% scale)
            if regime.light == RegimeLight.RED:
                continue
            if len(positions) >= tier.max_concurrent_positions:
                continue

            buys_today = sum(1 for t_b in buy_timestamps if t_b.date() == ts.date())
            if buys_today >= tier.max_trades_per_day:
                continue
            buys_7d = sum(1 for t_b in buy_timestamps if (ts - t_b) <= timedelta(days=7))
            if buys_7d >= tier.max_trades_per_week:
                continue

            cooldown_td = timedelta(hours=self.settings.strategy_cfg.entry.cooldown_hours_after_exit)
            pos_scale = (
                1.0
                if regime.light == RegimeLight.GREEN
                else self.settings.strategy_cfg.regime.yellow_position_scale
            )

            # Rank candidate symbols by strictly point-in-time 20-day RS (current 15m close vs prior completed daily bar)
            def _rs_20d(sym_key: str) -> float:
                d_full = symbol_daily.get(sym_key)
                if not d_full:
                    return 0.0
                idx_end = bisect_left(sym_1d_dates[sym_key], cur_d)
                if idx_end < 1:
                    return 0.0
                pair_now = bar_lookup.get(sym_key, {}).get(ts)
                c_now = pair_now[1].close if pair_now is not None else d_full[idx_end - 1].close
                lb = min(20, idx_end)
                c_prev = max(0.01, d_full[idx_end - lb].close)
                return (c_now - c_prev) / c_prev

            eligible_pool = [
                s
                for s in target_symbols
                if pit_filter is None or pit_filter.is_member_on(s, cur_d)
            ]
            sorted_by_rs = sorted(eligible_pool, key=_rs_20d, reverse=True)
            top_n_cap = self.settings.watchlist_cfg.scoring_thresholds.top_n_active
            sec_cap = self.settings.watchlist_cfg.scoring_thresholds.max_per_sector_in_top_n
            ranked_symbols: List[str] = []
            sec_rank_counts: Dict[str, int] = {}
            for s in sorted_by_rs:
                sec_name = universe_map[s].sector if s in universe_map else "Technology"
                if sec_rank_counts.get(sec_name, 0) >= sec_cap:
                    continue
                ranked_symbols.append(s)
                sec_rank_counts[sec_name] = sec_rank_counts.get(sec_name, 0) + 1
                if len(ranked_symbols) >= max(top_n_cap, 12):
                    break

            for sym in ranked_symbols:
                if sym in positions or sym not in bar_lookup:
                    continue
                if sym in last_exit_time and (ts - last_exit_time[sym]) < cooldown_td:
                    continue
                pair = bar_lookup[sym].get(ts)
                if pair is None:
                    continue
                s_idx, _ = pair
                if s_idx < warmup:
                    continue

                d_end = bisect_left(sym_1d_dates.get(sym, []), cur_d)
                sym_d_slice = symbol_daily.get(sym, [])[max(0, d_end - 35) : d_end]
                sec_meta = universe_map.get(sym)
                sector = sec_meta.sector if sec_meta else "Technology"
                sig = self.signal_engine.evaluate_15m(
                    symbol=sym,
                    sector=sector,
                    bars_15m=symbol_bars[sym][max(0, s_idx - 35) : s_idx + 1],
                    regime_light=regime.light,
                    watchlist_score=78.0,
                    stock_daily_bars=sym_d_slice,
                    qqq_daily_bars=q_d_slice,
                    qqq_curr_price=q_bar.close,
                )
                if sig is None:
                    continue

                # Sector concentration check
                sec_notional = sum(
                    p.shares * p.entry_price for p in positions.values() if p.sector == sector
                )
                plan, status = self.exit_engine.build_position_plan(
                    signal=sig,
                    account_equity_usd=current_equity,
                    available_cash_usd=cash * 0.992,
                    tier=tier,
                    position_scale=pos_scale,
                )
                if plan is None or status != "APPROVED":
                    continue
                if (sec_notional + plan.notional_usd) / max(1.0, current_equity) > tier.max_sector_weight_pct:
                    continue

                buy_fee = self.fee_model.estimate_leg(
                    plan.shares, plan.entry_price, OrderSide.BUY
                ).total_friction_usd
                total_cost = plan.shares * plan.entry_price + buy_fee
                if total_cost > cash:
                    continue

                cash -= total_cost
                total_fees += buy_fee
                buy_timestamps.append(ts)
                positions[sym] = OpenPosition(
                    symbol=sym,
                    sector=sector,
                    shares=plan.shares,
                    initial_shares=plan.shares,
                    entry_price=plan.entry_price,
                    entry_time=ts,
                    initial_stop=plan.initial_stop,
                    current_stop=plan.initial_stop,
                    highest_price=plan.entry_price,
                    risk_per_share=plan.risk_per_share,
                    setup=plan.setup.value,
                    metadata={"buy_fee_usd": buy_fee},
                )
                if len(positions) >= tier.max_concurrent_positions:
                    break

        # Final mark-to-market
        final_pos_val = 0.0
        for sym, pos in positions.items():
            last_px = symbol_bars[sym][-1].close
            final_pos_val += pos.shares * last_px
        final_eq = cash + final_pos_val
        net_pnl = final_eq - initial_eq
        wins = sum(1 for t in closed_trades if t["net_pnl_usd"] > 0)
        win_rate = (wins / len(closed_trades) * 100.0) if closed_trades else 0.0
        avg_r = (
            sum(t["r_multiple"] for t in closed_trades) / len(closed_trades)
            if closed_trades
            else 0.0
        )
        gross_wins = sum(t["net_pnl_usd"] for t in closed_trades if t["net_pnl_usd"] > 0)
        gross_losses = abs(sum(t["net_pnl_usd"] for t in closed_trades if t["net_pnl_usd"] < 0))
        profit_factor = (
            (gross_wins / gross_losses)
            if gross_losses > 0.01
            else (9.99 if gross_wins > 0 else 0.0)
        )
        fee_drag = (
            (total_fees / abs(gross_pnl_total)) * 100.0
            if abs(gross_pnl_total) > 0.01
            else 0.0
        )

        return BacktestResult(
            tier_key=resolved_tier_key,
            tier_label=tier.label,
            initial_equity_usd=round(initial_eq, 2),
            final_equity_usd=round(final_eq, 2),
            net_return_pct=round((net_pnl / initial_eq) * 100.0, 2),
            gross_pnl_usd=round(gross_pnl_total, 2),
            total_fees_usd=round(total_fees, 2),
            net_pnl_usd=round(net_pnl, 2),
            fee_drag_of_gross_pct=round(fee_drag, 2) if math.isfinite(fee_drag) else 0.0,
            total_closed_trades=len(closed_trades),
            win_rate_pct=round(win_rate, 1),
            avg_r_multiple=round(avg_r, 2),
            profit_factor=round(profit_factor, 2),
            max_drawdown_pct=round(max_dd_pct, 2),
            green_bars=green_bars,
            rest_bars=rest_bars,
            period_start=eval_timestamps[0].strftime("%Y-%m-%d") if eval_timestamps else "",
            period_end=eval_timestamps[-1].strftime("%Y-%m-%d") if eval_timestamps else "",
            clock_mode="intraday_15m",
            data_source=data_source,
            is_synthetic_data=used_synth,
            data_warnings=warnings_list,
            equity_curve=equity_curve,
            trades=closed_trades,
        )

    def _run_swing_backtest(
        self,
        tier_key: Optional[str],
        symbols: List[str] | None,
        days: int,
        start_date: Optional[str],
        end_date: Optional[str],
        pit_filter: Optional[PointInTimeUniverseFilter],
        clock_mode: str = "1d",
        earnings_events: Optional[Dict[str, EarningsEvent]] = None,
    ) -> BacktestResult:
        """
        Dual-Clock Daily Swing Backtester (`swing_1d`):
          - `clock_mode="1d"` (Primary Multi-Year Clock, `<100ms` across 6 years):
            Evaluates signals on completed day `T` bars and fills approved entries at `T+1` Open
            (with limit-gap protection), while checking stops on `T` `[open, low]` (`min(open, stop)`).
            Zero look-ahead: mutating `T_last`'s `16:00 ET` close has 0 effect on entries!
          - `clock_mode="hybrid_1545"` (Live-Parity Clock):
            Constructs the exact causal `15:40 ET partial_1d_bar` from `[09:30..15:40 ET]` `15m` bars
            appended to strictly `< cur_d` (`bisect_left`) `1d` bars, executing at `15:40 ET` close.
            Zero look-ahead: mutating ANY day's `16:00 ET` `1d` close has 0 effect on that day's decision!
        """
        resolved_tier_key = tier_key or self.settings.active_tier_key
        tier = self.settings.tiers_cfg.get_active(resolved_tier_key)
        initial_eq = tier.reference_equity_usd
        cash = initial_eq
        earn_map = earnings_events or {}

        now_utc = datetime.now(timezone.utc)
        now_et_d = now_utc.astimezone(ET_TZ).date()
        start_d = date.fromisoformat(start_date[:10]) if start_date else None
        end_d = date.fromisoformat(end_date[:10]) if end_date else None
        fetch_days = days if start_d is None else max(days, (now_et_d - start_d).days + 10)
        history_1d_days = max(self.settings.strategy_cfg.swing.min_daily_history_bars, fetch_days + 240)

        has_batch_api = hasattr(self.data, "get_bars_batch") and bool(
            getattr(self.data, "has_credentials", lambda: False)()
        )
        bar_cache_obj = getattr(self.data, "bar_cache", None)
        wide_cached = (
            bar_cache_obj is not None and bar_cache_obj.stats().get("distinct_symbols", 0) >= 25
        )
        universe_map = {u.symbol: u for u in self.settings.watchlist_cfg.universe}
        target_symbols = symbols or self._default_backtest_symbols(
            wide_data_available=(has_batch_api or wide_cached)
        )
        all_syms = list(dict.fromkeys(["QQQ", "SPY", *target_symbols]))

        end_dt = (
            datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc)
            if end_d
            else now_utc
        )
        start_1d_dt = (
            datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc)
            - timedelta(days=390)
            if start_d
            else None
        )

        if hasattr(self.data, "get_bars_batch") and (has_batch_api or wide_cached):
            batch_1d = self.data.get_bars_batch(  # type: ignore[attr-defined]
                all_syms, timeframe="1d", days=history_1d_days, start_dt=start_1d_dt, end_dt=end_dt
            )
            batch_15m = (
                self.data.get_bars_batch(  # type: ignore[attr-defined]
                    all_syms,
                    timeframe="15m",
                    days=fetch_days,
                    start_dt=(
                        datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc)
                        - timedelta(days=10)
                        if start_d
                        else None
                    ),
                    end_dt=end_dt,
                )
                if clock_mode == "hybrid_1545"
                else {}
            )
        else:
            batch_1d = {
                s: self.data.get_bars(s, timeframe="1d", days=history_1d_days) for s in all_syms
            }
            batch_15m = (
                {s: self.data.get_bars(s, timeframe="15m", days=fetch_days) for s in all_syms}
                if clock_mode == "hybrid_1545"
                else {}
            )

        qqq_1d = batch_1d.get("QQQ", [])
        spy_1d = batch_1d.get("SPY", [])
        symbol_daily: Dict[str, List[Bar]] = {
            s: batch_1d[s] for s in target_symbols if len(batch_1d.get(s, [])) >= 20
        }

        fb = getattr(self.data, "fallback", self.data)
        sub_fb = getattr(fb, "fallback", fb)
        used_alpaca = bool(getattr(self.data, "used_alpaca", False))
        used_synth = bool(getattr(sub_fb, "used_synthetic", False))
        used_yahoo = bool(getattr(sub_fb, "used_yahoo_fallback", False))
        is_futu_live = bool(getattr(fb, "is_connected", lambda: False)())
        has_cache_bars = bar_cache_obj is not None and bar_cache_obj.stats().get("total_bars", 0) > 0
        data_source = (
            "SYNTHETIC_FAKE"
            if used_synth
            else (
                "ALPACA_MARKET_DATA"
                if used_alpaca
                else (
                    "LOCAL_BAR_CACHE"
                    if has_cache_bars and qqq_1d
                    else ("FUTU_REALTIME" if is_futu_live else ("YAHOO_FALLBACK" if used_yahoo else "MISSING_DATA"))
                )
            )
        )
        warnings_list: List[str] = list(getattr(sub_fb, "fallback_warnings", []))[-5:]

        if not qqq_1d or not symbol_daily:
            return BacktestResult(
                tier_key=resolved_tier_key,
                tier_label=tier.label,
                initial_equity_usd=initial_eq,
                final_equity_usd=initial_eq,
                net_return_pct=0.0,
                gross_pnl_usd=0.0,
                total_fees_usd=0.0,
                net_pnl_usd=0.0,
                fee_drag_of_gross_pct=0.0,
                total_closed_trades=0,
                win_rate_pct=0.0,
                avg_r_multiple=0.0,
                profit_factor=0.0,
                max_drawdown_pct=0.0,
                green_bars=0,
                rest_bars=0,
                clock_mode=clock_mode,
                data_source=data_source,
                is_synthetic_data=used_synth,
                data_warnings=warnings_list or ["Insufficient daily market data for swing backtest."],
            )

        def _intraday_et_d(b: Bar) -> date:
            return b.timestamp.date() if b.timestamp.tzinfo is None else b.timestamp.astimezone(ET_TZ).date()

        def _daily_d(b: Bar) -> date:
            return daily_bar_session_date(b.timestamp)

        # Partition 15m bars by (symbol, et_date) when in hybrid_1545 mode
        intraday_by_day: Dict[str, Dict[date, List[Bar]]] = {}
        if clock_mode == "hybrid_1545":
            for sym_k, b15_list in batch_15m.items():
                by_d: Dict[date, List[Bar]] = {}
                for b in b15_list:
                    by_d.setdefault(_intraday_et_d(b), []).append(b)
                intraday_by_day[sym_k] = by_d

        daily_by_date: Dict[str, Dict[date, tuple[int, Bar]]] = {}
        sym_1d_dates: Dict[str, List[date]] = {}
        sym_1d_atrs: Dict[str, List[float]] = {}
        for s_k, d_list in {**symbol_daily, "QQQ": qqq_1d, "SPY": spy_1d}.items():
            daily_by_date[s_k] = {_daily_d(b): (idx, b) for idx, b in enumerate(d_list)}
            sym_1d_dates[s_k] = [_daily_d(b) for b in d_list]
            sym_1d_atrs[s_k] = compute_wilder_atr_series(d_list, 14)

        if clock_mode == "hybrid_1545":
            qqq_days_15m = sorted(intraday_by_day.get("QQQ", {}).keys())
            session_dates = qqq_days_15m[-days:] if not start_d else [
                d for d in qqq_days_15m if (start_d <= d) and (end_d is None or d <= end_d)
            ]
        else:
            all_q_dates = sym_1d_dates["QQQ"]
            if start_d:
                session_dates = [d for d in all_q_dates if d >= start_d and (end_d is None or d <= end_d)]
            else:
                session_dates = all_q_dates[-days:]

        positions: Dict[str, OpenPosition] = {}
        queued_next_open: List[tuple[Any, Any]] = []  # (PositionPlan, EntrySignal) queued for T+1 open in 1d mode
        closed_trades: List[Dict[str, Any]] = []
        buy_dates: List[date] = []
        stop_episode_dates: List[date] = []
        cooldown_until_d: Optional[date] = None
        cooldown_watermark_idx: int = 0
        sl_cfg = self.settings.strategy_cfg.daemon.stoploss_guard

        total_fees = 0.0
        gross_pnl_total = 0.0
        peak_equity = initial_eq
        max_dd_pct = 0.0
        green_days = 0
        rest_days = 0
        equity_curve: List[Dict[str, Any]] = []
        eval_days: List[date] = []

        def _decision_cutoff_utc(d_val: date) -> datetime:
            close_t = session_close_time(d_val)
            # 15:45 ET normal / 12:45 ET early-close: admits completed [09:30..15:30 ET] bars up to 15:45 ET
            dec_mins = (close_t.hour * 60 + close_t.minute) - 15
            et_dt = datetime.combine(d_val, dt_time(dec_mins // 60, dec_mins % 60), tzinfo=ET_TZ)
            return et_dt.astimezone(timezone.utc)

        def _record_exit(sym: str, pos: OpenPosition, sell_qty: float, exit_px: float, reason_str: str, exit_dt: datetime, exit_d: date) -> None:
            nonlocal cash, total_fees, gross_pnl_total, cooldown_until_d, cooldown_watermark_idx
            sell_fee = self.fee_model.estimate_leg(sell_qty, exit_px, OrderSide.SELL).total_friction_usd
            cash += sell_qty * exit_px - sell_fee
            total_fees += sell_fee
            alloc_buy = float(pos.metadata.get("buy_fee_usd", 0.0)) * (sell_qty / max(1e-6, pos.initial_shares))
            leg_gross = (exit_px - pos.entry_price) * sell_qty
            leg_fee = alloc_buy + sell_fee
            leg_net = leg_gross - leg_fee
            gross_pnl_total += leg_gross
            r_mult = leg_net / max(0.01, pos.risk_per_share * sell_qty)
            closed_trades.append(
                {
                    "symbol": sym,
                    "setup": pos.setup,
                    "shares": round(sell_qty, 4),
                    "entry_price": round(pos.entry_price, 2),
                    "exit_price": round(exit_px, 2),
                    "gross_pnl_usd": round(leg_gross, 2),
                    "fee_usd": round(leg_fee, 2),
                    "net_pnl_usd": round(leg_net, 2),
                    "r_multiple": round(r_mult, 2),
                    "exit_reason": reason_str,
                    "entry_time": pos.entry_time.isoformat(),
                    "exit_time": exit_dt.isoformat(),
                }
            )
            pos.shares = round(pos.shares - sell_qty, 6)
            if pos.shares <= 1e-5:
                del positions[sym]
                if reason_str == "INITIAL_ATR_STOP" and r_mult < -0.05 and sl_cfg.enabled:
                    stop_episode_dates.append(exit_d)
                    unconsumed = stop_episode_dates[cooldown_watermark_idx:]
                    in_win = [
                        sd for sd in unconsumed if count_trading_days_between(sd, exit_d) <= sl_cfg.lookback_trading_days
                    ]
                    if len(in_win) >= sl_cfg.max_stops_in_window:
                        cooldown_until_d = add_trading_days(exit_d, sl_cfg.cooldown_trading_days)
                        cooldown_watermark_idx = len(stop_episode_dates)

        for cur_d in session_dates:
            if end_d and cur_d > end_d:
                continue
            eval_days.append(cur_d)
            dec_utc = _decision_cutoff_utc(cur_d)

            # 0. In pure `1d` mode, execute T-1 queued entries at `cur_d` Open (09:30 ET)
            if clock_mode == "1d" and queued_next_open:
                open_utc = datetime.combine(cur_d, dt_time(9, 30), tzinfo=ET_TZ).astimezone(timezone.utc)
                for q_plan, q_sig in queued_next_open:
                    if len(positions) >= tier.max_concurrent_positions or q_plan.symbol in positions:
                        continue
                    pair = daily_by_date.get(q_plan.symbol, {}).get(cur_d)
                    if pair is None:
                        continue
                    open_px = pair[1].open
                    # Limit order gap protection: skip if opening auction gapped >1.2% past planned limit
                    if open_px > q_plan.limit_price * 1.012:
                        continue
                    fill_px = round(open_px, 2)
                    buy_fee = self.fee_model.estimate_leg(q_plan.shares, fill_px, OrderSide.BUY).total_friction_usd
                    cost = q_plan.shares * fill_px + buy_fee
                    if cost > cash:
                        continue
                    cash -= cost
                    total_fees += buy_fee
                    buy_dates.append(cur_d)
                    safe_stop = min(q_plan.initial_stop, round(fill_px * 0.99, 2))
                    positions[q_plan.symbol] = OpenPosition(
                        symbol=q_plan.symbol,
                        sector=q_plan.sector,
                        shares=q_plan.shares,
                        initial_shares=q_plan.shares,
                        entry_price=fill_px,
                        entry_time=open_utc,
                        initial_stop=safe_stop,
                        current_stop=safe_stop,
                        highest_price=fill_px,
                        risk_per_share=max(0.01, round(fill_px - safe_stop, 4)),
                        entry_atr_1d=q_sig.atr_14,
                        setup=q_plan.setup.value,
                        metadata={"buy_fee_usd": buy_fee, "last_mark_price": fill_px},
                    )
                queued_next_open = []

            # Build causal bars for `cur_d` in `hybrid_1545` mode
            qqq_partial = (
                build_causal_partial_1d_bar(
                    "QQQ", intraday_by_day.get("QQQ", {}).get(cur_d, []), decision_ts_utc=dec_utc
                )
                if clock_mode == "hybrid_1545"
                else None
            )
            spy_partial = (
                build_causal_partial_1d_bar(
                    "SPY", intraday_by_day.get("SPY", {}).get(cur_d, []), decision_ts_utc=dec_utc
                )
                if clock_mode == "hybrid_1545"
                else None
            )
            uni_partials: Optional[Dict[str, Optional[Bar]]] = (
                {
                    s_sym: build_causal_partial_1d_bar(
                        s_sym, intraday_by_day.get(s_sym, {}).get(cur_d, []), decision_ts_utc=dec_utc
                    )
                    for s_sym in symbol_daily.keys()
                }
                if clock_mode == "hybrid_1545"
                else None
            )

            # 1. Step existing open positions
            for sym in list(positions.keys()):
                pos = positions[sym]
                if clock_mode == "hybrid_1545":
                    step_bar = uni_partials.get(sym) if uni_partials is not None else None
                    if step_bar is None:
                        continue
                    causal_d = build_strictly_causal_daily_series(
                        symbol_daily[sym], as_of_date=cur_d, partial_today_bar=step_bar
                    )
                    atr_val = compute_wilder_atr_series(causal_d[-35:], 14)[-1] if causal_d else pos.entry_atr_1d
                else:
                    pair = daily_by_date.get(sym, {}).get(cur_d)
                    if pair is None:
                        continue
                    d_idx, step_bar = pair
                    atr_val = sym_1d_atrs[sym][d_idx]

                pos.metadata["last_mark_price"] = step_bar.close
                act = self.exit_engine.step_daily_position(
                    pos=pos,
                    daily_bar=step_bar,
                    atr_1d=atr_val,
                    tier=tier,
                    upcoming_earnings=earn_map.get(sym),
                    is_new_day=True,
                )
                if act.should_exit and act.shares_to_sell > 0:
                    _record_exit(
                        sym,
                        pos,
                        act.shares_to_sell,
                        act.exit_price,
                        act.reason.value if act.reason else "EXIT",
                        dec_utc,
                        cur_d,
                    )

            # Mark-to-market equity
            pos_mkt = sum(
                p.shares * float(p.metadata.get("last_mark_price") or p.entry_price)
                for p in positions.values()
            )
            curr_eq = cash + pos_mkt
            peak_equity = max(peak_equity, curr_eq)
            dd_now = ((peak_equity - curr_eq) / peak_equity) * 100.0 if peak_equity > 0 else 0.0
            max_dd_pct = max(max_dd_pct, dd_now)

            # 2. Evaluate Daily Macro & Breadth Regime
            include_today_1d = clock_mode == "1d"
            regime = self.swing_engine.evaluate_regime(
                qqq_daily=qqq_1d,
                spy_daily=spy_1d,
                universe_daily=symbol_daily,
                as_of_date=cur_d,
                qqq_partial_bar=qqq_partial,
                spy_partial_bar=spy_partial,
                universe_partial_bars=uni_partials,
                include_completed_as_of=include_today_1d,
            )
            if regime.light == RegimeLight.GREEN:
                green_days += 1
            else:
                rest_days += 1

            equity_curve.append(
                {
                    "timestamp": dec_utc.isoformat(),
                    "equity_usd": round(curr_eq, 2),
                    "regime": regime.light.value,
                }
            )

            # 3. Entry Evaluation (SwingWatchlistScorer + StoplossGuard + Portfolio Heat + 60d Correlation)
            if regime.light == RegimeLight.RED:
                continue
            if cooldown_until_d is not None and cur_d <= cooldown_until_d:
                continue
            if len(positions) >= tier.max_concurrent_positions:
                continue

            eligible_syms = [
                s for s in symbol_daily.keys() if pit_filter is None or pit_filter.is_member_on(s, cur_d)
            ]
            pit_universe = [
                universe_map[s]
                if s in universe_map
                else UniverseSecurity(
                    symbol=s,
                    sector="Technology",
                    cik="",
                    etf_core_score=0.82,
                    baseline_inst_pct=0.72,
                )
                for s in eligible_syms
            ]
            scored_list = self.swing_scorer.rank_swing_universe(
                quotes={},
                daily_bars_map=symbol_daily,
                qqq_daily_bars=qqq_1d,
                regime_light=regime.light,
                as_of_date=cur_d,
                earnings_events=earn_map,
                qqq_partial_bar=qqq_partial,
                partial_bars_map=uni_partials,
                include_completed_as_of=include_today_1d,
                universe_override=pit_universe,
            )
            scored_rows = [
                {
                    "symbol": c.symbol,
                    "sector": c.sector,
                    "price": c.price,
                    "total_score": c.total_score,
                    "passed_hard_filters": c.passed_hard_filters,
                    "filter_reason": c.filter_reason,
                }
                for c in scored_list
            ]
            top_candidates = self.swing_scorer.select_active_top_n(scored_rows)

            pos_scale = (
                1.0
                if regime.light == RegimeLight.GREEN
                else self.settings.strategy_cfg.regime.yellow_position_scale
            )
            batch_pending_syms = set(positions.keys()) | {qp.symbol for qp, _ in queued_next_open}
            avail_cash = cash - sum(qp.notional_usd for qp, _ in queued_next_open)
            queued_active_buys: List[Dict[str, Any]] = [
                {
                    "shares": qp.shares,
                    "filled_shares": 0.0,
                    "limit_price": qp.limit_price,
                    "plan_json": json.dumps(
                        {
                            "risk_per_share": qp.risk_per_share,
                            "initial_stop": qp.initial_stop,
                            "sector": qp.sector,
                        }
                    ),
                }
                for qp, _ in queued_next_open
            ]

            for cand_row in top_candidates:
                sym = str(cand_row["symbol"])
                if sym in batch_pending_syms:
                    continue
                buys_today = sum(1 for bd in buy_dates if bd == cur_d) + len(queued_next_open)
                if buys_today >= tier.max_trades_per_day:
                    break
                buys_7d = sum(1 for bd in buy_dates if (cur_d - bd).days <= 7) + len(queued_next_open)
                if buys_7d >= tier.max_trades_per_week:
                    break

                sector = str(cand_row["sector"])
                wl_score = float(cand_row["total_score"])
                part_bar = uni_partials.get(sym) if uni_partials is not None else None
                if clock_mode == "hybrid_1545" and part_bar is None:
                    continue

                sig = self.swing_engine.evaluate_entry(
                    symbol=sym,
                    sector=sector,
                    regime_light=regime.light,
                    watchlist_score=wl_score,
                    stock_daily_bars=symbol_daily[sym],
                    qqq_daily_bars=qqq_1d,
                    as_of_date=cur_d,
                    partial_today_bar=part_bar,
                    qqq_partial_bar=qqq_partial,
                    recent_earnings=earn_map.get(sym),
                    include_completed_as_of=include_today_1d,
                )
                if sig is None:
                    continue

                plan, st = self.exit_engine.build_swing_position_plan(
                    signal=sig,
                    account_equity_usd=curr_eq,
                    available_cash_usd=max(0.0, avail_cash) * 0.992,
                    tier=tier,
                    position_scale=pos_scale,
                )
                if plan is None or st != "APPROVED":
                    continue

                # Single-Asset Cap + Portfolio Heat (Principal & Open-Equity Drawdown) + Sector Cap + 60d Correlation
                single_cap = (
                    tier.max_sector_etf_pct
                    if sector.endswith("_ETF")
                    else tier.max_single_equity_pct
                )
                if (plan.notional_usd / max(1.0, curr_eq)) > single_cap + 1e-6:
                    continue

                heat = compute_portfolio_heat(positions, queued_active_buys, curr_eq)
                if (heat.principal_heat_usd + plan.total_risk_usd) / max(1.0, curr_eq) > tier.max_portfolio_heat_pct + 1e-6:
                    continue
                if (heat.open_drawdown_heat_usd + plan.total_risk_usd) / max(1.0, curr_eq) > tier.max_drawdown_heat_pct + 1e-6:
                    continue

                sec_notional = sum(
                    p.shares * p.entry_price for p in positions.values() if p.sector == sector
                ) + sum(
                    qp.shares * qp.limit_price for qp, _ in queued_next_open if qp.sector == sector
                )
                if (sec_notional + plan.notional_usd) / max(1.0, curr_eq) > tier.max_sector_weight_pct + 1e-6:
                    continue

                corr_ref_d = add_trading_days(cur_d, 1) if include_today_1d else cur_d
                max_corr, _ = compute_max_pairwise_correlation(
                    candidate_symbol=sym,
                    comparison_symbols=batch_pending_syms,
                    daily_bars_map=symbol_daily,
                    as_of_date=corr_ref_d,
                    lookback_days=self.settings.strategy_cfg.swing.correlation_lookback_days,
                )
                if max_corr is not None and max_corr > tier.max_pair_corr:
                    continue

                if clock_mode == "1d":
                    queued_next_open.append((plan, sig))
                    queued_active_buys.append(
                        {
                            "shares": plan.shares,
                            "filled_shares": 0.0,
                            "limit_price": plan.limit_price,
                            "plan_json": json.dumps(
                                {
                                    "risk_per_share": plan.risk_per_share,
                                    "initial_stop": plan.initial_stop,
                                    "sector": plan.sector,
                                }
                            ),
                        }
                    )
                    batch_pending_syms.add(sym)
                    avail_cash -= plan.notional_usd
                    if len(batch_pending_syms) >= tier.max_concurrent_positions:
                        break
                else:
                    buy_fee = self.fee_model.estimate_leg(plan.shares, plan.entry_price, OrderSide.BUY).total_friction_usd
                    cost = plan.shares * plan.entry_price + buy_fee
                    if cost > cash:
                        continue
                    cash -= cost
                    avail_cash = cash
                    total_fees += buy_fee
                    buy_dates.append(cur_d)
                    positions[sym] = OpenPosition(
                        symbol=sym,
                        sector=sector,
                        shares=plan.shares,
                        initial_shares=plan.shares,
                        entry_price=plan.entry_price,
                        entry_time=dec_utc,
                        initial_stop=plan.initial_stop,
                        current_stop=plan.initial_stop,
                        highest_price=plan.entry_price,
                        risk_per_share=plan.risk_per_share,
                        entry_atr_1d=sig.atr_14,
                        setup=plan.setup.value,
                        metadata={"buy_fee_usd": buy_fee, "last_mark_price": plan.entry_price},
                    )
                    batch_pending_syms.add(sym)
                    if len(positions) >= tier.max_concurrent_positions:
                        break

        final_pos_val = sum(
            p.shares * float(p.metadata.get("last_mark_price") or p.entry_price)
            for p in positions.values()
        )
        final_eq = cash + final_pos_val
        net_pnl = final_eq - initial_eq
        wins = sum(1 for t in closed_trades if t["net_pnl_usd"] > 0)
        win_rate = (wins / len(closed_trades) * 100.0) if closed_trades else 0.0
        avg_r = sum(t["r_multiple"] for t in closed_trades) / len(closed_trades) if closed_trades else 0.0
        gross_wins = sum(t["net_pnl_usd"] for t in closed_trades if t["net_pnl_usd"] > 0)
        gross_losses = abs(sum(t["net_pnl_usd"] for t in closed_trades if t["net_pnl_usd"] < 0))
        profit_factor = (
            (gross_wins / gross_losses) if gross_losses > 0.01 else (9.99 if gross_wins > 0 else 0.0)
        )
        fee_drag = (total_fees / abs(gross_pnl_total)) * 100.0 if abs(gross_pnl_total) > 0.01 else 0.0

        return BacktestResult(
            tier_key=resolved_tier_key,
            tier_label=tier.label,
            initial_equity_usd=round(initial_eq, 2),
            final_equity_usd=round(final_eq, 2),
            net_return_pct=round((net_pnl / initial_eq) * 100.0, 2),
            gross_pnl_usd=round(gross_pnl_total, 2),
            total_fees_usd=round(total_fees, 2),
            net_pnl_usd=round(net_pnl, 2),
            fee_drag_of_gross_pct=round(fee_drag, 2) if math.isfinite(fee_drag) else 0.0,
            total_closed_trades=len(closed_trades),
            win_rate_pct=round(win_rate, 1),
            avg_r_multiple=round(avg_r, 2),
            profit_factor=round(profit_factor, 2),
            max_drawdown_pct=round(max_dd_pct, 2),
            green_bars=green_days,
            rest_bars=rest_days,
            period_start=eval_days[0].isoformat() if eval_days else "",
            period_end=eval_days[-1].isoformat() if eval_days else "",
            clock_mode=clock_mode,
            data_source=data_source,
            is_synthetic_data=used_synth,
            data_warnings=warnings_list,
            equity_curve=equity_curve,
            trades=closed_trades,
        )
