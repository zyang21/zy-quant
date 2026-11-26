from __future__ import annotations

import math
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.base import MarketDataAdapter
from zy_quant.intel.market_hours import session_close_time
from zy_quant.intel.regime import MarketRegimeDetector
from zy_quant.intel.universe_manager import PointInTimeUniverseFilter
from zy_quant.models import Bar, OpenPosition, OrderSide, RegimeLight
from zy_quant.strategy.exit_models import ScientificExitAndSizingEngine
from zy_quant.strategy.indicators import compute_wilder_atr_series
from zy_quant.strategy.signals import IntradaySignalEngine


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
        self.signal_engine = IntradaySignalEngine(settings.strategy_cfg.entry)
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
    ) -> BacktestResult:
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
                days=max(60, fetch_days + 35),
                start_dt=start_1d_dt,
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

        qqq_1d_dates = [d.timestamp.date() for d in qqq_1d]
        spy_1d_dates = [d.timestamp.date() for d in spy_1d]
        sym_1d_dates: Dict[str, List[Any]] = {
            sym: [d.timestamp.date() for d in dlist] for sym, dlist in symbol_daily.items()
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
            data_source=data_source,
            is_synthetic_data=used_synth,
            data_warnings=warnings_list,
            equity_curve=equity_curve,
            trades=closed_trades,
        )
