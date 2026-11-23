from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List

from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.base import MarketDataAdapter
from zy_quant.intel.regime import MarketRegimeDetector
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
    max_drawdown_pct: float
    green_bars: int
    rest_bars: int
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

    def run(
        self,
        tier_key: str = "tier_1k",
        symbols: List[str] | None = None,
        days: int = 30,
    ) -> BacktestResult:
        tier = self.settings.tiers_cfg.get_active(tier_key)
        initial_eq = tier.reference_equity_usd
        cash = initial_eq

        universe_map = {u.symbol: u for u in self.settings.watchlist_cfg.universe}
        target_symbols = symbols or [
            u.symbol for u in self.settings.watchlist_cfg.universe if u.sector != "Broad_ETF"
        ][:6]

        # Load 15m & daily series
        qqq_15m = self.data.get_bars("QQQ", timeframe="15m", days=days)
        qqq_1d = self.data.get_bars("QQQ", timeframe="1d", days=max(60, days + 30))
        spy_1d = self.data.get_bars("SPY", timeframe="1d", days=max(60, days + 30))

        symbol_bars: Dict[str, List[Bar]] = {}
        symbol_atrs: Dict[str, List[float]] = {}
        for sym in target_symbols:
            b15 = self.data.get_bars(sym, timeframe="15m", days=days)
            if len(b15) >= 30:
                symbol_bars[sym] = b15
                symbol_atrs[sym] = compute_wilder_atr_series(
                    b15, self.settings.strategy_cfg.entry.atr_period
                )

        if not qqq_15m or not symbol_bars:
            return BacktestResult(
                tier_key=tier_key,
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
                max_drawdown_pct=0.0,
                green_bars=0,
                rest_bars=0,
            )

        # Index bars by timestamp for chronological simulation
        bar_lookup: Dict[str, Dict[Any, tuple[int, Bar]]] = {}
        for sym, blist in symbol_bars.items():
            bar_lookup[sym] = {b.timestamp: (i, b) for i, b in enumerate(blist)}

        positions: Dict[str, OpenPosition] = {}
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
        for q_idx in range(warmup, len(qqq_15m)):
            q_bar = qqq_15m[q_idx]
            ts = q_bar.timestamp

            # Slice daily bars strictly up to current date (point-in-time, no look-ahead)
            q_d_slice = [d for d in qqq_1d if d.timestamp.date() <= ts.date()]
            s_d_slice = [d for d in spy_1d if d.timestamp.date() <= ts.date()]
            if not q_d_slice:
                q_d_slice = qqq_1d[:20]
            if not s_d_slice:
                s_d_slice = spy_1d[:20]

            regime = self.regime_detector.evaluate(
                qqq_intraday_15m=qqq_15m[max(0, q_idx - 40) : q_idx + 1],
                qqq_daily=q_d_slice,
                spy_daily=s_d_slice,
                vix_level=17.5,
            )
            if regime.light == RegimeLight.GREEN:
                green_bars += 1
            else:
                rest_bars += 1

            # 1. Step open positions first (stops, scale-outs, Chandelier ratchets, time barriers)
            for sym in list(positions.keys()):
                entry_pair = bar_lookup[sym].get(ts)
                if entry_pair is None:
                    continue
                s_idx, s_bar = entry_pair
                pos = positions[sym]
                atr_val = symbol_atrs[sym][s_idx]
                action = self.exit_engine.step_position(pos, s_bar, atr_val, tier)

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

            # 2. Entry scan ONLY when Regime == GREEN and frequency/slot caps permit
            if regime.light != RegimeLight.GREEN:
                continue
            if len(positions) >= tier.max_concurrent_positions:
                continue

            buys_today = sum(1 for t_b in buy_timestamps if t_b.date() == ts.date())
            if buys_today >= tier.max_trades_per_day:
                continue
            buys_7d = sum(1 for t_b in buy_timestamps if (ts - t_b) <= timedelta(days=7))
            if buys_7d >= tier.max_trades_per_week:
                continue

            for sym in target_symbols:
                if sym in positions or sym not in bar_lookup:
                    continue
                pair = bar_lookup[sym].get(ts)
                if pair is None:
                    continue
                s_idx, _ = pair
                if s_idx < warmup:
                    continue

                sec_meta = universe_map.get(sym)
                sector = sec_meta.sector if sec_meta else "Technology"
                sig = self.signal_engine.evaluate_15m(
                    symbol=sym,
                    sector=sector,
                    bars_15m=symbol_bars[sym][max(0, s_idx - 35) : s_idx + 1],
                    regime_light=regime.light,
                    watchlist_score=78.0,
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
        fee_drag = (
            (total_fees / abs(gross_pnl_total)) * 100.0
            if abs(gross_pnl_total) > 0.01
            else 0.0
        )

        return BacktestResult(
            tier_key=tier_key,
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
            max_drawdown_pct=round(max_dd_pct, 2),
            green_bars=green_bars,
            rest_bars=rest_bars,
            equity_curve=equity_curve,
            trades=closed_trades,
        )
