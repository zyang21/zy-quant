from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from zy_quant.config import AppSettings
from zy_quant.storage.state_store import StateStore


class PortfolioAnalyticsService:
    """
    OOP Domain Service: Computes live portfolio valuation, mark-to-market positions,
    equity curve progression, and risk-adjusted performance metrics without exposing
    raw infrastructure credentials or endpoints.
    """

    def __init__(self, settings: AppSettings, store: StateStore) -> None:
        self.settings = settings
        self.store = store

    def build_live_portfolio_snapshot(
        self, broker_summary: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        tier_key = self.settings.active_tier_key
        tier = self.settings.active_tier_profile
        is_real = self.settings.runtime_cfg.futu.trd_env.upper() == "REAL"

        watchlist_rows = self.store.get_watchlist_scores()
        wl_prices = {w["symbol"]: float(w["price"]) for w in watchlist_rows}

        positions_out: List[Dict[str, Any]] = []
        unrealized_total = 0.0
        invested_cost = 0.0
        mark_val_total = 0.0

        for p in self.store.get_positions().values():
            mark_px = float(
                p.metadata.get("last_mark_price") or wl_prices.get(p.symbol) or p.entry_price
            )
            unreal = (mark_px - p.entry_price) * p.shares
            unreal_pct = ((mark_px - p.entry_price) / p.entry_price * 100.0) if p.entry_price > 0 else 0.0
            r_now = (mark_px - p.entry_price) / max(0.01, p.risk_per_share)
            unrealized_total += unreal
            invested_cost += p.shares * p.entry_price + float(p.metadata.get("buy_fee_usd", 0.0))
            mark_val_total += p.shares * mark_px

            positions_out.append(
                {
                    "symbol": p.symbol,
                    "sector": p.sector,
                    "setup": p.setup,
                    "shares": p.shares,
                    "entry_price": round(p.entry_price, 2),
                    "mark_price": round(mark_px, 2),
                    "market_value_usd": round(p.shares * mark_px, 2),
                    "unrealized_pnl_usd": round(unreal, 2),
                    "unrealized_pnl_pct": round(unreal_pct, 2),
                    "current_r": round(r_now, 2),
                    "initial_stop": round(p.initial_stop, 2),
                    "current_stop": round(p.current_stop, 2),
                    "highest_price": round(p.highest_price, 2),
                    "bars_held_15m": p.bars_held_15m,
                    "hold_hours": round(p.bars_held_15m * 0.25, 2),
                    "breakeven_armed": p.breakeven_armed,
                    "scaled_out": p.scaled_out,
                    "entry_time": p.entry_time.isoformat(),
                }
            )

        closed_all = self.store.query_closed_trades(limit=1000)
        orders_all = self.store.query_orders(limit=1000)

        realized_total = sum(float(t.get("net_pnl_usd", 0.0)) for t in closed_all)
        closed_gross = sum(float(t.get("gross_pnl_usd", 0.0)) for t in closed_all)
        filled_fees = sum(
            float(o.get("fee_usd", 0.0)) for o in orders_all if o.get("status") == "FILLED"
        )

        if is_real and broker_summary and float(broker_summary.get("equity_usd", 0.0)) > 0:
            equity_usd = float(broker_summary["equity_usd"])
            cash_usd = float(broker_summary.get("cash_usd", 0.0))
            base_eq = equity_usd - realized_total - unrealized_total
            if base_eq <= 0:
                base_eq = equity_usd
        else:
            base_eq = tier.reference_equity_usd
            cash_usd = max(0.0, base_eq + realized_total - invested_cost)
            equity_usd = cash_usd + mark_val_total

        net_pnl_usd = equity_usd - base_eq
        net_ret_pct = (net_pnl_usd / max(1.0, base_eq)) * 100.0
        wins = sum(1 for t in closed_all if float(t.get("net_pnl_usd", 0.0)) > 0)
        win_rate = (wins / len(closed_all) * 100.0) if closed_all else 0.0

        hist_key = f"live_eq_{tier_key}"
        hist_flag = self.store.get_flag(hist_key) or {
            "points": [],
            "peak": tier.reference_equity_usd,
            "max_dd_pct": 0.0,
        }
        pts = list(hist_flag.get("points", []))
        now_stamp = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
        if (
            not pts
            or pts[-1].get("t") != now_stamp
            or abs(float(pts[-1].get("equity_usd", 0.0)) - round(equity_usd, 2)) >= 0.05
        ):
            pts.append({"t": now_stamp, "equity_usd": round(equity_usd, 2)})
            pts = pts[-240:]
        peak_eq = max(float(hist_flag.get("peak", tier.reference_equity_usd)), equity_usd)
        dd_now = ((peak_eq - equity_usd) / peak_eq * 100.0) if peak_eq > 0 else 0.0
        max_dd = max(float(hist_flag.get("max_dd_pct", 0.0)), dd_now)
        self.store.set_flag(
            hist_key,
            {"points": pts, "peak": round(peak_eq, 2), "max_dd_pct": round(max_dd, 3)},
        )

        exposure_pct = (mark_val_total / max(1.0, equity_usd)) * 100.0
        return {
            "portfolio": {
                "profile_label": tier.label,
                "starting_equity_usd": round(base_eq, 2),
                "equity_usd": round(equity_usd, 2),
                "cash_usd": round(cash_usd, 2),
                "market_value_usd": round(mark_val_total, 2),
                "exposure_pct": round(exposure_pct, 1),
                "net_pnl_usd": round(net_pnl_usd, 2),
                "net_return_pct": round(net_ret_pct, 2),
                "gross_pnl_usd": round(closed_gross + unrealized_total, 2),
                "unrealized_pnl_usd": round(unrealized_total, 2),
                "realized_pnl_usd": round(realized_total, 2),
                "total_fees_usd": round(filled_fees, 2),
                "closed_trades_count": len(closed_all),
                "win_rate_pct": round(win_rate, 1),
                "max_drawdown_pct": round(max_dd, 2),
                "equity_curve": pts,
            },
            "positions": positions_out,
            "watchlist": watchlist_rows,
        }


class HistoryQueryService:
    """
    OOP Domain Service: Queries closed trades and raw order execution records across
    arbitrary time windows (`start_date`..`end_date` or presets) and aggregates period statistics.
    """

    def __init__(self, store: StateStore) -> None:
        self.store = store

    @staticmethod
    def resolve_window(
        preset: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        now = datetime.now(timezone.utc)
        start_iso: Optional[str] = None
        end_iso: Optional[str] = None

        if preset and preset != "ALL" and not start_date:
            days_map = {"24H": 1, "7D": 7, "30D": 30, "90D": 90, "YTD": 365}
            if preset.upper() == "YTD":
                start_iso = datetime(now.year, 1, 1, tzinfo=timezone.utc).isoformat()
            elif preset.upper() in days_map:
                start_iso = (now - timedelta(days=days_map[preset.upper()])).isoformat()

        if start_date:
            start_iso = f"{start_date[:10]}T00:00:00"
        if end_date:
            end_iso = f"{end_date[:10]}T23:59:59.999999+99:99"

        return start_iso, end_iso

    def query_history(
        self,
        preset: Optional[str] = "ALL",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        symbol: Optional[str] = None,
        exit_reason: Optional[str] = None,
        limit: int = 300,
    ) -> Dict[str, Any]:
        start_iso, end_iso = self.resolve_window(preset, start_date, end_date)
        trades = self.store.query_closed_trades(
            start_iso=start_iso,
            end_iso=end_iso,
            symbol=symbol,
            exit_reason=exit_reason,
            limit=limit,
        )
        orders = self.store.query_orders(
            start_iso=start_iso,
            end_iso=end_iso,
            symbol=symbol,
            limit=limit,
        )

        net_pnl = sum(float(t["net_pnl_usd"]) for t in trades)
        gross_pnl = sum(float(t["gross_pnl_usd"]) for t in trades)
        trade_fees = sum(float(t["fee_usd"]) for t in trades)
        wins = [t for t in trades if float(t["net_pnl_usd"]) > 0]
        losses = [t for t in trades if float(t["net_pnl_usd"]) < 0]
        win_rate = (len(wins) / len(trades) * 100.0) if trades else 0.0
        avg_r = (sum(float(t["r_multiple"]) for t in trades) / len(trades)) if trades else 0.0

        sum_wins = sum(float(t["net_pnl_usd"]) for t in wins)
        sum_losses = abs(sum(float(t["net_pnl_usd"]) for t in losses))
        profit_factor = (
            (sum_wins / sum_losses) if sum_losses > 0.01 else (9.99 if sum_wins > 0 else 0.0)
        )

        return {
            "query": {
                "preset": preset or "CUSTOM",
                "start_date": start_date or (start_iso[:10] if start_iso else ""),
                "end_date": end_date or "",
                "symbol": symbol or "",
                "exit_reason": exit_reason or "",
            },
            "summary": {
                "trades_count": len(trades),
                "orders_count": len(orders),
                "net_pnl_usd": round(net_pnl, 2),
                "gross_pnl_usd": round(gross_pnl, 2),
                "fees_usd": round(trade_fees, 2),
                "win_rate_pct": round(win_rate, 1),
                "avg_r_multiple": round(avg_r, 2),
                "profit_factor": round(profit_factor, 2),
            },
            "closed_trades": trades,
            "orders": orders,
        }
