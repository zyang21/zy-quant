from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Optional

import uvicorn

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.intel.market_hours import get_us_market_session
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.storage.state_store import StateStore


def _print_history(
    settings: AppSettings,
    tier_key: Optional[str],
    limit: int,
    symbol_filter: Optional[str],
    show_orders: bool,
    as_json: bool,
) -> None:
    store = StateStore(settings.root_dir / "data" / "zy_quant.db")
    tier = settings.tiers_cfg.get_active(tier_key)
    session = get_us_market_session()

    wl_prices = {w["symbol"]: float(w["price"]) for w in store.get_watchlist_scores()}
    open_positions = list(store.get_positions().values())
    closed_trades = store.list_closed_trades(limit=max(200, limit))
    recent_orders = store.list_recent_orders(limit=max(200, limit))

    if symbol_filter:
        sym_u = symbol_filter.upper()
        open_positions = [p for p in open_positions if p.symbol == sym_u]
        closed_trades = [t for t in closed_trades if t["symbol"] == sym_u]
        recent_orders = [o for o in recent_orders if o["symbol"] == sym_u]

    unrealized_total = 0.0
    invested_cost = 0.0
    mark_val_total = 0.0
    pos_rows = []
    for p in open_positions:
        mark_px = float(p.metadata.get("last_mark_price") or wl_prices.get(p.symbol) or p.entry_price)
        upnl = (mark_px - p.entry_price) * p.shares
        unrealized_total += upnl
        invested_cost += p.shares * p.entry_price + float(p.metadata.get("buy_fee_usd", 0.0))
        mark_val_total += p.shares * mark_px
        pos_rows.append(
            {
                "symbol": p.symbol,
                "setup": p.setup,
                "shares": p.shares,
                "entry_price": round(p.entry_price, 2),
                "mark_price": round(mark_px, 2),
                "unrealized_pnl_usd": round(upnl, 2),
                "current_stop": round(p.current_stop, 2),
                "bars_held_15m": p.bars_held_15m,
                "entry_time": p.entry_time.isoformat(),
            }
        )

    realized_total = store.realized_pnl_since("2000-01-01T00:00:00")
    sim_cash = max(0.0, tier.reference_equity_usd + realized_total - invested_cost)
    sim_equity = sim_cash + mark_val_total
    closed_gross = sum(float(t.get("gross_pnl_usd", 0.0)) for t in closed_trades)
    total_fees = sum(
        float(o.get("fee_usd", 0.0)) for o in recent_orders if o.get("status") == "FILLED"
    )
    wins = sum(1 for t in closed_trades if float(t.get("net_pnl_usd", 0.0)) > 0)
    win_rate = (wins / len(closed_trades) * 100.0) if closed_trades else 0.0
    net_ret_pct = ((sim_equity - tier.reference_equity_usd) / max(1.0, tier.reference_equity_usd)) * 100.0

    payload = {
        "market_session": session.message,
        "tier": tier.label,
        "kpis": {
            "starting_equity_usd": round(tier.reference_equity_usd, 2),
            "current_equity_usd": round(sim_equity, 2),
            "cash_usd": round(sim_cash, 2),
            "net_return_pct": round(net_ret_pct, 2),
            "gross_pnl_usd": round(closed_gross + unrealized_total, 2),
            "total_fees_usd": round(total_fees, 2),
            "realized_net_pnl_usd": round(realized_total, 2),
            "unrealized_pnl_usd": round(unrealized_total, 2),
            "closed_trades": len(closed_trades),
            "win_rate_pct": round(win_rate, 1),
        },
        "open_positions": pos_rows,
        "closed_trades": closed_trades[:limit],
        "orders": recent_orders[:limit] if show_orders else [],
    }

    if as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    k = payload["kpis"]
    print("=" * 86)
    print(f" zy-quant Trading Ledger  |  {tier.label}")
    print(f" Session : {session.message}")
    print("-" * 86)
    print(
        f" Equity: ${k['current_equity_usd']:,.2f} (Cash ${k['cash_usd']:,.2f})  |  "
        f"Net Return: {k['net_return_pct']:+.2f}%  |  Gross PnL: ${k['gross_pnl_usd']:+.2f}"
    )
    print(
        f" Futu Fees+Slip: -${k['total_fees_usd']:.2f}  |  "
        f"Closed Trades: {k['closed_trades']} (Win {k['win_rate_pct']:.1f}%)  |  "
        f"Unrealized: ${k['unrealized_pnl_usd']:+.2f}"
    )
    print("=" * 86)

    print(f"\n[Open Positions ({len(pos_rows)})]")
    if not pos_rows:
        print("  (None)")
    else:
        print(f"  {'SYM':<6} {'SETUP':<14} {'QTY':>7} {'ENTRY':>9} {'MARK':>9} {'STOP':>9} {'UNREAL':>10} {'BARS':>5}")
        for p in pos_rows:
            print(
                f"  {p['symbol']:<6} {p['setup']:<14} {p['shares']:>7.2f} "
                f"${p['entry_price']:>8.2f} ${p['mark_price']:>8.2f} ${p['current_stop']:>8.2f} "
                f"${p['unrealized_pnl_usd']:>+9.2f} {p['bars_held_15m']:>5d}"
            )

    shown_closed = closed_trades[:limit]
    print(f"\n[Closed Trades History ({len(shown_closed)}/{len(closed_trades)})]")
    if not shown_closed:
        print("  (No closed trades recorded yet)")
    else:
        print(
            f"  {'ID':<4} {'SYM':<6} {'QTY':>6} {'ENTRY -> EXIT':<21} "
            f"{'FEE':>8} {'NET PNL':>10} {'R-MULT':>7} {'EXIT REASON':<18} {'EXIT TIME (UTC)'}"
        )
        for t in shown_closed:
            pair = f"${t['entry_price']:.2f} -> ${t['exit_price']:.2f}"
            print(
                f"  {t['id']:<4} {t['symbol']:<6} {t['shares']:>6.2f} {pair:<21} "
                f"-${t['fee_usd']:>6.2f} ${t['net_pnl_usd']:>+9.2f} {t['r_multiple']:>+6.2f}R "
                f"{t['exit_reason']:<18} {t['exit_time'][:19]}"
            )

    if show_orders:
        shown_orders = recent_orders[:limit]
        print(f"\n[Order Journal ({len(shown_orders)}/{len(recent_orders)})]")
        if not shown_orders:
            print("  (No orders recorded yet)")
        else:
            for o in shown_orders:
                print(
                    f"  {o['created_at'][:19]}  {o['side']:<4} {o['symbol']:<6} "
                    f"qty={o['shares']} fill=${o['fill_price'] or 0:.2f} fee=${o['fee_usd']:.2f} "
                    f"[{o['status']}] ({o['broker_order_id']}) {o['reason']}"
                )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="zy-quant",
        description="zy-quant: US Equities Short-Term & Swing Quant System (Futu OpenAPI)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_dash = sub.add_parser("dashboard", help="Launch FastAPI Quantitative Control Center UI")
    p_dash.add_argument("--host", default="127.0.0.1")
    p_dash.add_argument("--port", type=int, default=8088)
    p_dash.add_argument("--auto-daemon", action="store_true", help="Auto-start overnight simulation loop on boot")
    p_dash.add_argument("--tier", default=None, help="Default account tier: tier_1k | tier_10k | tier_100k")

    p_daemon = sub.add_parser("daemon", help="Run resilient 24/7 two-speed background daemon")
    p_daemon.add_argument("--tier", default=None, help="tier_1k | tier_10k | tier_100k")
    p_daemon.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    p_daemon.add_argument("--edgar", action="store_true", help="Include live SEC EDGAR Form 4 scan")

    p_hist = sub.add_parser("history", help="View historical trades, open positions, and runtime KPIs")
    p_hist.add_argument("--tier", default="tier_10k", help="tier_1k | tier_10k | tier_100k")
    p_hist.add_argument("--limit", type=int, default=30, help="Max closed trades / orders to display")
    p_hist.add_argument("--symbol", default=None, help="Filter by ticker symbol (e.g. NVDA)")
    p_hist.add_argument("--orders", action="store_true", help="Include raw order execution journal")
    p_hist.add_argument("--json", action="store_true", help="Output raw JSON")

    p_bt = sub.add_parser("backtest", help="Run fee-accurate 15m US backtest across tiers")
    p_bt.add_argument("--tier", default="tier_1k", help="tier_1k | tier_10k | tier_100k")
    p_bt.add_argument("--days", type=int, default=30)

    p_fee = sub.add_parser("fee-calc", help="Calculate exact Futu HK 2026 US round-trip friction")
    p_fee.add_argument("--shares", type=float, required=True)
    p_fee.add_argument("--price", type=float, required=True)
    p_fee.add_argument("--scale-out", action="store_true")

    p_halt = sub.add_parser("halt", help="Engage or release global emergency Kill-Switch")
    p_halt.add_argument("--release", action="store_true", help="Release kill-switch")
    p_halt.add_argument("--reason", default="Manual CLI emergency halt")

    args = parser.parse_args()
    settings = AppSettings.load()

    if args.cmd == "dashboard":
        if args.auto_daemon:
            os.environ["ZY_AUTO_START_DAEMON"] = "1"
        if args.tier:
            os.environ["ZY_ACCOUNT_TIER"] = args.tier
        uvicorn.run("zy_quant.web.app:app", host=args.host, port=args.port, reload=False)
    elif args.cmd == "daemon":
        daemon = TradingDaemon(settings)
        if args.once:
            daemon.startup_reconcile()
            out = daemon.run_cycle(tier_override=args.tier, check_edgar=args.edgar, force_when_closed=True)
            print(json.dumps(out, indent=2, ensure_ascii=False))
            daemon.gateway.close()
        else:
            daemon.run_forever(tier_override=args.tier)
    elif args.cmd == "history":
        _print_history(
            settings=settings,
            tier_key=args.tier,
            limit=args.limit,
            symbol_filter=args.symbol,
            show_orders=args.orders,
            as_json=args.json,
        )
    elif args.cmd == "backtest":
        engine = BacktestEngine(settings, YahooMarketDataAdapter())
        res = engine.run(tier_key=args.tier, days=args.days)
        summary = {
            k: v for k, v in res.__dict__.items() if k not in ("equity_curve", "trades")
        }
        summary["sample_trades"] = res.trades[:5]
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    elif args.cmd == "fee-calc":
        fm = FutuUSFeeModel()
        print(
            json.dumps(
                fm.estimate_round_trip(args.shares, args.price, scale_out=args.scale_out),
                indent=2,
            )
        )
    elif args.cmd == "halt":
        store = StateStore(settings.root_dir / "data" / "zy_quant.db")
        store.set_halted(not args.release, "" if args.release else args.reason)
        print(json.dumps(store.get_halt_state(), indent=2))


if __name__ == "__main__":
    main()
