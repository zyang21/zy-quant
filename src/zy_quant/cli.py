from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict
from typing import Optional

import uvicorn

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.futu_adapter import FutuGatewayAdapter
from zy_quant.intel.market_hours import get_us_market_session
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.services.analytics import HistoryQueryService, PortfolioAnalyticsService
from zy_quant.storage.state_store import StateStore


class QuantCLI:
    """Object-oriented CLI controller sharing domain services with the Web Terminal."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings = settings or AppSettings.load()
        self.db_path = self.settings.root_dir / "data" / "zy_quant.db"

    def run_dashboard(
        self,
        host: Optional[str],
        port: Optional[int],
        auto_daemon: Optional[bool],
        tier: Optional[str],
    ) -> None:
        web_cfg = self.settings.runtime_cfg.web
        resolved_host = host or web_cfg.host
        resolved_port = port or web_cfg.port
        if auto_daemon is not None:
            os.environ["ZY_AUTO_START_DAEMON"] = "1" if auto_daemon else "0"
        if tier:
            os.environ["ZY_ACCOUNT_TIER"] = tier
        uvicorn.run("zy_quant.web.app:app", host=resolved_host, port=resolved_port, reload=False)

    def run_status(self, as_json: bool = False) -> None:
        store = StateStore(self.db_path)
        portfolio_svc = PortfolioAnalyticsService(self.settings, store)
        snap = portfolio_svc.build_live_portfolio_snapshot()
        session = get_us_market_session()
        regime = store.get_flag("latest_regime") or {}
        halt = store.get_halt_state()

        payload = {
            "market_session": asdict(session),
            "regime": regime,
            "halt_state": halt,
            "portfolio": snap["portfolio"],
            "open_positions": snap["positions"],
            "top_watchlist": snap["watchlist"][:8],
        }
        if as_json:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
            return

        p = snap["portfolio"]
        print("=" * 90)
        print(f" zy-quant Status  |  {p['profile_label']}")
        print(f" Session : {session.message}")
        print(
            f" Regime  : {regime.get('light', 'UNKNOWN')}  |  "
            f"QQQ ${regime.get('qqq_price', '—')} (VWAP ${regime.get('qqq_vwap', '—')}, "
            f"SMA20 ${regime.get('qqq_sma20', '—')}, Vol {regime.get('vix_level', '—')})"
        )
        print("-" * 90)
        print(
            f" NAV: ${p['equity_usd']:,.2f} (Cash ${p['cash_usd']:,.2f}, Invested {p['exposure_pct']:.1f}%)  |  "
            f"Net Return: {p['net_return_pct']:+.2f}% (${p['net_pnl_usd']:+.2f})"
        )
        print(
            f" Gross PnL: ${p['gross_pnl_usd']:+.2f}  |  Fees+Slip: -${p['total_fees_usd']:.2f}  |  "
            f"Win Rate: {p['win_rate_pct']:.1f}% ({p['closed_trades_count']} trades)  |  Max DD: -{p['max_drawdown_pct']:.2f}%"
        )
        print("=" * 90)

        print(f"\n[Watchlist Top Candidates]")
        for w in snap["watchlist"][:8]:
            status = "ELIGIBLE" if (w["passed_hard_filters"] and w["total_score"] >= 62) else w["filter_reason"]
            print(
                f"  {w['symbol']:<6} {w['sector']:<15} ${w['price']:>8.2f}  "
                f"Score {w['total_score']:>5.1f}  RS {w['rs_vs_qqq']:>+6.2f}%  [{status}]"
            )

        print(f"\n[Open Positions ({len(snap['positions'])})]")
        if not snap["positions"]:
            print("  (None)")
        else:
            for pos in snap["positions"]:
                print(
                    f"  {pos['symbol']:<6} {pos['setup']:<16} qty={pos['shares']:>6.2f}  "
                    f"${pos['entry_price']:.2f} -> ${pos['mark_price']:.2f}  "
                    f"PnL ${pos['unrealized_pnl_usd']:+.2f} ({pos['current_r']:+.2f}R)  Stop ${pos['current_stop']:.2f}"
                )

    def run_history(
        self,
        preset: str,
        start_date: Optional[str],
        end_date: Optional[str],
        symbol: Optional[str],
        exit_reason: Optional[str],
        limit: int,
        show_orders: bool,
        as_json: bool,
    ) -> None:
        store = StateStore(self.db_path)
        history_svc = HistoryQueryService(store)
        res = history_svc.query_history(
            preset=preset,
            start_date=start_date,
            end_date=end_date,
            symbol=symbol,
            exit_reason=exit_reason,
            limit=limit,
        )
        if as_json:
            print(json.dumps(res, indent=2, ensure_ascii=False))
            return

        s = res["summary"]
        q = res["query"]
        window_str = (
            f"{q['start_date']} -> {q['end_date'] or 'now'}"
            if q["start_date"]
            else f"Preset: {q['preset']}"
        )
        print("=" * 92)
        print(f" zy-quant Trade History  |  {window_str}" + (f"  |  Symbol: {q['symbol']}" if q["symbol"] else ""))
        print("-" * 92)
        print(
            f" Closed Trades: {s['trades_count']}  |  Net PnL: ${s['net_pnl_usd']:+.2f}  |  "
            f"Gross: ${s['gross_pnl_usd']:+.2f}  |  Fees: -${s['fees_usd']:.2f}"
        )
        print(
            f" Win Rate: {s['win_rate_pct']:.1f}%  |  Profit Factor: {s['profit_factor']:.2f}  |  "
            f"Avg R-Multiple: {s['avg_r_multiple']:+.2f}R"
        )
        print("=" * 92)

        trades = res["closed_trades"]
        print(f"\n[Closed Trades ({len(trades)})]")
        if not trades:
            print("  (No closed trades matched this time window)")
        else:
            print(
                f"  {'ID':<4} {'SYM':<6} {'QTY':>6} {'ENTRY -> EXIT':<21} "
                f"{'FEE':>8} {'NET PNL':>10} {'R-MULT':>7} {'EXIT RULE':<20} {'EXIT TIME (UTC)'}"
            )
            for t in trades:
                pair = f"${t['entry_price']:.2f} -> ${t['exit_price']:.2f}"
                print(
                    f"  {t['id']:<4} {t['symbol']:<6} {t['shares']:>6.2f} {pair:<21} "
                    f"-${t['fee_usd']:>6.2f} ${t['net_pnl_usd']:>+9.2f} {t['r_multiple']:>+6.2f}R "
                    f"{t['exit_reason']:<20} {t['exit_time'][:19]}"
                )

        if show_orders:
            orders = res["orders"]
            print(f"\n[Order Execution Journal ({len(orders)})]")
            if not orders:
                print("  (No orders matched this time window)")
            else:
                for o in orders:
                    print(
                        f"  {o['created_at'][:19]}  {o['side']:<4} {o['symbol']:<6} "
                        f"qty={o['shares']} fill=${o['fill_price'] or 0:.2f} fee=${o['fee_usd']:.2f} "
                        f"[{o['status']}] {o['reason']}"
                    )

    def run_backtest(
        self,
        tier: Optional[str],
        days: int,
        start_date: Optional[str],
        end_date: Optional[str],
        symbols_csv: Optional[str],
        as_json: bool,
    ) -> None:
        gw = FutuGatewayAdapter(futu_cfg=self.settings.runtime_cfg.futu)
        gw.connect()
        try:
            symbols = (
                [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
                if symbols_csv
                else None
            )
            engine = BacktestEngine(self.settings, gw)
            res = engine.run(
                tier_key=tier or self.settings.active_tier_key,
                symbols=symbols,
                days=days,
                start_date=start_date,
                end_date=end_date,
            )
        finally:
            gw.close()

        if as_json:
            print(json.dumps(asdict(res), indent=2, ensure_ascii=False))
            return

        print("=" * 90)
        print(f" zy-quant Strategy Backtest  |  {res.tier_label}")
        print(f" Window : {res.period_start or '—'} -> {res.period_end or '—'} ({res.green_bars} Bullish / {res.rest_bars} Defensive Bars)")
        print("-" * 90)
        print(
            f" Initial: ${res.initial_equity_usd:,.2f} -> Final: ${res.final_equity_usd:,.2f}  |  "
            f"Net Return: {res.net_return_pct:+.2f}% (${res.net_pnl_usd:+.2f})"
        )
        print(
            f" Gross: ${res.gross_pnl_usd:+.2f}  |  Futu Fees+Slip: -${res.total_fees_usd:.2f} ({res.fee_drag_of_gross_pct:.1f}% drag)  |  "
            f"Max Drawdown: -{res.max_drawdown_pct:.2f}%"
        )
        print(
            f" Trades: {res.total_closed_trades}  |  Win Rate: {res.win_rate_pct:.1f}%  |  "
            f"Profit Factor: {res.profit_factor:.2f}  |  Avg Expectancy: {res.avg_r_multiple:+.2f}R"
        )
        print("=" * 90)
        for t in res.trades[-10:]:
            print(
                f"  {t['exit_time'][:16]}  {t['symbol']:<6} {t['setup']:<14} qty={t['shares']}  "
                f"${t['entry_price']} -> ${t['exit_price']}  net=${t['net_pnl_usd']:+.2f} ({t['r_multiple']:+.2f}R) [{t['exit_reason']}]"
            )

    def run_config_show(self) -> None:
        cfg = self.settings.runtime_cfg.model_dump()
        if cfg["futu"].get("api_token"):
            tok = cfg["futu"]["api_token"]
            cfg["futu"]["api_token"] = f"{tok[:3]}***{tok[-2:]}" if len(tok) > 5 else "***"
        if cfg["futu"].get("trade_pwd_md5"):
            cfg["futu"]["trade_pwd_md5"] = "***"
        print(
            json.dumps(
                {
                    "settings_file": str(self.settings.root_dir / "config" / "settings.json"),
                    "local_override_file": str(self.settings.root_dir / "config" / "settings.local.json"),
                    "active_runtime_config": cfg,
                },
                indent=2,
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="zy-quant",
        description="zy-quant: Quantitative US Equities Trading Terminal & CLI",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="Show live market regime, portfolio NAV, watchlist & positions")
    p_status.add_argument("--json", action="store_true")

    p_dash = sub.add_parser("dashboard", help="Launch Web Quantitative Trading Terminal")
    p_dash.add_argument("--host", default=None)
    p_dash.add_argument("--port", type=int, default=None)
    p_dash.add_argument("--auto-daemon", action="store_true", default=None)
    p_dash.add_argument("--tier", default=None)

    p_daemon = sub.add_parser("daemon", help="Run headless 24/7 background trading daemon")
    p_daemon.add_argument("--tier", default=None)
    p_daemon.add_argument("--once", action="store_true")
    p_daemon.add_argument("--edgar", action="store_true")

    p_hist = sub.add_parser("history", help="Query closed trades & execution journal across time windows")
    p_hist.add_argument("--preset", default="ALL", help="24H | 7D | 30D | 90D | YTD | ALL")
    p_hist.add_argument("--from", dest="start_date", default=None, help="Start date YYYY-MM-DD")
    p_hist.add_argument("--to", dest="end_date", default=None, help="End date YYYY-MM-DD")
    p_hist.add_argument("--symbol", default=None, help="Filter by ticker (e.g. NVDA)")
    p_hist.add_argument("--reason", dest="exit_reason", default=None, help="Filter by exit rule")
    p_hist.add_argument("--limit", type=int, default=50)
    p_hist.add_argument("--orders", action="store_true", help="Also show raw order execution journal")
    p_hist.add_argument("--json", action="store_true")

    p_bt = sub.add_parser("backtest", help="Run fee-accurate 15m strategy backtest over past window")
    p_bt.add_argument("--tier", default=None)
    p_bt.add_argument("--days", type=int, default=30)
    p_bt.add_argument("--from", dest="start_date", default=None, help="Start date YYYY-MM-DD")
    p_bt.add_argument("--to", dest="end_date", default=None, help="End date YYYY-MM-DD")
    p_bt.add_argument("--symbols", default=None, help="Comma-separated symbols (e.g. NVDA,MSFT)")
    p_bt.add_argument("--json", action="store_true")

    sub.add_parser("config", help="Display active merged runtime configuration (secrets masked)")

    p_fee = sub.add_parser("fee-calc", help="Calculate exact 2026 Futu HK US round-trip friction")
    p_fee.add_argument("--shares", type=float, required=True)
    p_fee.add_argument("--price", type=float, required=True)
    p_fee.add_argument("--scale-out", action="store_true")

    p_halt = sub.add_parser("halt", help="Engage or release global emergency Kill-Switch")
    p_halt.add_argument("--release", action="store_true")
    p_halt.add_argument("--reason", default="Manual CLI emergency halt")

    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = build_parser().parse_args()
    cli = QuantCLI()

    if args.cmd == "status":
        cli.run_status(as_json=args.json)
    elif args.cmd == "dashboard":
        logging.getLogger().setLevel(logging.INFO)
        cli.run_dashboard(args.host, args.port, args.auto_daemon, args.tier)
    elif args.cmd == "daemon":
        logging.getLogger().setLevel(logging.INFO)
        daemon = TradingDaemon(cli.settings)
        if args.once:
            daemon.startup_reconcile()
            out = daemon.run_cycle(tier_override=args.tier, check_edgar=args.edgar, force_when_closed=True)
            print(json.dumps(out, indent=2, ensure_ascii=False))
            daemon.gateway.close()
        else:
            daemon.run_forever(tier_override=args.tier)
    elif args.cmd == "history":
        cli.run_history(
            preset=args.preset,
            start_date=args.start_date,
            end_date=args.end_date,
            symbol=args.symbol,
            exit_reason=args.exit_reason,
            limit=args.limit,
            show_orders=args.orders,
            as_json=args.json,
        )
    elif args.cmd == "backtest":
        cli.run_backtest(
            tier=args.tier,
            days=args.days,
            start_date=args.start_date,
            end_date=args.end_date,
            symbols_csv=args.symbols,
            as_json=args.json,
        )
    elif args.cmd == "config":
        cli.run_config_show()
    elif args.cmd == "fee-calc":
        fm = FutuUSFeeModel()
        print(
            json.dumps(
                fm.estimate_round_trip(args.shares, args.price, scale_out=args.scale_out),
                indent=2,
            )
        )
    elif args.cmd == "halt":
        store = StateStore(cli.db_path)
        store.set_halted(not args.release, "" if args.release else args.reason)
        print(json.dumps(store.get_halt_state(), indent=2))


if __name__ == "__main__":
    main()
