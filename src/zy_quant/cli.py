from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Optional

import uvicorn

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.alpaca_adapter import AlpacaMarketDataAdapter
from zy_quant.gateway.futu_adapter import FutuGatewayAdapter
from zy_quant.intel.earnings_calendar import EarningsCalendarFeed
from zy_quant.intel.market_hours import get_us_market_session
from zy_quant.intel.universe_manager import (
    PointInTimeUniverseFilter,
    rebalance_universe_with_buffer,
    save_universe_yaml,
)
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.services.analytics import HistoryQueryService, PortfolioAnalyticsService
from zy_quant.storage.bar_cache import HistoricalBarCache
from zy_quant.storage.state_store import StateStore


class QuantCLI:
    """Object-oriented CLI controller sharing domain services with the Web Terminal."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings = settings or AppSettings.load()
        self.db_path = self.settings.resolve_db_path()

    def _store_for_env(self, trd_env: Optional[str] = None) -> StateStore:
        return StateStore(self.settings.resolve_db_path(trd_env))

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

    def run_status(self, trd_env: Optional[str] = None, as_json: bool = False) -> None:
        store = self._store_for_env(trd_env)
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
        trd_env: Optional[str] = None,
    ) -> None:
        store = self._store_for_env(trd_env)
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
        pit_csv: Optional[str] = None,
    ) -> None:
        bar_cache = HistoricalBarCache(self.settings.resolve_bar_cache_path())
        alpaca_cfg = self.settings.runtime_cfg.alpaca
        gw: Optional[FutuGatewayAdapter] = None

        # Only spin up FutuOpenD TCP handshake during backtests if Alpaca API keys aren't configured yet
        if not (alpaca_cfg.enabled and alpaca_cfg.api_key and alpaca_cfg.secret_key):
            gw = FutuGatewayAdapter(
                futu_cfg=self.settings.runtime_cfg.futu,
                allow_synthetic_fallback=self.settings.runtime_cfg.allow_synthetic_fallback,
            )
            gw.connect()

        data_adapter = AlpacaMarketDataAdapter(
            alpaca_cfg=alpaca_cfg,
            bar_cache=bar_cache,
            fallback=gw,
        )
        pit_filter = PointInTimeUniverseFilter.from_csv(Path(pit_csv)) if pit_csv else None

        try:
            symbols = (
                [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
                if symbols_csv
                else None
            )
            engine = BacktestEngine(self.settings, data_adapter)
            res = engine.run(
                tier_key=tier or self.settings.active_tier_key,
                symbols=symbols,
                days=days,
                start_date=start_date,
                end_date=end_date,
                pit_filter=pit_filter,
            )
        finally:
            if gw is not None:
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

    def run_config_show(self, consolidate_local: bool = False) -> None:
        local_path = self.settings.root_dir / "config" / "settings.local.json"
        env_path = self.settings.root_dir / ".env"
        if consolidate_local:
            futu_cfg = self.settings.runtime_cfg.futu
            local_payload = {
                "futu": {
                    "trd_env": futu_cfg.trd_env,
                    "api_token": futu_cfg.api_token,
                    "trade_pwd_md5": futu_cfg.trade_pwd_md5,
                }
            }
            local_path.write_text(
                json.dumps(local_payload, indent=2) + "\n", encoding="utf-8"
            )
            local_path.chmod(0o600)
            if env_path.exists():
                env_path.unlink()
            self.settings = AppSettings.load(self.settings.root_dir)

        cfg = self.settings.runtime_cfg.model_dump()
        if cfg["futu"].get("api_token"):
            tok = cfg["futu"]["api_token"]
            cfg["futu"]["api_token"] = f"{tok[:3]}***{tok[-2:]}" if len(tok) > 5 else "***"
        if cfg["futu"].get("trade_pwd_md5"):
            cfg["futu"]["trade_pwd_md5"] = "***"
        if cfg.get("alpaca", {}).get("api_key"):
            ak = cfg["alpaca"]["api_key"]
            cfg["alpaca"]["api_key"] = f"{ak[:4]}***{ak[-2:]}" if len(ak) > 6 else "***"
        if cfg.get("alpaca", {}).get("secret_key"):
            cfg["alpaca"]["secret_key"] = "***"
        print(
            json.dumps(
                {
                    "settings_file": str(self.settings.root_dir / "config" / "settings.json"),
                    "local_override_file": str(self.settings.root_dir / "config" / "settings.local.json"),
                    "active_db_file": str(self.settings.resolve_db_path()),
                    "bar_cache_file": str(self.settings.resolve_bar_cache_path()),
                    "universe_size": len(self.settings.watchlist_cfg.universe),
                    "sim_db_file": str(self.settings.resolve_db_path("SIMULATE")),
                    "real_db_file": str(self.settings.resolve_db_path("REAL")),
                    "active_runtime_config": cfg,
                },
                indent=2,
            )
        )

    def run_test_alpaca(self, full_universe: bool = False) -> None:
        bar_cache = HistoricalBarCache(self.settings.resolve_bar_cache_path())
        adapter = AlpacaMarketDataAdapter(
            alpaca_cfg=self.settings.runtime_cfg.alpaca,
            bar_cache=bar_cache,
        )
        sample = (
            [u.symbol for u in self.settings.watchlist_cfg.universe if u.sector != "Broad_ETF"]
            if full_universe
            else ["SPY", "QQQ", "NVDA", "MSFT", "JPM", "XOM", "LLY", "CAT"]
        )
        res = adapter.test_connection(sample_symbols=sample)
        earn = EarningsCalendarFeed(
            cache_path=self.settings.root_dir / "data" / "cache" / "earnings_calendar.json"
        ).get_upcoming_earnings(symbols=sample)
        res["upcoming_earnings_in_sample"] = {k: v.isoformat() for k, v in earn.items()}
        res["universe_configured_count"] = len(self.settings.watchlist_cfg.universe)
        print(json.dumps(res, indent=2, ensure_ascii=False))

    def run_refresh_universe(self, apply_write: bool = False) -> None:
        bar_cache = HistoricalBarCache(self.settings.resolve_bar_cache_path())
        adapter = AlpacaMarketDataAdapter(
            alpaca_cfg=self.settings.runtime_cfg.alpaca,
            bar_cache=bar_cache,
        )
        if not adapter.has_credentials():
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": "Configure `alpaca.api_key` and `alpaca.secret_key` in `config/settings.local.json` before running `refresh-universe`.",
                    },
                    indent=2,
                )
            )
            return

        current = self.settings.watchlist_cfg.universe
        eq_syms = [u.symbol for u in current if not u.sector.endswith("_ETF")]
        bars_60d = adapter.get_bars_batch(eq_syms, timeframe="1d", days=65)
        metrics = []
        for u in current:
            if u.sector.endswith("_ETF"):
                continue
            blist = bars_60d.get(u.symbol, [])
            if not blist:
                continue
            recent = blist[-60:]
            adv_60 = sum(b.close * b.volume for b in recent) / len(recent)
            metrics.append(
                {
                    "symbol": u.symbol,
                    "sector": u.sector,
                    "cik": u.cik,
                    "etf_core_score": u.etf_core_score,
                    "baseline_inst_pct": u.baseline_inst_pct,
                    "price": recent[-1].close,
                    "adv_60d_usd": adv_60,
                }
            )

        updated_uni, summary = rebalance_universe_with_buffer(current, metrics)
        target_file = self.settings.root_dir / "config" / (
            self.settings.watchlist_cfg.universe_file or "universe_100.yaml"
        )
        if apply_write and summary.total_count > 0:
            save_universe_yaml(target_file, updated_uni)

        print(
            json.dumps(
                {
                    "applied": apply_write,
                    "target_file": str(target_file),
                    "equities_selected": summary.total_count,
                    "added": summary.added,
                    "removed": summary.removed,
                    "sector_distribution": summary.sector_distribution,
                },
                indent=2,
                ensure_ascii=False,
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="zy-quant",
        description="zy-quant: Quantitative US Equities Trading Terminal & CLI",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="Show live market regime, portfolio NAV, watchlist & positions")
    p_status.add_argument("--env", default=None, choices=["SIMULATE", "REAL"], help="Inspect SIMULATE or REAL ledger")
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
    p_hist.add_argument("--env", default=None, choices=["SIMULATE", "REAL"], help="Query SIMULATE or REAL ledger")
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
    p_bt.add_argument("--pit-csv", dest="pit_csv", default=None, help="Point-in-Time constituent CSV (symbol,start_date,end_date)")
    p_bt.add_argument("--json", action="store_true")

    p_alpaca = sub.add_parser("test-alpaca", help="Verify Alpaca Market Data API v2 connection, batch latency & earnings feed")
    p_alpaca.add_argument("--full", action="store_true", help="Benchmark full 110-symbol batch snapshot scan")

    p_uni = sub.add_parser("refresh-universe", help="Run quarterly objective ADV/price universe rebalance with 85/120 buffer")
    p_uni.add_argument("--apply", action="store_true", help="Write rebalanced universe to config/universe_100.yaml")

    p_cfg = sub.add_parser("config", help="Display active merged runtime configuration (secrets masked)")
    p_cfg.add_argument(
        "--consolidate-local",
        action="store_true",
        help="Consolidate .env credentials into gitignored config/settings.local.json (chmod 600)",
    )

    p_svc = sub.add_parser("service", help="Manage macOS launchd background service (start|stop|restart)")
    p_svc.add_argument("action", choices=["start", "stop", "restart"], help="Service lifecycle action")
    p_svc.add_argument("--superkill", action="store_true", help="Completely uninstall launchd schedule, processes & wake timers")

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
        cli.run_status(trd_env=args.env, as_json=args.json)
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
            trd_env=args.env,
        )
    elif args.cmd == "backtest":
        cli.run_backtest(
            tier=args.tier,
            days=args.days,
            start_date=args.start_date,
            end_date=args.end_date,
            symbols_csv=args.symbols,
            as_json=args.json,
            pit_csv=args.pit_csv,
        )
    elif args.cmd == "test-alpaca":
        cli.run_test_alpaca(full_universe=args.full)
    elif args.cmd == "refresh-universe":
        cli.run_refresh_universe(apply_write=args.apply)
    elif args.cmd == "config":
        cli.run_config_show(consolidate_local=args.consolidate_local)
    elif args.cmd == "service":
        start_sh = cli.settings.root_dir / "deploy" / "start.sh"
        stop_sh = cli.settings.root_dir / "deploy" / "stop.sh"
        manual_flag = cli.settings.root_dir / "data" / ".manual_start"
        uid = os.getuid()
        plist = Path.home() / "Library" / "LaunchAgents" / "com.zyquant.daemon.plist"

        if args.action == "stop":
            if stop_sh.exists():
                cmd = ["/bin/bash", str(stop_sh)]
                if args.superkill:
                    cmd.append("--superkill")
                subprocess.run(cmd, check=False)
            else:
                manual_flag.unlink(missing_ok=True)
                subprocess.run(["launchctl", "bootout", f"gui/{uid}/com.zyquant.daemon"], check=False)
                if args.superkill and plist.exists():
                    plist.unlink()
                print("Background service stopped.")
        elif args.action in ("start", "restart"):
            manual_flag.parent.mkdir(parents=True, exist_ok=True)
            manual_flag.touch()
            if start_sh.exists():
                subprocess.run(["/bin/bash", str(start_sh)], check=False)
            else:
                if args.action == "start":
                    subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", str(plist)], check=False)
                subprocess.run(["launchctl", "kickstart", "-k", f"gui/{uid}/com.zyquant.daemon"], check=False)
                print(f"Background service {args.action}ed (manual dashboard flag armed).")
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
