from __future__ import annotations

import argparse
import json
import logging
import uvicorn

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.storage.state_store import StateStore


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
        import os

        if args.auto_daemon:
            os.environ["ZY_AUTO_START_DAEMON"] = "1"
        if args.tier:
            os.environ["ZY_ACCOUNT_TIER"] = args.tier
        uvicorn.run("zy_quant.web.app:app", host=args.host, port=args.port, reload=False)
    elif args.cmd == "daemon":
        daemon = TradingDaemon(settings)
        if args.once:
            daemon.startup_reconcile()
            out = daemon.run_cycle(tier_override=args.tier, check_edgar=args.edgar)
            print(json.dumps(out, indent=2, ensure_ascii=False))
            daemon.gateway.close()
        else:
            daemon.run_forever(tier_override=args.tier)
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
