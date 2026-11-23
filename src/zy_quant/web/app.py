from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, AsyncIterator, Deque, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.storage.state_store import StateStore

settings = AppSettings.load()
log_dir = settings.root_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
_file_handler = RotatingFileHandler(
    log_dir / "overnight.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"
)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logging.getLogger().addHandler(_file_handler)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("zy_quant.overnight")

store = StateStore(settings.root_dir / "data" / "zy_quant.db")
data_adapter = YahooMarketDataAdapter()
daemon = TradingDaemon(settings=settings, store=store)

_event_log: Deque[Dict[str, str]] = deque(maxlen=60)
_auto_loop_task: Optional[asyncio.Task[None]] = None
_auto_loop_enabled: bool = False
_active_tier_key: str = os.getenv("ZY_ACCOUNT_TIER", settings.tiers_cfg.active_tier)


def _log_event(kind: str, detail: str) -> None:
    logger.info("[%s] %s", kind, detail)
    _event_log.appendleft(
        {
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            "kind": kind,
            "detail": detail,
        }
    )


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    global _auto_loop_enabled, _auto_loop_task
    if os.getenv("ZY_AUTO_START_DAEMON", "0") == "1" and not _auto_loop_enabled:
        _auto_loop_enabled = True
        _log_event(
            "DAEMON",
            f"Auto-started Overnight Paper Simulation Loop (tier={_active_tier_key})",
        )
        _auto_loop_task = asyncio.create_task(_embedded_daemon_loop())
    yield
    _auto_loop_enabled = False
    if _auto_loop_task is not None:
        _auto_loop_task.cancel()
    daemon.gateway.close()


app = FastAPI(title="zy-quant Control Center", version="0.1.0", lifespan=_lifespan)
STATIC_DIR = Path(__file__).parent / "static"


_log_event("BOOT", "Control Center initialized (default safe mode: SIMULATE)")
if daemon.gateway.api_token:
    if daemon.gateway.connect():
        daemon.startup_reconcile()
        _log_event("FUTU_OK", f"Auto-connected to Futu OpenAPI ({daemon.gateway.trd_env_name})")


class HaltRequest(BaseModel):
    halted: bool
    reason: str = "Dashboard operator action"


class BacktestRequest(BaseModel):
    tier_key: str = "tier_1k"
    days: int = 30


class FeeCalcRequest(BaseModel):
    shares: float
    price: float
    scale_out: bool = False


class FutuConnectRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = 11111
    trd_env: str = "SIMULATE"
    api_token: Optional[str] = None


class DaemonToggleRequest(BaseModel):
    running: bool
    tier_key: str = "tier_1k"


async def _embedded_daemon_loop() -> None:
    global _auto_loop_enabled
    while _auto_loop_enabled:
        try:
            res = await asyncio.to_thread(daemon.run_cycle, _active_tier_key, False)
            _log_event(
                "CYCLE",
                f"Regime={res['regime']} | Entries={len(res['entries'])} | Exits={len(res['exits'])} | Next={res['next_sleep_minutes']}m",
            )
            sleep_s = max(30, int(res["next_sleep_minutes"] * 60))
        except Exception as exc:
            _log_event("ERROR", f"Daemon cycle error: {exc}")
            sleep_s = 60

        for _ in range(sleep_s):
            if not _auto_loop_enabled:
                break
            await asyncio.sleep(1)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/api/overview")
def get_overview(tier_key: Optional[str] = None) -> Dict[str, Any]:
    global _active_tier_key
    if tier_key:
        _active_tier_key = tier_key
    tier = settings.tiers_cfg.get_active(_active_tier_key)
    regime = store.get_flag("latest_regime")
    if regime is None:
        snap = daemon.evaluate_regime()
        regime = store.get_flag("latest_regime")
        if not store.get_watchlist_scores():
            daemon.refresh_watchlist(snap, check_edgar=False)

    watchlist_rows = store.get_watchlist_scores()
    wl_prices = {w["symbol"]: float(w["price"]) for w in watchlist_rows}

    positions = []
    unrealized_total = 0.0
    invested_cost = 0.0
    for p in store.get_positions().values():
        mark_px = float(p.metadata.get("last_mark_price") or wl_prices.get(p.symbol) or p.entry_price)
        unreal = (mark_px - p.entry_price) * p.shares
        unrealized_total += unreal
        invested_cost += p.shares * p.entry_price + float(p.metadata.get("buy_fee_usd", 0.0))
        positions.append(
            {
                "symbol": p.symbol,
                "sector": p.sector,
                "shares": p.shares,
                "entry_price": p.entry_price,
                "mark_price": round(mark_px, 2),
                "unrealized_pnl_usd": round(unreal, 2),
                "current_stop": p.current_stop,
                "highest_price": p.highest_price,
                "bars_held_15m": p.bars_held_15m,
                "breakeven_armed": p.breakeven_armed,
                "scaled_out": p.scaled_out,
                "broker_stop_order_id": p.broker_stop_order_id,
                "setup": p.setup,
            }
        )

    realized_total = store.realized_pnl_since("2000-01-01T00:00:00")
    closed_trades = store.list_closed_trades(limit=50)
    recent_orders = store.list_recent_orders(limit=50)

    sim_cash = max(0.0, tier.reference_equity_usd + realized_total - invested_cost)
    sim_equity = sim_cash + sum(pos["shares"] * pos["mark_price"] for pos in positions)

    # Compute the exact same 5 Live Runtime KPIs as Backtest Lab
    closed_gross = sum(float(t.get("gross_pnl_usd", 0.0)) for t in closed_trades)
    total_gross_pnl = closed_gross + unrealized_total
    filled_fees = sum(
        float(o.get("fee_usd", 0.0))
        for o in recent_orders
        if o.get("status") == "FILLED"
    )
    net_pnl = sim_equity - tier.reference_equity_usd
    net_ret_pct = (net_pnl / max(1.0, tier.reference_equity_usd)) * 100.0
    wins = sum(1 for t in closed_trades if float(t.get("net_pnl_usd", 0.0)) > 0)
    win_rate = (wins / len(closed_trades) * 100.0) if closed_trades else 0.0

    # Track live runtime equity curve & max drawdown across polls/cycles
    hist_flag = store.get_flag(f"live_eq_{_active_tier_key}") or {"points": [], "peak": tier.reference_equity_usd, "max_dd_pct": 0.0}
    pts = list(hist_flag.get("points", []))
    now_hm = datetime.now(timezone.utc).strftime("%H:%M")
    if not pts or pts[-1].get("t") != now_hm or abs(pts[-1].get("equity_usd", 0.0) - round(sim_equity, 2)) >= 0.05:
        pts.append({"t": now_hm, "equity_usd": round(sim_equity, 2)})
        pts = pts[-180:]
    peak_eq = max(float(hist_flag.get("peak", tier.reference_equity_usd)), sim_equity)
    dd_now = ((peak_eq - sim_equity) / peak_eq * 100.0) if peak_eq > 0 else 0.0
    max_dd = max(float(hist_flag.get("max_dd_pct", 0.0)), dd_now)
    store.set_flag(
        f"live_eq_{_active_tier_key}",
        {"points": pts, "peak": round(peak_eq, 2), "max_dd_pct": round(max_dd, 3)},
    )

    broker_status = daemon.gateway.get_broker_status()

    return {
        "server_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "auto_loop_running": _auto_loop_enabled,
        "active_tier_key": _active_tier_key,
        "active_tier": tier.model_dump(),
        "available_tiers": {k: v.model_dump() for k, v in settings.tiers_cfg.tiers.items()},
        "sim_portfolio": {
            "starting_equity_usd": round(tier.reference_equity_usd, 2),
            "sim_equity_usd": round(sim_equity, 2),
            "sim_cash_usd": round(sim_cash, 2),
            "unrealized_pnl_usd": round(unrealized_total, 2),
            "realized_pnl_usd": round(realized_total, 2),
            "net_return_pct": round(net_ret_pct, 2),
            "gross_pnl_usd": round(total_gross_pnl, 2),
            "total_fees_usd": round(filled_fees, 2),
            "total_closed_trades": len(closed_trades),
            "win_rate_pct": round(win_rate, 1),
            "max_drawdown_pct": round(max_dd, 2),
            "equity_curve": pts,
        },
        "broker": broker_status,
        "halt_state": store.get_halt_state(),
        "heartbeat": store.get_flag("daemon_heartbeat") or {"alive": _auto_loop_enabled, "futu_connected": broker_status["connected"]},
        "regime": regime,
        "watchlist": watchlist_rows,
        "positions": positions,
        "recent_orders": recent_orders[:20],
        "closed_trades": closed_trades[:25],
        "events": list(_event_log),
        "scoring_weights": settings.watchlist_cfg.weights.model_dump(),
        "hard_filters": settings.watchlist_cfg.hard_filters.model_dump(),
    }


@app.post("/api/futu/connect")
def connect_futu(req: FutuConnectRequest) -> Dict[str, Any]:
    token_arg = req.api_token if (req.api_token is not None and req.api_token != "") else None
    ok = daemon.gateway.reconfigure(
        host=req.host, port=req.port, trd_env=req.trd_env, api_token=token_arg
    )
    rec: Dict[str, Any] = {}
    if ok:
        # Persist non-secret host/port/env + local token in gitignored .env (chmod 600) so restarts auto-connect
        env_path = settings.root_dir / ".env"
        lines = [
            f"ZY_FUTU_HOST={daemon.gateway.host}",
            f"ZY_FUTU_PORT={daemon.gateway.port}",
            f"ZY_TRD_ENV={daemon.gateway.trd_env_name}",
            f"ZY_FUTU_API_TOKEN={daemon.gateway.api_token}",
        ]
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        env_path.chmod(0o600)

        rec = daemon.startup_reconcile()
        _log_event(
            "FUTU_OK",
            f"Connected to Futu OpenAPI at {req.host}:{req.port} ({req.trd_env})",
        )
    else:
        _log_event("FUTU_AUTH", daemon.gateway.last_error)
    return {
        "ok": ok,
        "broker": daemon.gateway.get_broker_status(),
        "reconciliation": rec,
    }


@app.post("/api/daemon/toggle")
async def toggle_daemon(req: DaemonToggleRequest) -> Dict[str, Any]:
    global _auto_loop_enabled, _auto_loop_task, _active_tier_key
    _active_tier_key = req.tier_key
    if req.running and not _auto_loop_enabled:
        _auto_loop_enabled = True
        _log_event("DAEMON", f"Started embedded Two-Speed Auto-Daemon ({req.tier_key})")
        _auto_loop_task = asyncio.create_task(_embedded_daemon_loop())
    elif not req.running and _auto_loop_enabled:
        _auto_loop_enabled = False
        if _auto_loop_task is not None:
            _auto_loop_task.cancel()
            _auto_loop_task = None
        _log_event("DAEMON", "Paused embedded Two-Speed Auto-Daemon")
    return {"auto_loop_running": _auto_loop_enabled}


@app.post("/api/scan")
def trigger_scan(tier_key: str = "tier_1k", check_edgar: bool = False) -> Dict[str, Any]:
    # Force immediate watchlist & regime refresh when operator clicks Scan manually
    daemon._last_watchlist_scan_at = None
    cycle = daemon.run_cycle(tier_override=tier_key, check_edgar=check_edgar)
    _log_event(
        "SCAN",
        f"Manual scan finished: Regime={cycle['regime']}, Watchlist refreshed={cycle['watchlist_refreshed']}, Entries={len(cycle['entries'])}",
    )
    return {
        "cycle": cycle,
        "watchlist": store.get_watchlist_scores(),
        "regime": store.get_flag("latest_regime"),
    }


@app.post("/api/backtest")
def run_backtest(req: BacktestRequest) -> Dict[str, Any]:
    # Use live Futu OpenAPI bars when connected; otherwise fallback adapter
    engine = BacktestEngine(settings, daemon.gateway)
    result = engine.run(tier_key=req.tier_key, days=max(10, min(58, req.days)))
    _log_event(
        "BACKTEST",
        f"Ran {req.days}d backtest on {req.tier_key}: Net={result.net_return_pct:+.2f}%, Trades={result.total_closed_trades}, Fees=${result.total_fees_usd:.2f}",
    )
    return asdict(result)


@app.post("/api/fee-calc")
def calc_fees(req: FeeCalcRequest) -> Dict[str, float]:
    fm = FutuUSFeeModel()
    return fm.estimate_round_trip(req.shares, req.price, scale_out=req.scale_out)


@app.post("/api/halt")
def toggle_halt(req: HaltRequest) -> Dict[str, Any]:
    store.set_halted(req.halted, req.reason if req.halted else "")
    _log_event("KILL_SWITCH", f"Halted={req.halted} ({req.reason})")
    return store.get_halt_state()
