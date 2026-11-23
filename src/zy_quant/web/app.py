from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Optional

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
store = StateStore(settings.root_dir / "data" / "zy_quant.db")
data_adapter = YahooMarketDataAdapter()
daemon = TradingDaemon(settings=settings, store=store)

app = FastAPI(title="zy-quant Control Center", version="0.1.0")
STATIC_DIR = Path(__file__).parent / "static"

_event_log: Deque[Dict[str, str]] = deque(maxlen=40)
_auto_loop_task: Optional[asyncio.Task[None]] = None
_auto_loop_enabled: bool = False
_active_tier_key: str = settings.tiers_cfg.active_tier


def _log_event(kind: str, detail: str) -> None:
    _event_log.appendleft(
        {
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            "kind": kind,
            "detail": detail,
        }
    )


_log_event("BOOT", "Control Center initialized (default safe mode: SIMULATE)")


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
def get_overview(tier_key: str = "tier_1k") -> Dict[str, Any]:
    global _active_tier_key
    _active_tier_key = tier_key
    tier = settings.tiers_cfg.get_active(tier_key)
    regime = store.get_flag("latest_regime")
    if regime is None:
        snap = daemon.evaluate_regime()
        regime = store.get_flag("latest_regime")
        if not store.get_watchlist_scores():
            daemon.refresh_watchlist(snap, check_edgar=False)

    positions = [
        {
            "symbol": p.symbol,
            "sector": p.sector,
            "shares": p.shares,
            "entry_price": p.entry_price,
            "current_stop": p.current_stop,
            "highest_price": p.highest_price,
            "bars_held_15m": p.bars_held_15m,
            "breakeven_armed": p.breakeven_armed,
            "scaled_out": p.scaled_out,
            "broker_stop_order_id": p.broker_stop_order_id,
            "setup": p.setup,
        }
        for p in store.get_positions().values()
    ]

    broker_status = daemon.gateway.get_broker_status()

    return {
        "server_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "auto_loop_running": _auto_loop_enabled,
        "active_tier_key": tier_key,
        "active_tier": tier.model_dump(),
        "available_tiers": {k: v.model_dump() for k, v in settings.tiers_cfg.tiers.items()},
        "broker": broker_status,
        "halt_state": store.get_halt_state(),
        "heartbeat": store.get_flag("daemon_heartbeat") or {"alive": _auto_loop_enabled, "futu_connected": broker_status["connected"]},
        "regime": regime,
        "watchlist": store.get_watchlist_scores(),
        "positions": positions,
        "recent_orders": store.list_recent_orders(limit=20),
        "closed_trades": store.list_closed_trades(limit=25),
        "events": list(_event_log),
        "scoring_weights": settings.watchlist_cfg.weights.model_dump(),
        "hard_filters": settings.watchlist_cfg.hard_filters.model_dump(),
    }


@app.post("/api/futu/connect")
def connect_futu(req: FutuConnectRequest) -> Dict[str, Any]:
    ok = daemon.gateway.reconfigure(host=req.host, port=req.port, trd_env=req.trd_env)
    rec: Dict[str, Any] = {}
    if ok:
        rec = daemon.startup_reconcile()
        _log_event(
            "FUTU_OK",
            f"Connected to FutuOpenD at {req.host}:{req.port} ({req.trd_env})",
        )
    else:
        _log_event("FUTU_OFFLINE", daemon.gateway.last_error)
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
    engine = BacktestEngine(settings, data_adapter)
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
