from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

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


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/overview")
def get_overview(tier_key: str = "tier_1k") -> Dict[str, Any]:
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

    return {
        "active_tier_key": tier_key,
        "active_tier": tier.model_dump(),
        "available_tiers": {k: v.model_dump() for k, v in settings.tiers_cfg.tiers.items()},
        "halt_state": store.get_halt_state(),
        "heartbeat": store.get_flag("daemon_heartbeat") or {"alive": False, "futu_connected": False},
        "regime": regime,
        "watchlist": store.get_watchlist_scores(),
        "positions": positions,
        "recent_orders": store.list_recent_orders(limit=20),
        "closed_trades": store.list_closed_trades(limit=25),
        "scoring_weights": settings.watchlist_cfg.weights.model_dump(),
        "hard_filters": settings.watchlist_cfg.hard_filters.model_dump(),
    }


@app.post("/api/scan")
def trigger_scan(tier_key: str = "tier_1k", check_edgar: bool = False) -> Dict[str, Any]:
    cycle = daemon.run_cycle(tier_override=tier_key, check_edgar=check_edgar)
    return {"cycle": cycle, "watchlist": store.get_watchlist_scores(), "regime": store.get_flag("latest_regime")}


@app.post("/api/backtest")
def run_backtest(req: BacktestRequest) -> Dict[str, Any]:
    engine = BacktestEngine(settings, data_adapter)
    result = engine.run(tier_key=req.tier_key, days=max(10, min(58, req.days)))
    return asdict(result)


@app.post("/api/fee-calc")
def calc_fees(req: FeeCalcRequest) -> Dict[str, float]:
    fm = FutuUSFeeModel()
    return fm.estimate_round_trip(req.shares, req.price, scale_out=req.scale_out)


@app.post("/api/halt")
def toggle_halt(req: HaltRequest) -> Dict[str, Any]:
    store.set_halted(req.halted, req.reason if req.halted else "")
    return store.get_halt_state()
