from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from zy_quant.backtest.engine import BacktestEngine
from zy_quant.config import AppSettings
from zy_quant.intel.market_hours import get_us_market_session
from zy_quant.runtime.daemon import TradingDaemon
from zy_quant.services.analytics import HistoryQueryService, PortfolioAnalyticsService
from zy_quant.services.log_stream import RingBufferLogHandler
from zy_quant.storage.state_store import StateStore


class ControlCenterApplication:
    """
    Encapsulates backend application state, background scheduler lifecycle,
    and domain services behind clean OOP boundaries.
    """

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings = settings or AppSettings.load()
        self.log_buffer = RingBufferLogHandler(capacity=400)
        self._configure_logging()

        self.store = StateStore(self.settings.resolve_db_path())
        self.daemon = TradingDaemon(settings=self.settings, store=self.store)
        self.portfolio_svc = PortfolioAnalyticsService(self.settings, self.store)
        self.history_svc = HistoryQueryService(self.store)

        self.auto_loop_enabled: bool = False
        self._auto_loop_task: Optional[asyncio.Task[None]] = None
        self.logger = logging.getLogger("zy_quant.server")

        self.logger.info(
            "zy-quant initialized (profile=%s, env=%s)",
            self.settings.active_tier_key,
            self.daemon.gateway.trd_env_name,
        )
        if self.daemon.gateway.api_token or self.daemon.gateway._tcp_reachable(timeout=0.4):
            if self.daemon.gateway.connect():
                self.daemon.startup_reconcile()

    def _configure_logging(self) -> None:
        log_dir = self.settings.root_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        if not any(isinstance(h, RingBufferLogHandler) for h in root_logger.handlers):
            root_logger.addHandler(self.log_buffer)
        if not any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
            fh = RotatingFileHandler(
                log_dir / "overnight.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"
            )
            fh.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            )
            root_logger.addHandler(fh)

    async def start_auto_loop(self) -> None:
        if not self.auto_loop_enabled:
            self.auto_loop_enabled = True
            self.logger.info("Background strategy scheduler active")
            self._auto_loop_task = asyncio.create_task(self._scheduler_loop())

    async def stop_auto_loop(self) -> None:
        self.auto_loop_enabled = False
        if self._auto_loop_task is not None:
            self._auto_loop_task.cancel()
            self._auto_loop_task = None
        self.logger.info("Background strategy scheduler paused")

    async def _scheduler_loop(self) -> None:
        was_closed_logged = False
        while self.auto_loop_enabled:
            try:
                res = await asyncio.to_thread(
                    self.daemon.run_cycle, self.settings.active_tier_key, False, False
                )
                if not res.get("market_open", True):
                    if not was_closed_logged:
                        self.logger.info("%s", res.get("session_message", "Market closed — scan paused"))
                        was_closed_logged = True
                else:
                    was_closed_logged = False
                    self.logger.info(
                        "Scan cycle completed: regime=%s, entries=%d, exits=%d, next=%dm",
                        res["regime"],
                        len(res["entries"]),
                        len(res["exits"]),
                        res["next_sleep_minutes"],
                    )
                sleep_s = max(30, int(res["next_sleep_minutes"] * 60))
            except Exception as exc:
                self.logger.error("Scheduler cycle error: %s", exc)
                sleep_s = 60

            for _ in range(sleep_s):
                if not self.auto_loop_enabled:
                    break
                await asyncio.sleep(1)

    def refresh_config_if_needed(self) -> None:
        """Hot-reloads config/settings.json & config/*.yaml, including live SIMULATE <-> REAL DB & gateway switches."""
        fresh = AppSettings.load(self.settings.root_dir)
        old_futu = self.settings.runtime_cfg.futu
        new_futu = fresh.runtime_cfg.futu
        self.settings = fresh
        self.daemon.settings = fresh
        self.portfolio_svc.settings = fresh

        new_db_path = fresh.resolve_db_path()
        if new_db_path != self.store.db_path:
            new_store = StateStore(new_db_path)
            self.store = new_store
            self.daemon.store = new_store
            self.daemon.risk_gate.store = new_store
            self.portfolio_svc.store = new_store
            self.history_svc.store = new_store
            self.logger.info("Switched active ledger database to %s", new_db_path.name)

        if (
            new_futu.trd_env.upper() != self.daemon.gateway.trd_env_name
            or new_futu.host != old_futu.host
            or new_futu.port != old_futu.port
            or new_futu.trade_pwd_md5 != old_futu.trade_pwd_md5
        ):
            self.daemon.gateway.trade_pwd_md5 = new_futu.trade_pwd_md5
            ok = self.daemon.gateway.reconfigure(
                host=new_futu.host,
                port=new_futu.port,
                trd_env=new_futu.trd_env,
                api_token=new_futu.api_token or self.daemon.gateway.api_token,
            )
            if ok:
                self.daemon.startup_reconcile()
            self.logger.info(
                "Hot-reloaded Futu gateway configuration (env=%s, connected=%s)",
                new_futu.trd_env.upper(),
                ok,
            )

    def get_sanitized_connection_status(self) -> Dict[str, Any]:
        """Returns broker connection health & data integrity alerts without exposing secrets."""
        raw = self.daemon.gateway.get_broker_status()
        connected = bool(raw.get("connected"))
        env_mode = str(raw.get("trd_env", "SIMULATE"))
        is_simulated = env_mode != "REAL"
        acct = raw.get("account_summary") or {}
        fb = self.daemon.gateway.fallback
        fallback_alert = ""
        if not connected:
            if fb.used_synthetic:
                fallback_alert = "SYNTHETIC FAKE DATA ACTIVE — Results are artificial! Connect Futu OpenAPI immediately."
            elif fb.used_yahoo_fallback:
                fallback_alert = "LOCAL FALLBACK ACTIVE — Futu OpenAPI is disconnected; reading delayed Yahoo Finance data."
            else:
                fallback_alert = "Futu OpenAPI is disconnected — real-time market data unavailable."
        return {
            "connected": connected,
            "is_simulated": is_simulated,
            "execution_mode": "Paper Simulation" if is_simulated else "Live Execution",
            "data_feed": "Futu Real-Time Feed" if connected else ("Synthetic Fake Data" if fb.used_synthetic else "Fallback / Offline"),
            "is_fallback_or_fake": not connected or fb.used_synthetic,
            "fallback_alert": fallback_alert,
            "linked_broker_equity_usd": 0.0 if is_simulated else round(float(acct.get("equity_usd", 0.0)), 2),
        }

    def get_strategy_summary(self) -> Dict[str, Any]:
        e = self.settings.strategy_cfg.entry
        x = self.settings.strategy_cfg.exit_models
        t = self.settings.active_tier_profile
        return {
            "profile": t.label,
            "timeframe": e.bar_timeframe,
            "entry_setups": ["VWAP Institutional Pullback", f"{e.opening_range_breakout.orb_minutes}m Opening Range Breakout"],
            "initial_stop_rule": f"{x.atr_stop_multiplier}x Wilder ATR({e.atr_period}) (Max {x.max_hard_stop_pct*100:.1f}%)",
            "profit_protection": f"Breakeven @ +{x.breakeven_trigger_r}R -> Chandelier {x.chandelier_atr_multiplier}x ATR Trail",
            "time_barrier": f"{x.time_stop_bars_15m} bars ({x.time_stop_bars_15m * 0.25:.1f}h) if < +{x.time_stop_min_progress_r}R",
            "min_expectancy": f"+{x.min_expectancy_r}R net of Futu fees",
        }


ctx = ControlCenterApplication()
STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    auto_env = os.getenv("ZY_AUTO_START_DAEMON")
    should_auto = (
        auto_env == "1"
        if auto_env is not None
        else ctx.settings.runtime_cfg.web.auto_start_daemon
    )
    if should_auto:
        await ctx.start_auto_loop()
    yield
    await ctx.stop_auto_loop()
    ctx.daemon.gateway.close()


app = FastAPI(title="zy-quant", version="0.2.0", lifespan=_lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class HaltRequest(BaseModel):
    halted: bool
    reason: str = "Operator kill-switch action"


class BacktestRequest(BaseModel):
    days: int = 30
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    symbols: Optional[List[str]] = None


class DaemonToggleRequest(BaseModel):
    running: bool


@app.get("/")
@app.get("/history")
@app.get("/backtest")
def serve_spa() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/api/dashboard")
@app.get("/api/overview")
def get_dashboard() -> Dict[str, Any]:
    ctx.refresh_config_if_needed()
    regime = ctx.store.get_flag("latest_regime")
    if regime is None:
        snap = ctx.daemon.evaluate_regime()
        regime = ctx.store.get_flag("latest_regime")
        if not ctx.store.get_watchlist_scores():
            ctx.daemon.refresh_watchlist(snap, check_edgar=False)

    raw_broker = ctx.daemon.gateway.get_broker_status()
    live_snap = ctx.portfolio_svc.build_live_portfolio_snapshot(
        broker_summary=raw_broker.get("account_summary")
    )
    session_status = asdict(get_us_market_session())
    conn_status = ctx.get_sanitized_connection_status()

    return {
        "server_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "market_session": session_status,
        "connection": conn_status,
        "daemon_running": ctx.auto_loop_enabled,
        "halt_state": ctx.store.get_halt_state(),
        "regime": regime,
        "portfolio": live_snap["portfolio"],
        "sim_portfolio": live_snap["portfolio"],  # Compatibility alias
        "watchlist": live_snap["watchlist"],
        "positions": live_snap["positions"],
        "strategy_summary": ctx.get_strategy_summary(),
    }


@app.get("/api/history")
def get_trade_history(
    preset: str = Query(default="ALL"),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    exit_reason: Optional[str] = Query(default=None),
    limit: int = Query(default=300, ge=1, le=1000),
) -> Dict[str, Any]:
    return ctx.history_svc.query_history(
        preset=preset,
        start_date=start_date,
        end_date=end_date,
        symbol=symbol,
        exit_reason=exit_reason,
        limit=limit,
    )


@app.post("/api/backtest")
def run_strategy_backtest(req: BacktestRequest) -> Dict[str, Any]:
    ctx.refresh_config_if_needed()
    engine = BacktestEngine(ctx.settings, ctx.daemon.gateway)
    result = engine.run(
        tier_key=ctx.settings.active_tier_key,
        symbols=req.symbols,
        days=max(7, min(58, req.days)),
        start_date=req.start_date,
        end_date=req.end_date,
    )
    ctx.logger.info(
        "Backtest completed (%s -> %s): Net=%+.2f%%, Trades=%d, Fees=$%.2f",
        result.period_start or f"{req.days}d",
        result.period_end or "now",
        result.net_return_pct,
        result.total_closed_trades,
        result.total_fees_usd,
    )
    out = asdict(result)
    out["strategy_summary"] = ctx.get_strategy_summary()
    return out


@app.get("/api/logs")
def get_backend_logs(
    since_id: int = Query(default=0, ge=0),
    level: Optional[str] = Query(default=None),
    limit: int = Query(default=150, ge=1, le=400),
) -> Dict[str, Any]:
    entries = ctx.log_buffer.query(since_id=since_id, level=level, limit=limit)
    return {
        "logs": entries,
        "latest_id": entries[-1]["id"] if entries else since_id,
    }


@app.post("/api/scan")
def trigger_manual_scan() -> Dict[str, Any]:
    ctx.daemon._last_watchlist_scan_at = None
    cycle = ctx.daemon.run_cycle(
        tier_override=ctx.settings.active_tier_key,
        check_edgar=False,
        force_when_closed=True,
    )
    ctx.logger.info(
        "Manual market scan completed: regime=%s, watchlist_refreshed=%s",
        cycle["regime"],
        cycle["watchlist_refreshed"],
    )
    return {"cycle": cycle}


@app.post("/api/daemon/toggle")
async def toggle_daemon(req: DaemonToggleRequest) -> Dict[str, Any]:
    if req.running:
        await ctx.start_auto_loop()
    else:
        await ctx.stop_auto_loop()
    return {"daemon_running": ctx.auto_loop_enabled}


@app.post("/api/halt")
def toggle_kill_switch(req: HaltRequest) -> Dict[str, Any]:
    ctx.store.set_halted(req.halted, req.reason if req.halted else "")
    ctx.logger.warning("Kill-Switch toggled: halted=%s (%s)", req.halted, req.reason)
    return ctx.store.get_halt_state()
