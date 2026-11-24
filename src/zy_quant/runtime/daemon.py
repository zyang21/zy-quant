from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.futu_adapter import FutuGatewayAdapter
from zy_quant.intel.edgar_form4 import SecEdgarForm4Scanner
from zy_quant.intel.market_hours import get_us_market_session
from zy_quant.intel.regime import MarketRegimeDetector
from zy_quant.intel.watchlist_scorer import ConservativeWatchlistScorer
from zy_quant.models import (
    CapitalFlowSnapshot,
    InsiderActivity,
    OpenPosition,
    OrderSide,
    OrderStatus,
    QuoteSnapshot,
    RegimeLight,
    RegimeSnapshot,
)
from zy_quant.risk.mandate_gate import MandateRiskGate
from zy_quant.runtime.alerts import AlertNotifier
from zy_quant.storage.state_store import StateStore
from zy_quant.strategy.exit_models import ScientificExitAndSizingEngine
from zy_quant.strategy.indicators import compute_wilder_atr_series
from zy_quant.strategy.signals import IntradaySignalEngine

logger = logging.getLogger(__name__)


class TradingDaemon:
    """
    Resilient Two-Speed Background Trading Daemon:
    1. Startup Crash-Recovery Reconciliation with FutuOpenD holdings
    2. Step 1 Market Regime Traffic Light (QQQ/SPY/VIX):
       - GREEN: 15m active trading & watchlist cycle
       - YELLOW / RED: Locks all new buys (0 trades, $0 churn fees); if flat, runs a low-frequency
         2-hour (120m) Relative Strength (RS) & Institutional Flow watchlist scan so leaders are
         ready the moment the market turns GREEN.
    3. Idempotent SQLite WAL Order State Machine + Server-Side Protective Stop Orders.
    """

    def __init__(
        self,
        settings: AppSettings,
        store: Optional[StateStore] = None,
        gateway: Optional[FutuGatewayAdapter] = None,
        notifier: Optional[AlertNotifier] = None,
    ) -> None:
        self.settings = settings
        self.store = store or StateStore(settings.root_dir / "data" / "zy_quant.db")
        self.gateway = gateway or FutuGatewayAdapter(futu_cfg=settings.runtime_cfg.futu)
        self.notifier = notifier or AlertNotifier()
        self.fee_model = FutuUSFeeModel()

        self.regime_detector = MarketRegimeDetector(settings.strategy_cfg.regime)
        self.watchlist_scorer = ConservativeWatchlistScorer(settings.watchlist_cfg)
        self.edgar_scanner = SecEdgarForm4Scanner()
        self.signal_engine = IntradaySignalEngine(settings.strategy_cfg.entry)
        self.exit_engine = ScientificExitAndSizingEngine(
            settings.strategy_cfg.exit_models, self.fee_model
        )
        self.risk_gate = MandateRiskGate(self.store, settings.strategy_cfg.daemon)

        self._stop_requested = False
        self._last_watchlist_scan_at: Optional[datetime] = None

    def startup_reconcile(self) -> Dict[str, Any]:
        connected = self.gateway.is_connected() or self.gateway.connect()
        summary = {"futu_connected": connected, "reconciliation": {}}
        # Only overwrite SQLite positions from broker when the target trading environment
        # (e.g. REAL, or an activated Futu SIMULATE sub-account) actually exists on the broker.
        # In local paper-simulation mode (`PAPER-LOCAL`), SQLite is the source of truth for virtual holdings.
        if connected and self.gateway._select_acc_id(self.gateway.trd_env_name) != 0:
            broker_pos = self.gateway.get_open_positions()
            rec = self.store.reconcile_broker_positions(broker_pos)
            summary["reconciliation"] = rec
            if any(rec.values()):
                self.notifier.send(
                    "Startup Position Reconciliation",
                    f"Adopted={rec['adopted']}, Updated={rec['updated']}, ClosedRemotely={rec['closed_remotely']}",
                )
        self.store.set_flag(
            "daemon_heartbeat",
            {
                "alive": True,
                "futu_connected": connected,
                "trd_env": self.gateway.trd_env_name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        return summary

    def evaluate_regime(self) -> RegimeSnapshot:
        reg_cfg = self.settings.strategy_cfg.regime
        qqq_15m = self.gateway.get_bars(reg_cfg.primary_benchmark, timeframe="15m", days=5)
        qqq_1d = self.gateway.get_bars(reg_cfg.primary_benchmark, timeframe="1d", days=40)
        spy_1d = self.gateway.get_bars(reg_cfg.secondary_benchmark, timeframe="1d", days=40)
        vix = self.gateway.get_vix_level()
        snap = self.regime_detector.evaluate(qqq_15m, qqq_1d, spy_1d, vix_level=vix)
        self.store.set_flag(
            "latest_regime",
            {
                "light": snap.light.value,
                "qqq_price": round(snap.qqq_price, 2),
                "qqq_vwap": round(snap.qqq_vwap, 2),
                "qqq_sma20": round(snap.qqq_sma20, 2),
                "qqq_intraday_return_pct": round(snap.qqq_intraday_return * 100.0, 2),
                "spy_price": round(snap.spy_price, 2),
                "vix_level": round(snap.vix_level, 2),
                "recommended_scan_minutes": snap.recommended_scan_minutes,
                "reason": snap.reason,
                "evaluated_at": snap.evaluated_at.isoformat(),
            },
        )
        return snap

    def refresh_watchlist(
        self, regime: RegimeSnapshot, check_edgar: bool = False
    ) -> List[Dict[str, Any]]:
        quotes: Dict[str, QuoteSnapshot] = {}
        intraday_rets: Dict[str, float] = {}
        flows: Dict[str, CapitalFlowSnapshot] = {}
        insiders: Dict[str, InsiderActivity] = {}

        for sec in self.settings.watchlist_cfg.universe:
            if sec.sector == "Broad_ETF":
                continue
            q = self.gateway.get_quote(sec.symbol)
            if q is not None:
                quotes[sec.symbol] = q
            d_bars = self.gateway.get_bars(sec.symbol, timeframe="1d", days=5)
            if len(d_bars) >= 2 and d_bars[-2].close > 0:
                intraday_rets[sec.symbol] = (d_bars[-1].close - d_bars[-2].close) / d_bars[-2].close
            flow = self.gateway.get_capital_flow(sec.symbol)
            if flow is not None:
                flows[sec.symbol] = flow
            if check_edgar:
                insiders[sec.symbol] = self.edgar_scanner.fetch_insider_activity(sec.symbol, sec.cik)

        candidates = self.watchlist_scorer.rank_universe(
            quotes=quotes,
            intraday_returns=intraday_rets,
            qqq_intraday_return=regime.qqq_intraday_return,
            regime_light=regime.light,
            capital_flows=flows,
            insiders=insiders,
        )
        self.store.save_watchlist_scores(candidates)
        self._last_watchlist_scan_at = datetime.now(timezone.utc)
        return self.store.get_watchlist_scores()

    def run_cycle(
        self,
        tier_override: Optional[str] = None,
        check_edgar: bool = False,
        force_when_closed: bool = False,
    ) -> Dict[str, Any]:
        """Executes a single deterministic daemon tick safely."""
        now = datetime.now(timezone.utc)
        session = get_us_market_session(now)

        # Automatic background loop pauses all scans while US Regular Trading Hours are closed
        if not session.is_open and not force_when_closed:
            sleep_m = max(1, min(15, session.minutes_until_open))
            self.store.set_flag(
                "daemon_heartbeat",
                {
                    "alive": True,
                    "futu_connected": self.gateway.is_connected(),
                    "trd_env": self.gateway.trd_env_name,
                    "market_open": False,
                    "market_phase": session.phase,
                    "session_message": session.message,
                    "open_positions": len(self.store.get_positions()),
                    "watchlist_refreshed": False,
                    "timestamp": now.isoformat(),
                },
            )
            return {
                "market_open": False,
                "market_phase": session.phase,
                "session_message": session.message,
                "regime": "MARKET_CLOSED",
                "reason": session.message,
                "watchlist_refreshed": False,
                "entries": [],
                "exits": [],
                "next_sleep_minutes": sleep_m,
            }

        tier = self.settings.tiers_cfg.get_active(tier_override)

        # Watchdog check: reconnect FutuOpenD if dropped
        if not self.gateway.is_connected():
            self.gateway.connect()

        regime = self.evaluate_regime()
        open_positions = self.store.get_positions()

        # Decide whether watchlist refresh is due:
        # - GREEN: 15m | YELLOW: 45m | RED: 120m (2 hours)
        watchlist_interval = timedelta(minutes=regime.recommended_scan_minutes)
        watchlist_refreshed = False
        if (
            self._last_watchlist_scan_at is None
            or (now - self._last_watchlist_scan_at) >= watchlist_interval
        ):
            self.refresh_watchlist(regime, check_edgar=check_edgar)
            watchlist_refreshed = True

        # 1. Always step & protect existing open positions (even during YELLOW/RED regimes)
        exits_triggered: List[str] = []
        for sym, pos in list(open_positions.items()):
            bars_15m = self.gateway.get_bars(sym, timeframe="15m", days=5)
            if not bars_15m:
                continue
            atrs = compute_wilder_atr_series(
                bars_15m, self.settings.strategy_cfg.entry.atr_period
            )
            latest_bar = bars_15m[-1]
            bar_ts_iso = latest_bar.timestamp.isoformat()
            is_new_bar = pos.metadata.get("last_bar_ts") != bar_ts_iso
            pos.metadata["last_bar_ts"] = bar_ts_iso
            pos.metadata["last_mark_price"] = latest_bar.close

            prev_stop = pos.current_stop
            action = self.exit_engine.step_position(
                pos, latest_bar, atrs[-1], tier, is_new_bar=is_new_bar
            )

            if action.should_exit and action.shares_to_sell > 0:
                cid = f"EXIT-{sym}-{latest_bar.timestamp.strftime('%Y%m%d%H%M')}-{action.reason.value if action.reason else 'X'}"
                if self.store.record_order_intent(
                    cid, sym, OrderSide.SELL, action.shares_to_sell, action.exit_price, action.note
                ):
                    if pos.broker_stop_order_id and action.shares_to_sell >= pos.shares:
                        self.gateway.cancel_order(pos.broker_stop_order_id)

                    has_target_acc = self.gateway._select_acc_id(self.gateway.trd_env_name) != 0
                    broker_oid = (
                        self.gateway.place_limit_order(
                            sym, OrderSide.SELL, action.shares_to_sell, action.exit_price, remark=cid
                        )
                        if has_target_acc
                        else None
                    )
                    sell_fee = self.fee_model.estimate_leg(
                        action.shares_to_sell, action.exit_price, OrderSide.SELL
                    ).total_friction_usd
                    self.store.update_order_status(
                        cid,
                        OrderStatus.FILLED if (broker_oid or not has_target_acc) else OrderStatus.SUBMITTED,
                        broker_order_id=broker_oid or "PAPER-LOCAL",
                        fill_price=action.exit_price,
                        fee_usd=sell_fee,
                    )
                    self.store.record_closed_trade(
                        symbol=sym,
                        setup=pos.setup,
                        shares=action.shares_to_sell,
                        entry_price=pos.entry_price,
                        exit_price=action.exit_price,
                        fee_usd=sell_fee,
                        risk_per_share=pos.risk_per_share,
                        exit_reason=action.reason.value if action.reason else "EXIT",
                        entry_time=pos.entry_time.isoformat(),
                    )
                    pos.shares = round(pos.shares - action.shares_to_sell, 6)
                    if pos.shares <= 1e-5:
                        self.store.remove_position(sym)
                        open_positions.pop(sym, None)
                    else:
                        if self.settings.strategy_cfg.daemon.place_server_side_stop:
                            pos.broker_stop_order_id = self.gateway.place_or_update_protective_stop(
                                sym, pos.shares, pos.current_stop, pos.broker_stop_order_id
                            )
                        self.store.upsert_position(pos)
                    exits_triggered.append(f"{sym} ({action.note})")
                    self.notifier.send("Position Exit", f"{sym}: {action.note}", level="WARN")
            else:
                # If Chandelier / Breakeven ratcheted stop upward, sync protective stop to Futu server!
                if (
                    pos.current_stop > prev_stop
                    and self.settings.strategy_cfg.daemon.place_server_side_stop
                ):
                    pos.broker_stop_order_id = self.gateway.place_or_update_protective_stop(
                        sym, pos.shares, pos.current_stop, pos.broker_stop_order_id
                    )
                self.store.upsert_position(pos)

        # 2. New Entry Evaluation — GREEN (100% scale) or YELLOW (Selective Alpha Leaders @ 65% scale)
        entries_placed: List[str] = []
        if session.is_open and regime.light in (RegimeLight.GREEN, RegimeLight.YELLOW):
            has_target_acc = self.gateway._select_acc_id(self.gateway.trd_env_name) != 0
            if has_target_acc and self.gateway.trd_env_name == "REAL":
                acct = self.gateway.get_account_summary()
                equity = acct["equity_usd"] if acct["equity_usd"] > 0 else tier.reference_equity_usd
                cash = acct["cash_usd"]
            else:
                # Paper Simulation Mode: size strictly from virtual Tier capital + realized paper PnL
                realized_all = self.store.realized_pnl_since("2000-01-01T00:00:00")
                equity = tier.reference_equity_usd + realized_all
                invested = sum(
                    p.shares * p.entry_price + float(p.metadata.get("buy_fee_usd", 0.0))
                    for p in open_positions.values()
                )
                cash = max(0.0, equity - invested)

            pos_scale = (
                1.0
                if regime.light == RegimeLight.GREEN
                else self.settings.strategy_cfg.regime.yellow_position_scale
            )
            cooldown_cutoff = (
                now - timedelta(hours=self.settings.strategy_cfg.entry.cooldown_hours_after_exit)
            ).isoformat()
            recent_exits = {
                t["symbol"]
                for t in self.store.query_closed_trades(start_iso=cooldown_cutoff, limit=100)
            }
            qqq_1d = self.gateway.get_bars(
                self.settings.strategy_cfg.regime.primary_benchmark, timeframe="1d", days=40
            )

            top_n = self.settings.watchlist_cfg.scoring_thresholds.top_n_active
            min_score = self.settings.watchlist_cfg.scoring_thresholds.min_watch_score
            watchlist = [
                w
                for w in self.store.get_watchlist_scores()
                if w["passed_hard_filters"] and w["total_score"] >= min_score
            ][:top_n]

            for item in watchlist:
                sym = item["symbol"]
                if sym in open_positions or sym in recent_exits:
                    continue
                bars_15m = self.gateway.get_bars(sym, timeframe="15m", days=5)
                bars_1d = self.gateway.get_bars(sym, timeframe="1d", days=40)
                flow = self.gateway.get_capital_flow(sym)
                sig = self.signal_engine.evaluate_15m(
                    symbol=sym,
                    sector=item["sector"],
                    bars_15m=bars_15m,
                    regime_light=regime.light,
                    watchlist_score=float(item["total_score"]),
                    capital_flow=flow,
                    stock_daily_bars=bars_1d,
                    qqq_daily_bars=qqq_1d,
                )
                if sig is None:
                    continue

                plan, plan_status = self.exit_engine.build_position_plan(
                    signal=sig,
                    account_equity_usd=equity,
                    available_cash_usd=max(0.0, cash),
                    tier=tier,
                    position_scale=pos_scale,
                )
                if plan is None or plan_status != "APPROVED":
                    continue

                gate = self.risk_gate.check_entry(plan, tier, equity, open_positions, now=now)
                if not gate.allowed:
                    logger.info("Risk Gate blocked %s: %s", sym, gate.reason)
                    continue

                cid = f"BUY-{sym}-{sig.timestamp.strftime('%Y%m%d%H%M')}"
                if not self.store.record_order_intent(
                    cid, sym, OrderSide.BUY, plan.shares, plan.limit_price, sig.rationale
                ):
                    continue

                broker_oid = (
                    self.gateway.place_limit_order(
                        sym, OrderSide.BUY, plan.shares, plan.limit_price, remark=cid
                    )
                    if has_target_acc
                    else None
                )
                buy_fee = self.fee_model.estimate_leg(
                    plan.shares, plan.entry_price, OrderSide.BUY
                ).total_friction_usd
                self.store.update_order_status(
                    cid,
                    OrderStatus.FILLED if (broker_oid or not has_target_acc) else OrderStatus.SUBMITTED,
                    broker_order_id=broker_oid or "PAPER-LOCAL",
                    fill_price=plan.entry_price,
                    fee_usd=buy_fee,
                )

                stop_oid = None
                if self.settings.strategy_cfg.daemon.place_server_side_stop:
                    stop_oid = self.gateway.place_or_update_protective_stop(
                        sym, plan.shares, plan.initial_stop
                    )

                new_pos = OpenPosition(
                    symbol=sym,
                    sector=plan.sector,
                    shares=plan.shares,
                    initial_shares=plan.shares,
                    entry_price=plan.entry_price,
                    entry_time=now,
                    initial_stop=plan.initial_stop,
                    current_stop=plan.initial_stop,
                    highest_price=plan.entry_price,
                    risk_per_share=plan.risk_per_share,
                    broker_stop_order_id=stop_oid,
                    setup=plan.setup.value,
                    metadata={
                        "buy_fee_usd": buy_fee,
                        "expectancy_r": plan.expectancy_r,
                        "last_bar_ts": sig.timestamp.isoformat(),
                        "last_mark_price": plan.entry_price,
                    },
                )
                self.store.upsert_position(new_pos)
                open_positions[sym] = new_pos
                cash -= plan.notional_usd + buy_fee
                entries_placed.append(f"{sym} x{plan.shares} @ ${plan.entry_price:.2f}")
                self.notifier.send(
                    "New Entry Executed",
                    f"{sym} ({plan.setup.value}): {plan.shares} sh @ ${plan.entry_price:.2f}, "
                    f"Server Stop=${plan.initial_stop:.2f}, Expectancy=+{plan.expectancy_r:.2f}R",
                )

        # Update daemon heartbeat
        self.store.set_flag(
            "daemon_heartbeat",
            {
                "alive": True,
                "futu_connected": self.gateway.is_connected(),
                "trd_env": self.gateway.trd_env_name,
                "market_open": session.is_open,
                "market_phase": session.phase,
                "session_message": session.message,
                "regime": regime.light.value,
                "open_positions": len(open_positions),
                "watchlist_refreshed": watchlist_refreshed,
                "timestamp": now.isoformat(),
            },
        )
        # Check QQQ/SPY regime & open stops frequently (every 3m) during live market hours so we never
        # miss a YELLOW -> GREEN VWAP crossover, while heavy 14-stock watchlist scoring stays throttled
        # to 15m (GREEN) / 45m (YELLOW) / 120m (RED) via `watchlist_interval` above!
        return {
            "market_open": session.is_open,
            "market_phase": session.phase,
            "session_message": session.message,
            "regime": regime.light.value,
            "reason": regime.reason,
            "watchlist_refreshed": watchlist_refreshed,
            "entries": entries_placed,
            "exits": exits_triggered,
            "next_sleep_minutes": 3 if session.is_open else max(1, min(15, session.minutes_until_open)),
        }

    def run_forever(self, tier_override: Optional[str] = None) -> None:
        def _handle_sig(_sig: int, _frame: Any) -> None:
            self._stop_requested = True

        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)

        self.startup_reconcile()
        logger.info("zy-quant Two-Speed Daemon started.")
        while not self._stop_requested:
            try:
                res = self.run_cycle(tier_override=tier_override)
                sleep_sec = max(30, int(res["next_sleep_minutes"] * 60))
                logger.info(
                    "Cycle complete: regime=%s, entries=%d, exits=%d, next_check=%dm",
                    res["regime"],
                    len(res["entries"]),
                    len(res["exits"]),
                    res["next_sleep_minutes"],
                )
            except Exception as exc:
                logger.exception("Unhandled error in daemon cycle: %s", exc)
                self.notifier.send("Daemon Error", str(exc), level="ERROR")
                sleep_sec = 60

            for _ in range(sleep_sec):
                if self._stop_requested:
                    break
                time.sleep(1)
        self.gateway.close()
