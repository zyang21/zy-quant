from __future__ import annotations

import json
import logging
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from zy_quant.config import AppSettings
from zy_quant.fees import FutuUSFeeModel
from zy_quant.gateway.alpaca_adapter import AlpacaMarketDataAdapter
from zy_quant.gateway.base import BrokerExecutionAdapter, MarketDataAdapter
from zy_quant.gateway.futu_adapter import FutuGatewayAdapter
from zy_quant.intel.earnings_calendar import EarningsCalendarFeed
from zy_quant.intel.edgar_form4 import SecEdgarForm4Scanner
from zy_quant.intel.market_hours import get_us_market_session, session_close_time
from zy_quant.intel.regime import MarketRegimeDetector
from zy_quant.intel.watchlist_scorer import ConservativeWatchlistScorer
from zy_quant.models import (
    BrokerOrderUpdate,
    CapitalFlowSnapshot,
    ExitReason,
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
from zy_quant.runtime.pipeline import DailySwingPipeline
from zy_quant.storage.bar_cache import HistoricalBarCache
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
        gateway: Optional[Any] = None,
        notifier: Optional[AlertNotifier] = None,
        market_data: Optional[MarketDataAdapter] = None,
    ) -> None:
        self.settings = settings
        self.store = store or StateStore(settings.resolve_db_path())
        self.gateway = gateway or FutuGatewayAdapter(
            futu_cfg=settings.runtime_cfg.futu,
            allow_synthetic_fallback=settings.runtime_cfg.allow_synthetic_fallback,
        )
        if market_data is not None:
            self.market_data: MarketDataAdapter = market_data
            self.bar_cache: Optional[HistoricalBarCache] = getattr(market_data, "bar_cache", None)
        elif gateway is not None:
            # Isolated test gateway injection: route market data directly to injected test double
            self.market_data = self.gateway
            self.bar_cache = None
        else:
            self.bar_cache = HistoricalBarCache(settings.resolve_bar_cache_path())
            self.market_data = AlpacaMarketDataAdapter(
                alpaca_cfg=settings.runtime_cfg.alpaca,
                bar_cache=self.bar_cache,
                fallback=self.gateway,
            )

        self.notifier = notifier or AlertNotifier()
        self.fee_model = FutuUSFeeModel()

        self.regime_detector = MarketRegimeDetector(settings.strategy_cfg.regime)
        self.watchlist_scorer = ConservativeWatchlistScorer(settings.watchlist_cfg)
        self.edgar_scanner = SecEdgarForm4Scanner()
        self.earnings_feed = EarningsCalendarFeed(
            cache_path=settings.root_dir / "data" / "cache" / "earnings_calendar.json"
        )
        self.signal_engine = IntradaySignalEngine(settings.strategy_cfg.entry)
        self.exit_engine = ScientificExitAndSizingEngine(
            settings.strategy_cfg.exit_models, self.fee_model
        )
        self.risk_gate = MandateRiskGate(self.store, settings.strategy_cfg.daemon)
        self.swing_pipeline = DailySwingPipeline(self)

        self._stop_requested = False
        self._last_watchlist_scan_at: Optional[datetime] = None
        self._order_lock = threading.RLock()
        self.gateway.register_order_callback(self.on_broker_order_update)

    def _place_stop_order(
        self,
        symbol: str,
        shares: float,
        stop_price: float,
        existing_order_id: Optional[str] = None,
    ) -> Optional[str]:
        mode = self.settings.strategy_cfg.daemon.server_stop_order_type
        try:
            return self.gateway.place_or_update_protective_stop(
                symbol, shares, stop_price, existing_order_id, order_mode=mode
            )
        except TypeError:
            # Backward compatibility for lightweight test mocks with 4-arg signature
            return self.gateway.place_or_update_protective_stop(
                symbol, shares, stop_price, existing_order_id
            )

    def _sync_sim_virtual_cash(self, delta_usd: float, tier_key_hint: Optional[str] = None) -> None:
        if getattr(self.gateway, "trd_env_name", "SIMULATE") == "REAL":
            return
        t_key = tier_key_hint or self.settings.active_tier_key
        t_prof = self.settings.tiers_cfg.get_active(t_key)
        self.store.adjust_virtual_cash(t_key, delta_usd, t_prof.reference_equity_usd)

    def _ensure_sim_virtual_cash_bootstrapped(self, tier_key_hint: Optional[str] = None) -> None:
        if getattr(self.gateway, "trd_env_name", "SIMULATE") == "REAL":
            return
        t_key = tier_key_hint or self.settings.active_tier_key
        t_prof = self.settings.tiers_cfg.get_active(t_key)
        self.store.get_virtual_cash(t_key, t_prof.reference_equity_usd)

    def _mark_order_submitted_safely(
        self, cid: str, broker_oid: Optional[str], fail_reason: str = "Broker rejected order"
    ) -> bool:
        """
        Thread-safe status transition guarding against fast push callbacks that may have already
        transitioned `cid` from `PENDING -> PARTIAL_FILLED / FILLED` before `place_limit_order` returned!
        """
        if not broker_oid:
            self.store.update_order_status(cid, status=OrderStatus.FAILED, reason=fail_reason)
            return False
        with self._order_lock:
            curr_ord = self.store.get_order(cid)
            if curr_ord and curr_ord["status"] == OrderStatus.PENDING.value:
                self.store.update_order_status(
                    cid, status=OrderStatus.SUBMITTED, broker_order_id=broker_oid
                )
            elif curr_ord and not curr_ord.get("broker_order_id"):
                self.store.update_order_status(
                    cid, status=OrderStatus(curr_ord["status"]), broker_order_id=broker_oid
                )
        return True

    def _settle_paper_local_buy(
        self,
        cid: str,
        sym: str,
        plan: Any,
        sig: Any,
        est_buy_fee: float,
        now_utc: datetime,
        tier_key: str,
        tier: Any,
    ) -> None:
        self._ensure_sim_virtual_cash_bootstrapped(tier_key)
        self.store.update_order_status(
            cid,
            OrderStatus.FILLED,
            broker_order_id="PAPER-LOCAL",
            fill_price=plan.entry_price,
            avg_fill_price=plan.entry_price,
            fee_usd=est_buy_fee,
            filled_shares=plan.shares,
            reconciled_shares=plan.shares,
        )
        stop_oid = (
            self._place_stop_order(sym, plan.shares, plan.initial_stop)
            if self.settings.strategy_cfg.daemon.place_server_side_stop
            else None
        )
        stop_type = "CLIENT_FRAC_RTH" if plan.shares < 1.0 else f"GTC_{self.settings.strategy_cfg.daemon.server_stop_order_type}"
        new_pos = OpenPosition(
            symbol=sym,
            sector=plan.sector,
            shares=plan.shares,
            initial_shares=plan.shares,
            entry_price=plan.entry_price,
            entry_time=now_utc,
            initial_stop=plan.initial_stop,
            current_stop=plan.initial_stop,
            highest_price=plan.entry_price,
            risk_per_share=plan.risk_per_share,
            entry_atr_1d=sig.atr_14,
            stop_order_type=stop_type,
            broker_stop_order_id=stop_oid,
            setup=plan.setup.value,
            metadata={
                "buy_fee_usd": est_buy_fee,
                "expectancy_r": plan.expectancy_r,
                "last_bar_ts": sig.timestamp.isoformat(),
                "last_mark_price": plan.entry_price,
                "tier_key": tier_key,
            },
        )
        self.store.upsert_position(new_pos)
        self._sync_sim_virtual_cash(-(plan.shares * plan.entry_price + est_buy_fee), tier_key)
        self.notifier.send(
            "New Entry Executed",
            f"{sym} ({plan.setup.value}): {plan.shares} sh @ ${plan.entry_price:.2f}, "
            f"Stop=${plan.initial_stop:.2f}, Expectancy=+{plan.expectancy_r:.2f}R",
        )

    def _settle_paper_local_sell(
        self,
        cid: str,
        sym: str,
        pos: OpenPosition,
        shares_to_sell: float,
        exit_price: float,
        exit_reason: str,
        tier_key: str,
        tier: Any,
    ) -> None:
        self._ensure_sim_virtual_cash_bootstrapped(tier_key)
        sell_fee = self.fee_model.estimate_leg(
            shares_to_sell, exit_price, OrderSide.SELL
        ).total_friction_usd
        alloc_buy_fee = float(pos.metadata.get("buy_fee_usd") or 0.0) * (
            shares_to_sell / max(1e-6, pos.initial_shares)
        )
        self.store.update_order_status(
            cid,
            OrderStatus.FILLED,
            broker_order_id="PAPER-LOCAL",
            fill_price=exit_price,
            avg_fill_price=exit_price,
            fee_usd=sell_fee,
            filled_shares=shares_to_sell,
            reconciled_shares=shares_to_sell,
        )
        self.store.record_closed_trade(
            symbol=sym,
            setup=pos.setup,
            shares=shares_to_sell,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            fee_usd=round(alloc_buy_fee + sell_fee, 4),
            risk_per_share=pos.risk_per_share,
            exit_reason=exit_reason,
            entry_time=pos.entry_time.isoformat(),
        )
        pos.shares = round(pos.shares - shares_to_sell, 6)
        if pos.shares <= 1e-5:
            self.store.remove_position(sym)
        else:
            if self.settings.strategy_cfg.daemon.place_server_side_stop:
                pos.broker_stop_order_id = self._place_stop_order(
                    sym, pos.shares, pos.current_stop, pos.broker_stop_order_id
                )
            self.store.upsert_position(pos)
        self._sync_sim_virtual_cash(shares_to_sell * exit_price - sell_fee, tier_key)
        self.notifier.send("Position Exit", f"{sym}: {exit_reason} @ ${exit_price:.2f}", level="WARN")

    def startup_reconcile(self) -> Dict[str, Any]:
        connected = self.gateway.is_connected() or self.gateway.connect()
        summary: Dict[str, Any] = {
            "futu_connected": connected,
            "reconciliation": {},
            "order_events": [],
        }
        # Only overwrite SQLite positions from broker when the target trading environment
        # (e.g. REAL, or an activated Futu SIMULATE sub-account) actually exists on the broker.
        # In local paper-simulation mode (`PAPER-LOCAL`), SQLite is the source of truth for virtual holdings.
        if connected and self.gateway.has_target_account():
            summary["order_events"] = self.reconcile_open_orders()
            broker_pos = self.gateway.get_open_positions()
            if getattr(self.gateway, "last_position_sync_ok", True):
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
                "trd_env": getattr(self.gateway, "trd_env_name", "SIMULATE"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        return summary

    def _resolve_order_fee(
        self,
        broker_order_id: str,
        shares: float,
        fill_price: float,
        side: OrderSide,
        is_terminal: bool,
    ) -> float:
        """
        Prefers exact broker billing fee via order_fee_query() on terminal REAL fills;
        gracefully falls back to FutuUSFeeModel so fees are never recorded as $0.00.
        """
        if shares <= 0 or fill_price <= 0:
            return 0.0
        if is_terminal and broker_order_id:
            exact_fee = self.gateway.query_order_fee(broker_order_id)
            if exact_fee is not None and exact_fee > 0.0:
                return round(exact_fee, 4)
        return self.fee_model.estimate_leg(shares, fill_price, side).total_friction_usd

    def on_broker_order_update(self, update: BrokerOrderUpdate) -> Optional[str]:
        """
        Thread-safe, idempotent handler invoked by both real-time ZyTradeOrderHandler push callbacks
        and pull-based reconcile_open_orders() polling:
        - Incrementally reconciles `delta_shares = update.filled_shares - order.reconciled_shares`
          so push + poll concurrency never double-counts shares or closed_trades!
        - Reconstructs OpenPosition from `orders.plan_json` on first BUY fill and immediately
          arms/updates the Futu server-side protective stop order.
        - Detects server-side protective stop order executions (`pos.broker_stop_order_id`).
        """
        with self._order_lock:
            # 1. Check if this update belongs to an active server-side protective stop order
            open_positions = self.store.get_positions()
            for sym, pos in open_positions.items():
                if (
                    pos.broker_stop_order_id
                    and update.broker_order_id
                    and pos.broker_stop_order_id == update.broker_order_id
                ):
                    return self._handle_protective_stop_update(pos, update)

            # 2. Match against tracked entry/exit order in SQLite
            order_row = self.store.find_order_for_broker_update(
                update.broker_order_id, update.remark
            )
            if order_row is None:
                return None

            cid = str(order_row["client_order_id"])
            sym = str(order_row["symbol"])
            side = OrderSide(str(order_row["side"]))
            prev_reconciled = float(order_row.get("reconciled_shares") or 0.0)
            new_filled = max(prev_reconciled, float(update.filled_shares or 0.0))
            delta_shares = round(new_filled - prev_reconciled, 6)

            eff_fill_px = float(
                update.avg_fill_price
                or order_row.get("avg_fill_price")
                or order_row.get("fill_price")
                or order_row["limit_price"]
            )
            total_fee = self._resolve_order_fee(
                update.broker_order_id,
                new_filled,
                eff_fill_px,
                side,
                is_terminal=update.status.is_terminal,
            )
            prev_fee = float(order_row.get("fee_usd") or 0.0)
            delta_fee = max(0.0, round(total_fee - prev_fee, 4))

            try:
                plan_meta = json.loads(order_row.get("plan_json") or "{}")
            except (TypeError, ValueError):
                plan_meta = {}

            event_summary: Optional[str] = None
            tier_hint = str(plan_meta.get("tier_key") or self.settings.active_tier_key)
            if delta_shares > 1e-6:
                self._ensure_sim_virtual_cash_bootstrapped(tier_hint)
                # Compute exact incremental notional from cumulative dealt_avg_price so multi-print
                # partial fills at different prices preserve $0.0000 cash/PnL ledger identity!
                prev_avg_px = float(
                    order_row.get("avg_fill_price")
                    or order_row.get("fill_price")
                    or eff_fill_px
                )
                prev_notional = prev_reconciled * prev_avg_px
                new_notional = new_filled * eff_fill_px
                delta_notional = max(0.0, round(new_notional - prev_notional, 6))
                slice_fill_px = round(delta_notional / delta_shares, 4)

                if side == OrderSide.BUY:
                    pos = open_positions.get(sym)
                    planned_stop = float(
                        plan_meta.get("initial_stop") or round(eff_fill_px * 0.975, 2)
                    )
                    safe_stop = min(planned_stop, round(eff_fill_px * 0.995, 2))
                    risk_ps = max(0.01, round(eff_fill_px - safe_stop, 4))
                    stop_type = "CLIENT_FRAC_RTH" if delta_shares < 1.0 and pos is None else f"GTC_{self.settings.strategy_cfg.daemon.server_stop_order_type}"

                    if pos is None:
                        pos = OpenPosition(
                            symbol=sym,
                            sector=str(plan_meta.get("sector") or "Technology"),
                            shares=delta_shares,
                            initial_shares=delta_shares,
                            entry_price=round(eff_fill_px, 4),
                            entry_time=datetime.now(timezone.utc),
                            initial_stop=safe_stop,
                            current_stop=safe_stop,
                            highest_price=round(eff_fill_px, 4),
                            risk_per_share=risk_ps,
                            entry_atr_1d=float(plan_meta.get("entry_atr_1d") or 0.0),
                            stop_order_type=stop_type,
                            setup=str(plan_meta.get("setup") or "VWAP_PULLBACK"),
                            metadata={
                                "buy_fee_usd": round(total_fee, 4),
                                "expectancy_r": float(plan_meta.get("expectancy_r") or 0.0),
                                "last_bar_ts": str(plan_meta.get("last_bar_ts") or ""),
                                "last_mark_price": round(eff_fill_px, 4),
                                "tier_key": tier_hint,
                            },
                        )
                    else:
                        new_total_sh = round(pos.shares + delta_shares, 6)
                        pos.shares = new_total_sh
                        pos.initial_shares = round(pos.initial_shares + delta_shares, 6)
                        pos.entry_price = round(eff_fill_px, 4)
                        pos.highest_price = max(pos.highest_price, round(eff_fill_px, 4))
                        pos.risk_per_share = max(0.01, round(pos.entry_price - pos.initial_stop, 4))
                        pos.stop_order_type = "CLIENT_FRAC_RTH" if pos.shares < 1.0 else f"GTC_{self.settings.strategy_cfg.daemon.server_stop_order_type}"
                        pos.metadata["buy_fee_usd"] = round(
                            float(pos.metadata.get("buy_fee_usd") or 0.0) + delta_fee, 4
                        )

                    if self.settings.strategy_cfg.daemon.place_server_side_stop:
                        pos.broker_stop_order_id = self._place_stop_order(
                            sym, pos.shares, pos.current_stop, pos.broker_stop_order_id
                        )
                    self.store.upsert_position(pos)
                    self._sync_sim_virtual_cash(-(delta_notional + delta_fee), tier_hint)
                    event_summary = (
                        f"BUY {sym} filled +{delta_shares} sh (total {pos.shares} sh) "
                        f"@ ${eff_fill_px:.2f} [{update.status.value}]"
                    )
                    if update.status == OrderStatus.FILLED:
                        self.notifier.send(
                            "Entry Order Filled",
                            f"{sym} ({pos.setup}): {pos.shares} sh @ ${eff_fill_px:.2f}, "
                            f"Stop=${pos.current_stop:.2f}",
                        )

                else:
                    # OrderSide.SELL fill reconciliation
                    pos = open_positions.get(sym)
                    entry_px = (
                        pos.entry_price
                        if pos is not None
                        else float(plan_meta.get("entry_price") or slice_fill_px)
                    )
                    risk_ps = (
                        pos.risk_per_share
                        if pos is not None
                        else float(plan_meta.get("risk_per_share") or 1.0)
                    )
                    setup_name = (
                        pos.setup
                        if pos is not None
                        else str(plan_meta.get("setup") or "VWAP_PULLBACK")
                    )
                    entry_time_iso = (
                        pos.entry_time.isoformat()
                        if pos is not None
                        else str(
                            plan_meta.get("entry_time")
                            or datetime.now(timezone.utc).isoformat()
                        )
                    )
                    exit_reason_val = str(plan_meta.get("exit_reason") or "EXIT")

                    alloc_buy_fee = 0.0
                    if pos is not None and pos.initial_shares > 0:
                        alloc_buy_fee = float(pos.metadata.get("buy_fee_usd") or 0.0) * (
                            delta_shares / pos.initial_shares
                        )
                    leg_round_trip_fee = round(alloc_buy_fee + delta_fee, 4)

                    self.store.record_closed_trade(
                        symbol=sym,
                        setup=setup_name,
                        shares=delta_shares,
                        entry_price=entry_px,
                        exit_price=slice_fill_px,
                        fee_usd=leg_round_trip_fee,
                        risk_per_share=risk_ps,
                        exit_reason=exit_reason_val,
                        entry_time=entry_time_iso,
                    )

                    if pos is not None:
                        pos.shares = round(pos.shares - delta_shares, 6)
                        if plan_meta.get("is_scale_out"):
                            pos.scaled_out = True
                            pos.breakeven_armed = True
                            post_stop = float(plan_meta.get("post_scale_stop") or pos.current_stop)
                            pos.current_stop = max(pos.current_stop, post_stop)
                            if plan_meta.get("earnings_cushion_date"):
                                pos.earnings_cushion_applied_date = str(plan_meta["earnings_cushion_date"])

                        if pos.shares <= 1e-5:
                            if pos.broker_stop_order_id:
                                self.gateway.cancel_order(pos.broker_stop_order_id)
                            self.store.remove_position(sym)
                        else:
                            if self.settings.strategy_cfg.daemon.place_server_side_stop:
                                pos.broker_stop_order_id = self._place_stop_order(
                                    sym,
                                    pos.shares,
                                    pos.current_stop,
                                    pos.broker_stop_order_id,
                                )
                            self.store.upsert_position(pos)

                    self._sync_sim_virtual_cash(delta_notional - delta_fee, tier_hint)

                    event_summary = (
                        f"SELL {sym} filled {delta_shares} sh @ ${eff_fill_px:.2f} "
                        f"({exit_reason_val}) [{update.status.value}]"
                    )
                    if update.status == OrderStatus.FILLED:
                        self.notifier.send(
                            "Exit Order Filled",
                            f"{sym}: sold {new_filled} sh @ ${eff_fill_px:.2f} ({exit_reason_val})",
                            level="WARN",
                        )

            reason_note = order_row.get("reason")
            if update.last_err_msg and update.status == OrderStatus.FAILED:
                reason_note = f"{reason_note or ''} | Broker error: {update.last_err_msg}".strip(" |")

            self.store.update_order_status(
                cid,
                status=update.status,
                broker_order_id=update.broker_order_id or order_row.get("broker_order_id"),
                fill_price=round(eff_fill_px, 4) if new_filled > 0 else None,
                avg_fill_price=round(eff_fill_px, 4) if new_filled > 0 else None,
                fee_usd=total_fee,
                filled_shares=new_filled,
                reconciled_shares=new_filled,
                reason=reason_note,
            )
            return event_summary

    def _handle_protective_stop_update(
        self, pos: OpenPosition, update: BrokerOrderUpdate
    ) -> Optional[str]:
        prev_stop_filled = float(pos.metadata.get("stop_reconciled_shares") or 0.0)
        new_filled = max(prev_stop_filled, float(update.filled_shares or 0.0))
        delta = round(new_filled - prev_stop_filled, 6)
        if delta <= 1e-6:
            return None

        cum_avg_px = float(update.avg_fill_price or update.price or pos.current_stop)
        prev_notional = float(pos.metadata.get("stop_reconciled_notional") or 0.0)
        new_notional = new_filled * cum_avg_px
        delta_notional = max(0.0, new_notional - prev_notional)
        slice_px = round(delta_notional / delta, 4) if delta_notional > 0 else cum_avg_px

        if pos.current_stop > pos.entry_price:
            reason = (
                ExitReason.CHANDELIER_TRAIL
                if (pos.highest_price - pos.entry_price) >= 2.0 * pos.risk_per_share
                else ExitReason.BREAKEVEN_STOP
            )
        else:
            reason = ExitReason.INITIAL_ATR_STOP

        prev_fee = float(pos.metadata.get("stop_reconciled_fee") or 0.0)
        exact_total_fee = (
            self.gateway.query_order_fee(update.broker_order_id)
            if (update.status.is_terminal and update.broker_order_id)
            else None
        )
        if exact_total_fee is not None and exact_total_fee > 0.0:
            total_stop_fee = round(exact_total_fee, 4)
            delta_fee = max(0.0, round(total_stop_fee - prev_fee, 4))
        else:
            delta_fee = self.fee_model.estimate_leg(delta, slice_px, OrderSide.SELL).total_friction_usd
            total_stop_fee = round(prev_fee + delta_fee, 4)

        alloc_buy_fee = (
            float(pos.metadata.get("buy_fee_usd") or 0.0)
            * (delta / max(1e-6, pos.initial_shares))
        )
        stop_cid = f"SRVSTOP-{pos.symbol}-{update.broker_order_id}"
        self.store.record_order_intent(
            stop_cid,
            pos.symbol,
            OrderSide.SELL,
            pos.initial_shares,
            pos.current_stop,
            f"Server-side protective stop ({reason.value})",
        )
        self.store.update_order_status(
            stop_cid,
            status=update.status,
            broker_order_id=update.broker_order_id,
            fill_price=round(cum_avg_px, 4),
            avg_fill_price=round(cum_avg_px, 4),
            fee_usd=total_stop_fee,
            filled_shares=new_filled,
            reconciled_shares=new_filled,
        )
        tier_hint = str(pos.metadata.get("tier_key") or self.settings.active_tier_key)
        self._ensure_sim_virtual_cash_bootstrapped(tier_hint)
        self.store.record_closed_trade(
            symbol=pos.symbol,
            setup=pos.setup,
            shares=delta,
            entry_price=pos.entry_price,
            exit_price=slice_px,
            fee_usd=round(alloc_buy_fee + delta_fee, 4),
            risk_per_share=pos.risk_per_share,
            exit_reason=reason.value,
            entry_time=pos.entry_time.isoformat(),
        )

        pos.shares = round(pos.shares - delta, 6)
        pos.metadata["stop_reconciled_shares"] = new_filled
        pos.metadata["stop_reconciled_notional"] = round(new_notional, 6)
        pos.metadata["stop_reconciled_fee"] = total_stop_fee
        if pos.shares <= 1e-5:
            self.store.remove_position(pos.symbol)
        else:
            self.store.upsert_position(pos)
        self._sync_sim_virtual_cash(delta_notional - delta_fee, tier_hint)

        msg = f"{pos.symbol}: server-side stop filled {delta} sh @ ${slice_px:.2f} ({reason.value})"
        self.notifier.send("Server-Side Stop Triggered", msg, level="WARN")
        return msg

    def reconcile_open_orders(self, now: Optional[datetime] = None) -> List[str]:
        """
        Pulls latest broker order statuses via order_list_query() and cancels any
        SUBMITTED / PARTIAL_FILLED orders that exceeded their configured TTL window.
        """
        ref_now = now or datetime.now(timezone.utc)
        events: List[str] = []

        for upd in self.gateway.sync_open_orders(refresh_cache=True):
            ev = self.on_broker_order_update(upd)
            if ev:
                events.append(ev)

        daemon_cfg = self.settings.strategy_cfg.daemon
        for order_row in self.store.get_active_orders():
            cid = str(order_row["client_order_id"])
            side_str = str(order_row["side"]).upper()
            ttl_sec = (
                daemon_cfg.entry_order_ttl_seconds
                if side_str == "BUY"
                else daemon_cfg.exit_order_ttl_seconds
            )
            created_dt = datetime.fromisoformat(str(order_row["created_at"]))
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)

            elapsed_sec = (ref_now - created_dt).total_seconds()
            if elapsed_sec < ttl_sec:
                continue

            broker_oid = str(order_row.get("broker_order_id") or "")
            cancel_ok = True
            if broker_oid:
                cancel_ok = self.gateway.cancel_order(broker_oid)
                # Re-poll once after cancel in case additional shares filled right before cancel
                for upd in self.gateway.sync_open_orders(refresh_cache=True):
                    if upd.broker_order_id == broker_oid:
                        ev = self.on_broker_order_update(upd)
                        if ev:
                            events.append(ev)

            with self._order_lock:
                fresh = self.store.get_order(cid)
                if (
                    fresh
                    and fresh["status"] != OrderStatus.FILLED.value
                    and (cancel_ok or fresh["status"] in (OrderStatus.CANCELLED.value, OrderStatus.EXPIRED.value, OrderStatus.FAILED.value))
                ):
                    filled_sh = float(fresh.get("filled_shares") or 0.0)
                    term_status = (
                        OrderStatus.CANCELLED if filled_sh > 0.0 else OrderStatus.EXPIRED
                    )
                    base_reason = str(fresh.get("reason") or "")
                    timeout_note = (
                        f"{base_reason} | TTL {ttl_sec}s partial-fill lock ({filled_sh} sh)"
                        if filled_sh > 0.0
                        else f"{base_reason} | TTL {ttl_sec}s unfilled timeout"
                    ).strip(" |")
                    self.store.update_order_status(
                        cid,
                        status=term_status,
                        reason=timeout_note,
                    )
                    # Safety net: if a SELL exit limit order timed out/cancelled while shares remain open,
                    # re-arm the server-side protective stop immediately so the position is never naked!
                    if side_str == "SELL" and self.settings.strategy_cfg.daemon.place_server_side_stop:
                        rem_pos = self.store.get_positions().get(str(fresh["symbol"]))
                        if rem_pos is not None and rem_pos.shares > 1e-5:
                            rem_pos.broker_stop_order_id = self._place_stop_order(
                                rem_pos.symbol,
                                rem_pos.shares,
                                rem_pos.current_stop,
                                rem_pos.broker_stop_order_id,
                            )
                            self.store.upsert_position(rem_pos)
                    events.append(
                        f"TTL timeout ({int(elapsed_sec)}s >= {ttl_sec}s): {cid} -> {term_status.value} (filled={filled_sh})"
                    )
                    logger.info(events[-1])

        return events

    def evaluate_regime(self) -> RegimeSnapshot:
        reg_cfg = self.settings.strategy_cfg.regime
        qqq_15m = self.market_data.get_bars(reg_cfg.primary_benchmark, timeframe="15m", days=5)
        qqq_1d = self.market_data.get_bars(reg_cfg.primary_benchmark, timeframe="1d", days=40)
        spy_1d = self.market_data.get_bars(reg_cfg.secondary_benchmark, timeframe="1d", days=40)
        vix = self.market_data.get_vix_level()
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

    def _select_quota_safe_universe(self) -> List[Any]:
        """
        When Alpaca Market Data API is configured, unlocks the full 100-stock + sector ETF pool
        in a single batch request (<1.5s, 0 Futu quota).
        When running without Alpaca credentials, caps scan to top 16 cross-sector leaders so
        Futu's 60 req/30s & 30-day K-line subscription quotas are never exhausted.
        """
        non_broad = [
            s for s in self.settings.watchlist_cfg.universe if s.sector != "Broad_ETF"
        ]
        alpaca_ready = hasattr(self.market_data, "has_credentials") and bool(
            getattr(self.market_data, "has_credentials")()
        )
        if alpaca_ready or len(non_broad) <= 18:
            return non_broad

        ordered = sorted(non_broad, key=lambda u: u.etf_core_score, reverse=True)
        picked: List[Any] = []
        sec_counts: Dict[str, int] = {}
        for u in ordered:
            if sec_counts.get(u.sector, 0) >= 3:
                continue
            picked.append(u)
            sec_counts[u.sector] = sec_counts.get(u.sector, 0) + 1
            if len(picked) >= 16:
                break
        return picked

    def refresh_watchlist(
        self,
        regime: RegimeSnapshot,
        check_edgar: bool = False,
        check_earnings: bool = True,
    ) -> List[Dict[str, Any]]:
        quotes: Dict[str, QuoteSnapshot] = {}
        intraday_rets: Dict[str, float] = {}
        flows: Dict[str, CapitalFlowSnapshot] = {}
        insiders: Dict[str, InsiderActivity] = {}

        scan_pool = self._select_quota_safe_universe()
        pool_syms = [sec.symbol for sec in scan_pool]
        alpaca_ready = hasattr(self.market_data, "has_credentials") and bool(
            getattr(self.market_data, "has_credentials")()
        )

        if alpaca_ready and hasattr(self.market_data, "get_snapshots_batch"):
            quotes = self.market_data.get_snapshots_batch(pool_syms)  # type: ignore[attr-defined]
            prev_closes = getattr(self.market_data, "last_prev_closes", {})
            for sym, q_snap in quotes.items():
                prev_c = float(prev_closes.get(sym) or 0.0)
                if prev_c > 0:
                    intraday_rets[sym] = (q_snap.last_price - prev_c) / prev_c
            if check_edgar:
                for sec in scan_pool:
                    insiders[sec.symbol] = self.edgar_scanner.fetch_insider_activity(
                        sec.symbol, sec.cik
                    )
        else:
            for sec in scan_pool:
                q = self.market_data.get_quote(sec.symbol)
                if q is not None:
                    quotes[sec.symbol] = q
                d_bars = self.market_data.get_bars(sec.symbol, timeframe="1d", days=5)
                if len(d_bars) >= 2 and d_bars[-2].close > 0:
                    intraday_rets[sec.symbol] = (
                        d_bars[-1].close - d_bars[-2].close
                    ) / d_bars[-2].close
                flow = self.gateway.get_capital_flow(sec.symbol)
                if flow is not None:
                    flows[sec.symbol] = flow
                if check_edgar:
                    insiders[sec.symbol] = self.edgar_scanner.fetch_insider_activity(
                        sec.symbol, sec.cik
                    )

        earn_map = (
            self.earnings_feed.get_upcoming_earnings(
                symbols=pool_syms, allow_network=(self.bar_cache is not None)
            )
            if check_earnings
            else {}
        )

        candidates = self.watchlist_scorer.rank_universe(
            quotes=quotes,
            intraday_returns=intraday_rets,
            qqq_intraday_return=regime.qqq_intraday_return,
            regime_light=regime.light,
            capital_flows=flows,
            insiders=insiders,
            earnings_dates=earn_map,
        )
        self.store.save_watchlist_scores(candidates)
        self._last_watchlist_scan_at = datetime.now(timezone.utc)
        return self.store.get_watchlist_scores()

    def run_cycle(
        self,
        tier_override: Optional[str] = None,
        check_edgar: bool = False,
        force_when_closed: bool = False,
        now_override: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Executes a single deterministic daemon tick safely."""
        now = now_override or datetime.now(timezone.utc)
        session = get_us_market_session(now)

        # Automatic background loop pauses all scans while US Regular Trading Hours are closed
        if not session.is_open and not force_when_closed:
            sleep_m = max(1, min(15, session.minutes_until_open))
            self.store.set_flag(
                "daemon_heartbeat",
                {
                    "alive": True,
                    "futu_connected": self.gateway.is_connected(),
                    "trd_env": getattr(self.gateway, "trd_env_name", "SIMULATE"),
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
                "order_events": [],
                "next_sleep_minutes": sleep_m,
            }

        tier = self.settings.tiers_cfg.get_active(tier_override)

        # Watchdog check: reconnect FutuOpenD if dropped
        if not self.gateway.is_connected() and hasattr(self.gateway, "connect"):
            self.gateway.connect()

        has_target_acc = self.gateway.has_target_account()
        order_events: List[str] = []
        if has_target_acc:
            order_events = self.reconcile_open_orders(now=now)

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
        open_positions = self.store.get_positions()
        for sym, pos in list(open_positions.items()):
            # Skip duplicate exit evaluation while an exit order for this symbol is already in-flight
            if self.store.get_active_orders(side=OrderSide.SELL, symbol=sym):
                continue

            bars_15m = self.market_data.get_bars(sym, timeframe="15m", days=5)
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
            prev_scaled_out = pos.scaled_out
            prev_be_armed = pos.breakeven_armed
            session_bar_num = sum(
                1 for b in bars_15m if b.timestamp.date() == latest_bar.timestamp.date()
            )
            close_t = session_close_time(latest_bar.timestamp.date())
            total_session_bars = ((close_t.hour * 60 + close_t.minute) - (9 * 60 + 30)) // 15
            eod_threshold = max(1, total_session_bars - 1)
            action = self.exit_engine.step_position(
                pos,
                latest_bar,
                atrs[-1],
                tier,
                is_new_bar=is_new_bar,
                is_eod_window=(session_bar_num >= eod_threshold),
            )

            if action.should_exit and action.shares_to_sell > 0:
                is_scale_out = action.reason == ExitReason.SCALE_OUT_2R
                post_scale_stop = pos.current_stop
                cid = f"EXIT-{sym}-{latest_bar.timestamp.strftime('%Y%m%d%H%M')}-{action.reason.value if action.reason else 'X'}"
                exit_plan_meta = {
                    "symbol": sym,
                    "entry_price": pos.entry_price,
                    "risk_per_share": pos.risk_per_share,
                    "setup": pos.setup,
                    "entry_time": pos.entry_time.isoformat(),
                    "exit_reason": action.reason.value if action.reason else "EXIT",
                    "is_scale_out": is_scale_out,
                    "post_scale_stop": post_scale_stop,
                }

                if self.store.record_order_intent(
                    cid,
                    sym,
                    OrderSide.SELL,
                    action.shares_to_sell,
                    action.exit_price,
                    action.note,
                    plan_meta=exit_plan_meta,
                    created_at=now,
                ):
                    if has_target_acc:
                        # Async broker lifecycle (SIMULATE sub-account or REAL):
                        # Keep position intact in SQLite until ZyTradeOrderHandler / sync_open_orders confirms fill!
                        prior_stop_oid = pos.broker_stop_order_id
                        if is_scale_out:
                            pos.scaled_out = prev_scaled_out
                            pos.current_stop = prev_stop
                            pos.breakeven_armed = prev_be_armed
                        if prior_stop_oid and action.shares_to_sell >= pos.shares - 1e-5:
                            if not self.gateway.cancel_order(prior_stop_oid):
                                self.store.update_order_status(
                                    cid,
                                    status=OrderStatus.FAILED,
                                    reason=f"{action.note} | Aborted SELL: existing stop {prior_stop_oid} cancel failed",
                                )
                                continue
                            pos.broker_stop_order_id = None
                        with self._order_lock:
                            live_p = self.store.get_positions().get(sym)
                            if live_p is None or abs(live_p.shares - pos.shares) > 1e-5:
                                self.store.update_order_status(
                                    cid,
                                    status=OrderStatus.CANCELLED,
                                    reason=f"{action.note} | Position changed concurrently before SELL submit",
                                )
                                continue
                            self.store.upsert_position(pos)

                        broker_oid = self.gateway.place_limit_order(
                            sym, OrderSide.SELL, action.shares_to_sell, action.exit_price, remark=cid
                        )
                        if self._mark_order_submitted_safely(
                            cid, broker_oid, f"{action.note} | Broker rejected SELL submission"
                        ):
                            exits_triggered.append(f"{sym} SUBMITTED ({action.note})")
                        elif not is_scale_out and self.settings.strategy_cfg.daemon.place_server_side_stop:
                            with self._order_lock:
                                rem_p = self.store.get_positions().get(sym)
                                if rem_p is not None and rem_p.shares > 1e-5 and not rem_p.broker_stop_order_id:
                                    rem_p.broker_stop_order_id = self._place_stop_order(
                                        sym, rem_p.shares, rem_p.current_stop, None
                                    )
                                    self.store.upsert_position(rem_p)
                    else:
                        # PAPER-LOCAL mode (no Futu sub-account): preserve immediate virtual fill
                        self._settle_paper_local_sell(
                            cid,
                            sym,
                            pos,
                            action.shares_to_sell,
                            action.exit_price,
                            action.reason.value if action.reason else "EXIT",
                            tier_override or self.settings.active_tier_key,
                            tier,
                        )
                        if pos.shares <= 1e-5:
                            open_positions.pop(sym, None)
                        exits_triggered.append(f"{sym} ({action.note})")
            else:
                # If Chandelier / Breakeven ratcheted stop upward, sync protective stop under _order_lock!
                with self._order_lock:
                    live_p = self.store.get_positions().get(sym)
                    if live_p is not None and abs(live_p.shares - pos.shares) <= 1e-5:
                        if (
                            pos.current_stop > prev_stop
                            and self.settings.strategy_cfg.daemon.place_server_side_stop
                        ):
                            pos.broker_stop_order_id = self._place_stop_order(
                                sym, pos.shares, pos.current_stop, live_p.broker_stop_order_id
                            )
                        self.store.upsert_position(pos)

        # Refresh open_positions snapshot in case push callback settled an exit immediately
        open_positions = self.store.get_positions()

        # 2. New Entry Evaluation — GREEN (100% scale) or YELLOW (Selective Alpha Leaders @ 65% scale)
        entries_placed: List[str] = []
        if session.is_open and regime.light in (RegimeLight.GREEN, RegimeLight.YELLOW):
            trd_env_name = getattr(self.gateway, "trd_env_name", "SIMULATE")
            active_buys = self.store.get_active_orders(side=OrderSide.BUY)
            in_flight_reserved_cash = sum(
                max(0.0, float(o.get("shares") or 0.0) - float(o.get("filled_shares") or 0.0))
                * float(o.get("limit_price") or 0.0)
                for o in active_buys
            )

            if has_target_acc and trd_env_name == "REAL":
                acct = self.gateway.get_account_summary()
                equity = acct["equity_usd"] if acct["equity_usd"] > 0 else tier.reference_equity_usd
                cash = max(0.0, acct["cash_usd"] - in_flight_reserved_cash)
            else:
                # Paper Simulation Mode: size strictly from virtual Tier capital + realized paper PnL
                realized_all = self.store.realized_pnl_since("2000-01-01T00:00:00")
                equity = tier.reference_equity_usd + realized_all
                invested = sum(
                    p.shares * p.entry_price
                    + float(p.metadata.get("buy_fee_usd", 0.0)) * (p.shares / max(1e-6, p.initial_shares))
                    for p in open_positions.values()
                )
                cash = max(0.0, equity - invested - in_flight_reserved_cash)

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
            pending_buy_syms = {str(o["symbol"]) for o in active_buys}
            qqq_1d = self.market_data.get_bars(
                self.settings.strategy_cfg.regime.primary_benchmark, timeframe="1d", days=40
            )

            # Select active Top-N candidates with GICS Sector Cap (max 3 per sector)
            watchlist = self.watchlist_scorer.select_active_top_n(
                self.store.get_watchlist_scores()
            )

            for item in watchlist:
                sym = item["symbol"]
                if sym in open_positions or sym in pending_buy_syms or sym in recent_exits:
                    continue
                bars_15m = self.market_data.get_bars(sym, timeframe="15m", days=5)
                bars_1d = self.market_data.get_bars(sym, timeframe="1d", days=40)
                flow = self.gateway.get_capital_flow(sym) if self.gateway.is_connected() else self.market_data.get_capital_flow(sym)
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
                entry_plan_meta = {
                    "symbol": sym,
                    "sector": plan.sector,
                    "planned_shares": plan.shares,
                    "planned_entry_price": plan.entry_price,
                    "limit_price": plan.limit_price,
                    "initial_stop": plan.initial_stop,
                    "risk_per_share": plan.risk_per_share,
                    "setup": plan.setup.value,
                    "expectancy_r": plan.expectancy_r,
                    "last_bar_ts": sig.timestamp.isoformat(),
                }
                if not self.store.record_order_intent(
                    cid,
                    sym,
                    OrderSide.BUY,
                    plan.shares,
                    plan.limit_price,
                    sig.rationale,
                    plan_meta=entry_plan_meta,
                    created_at=now,
                ):
                    continue

                est_buy_fee = self.fee_model.estimate_leg(
                    plan.shares, plan.entry_price, OrderSide.BUY
                ).total_friction_usd

                if has_target_acc:
                    # Real / Futu SIMULATE Sub-Account Mode:
                    # Submit limit order as SUBMITTED; do NOT create OpenPosition or protective stop
                    # until ZyTradeOrderHandler push or sync_open_orders confirms dealt_qty > 0!
                    broker_oid = self.gateway.place_limit_order(
                        sym, OrderSide.BUY, plan.shares, plan.limit_price, remark=cid
                    )
                    if not broker_oid:
                        self.store.update_order_status(
                            cid,
                            status=OrderStatus.FAILED,
                            reason=f"{sig.rationale} | Broker rejected BUY submission",
                        )
                        continue

                    with self._order_lock:
                        curr_ord = self.store.get_order(cid)
                        if curr_ord and curr_ord["status"] == OrderStatus.PENDING.value:
                            self.store.update_order_status(
                                cid,
                                status=OrderStatus.SUBMITTED,
                                broker_order_id=broker_oid,
                            )
                        elif curr_ord and not curr_ord.get("broker_order_id"):
                            self.store.update_order_status(
                                cid,
                                status=OrderStatus(curr_ord["status"]),
                                broker_order_id=broker_oid,
                            )

                    pending_buy_syms.add(sym)
                    cash -= plan.shares * plan.limit_price + est_buy_fee
                    entries_placed.append(
                        f"{sym} SUBMITTED x{plan.shares} @ limit ${plan.limit_price:.2f} (oid={broker_oid})"
                    )
                    open_positions = self.store.get_positions()
                else:
                    # PAPER-LOCAL Mode (no activated Futu sub-account): immediate virtual fill
                    t_key_eff = tier_override or self.settings.active_tier_key
                    self._settle_paper_local_buy(
                        cid, sym, plan, sig, est_buy_fee, now, t_key_eff, tier
                    )
                    open_positions = self.store.get_positions()
                    cash -= plan.notional_usd + est_buy_fee
                    entries_placed.append(f"{sym} x{plan.shares} @ ${plan.entry_price:.2f}")

        open_positions = self.store.get_positions()
        in_flight_count = len(self.store.get_active_orders())

        # Update daemon heartbeat
        self.store.set_flag(
            "daemon_heartbeat",
            {
                "alive": True,
                "futu_connected": self.gateway.is_connected(),
                "trd_env": getattr(self.gateway, "trd_env_name", "SIMULATE"),
                "market_open": session.is_open,
                "market_phase": session.phase,
                "session_message": session.message,
                "regime": regime.light.value,
                "open_positions": len(open_positions),
                "in_flight_orders": in_flight_count,
                "watchlist_refreshed": watchlist_refreshed,
                "timestamp": now.isoformat(),
            },
        )
        # If there are active in-flight orders waiting for fills/TTL, wake up in 1m instead of 3m
        # so unfilled limit orders are cancelled promptly around their 60s-90s TTL!
        sleep_min = (
            1
            if (session.is_open and in_flight_count > 0)
            else (3 if session.is_open else max(1, min(15, session.minutes_until_open)))
        )
        return {
            "market_open": session.is_open,
            "market_phase": session.phase,
            "session_message": session.message,
            "regime": regime.light.value,
            "reason": regime.reason,
            "watchlist_refreshed": watchlist_refreshed,
            "entries": entries_placed,
            "exits": exits_triggered,
            "order_events": order_events,
            "next_sleep_minutes": sleep_min,
        }

    def step_scheduled(
        self,
        tier_override: Optional[str] = None,
        check_edgar: bool = False,
        force_when_closed: bool = False,
        now_override: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Dispatches to the 4-Phase Daily Swing Pipeline when `strategy.mode == 'swing_1d'`,
        or the legacy 15m intraday loop when `strategy.mode == 'intraday_15m'`.
        """
        if self.settings.strategy_cfg.mode == "swing_1d":
            return self.swing_pipeline.run_due_phase(
                now_utc=now_override,
                tier_override=tier_override,
                force_eod_when_closed=force_when_closed,
                check_edgar=check_edgar,
            )
        return self.run_cycle(
            tier_override=tier_override,
            check_edgar=check_edgar,
            force_when_closed=force_when_closed,
            now_override=now_override,
        )

    def run_forever(self, tier_override: Optional[str] = None) -> None:
        def _handle_sig(_sig: int, _frame: Any) -> None:
            self._stop_requested = True

        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)

        self.startup_reconcile()
        logger.info("zy-quant Daemon started (mode=%s).", self.settings.strategy_cfg.mode)
        while not self._stop_requested:
            try:
                res = self.step_scheduled(tier_override=tier_override)
                sleep_sec = max(30, int(res.get("next_sleep_minutes", 5) * 60))
                logger.info(
                    "Daemon step complete: phase=%s, regime=%s, entries=%d, exits=%d, next_check=%dm",
                    res.get("phase", res.get("market_phase", "N/A")),
                    res.get("regime", "IDLE"),
                    len(res.get("entries", [])),
                    len(res.get("exits", [])),
                    res.get("next_sleep_minutes", 5),
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
