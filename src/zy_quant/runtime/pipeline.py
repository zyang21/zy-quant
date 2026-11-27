from __future__ import annotations

import logging
import math
from datetime import date, datetime, time, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from zy_quant.gateway.alpaca_adapter import build_causal_partial_1d_bar
from zy_quant.intel.market_hours import ET_TZ, is_trading_day, session_close_time
from zy_quant.intel.watchlist_scorer import SwingWatchlistScorer
from zy_quant.models import (
    Bar,
    CapitalFlowSnapshot,
    EarningsEvent,
    ExitReason,
    InsiderActivity,
    OpenPosition,
    OrderSide,
    OrderStatus,
    PipelinePhase,
    QuoteSnapshot,
    RegimeLight,
)
from zy_quant.strategy.indicators import compute_wilder_atr_series
from zy_quant.strategy.signals_swing import (
    DailySwingSignalEngine,
    build_strictly_causal_daily_series,
)

if TYPE_CHECKING:
    from zy_quant.runtime.daemon import TradingDaemon

logger = logging.getLogger(__name__)


def resolve_swing_phase(now_utc: datetime) -> PipelinePhase:
    """
    Maps a UTC timestamp onto the deterministic 4-Phase US Daily Swing DAG (`ET_TZ`):
      1. `PRE_OPEN_SYNC`         : 09:15 - 09:30 ET
      2. `OPEN_GAP_GUARD`        : 09:30 - 09:50 ET
      3. `IDLE_GTC_ARMED`        : 09:50 - (close - 25m) ET
      4. `EOD_DECISION_WINDOW`   : (close - 25m) - close ET (15:35-16:00 normal / 12:35-13:00 early-close)
      5. `POST_CLOSE_SETTLEMENT` : close - (close + 20m) ET (16:00-16:20 normal / 13:00-13:20 early-close)
      6. `MARKET_CLOSED`         : off-hours, weekends, and NYSE holidays
    """
    ref = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    now_et = ref.astimezone(ET_TZ)
    d = now_et.date()
    if not is_trading_day(d):
        return PipelinePhase.MARKET_CLOSED

    hm = now_et.hour * 60 + now_et.minute
    close_t = session_close_time(d)
    close_m = close_t.hour * 60 + close_t.minute

    if (9 * 60 + 15) <= hm < (9 * 60 + 30):
        return PipelinePhase.PRE_OPEN_SYNC
    if (9 * 60 + 30) <= hm < (9 * 60 + 50):
        return PipelinePhase.OPEN_GAP_GUARD
    if (9 * 60 + 50) <= hm < (close_m - 25):
        return PipelinePhase.IDLE_GTC_ARMED
    if (close_m - 25) <= hm < close_m:
        return PipelinePhase.EOD_DECISION_WINDOW
    if close_m <= hm <= (close_m + 120):
        return PipelinePhase.POST_CLOSE_SETTLEMENT
    return PipelinePhase.MARKET_CLOSED


class DailySwingPipeline:
    """
    Deterministic, SQLite-checkpointed 4-Phase Daily Swing Execution Pipeline (`swing_1d`).
    Replaces continuous 3-minute intraday polling with idempotent phase execution.
    """

    def __init__(self, daemon: "TradingDaemon") -> None:
        self.daemon = daemon
        self.swing_engine = DailySwingSignalEngine(
            entry_cfg=daemon.settings.strategy_cfg.entry,
            regime_cfg=daemon.settings.strategy_cfg.regime,
            swing_cfg=daemon.settings.strategy_cfg.swing,
        )
        self.swing_scorer = SwingWatchlistScorer(
            cfg=daemon.settings.watchlist_cfg,
            swing_cfg=daemon.settings.strategy_cfg.swing,
        )

    def heal_overnight_gtc_stops(self, now_utc: datetime) -> List[str]:
        """
        GTC Stop-Audit Healer (`PRE_OPEN_SYNC` & `POST_CLOSE_SETTLEMENT`):
          - Distinguishes `FILLED` stops (`dealt_qty > 0`, already settled by `reconcile_open_orders`)
            from exchange ex-dividend / 90-day `CANCELLED / EXPIRED / FAILED` `0`-fill GTC stops!
          - Ensures every open whole-share (`>= 1.0 sh`) position has an active, matching-quantity
            `GTC` stop on Futu's servers, and marks `< 1.0 sh` fractional lots as `CLIENT_FRAC_RTH`.
        """
        healed: List[str] = []
        d = self.daemon
        has_acc = d.gateway.has_target_account()
        broker_map = {}
        if has_acc:
            d.reconcile_open_orders(now=now_utc)
            for upd in d.gateway.sync_open_orders(refresh_cache=True):
                broker_map[upd.broker_order_id] = upd

        daemon_cfg = d.settings.strategy_cfg.daemon
        for sym, pos in d.store.get_positions().items():
            if pos.shares < 1.0:
                expected_frac_id = f"SIM-CLIENT-FRAC-STOP-{sym}"
                if pos.stop_order_type != "CLIENT_FRAC_RTH" or pos.broker_stop_order_id != expected_frac_id:
                    pos.stop_order_type = "CLIENT_FRAC_RTH"
                    pos.broker_stop_order_id = expected_frac_id
                    d.store.upsert_position(pos)
                    healed.append(f"{sym}: armed client-side RTH fractional stop ({pos.shares} sh @ ${pos.current_stop:.2f})")
                continue

            if not daemon_cfg.place_server_side_stop:
                continue

            needs_rearm = False
            expected_whole_qty = float(math.floor(pos.shares))
            oid = pos.broker_stop_order_id

            if not oid:
                needs_rearm = True
            elif has_acc and getattr(d.gateway, "trd_env_name", "SIMULATE") == "REAL":
                b_upd = broker_map.get(oid)
                if b_upd is None:
                    # Missing from broker open/today order list -> re-arm fresh GTC stop!
                    needs_rearm = True
                elif b_upd.status in (OrderStatus.CANCELLED, OrderStatus.EXPIRED, OrderStatus.FAILED):
                    # Includes both 0-fill ex-dividend cancellations AND CANCELLED_PART where remaining shares need protection!
                    needs_rearm = True
                elif abs(b_upd.qty - expected_whole_qty) > 1e-5:
                    # Quantity mismatch after partial scale-out -> modify GTC stop qty immediately!
                    new_oid = d._place_stop_order(sym, pos.shares, pos.current_stop, oid)
                    if new_oid:
                        pos.broker_stop_order_id = new_oid
                        pos.stop_order_type = f"GTC_{daemon_cfg.server_stop_order_type}"
                        d.store.upsert_position(pos)
                        healed.append(f"{sym}: synced GTC stop qty -> {expected_whole_qty:.0f} sh @ ${pos.current_stop:.2f}")
                    continue

            if needs_rearm:
                new_oid = d._place_stop_order(sym, pos.shares, pos.current_stop, None)
                pos.broker_stop_order_id = new_oid
                pos.stop_order_type = f"GTC_{daemon_cfg.server_stop_order_type}"
                d.store.upsert_position(pos)
                healed.append(
                    f"{sym}: re-armed server-side GTC stop ({expected_whole_qty:.0f} sh @ ${pos.current_stop:.2f}, oid={new_oid})"
                )

        return healed

    def execute_fractional_client_stops(self, now_utc: datetime) -> List[str]:
        """
        Executes client-side RTH stop exits for `< 1.0 sh` fractional positions unsupported by
        Futu US server-side `OrderType.STOP` / `GTC`.
        """
        d = self.daemon
        triggered: List[str] = []
        has_acc = d.gateway.has_target_account()
        active_tier_key = d.settings.active_tier_key
        tier = d.settings.tiers_cfg.get_active(active_tier_key)

        today_et = now_utc.astimezone(ET_TZ).date()
        for sym, pos in list(d.store.get_positions().items()):
            if pos.shares >= 1.0:
                continue
            if d.store.get_active_orders(side=OrderSide.SELL, symbol=sym):
                continue

            entry_utc = (
                pos.entry_time.replace(tzinfo=timezone.utc)
                if pos.entry_time.tzinfo is None
                else pos.entry_time.astimezone(timezone.utc)
            )
            raw_15m = d.market_data.get_bars(sym, timeframe="15m", days=2)
            today_15m = [
                b
                for b in raw_15m
                if b.timestamp.astimezone(ET_TZ).date() == today_et and b.timestamp >= entry_utc
            ]
            q = d.market_data.get_quote(sym)
            if today_15m:
                low_px = min(today_15m[-1].low, q.last_price) if q else today_15m[-1].low
                open_px = today_15m[0].open
                live_mark_px = q.bid if (q and q.bid > 0) else today_15m[-1].close
            elif q is not None:
                low_px = q.last_price
                open_px = q.last_price
                live_mark_px = q.bid if q.bid > 0 else q.last_price
            else:
                continue

            if low_px > pos.current_stop:
                continue

            paper_fill_px = round(min(open_px, pos.current_stop) if open_px < pos.current_stop else pos.current_stop, 2)
            # Marketable limit sell price: anchor to live bid/mark (never submit limit $20 above crashing market!)
            marketable_sell_px = round(
                min(live_mark_px, paper_fill_px) * (1.0 - max(5.0, tier.order_protection_bps) / 10000.0),
                2,
            )
            reason = ExitReason.CHANDELIER_TRAIL if pos.current_stop > pos.entry_price else ExitReason.INITIAL_ATR_STOP
            cid = f"FRACSTOP-{sym}-{now_utc.strftime('%Y%m%d%H%M')}"
            note = f"Client-side fractional RTH stop hit (stop=${pos.current_stop:.2f}, mkt=${live_mark_px:.2f})"
            plan_meta = {
                "symbol": sym,
                "entry_price": pos.entry_price,
                "risk_per_share": pos.risk_per_share,
                "setup": pos.setup,
                "entry_time": pos.entry_time.isoformat(),
                "exit_reason": reason.value,
                "is_scale_out": False,
                "tier_key": active_tier_key,
            }
            if not d.store.record_order_intent(
                cid, sym, OrderSide.SELL, pos.shares, marketable_sell_px, note, plan_meta=plan_meta, created_at=now_utc
            ):
                continue

            if has_acc:
                oid = d.gateway.place_limit_order(sym, OrderSide.SELL, pos.shares, marketable_sell_px, remark=cid)
                if d._mark_order_submitted_safely(cid, oid, f"{note} | Broker rejected fractional SELL"):
                    triggered.append(f"{sym} SUBMITTED fractional stop ({pos.shares} sh @ limit ${marketable_sell_px:.2f})")
            else:
                d._settle_paper_local_sell(cid, sym, pos, pos.shares, paper_fill_px, reason.value, active_tier_key, tier)
                triggered.append(f"{sym} fractional stop filled ({pos.shares} sh @ ${paper_fill_px:.2f})")

        return triggered

    def _fetch_260d_universe(self, symbols: List[str]) -> Dict[str, List[Bar]]:
        d = self.daemon
        min_bars = d.settings.strategy_cfg.swing.min_daily_history_bars
        cal_days = max(380, int(min_bars * 1.55))
        all_syms = list(dict.fromkeys(["QQQ", "SPY", *symbols]))
        alpaca_ready = hasattr(d.market_data, "has_credentials") and bool(
            getattr(d.market_data, "has_credentials")()
        )
        bar_cache = getattr(d.market_data, "bar_cache", None)
        wide_cached = bar_cache is not None and bar_cache.stats().get("distinct_symbols", 0) >= 20

        if hasattr(d.market_data, "get_bars_batch") and (alpaca_ready or wide_cached):
            return d.market_data.get_bars_batch(all_syms, timeframe="1d", days=cal_days)  # type: ignore[attr-defined]
        return {s: d.market_data.get_bars(s, timeframe="1d", days=cal_days) for s in all_syms}

    def run_phase(
        self,
        phase: PipelinePhase,
        now_utc: Optional[datetime] = None,
        tier_override: Optional[str] = None,
        force_rerun: bool = False,
        check_edgar: bool = False,
    ) -> Dict[str, Any]:
        """Executes a single deterministic phase of the 4-Phase Daily Swing DAG."""
        d = self.daemon
        now = now_utc or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now_et = now.astimezone(ET_TZ)
        trading_date_iso = now_et.date().isoformat()
        tier_key = tier_override or d.settings.active_tier_key
        tier = d.settings.tiers_cfg.get_active(tier_key)

        if not d.gateway.is_connected() and hasattr(d.gateway, "connect"):
            d.gateway.connect()
        has_acc = d.gateway.has_target_account()

        order_events: List[str] = []
        if has_acc:
            order_events.extend(d.reconcile_open_orders(now=now))

        # Idempotent checkpoint guard for once-per-day phases
        checkpointed_phases = {
            PipelinePhase.PRE_OPEN_SYNC,
            PipelinePhase.OPEN_GAP_GUARD,
            PipelinePhase.EOD_DECISION_WINDOW,
            PipelinePhase.POST_CLOSE_SETTLEMENT,
        }
        if (
            phase in checkpointed_phases
            and not force_rerun
            and d.store.has_completed_pipeline_phase(trading_date_iso, phase.value)
        ):
            frac_exits = (
                self.execute_fractional_client_stops(now)
                if phase in (PipelinePhase.OPEN_GAP_GUARD, PipelinePhase.IDLE_GTC_ARMED)
                else []
            )
            in_flight = len(d.store.get_active_orders())
            return {
                "trading_date": trading_date_iso,
                "phase": phase.value,
                "already_completed": True,
                "order_events": order_events,
                "entries": [],
                "exits": frac_exits,
                "in_flight_orders": in_flight,
                "next_sleep_minutes": 1 if in_flight > 0 else 15,
            }

        summary: Dict[str, Any] = {
            "trading_date": trading_date_iso,
            "phase": phase.value,
            "already_completed": False,
            "order_events": order_events,
            "entries": [],
            "exits": [],
        }

        scan_pool = d._select_quota_safe_universe()
        pool_syms = [u.symbol for u in scan_pool]

        if phase == PipelinePhase.PRE_OPEN_SYNC:
            if has_acc:
                broker_pos = d.gateway.get_open_positions()
                summary["reconciliation"] = d.store.reconcile_broker_positions(broker_pos)
            self._fetch_260d_universe(pool_syms)
            if hasattr(d.market_data, "calibrate_sip_iex_volume_ratios"):
                d.market_data.calibrate_sip_iex_volume_ratios(pool_syms)  # type: ignore[attr-defined]
            earn_events = d.earnings_feed.get_upcoming_events(
                symbols=pool_syms, ref_day=now_et.date(), allow_network=(d.bar_cache is not None)
            )
            if check_edgar or d.bar_cache is not None:
                for u in scan_pool[:16]:
                    d.edgar_scanner.fetch_insider_activity(u.symbol, u.cik)
            healed = self.heal_overnight_gtc_stops(now)
            summary["gtc_healed"] = healed
            summary["upcoming_earnings_count"] = len(earn_events)
            summary["next_sleep_minutes"] = 5
            d.store.record_pipeline_phase(trading_date_iso, phase.value, "COMPLETED", summary)

        elif phase == PipelinePhase.OPEN_GAP_GUARD:
            frac_exits = self.execute_fractional_client_stops(now)
            healed = self.heal_overnight_gtc_stops(now)
            summary["exits"] = frac_exits
            summary["gtc_healed"] = healed
            in_flight = len(d.store.get_active_orders())
            has_frac = any(p.shares < 1.0 for p in d.store.get_positions().values())
            summary["next_sleep_minutes"] = 1 if in_flight > 0 else (15 if has_frac else 30)
            d.store.record_pipeline_phase(trading_date_iso, phase.value, "COMPLETED", summary)

        elif phase == PipelinePhase.IDLE_GTC_ARMED:
            frac_exits = self.execute_fractional_client_stops(now)
            summary["exits"] = frac_exits
            in_flight = len(d.store.get_active_orders())
            has_frac = any(p.shares < 1.0 for p in d.store.get_positions().values())
            summary["next_sleep_minutes"] = 1 if in_flight > 0 else (15 if has_frac else 30)

        elif phase == PipelinePhase.EOD_DECISION_WINDOW:
            summary.update(self._execute_eod_decision_dag(now, now_et.date(), tier_key, tier, pool_syms, check_edgar))
            d.store.record_pipeline_phase(trading_date_iso, phase.value, "COMPLETED", summary)

        elif phase == PipelinePhase.POST_CLOSE_SETTLEMENT:
            # Cancel any dangling DAY limit orders left unfilled past market close
            cancelled_day: List[str] = []
            for ord_row in d.store.get_active_orders():
                b_oid = str(ord_row.get("broker_order_id") or "")
                if b_oid:
                    d.gateway.cancel_order(b_oid)
                d.store.update_order_status(
                    str(ord_row["client_order_id"]),
                    status=OrderStatus.CANCELLED if float(ord_row.get("filled_shares") or 0.0) > 0 else OrderStatus.EXPIRED,
                    reason=f"{ord_row.get('reason') or ''} | Post-close settlement sweep".strip(" |"),
                )
                cancelled_day.append(str(ord_row["client_order_id"]))

            self._fetch_260d_universe(pool_syms)
            if has_acc:
                summary["post_close_reconciliation"] = d.store.reconcile_broker_positions(
                    d.gateway.get_open_positions()
                )
            healed = self.heal_overnight_gtc_stops(now)

            trd_env_name = getattr(d.gateway, "trd_env_name", "SIMULATE")
            open_pos = d.store.get_positions()
            if has_acc and trd_env_name == "REAL":
                acct = d.gateway.get_account_summary()
                equity_usd = acct["equity_usd"] if acct["equity_usd"] > 0 else tier.reference_equity_usd
                cash_usd = acct["cash_usd"]
                ledger_check = {
                    "ok": True,
                    "mode": "REAL_CASH_PLUS",
                    "cash_usd": cash_usd,
                    "fund_assets_usd": acct.get("fund_assets_usd", 0.0),
                }
            else:
                ledger_check = d.store.reconcile_stateful_ledger(tier_key, tier.reference_equity_usd)
                if not ledger_check["ok"]:
                    d.notifier.send(
                        "Ledger Arithmetic Drift Alert",
                        f"Tier {tier_key} drift=${ledger_check['drift_usd']:+.4f} exceeds tolerance",
                        level="ERROR",
                    )
                cash_usd = float(ledger_check["virtual_cash_usd"])
                mkt_val = sum(
                    p.shares * float(p.metadata.get("last_mark_price") or p.entry_price)
                    for p in open_pos.values()
                )
                equity_usd = cash_usd + mkt_val

            heat = d.risk_gate.get_portfolio_heat_snapshot(open_pos, equity_usd)
            d.store.record_daily_nav_snapshot(
                trading_date_iso=trading_date_iso,
                trd_env=trd_env_name,
                tier_key=tier_key,
                equity_usd=equity_usd,
                cash_usd=cash_usd,
                accrued_sweep_yield_usd=0.0,
                principal_heat_pct=heat.principal_heat_pct,
                open_drawdown_heat_pct=heat.open_drawdown_heat_pct,
                open_positions_count=len(open_pos),
            )
            gtc_covered = all(
                (p.shares < 1.0) or bool(p.broker_stop_order_id) for p in open_pos.values()
            )
            d.notifier.send(
                "Daily Swing Settlement Digest",
                f"[{trading_date_iso}] NAV=${equity_usd:,.2f} (Cash=${cash_usd:,.2f}), "
                f"Positions={len(open_pos)}, Heat={heat.principal_heat_pct:.2f}%, GTC_OK={gtc_covered}",
            )
            summary["cancelled_dangling_orders"] = cancelled_day
            summary["gtc_healed"] = healed
            summary["gtc_coverage_100pct"] = gtc_covered
            summary["ledger_reconciliation"] = ledger_check
            summary["portfolio_heat"] = {
                "principal_heat_pct": heat.principal_heat_pct,
                "open_drawdown_heat_pct": heat.open_drawdown_heat_pct,
            }
            summary["next_sleep_minutes"] = 15
            d.store.record_pipeline_phase(trading_date_iso, phase.value, "COMPLETED", summary)

        else:
            summary["next_sleep_minutes"] = 15

        open_pos_now = d.store.get_positions()
        in_flight_now = len(d.store.get_active_orders())
        d.store.set_flag(
            "daemon_heartbeat",
            {
                "alive": True,
                "futu_connected": d.gateway.is_connected(),
                "trd_env": getattr(d.gateway, "trd_env_name", "SIMULATE"),
                "strategy_mode": "swing_1d",
                "pipeline_phase": phase.value,
                "trading_date": trading_date_iso,
                "open_positions": len(open_pos_now),
                "in_flight_orders": in_flight_now,
                "timestamp": now.isoformat(),
            },
        )
        return summary

    def _execute_eod_decision_dag(
        self,
        now_utc: datetime,
        session_date: date,
        tier_key: str,
        tier: Any,
        pool_syms: List[str],
        check_edgar: bool,
    ) -> Dict[str, Any]:
        """
        Phase 3 (`EOD_DECISION_WINDOW`, `15:35-15:55 ET`):
          (a) Build causal `partial_1d_bar` (`09:30 -> decision_ts`) & evaluate Daily Macro + Breadth Regime
          (b) Step open positions: `BMO` (`T-1`) / `AMC` (`T-0`) Pre-Earnings Cushion, Daily Time-Stop,
              and Chandelier `1d` ATR upward ratchets (syncing Futu `GTC STOP` qty & trigger price)
          (c) Rank 100-stock Daily RS watchlist & evaluate Swing Entry Signals with Portfolio Heat,
              `StoplossGuard` watermark, and 60d Pairwise Correlation vs `open_positions | pending_buy_syms`!
        """
        d = self.daemon
        has_acc = d.gateway.has_target_account()
        open_positions = d.store.get_positions()
        held_syms = list(open_positions.keys())
        all_target_syms = list(dict.fromkeys([*pool_syms, *held_syms]))

        # Load >= 260 completed daily bars (`<= T-1` enforced by `build_strictly_causal_daily_series`!)
        daily_bars_map = self._fetch_260d_universe(all_target_syms)
        earn_events = d.earnings_feed.get_upcoming_events(
            symbols=all_target_syms, ref_day=session_date, allow_network=(d.bar_cache is not None)
        )

        # Build causal `partial_1d_bar` for benchmarks & open positions
        alpaca_ready = hasattr(d.market_data, "has_credentials") and bool(
            getattr(d.market_data, "has_credentials")()
        )
        vol_ratios = getattr(d.market_data, "sip_iex_vol_ratios", {})
        quotes: Dict[str, QuoteSnapshot] = {}
        if alpaca_ready and hasattr(d.market_data, "get_snapshots_batch"):
            quotes = d.market_data.get_snapshots_batch(["QQQ", "SPY", *all_target_syms])  # type: ignore[attr-defined]
        else:
            for s in ["QQQ", *all_target_syms]:
                q_snap = d.market_data.get_quote(s)
                if q_snap is not None:
                    quotes[s] = q_snap

        def _make_partial_bar(sym_key: str) -> Optional[Bar]:
            b15 = d.market_data.get_bars(sym_key, timeframe="15m", days=2)
            v_mult = (
                d.market_data.get_effective_15m_volume_scale(sym_key)  # type: ignore[attr-defined]
                if hasattr(d.market_data, "get_effective_15m_volume_scale")
                else 1.0
            )
            p_bar = build_causal_partial_1d_bar(sym_key, b15, decision_ts_utc=now_utc, volume_scale_mult=v_mult)
            if p_bar is not None:
                return p_bar
            q_s = quotes.get(sym_key)
            if q_s is not None:
                anchor = datetime(session_date.year, session_date.month, session_date.day, 12, 0, tzinfo=timezone.utc)
                return Bar(sym_key, anchor, q_s.last_price, q_s.last_price, q_s.last_price, q_s.last_price, max(1.0, q_s.avg_daily_dollar_vol / max(1.0, q_s.last_price)))
            return None

        qqq_partial = _make_partial_bar("QQQ")
        spy_partial = _make_partial_bar("SPY")
        uni_partials = {
            s: (
                Bar(
                    s,
                    datetime(session_date.year, session_date.month, session_date.day, 12, 0, tzinfo=timezone.utc),
                    quotes[s].last_price,
                    quotes[s].last_price,
                    quotes[s].last_price,
                    quotes[s].last_price,
                    1.0,
                )
                if s in quotes
                else None
            )
            for s in all_target_syms
        }

        # --- Step (a): Daily Macro & Breadth Regime ---
        regime = self.swing_engine.regime_detector.evaluate_daily(
            qqq_daily=daily_bars_map.get("QQQ", []),
            spy_daily=daily_bars_map.get("SPY", []),
            universe_daily=daily_bars_map,
            as_of_date=session_date,
            qqq_partial_bar=qqq_partial,
            spy_partial_bar=spy_partial,
            universe_partial_bars=uni_partials,
        )
        d.store.set_flag(
            "latest_regime",
            {
                "light": regime.light.value,
                "mode": "swing_1d",
                "qqq_price": round(regime.qqq_price, 2),
                "qqq_vwap": round(regime.qqq_vwap, 2),
                "qqq_sma20": round(regime.qqq_sma20, 2),
                "qqq_sma50": regime.qqq_sma50,
                "qqq_sma200": regime.qqq_sma200,
                "spy_price": round(regime.spy_price, 2),
                "spy_sma50": regime.spy_sma50,
                "spy_sma200": regime.spy_sma200,
                "breadth_above_sma50_pct": regime.breadth_above_sma50_pct,
                "qqq_intraday_return_pct": round(regime.qqq_intraday_return * 100.0, 2),
                "vix_level": round(regime.vix_level, 2),
                "recommended_scan_minutes": regime.recommended_scan_minutes,
                "reason": regime.reason,
                "evaluated_at": regime.evaluated_at.isoformat(),
            },
        )

        # --- Step (b): Step Open Positions (Pre-Earnings Cushion, Time Stop, Chandelier GTC Ratchet) ---
        exits_triggered: List[str] = []
        for sym, pos in list(open_positions.items()):
            if d.store.get_active_orders(side=OrderSide.SELL, symbol=sym):
                continue
            p_bar = _make_partial_bar(sym)
            causal_d = build_strictly_causal_daily_series(
                daily_bars_map.get(sym, []), as_of_date=session_date, partial_today_bar=p_bar
            )
            if not causal_d:
                continue
            atr_1d = compute_wilder_atr_series(causal_d, 14)[-1]
            today_bar = causal_d[-1]
            is_new_day = pos.metadata.get("last_swing_date") != session_date.isoformat()
            pos.metadata["last_swing_date"] = session_date.isoformat()
            pos.metadata["last_mark_price"] = today_bar.close

            prev_stop = pos.current_stop
            prev_scaled = pos.scaled_out
            prev_be = pos.breakeven_armed
            prev_earn_date = pos.earnings_cushion_applied_date

            action = d.exit_engine.step_daily_position(
                pos=pos,
                daily_bar=today_bar,
                atr_1d=atr_1d,
                tier=tier,
                upcoming_earnings=earn_events.get(sym),
                is_new_day=is_new_day,
            )

            if action.should_exit and action.shares_to_sell > 0:
                is_partial_exit = action.shares_to_sell < pos.shares - 1e-5
                post_scale_stop = pos.current_stop
                cid = f"EXIT-{sym}-{session_date.strftime('%Y%m%d')}-{action.reason.value if action.reason else 'X'}"
                exit_plan_meta = {
                    "symbol": sym,
                    "entry_price": pos.entry_price,
                    "risk_per_share": pos.risk_per_share,
                    "setup": pos.setup,
                    "entry_time": pos.entry_time.isoformat(),
                    "exit_reason": action.reason.value if action.reason else "EXIT",
                    "is_scale_out": is_partial_exit,
                    "post_scale_stop": post_scale_stop,
                    "earnings_cushion_date": pos.earnings_cushion_applied_date,
                    "tier_key": tier_key,
                }
                if d.store.record_order_intent(
                    cid, sym, OrderSide.SELL, action.shares_to_sell, action.exit_price, action.note, plan_meta=exit_plan_meta, created_at=now_utc
                ):
                    if has_acc:
                        if is_partial_exit:
                            pos.scaled_out = prev_scaled
                            pos.current_stop = prev_stop
                            pos.breakeven_armed = prev_be
                            pos.earnings_cushion_applied_date = prev_earn_date
                        elif pos.broker_stop_order_id:
                            d.gateway.cancel_order(pos.broker_stop_order_id)
                            pos.broker_stop_order_id = None
                        d.store.upsert_position(pos)
                        # Marketable sell limit anchored to live price so stop exits never hang above market!
                        q_live = quotes.get(sym)
                        live_ref = q_live.bid if (q_live and q_live.bid > 0) else today_bar.close
                        sell_lim_px = round(
                            min(live_ref, action.exit_price) * (1.0 - max(4.0, tier.order_protection_bps) / 10000.0),
                            2,
                        )
                        b_oid = d.gateway.place_limit_order(
                            sym, OrderSide.SELL, action.shares_to_sell, sell_lim_px, remark=cid
                        )
                        if d._mark_order_submitted_safely(cid, b_oid, f"{action.note} | Broker rejected SELL"):
                            exits_triggered.append(f"{sym} SUBMITTED ({action.note})")
                    else:
                        d._settle_paper_local_sell(
                            cid, sym, pos, action.shares_to_sell, action.exit_price, action.reason.value if action.reason else "EXIT", tier_key, tier
                        )
                        exits_triggered.append(f"{sym} ({action.note})")
            else:
                if pos.current_stop > prev_stop and d.settings.strategy_cfg.daemon.place_server_side_stop:
                    pos.broker_stop_order_id = d._place_stop_order(
                        sym, pos.shares, pos.current_stop, pos.broker_stop_order_id
                    )
                d.store.upsert_position(pos)

        # --- Step (c): 100-Stock Daily RS Ranking & Swing Entry Signals ---
        insiders: Dict[str, InsiderActivity] = {}
        flows: Dict[str, CapitalFlowSnapshot] = {}
        if check_edgar:
            for u in d._select_quota_safe_universe():
                insiders[u.symbol] = d.edgar_scanner.fetch_insider_activity(u.symbol, u.cik)

        scored_candidates = self.swing_scorer.rank_swing_universe(
            quotes=quotes,
            daily_bars_map=daily_bars_map,
            qqq_daily_bars=daily_bars_map.get("QQQ", []),
            regime_light=regime.light,
            as_of_date=session_date,
            capital_flows=flows,
            insiders=insiders,
            earnings_events=earn_events,
            qqq_partial_bar=qqq_partial,
        )
        d.store.save_watchlist_scores(scored_candidates, prune_stale=True)
        top_candidates = self.swing_scorer.select_active_top_n(d.store.get_watchlist_scores())

        entries_placed: List[str] = []
        open_positions = d.store.get_positions()
        if regime.light != RegimeLight.RED:
            trd_env_name = getattr(d.gateway, "trd_env_name", "SIMULATE")
            active_buys = d.store.get_active_orders(side=OrderSide.BUY)
            in_flight_cash = sum(
                max(0.0, float(o.get("shares") or 0.0) - float(o.get("filled_shares") or 0.0))
                * float(o.get("limit_price") or 0.0)
                for o in active_buys
            )
            if has_acc and trd_env_name == "REAL":
                acct = d.gateway.get_account_summary()
                equity = acct["equity_usd"] if acct["equity_usd"] > 0 else tier.reference_equity_usd
                cash = max(0.0, acct["cash_usd"] - in_flight_cash)
            else:
                v_cash = d.store.get_virtual_cash(tier_key, tier.reference_equity_usd)
                mkt_val = sum(
                    p.shares * float(p.metadata.get("last_mark_price") or p.entry_price)
                    for p in open_positions.values()
                )
                equity = v_cash + mkt_val
                cash = max(0.0, v_cash - in_flight_cash)

            pos_scale = (
                1.0
                if regime.light == RegimeLight.GREEN
                else d.settings.strategy_cfg.regime.yellow_position_scale
            )
            pending_buy_syms = {str(o["symbol"]).upper() for o in active_buys}

            for item in top_candidates:
                sym = str(item["symbol"]).upper()
                if sym in open_positions or sym in pending_buy_syms:
                    continue
                p_bar = _make_partial_bar(sym)
                flow = d.gateway.get_capital_flow(sym) if d.gateway.is_connected() else None
                sig = self.swing_engine.evaluate_entry(
                    symbol=sym,
                    sector=str(item["sector"]),
                    regime_light=regime.light,
                    watchlist_score=float(item["total_score"]),
                    stock_daily_bars=daily_bars_map.get(sym, []),
                    qqq_daily_bars=daily_bars_map.get("QQQ", []),
                    as_of_date=session_date,
                    partial_today_bar=p_bar,
                    qqq_partial_bar=qqq_partial,
                    capital_flow=flow,
                    recent_earnings=earn_events.get(sym),
                )
                if sig is None:
                    continue

                plan, plan_status = d.exit_engine.build_swing_position_plan(
                    signal=sig,
                    account_equity_usd=equity,
                    available_cash_usd=max(0.0, cash),
                    tier=tier,
                    position_scale=pos_scale,
                )
                if plan is None or plan_status != "APPROVED":
                    continue

                gate = d.risk_gate.check_entry(
                    plan=plan,
                    tier=tier,
                    account_equity_usd=equity,
                    open_positions=open_positions,
                    now=now_utc,
                    daily_bars_map=daily_bars_map,
                    as_of_date=session_date,
                    extra_pending_symbols=pending_buy_syms,
                    enforce_swing_heat=True,
                    corr_lookback_days=d.settings.strategy_cfg.swing.correlation_lookback_days,
                )
                if not gate.allowed:
                    logger.info("Swing Risk Gate blocked %s: %s", sym, gate.reason)
                    continue

                cid = f"SWBUY-{sym}-{session_date.strftime('%Y%m%d')}"
                entry_meta = {
                    "symbol": sym,
                    "sector": plan.sector,
                    "planned_shares": plan.shares,
                    "planned_entry_price": plan.entry_price,
                    "limit_price": plan.limit_price,
                    "initial_stop": plan.initial_stop,
                    "risk_per_share": plan.risk_per_share,
                    "entry_atr_1d": sig.atr_14,
                    "setup": plan.setup.value,
                    "expectancy_r": plan.expectancy_r,
                    "last_bar_ts": sig.timestamp.isoformat(),
                    "tier_key": tier_key,
                }
                if not d.store.record_order_intent(
                    cid, sym, OrderSide.BUY, plan.shares, plan.limit_price, sig.rationale, plan_meta=entry_meta, created_at=now_utc
                ):
                    continue

                est_buy_fee = d.fee_model.estimate_leg(
                    plan.shares, plan.entry_price, OrderSide.BUY
                ).total_friction_usd

                if has_acc:
                    b_oid = d.gateway.place_limit_order(
                        sym, OrderSide.BUY, plan.shares, plan.limit_price, remark=cid
                    )
                    if not d._mark_order_submitted_safely(
                        cid, b_oid, f"{sig.rationale} | Broker rejected BUY"
                    ):
                        continue
                    pending_buy_syms.add(sym)
                    cash -= plan.shares * plan.limit_price + est_buy_fee
                    entries_placed.append(
                        f"{sym} SUBMITTED x{plan.shares} @ limit ${plan.limit_price:.2f} (oid={b_oid})"
                    )
                    open_positions = d.store.get_positions()
                else:
                    d._settle_paper_local_buy(cid, sym, plan, sig, est_buy_fee, now_utc, tier_key, tier)
                    open_positions = d.store.get_positions()
                    pending_buy_syms.add(sym)
                    cash -= plan.notional_usd + est_buy_fee
                    entries_placed.append(f"{sym} x{plan.shares} @ ${plan.entry_price:.2f}")

        in_flight = len(d.store.get_active_orders())
        return {
            "regime": regime.light.value,
            "reason": regime.reason,
            "breadth_above_sma50_pct": regime.breadth_above_sma50_pct,
            "watchlist_refreshed": True,
            "entries": entries_placed,
            "exits": exits_triggered,
            "in_flight_orders": in_flight,
            "next_sleep_minutes": 1 if in_flight > 0 else 5,
        }

    def run_due_phase(
        self,
        now_utc: Optional[datetime] = None,
        tier_override: Optional[str] = None,
        force_eod_when_closed: bool = False,
        check_edgar: bool = False,
    ) -> Dict[str, Any]:
        now = now_utc or datetime.now(timezone.utc)
        phase = resolve_swing_phase(now)
        if phase == PipelinePhase.MARKET_CLOSED and force_eod_when_closed:
            phase = PipelinePhase.EOD_DECISION_WINDOW
            return self.run_phase(
                phase, now_utc=now, tier_override=tier_override, force_rerun=True, check_edgar=check_edgar
            )
        return self.run_phase(
            phase, now_utc=now, tier_override=tier_override, force_rerun=False, check_edgar=check_edgar
        )
