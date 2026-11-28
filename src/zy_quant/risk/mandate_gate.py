from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from zy_quant.config import DaemonConfig, TierProfile
from zy_quant.intel.market_hours import (
    ET_TZ,
    add_trading_days,
    count_trading_days_between,
)
from zy_quant.models import Bar, OpenPosition, OrderSide, PositionPlan
from zy_quant.risk.correlation import compute_max_pairwise_correlation
from zy_quant.storage.state_store import StateStore


@dataclass
class RiskDecision:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class PortfolioHeatSnapshot:
    principal_heat_usd: float
    principal_heat_pct: float
    open_drawdown_heat_usd: float
    open_drawdown_heat_pct: float
    in_flight_risk_usd: float


def compute_portfolio_heat(
    open_positions: Dict[str, OpenPosition],
    active_buys: Sequence[Dict[str, Any]],
    account_equity_usd: float,
) -> PortfolioHeatSnapshot:
    """
    Computes dual Portfolio Heat metrics:
      1. Principal Heat: `sum(max(0, entry_price - current_stop) * shares)` + in-flight BUY risk.
         Once a winning position ratchets `current_stop >= entry_price` (breakeven armed),
         its Principal Heat drops to $0.00 (0%), freeing risk budget for new trend leaders!
      2. Open-Equity Drawdown Heat: `sum(max(0, mark_price - current_stop) * shares)` + in-flight risk.
    """
    eq = max(1.0, account_equity_usd)
    principal_usd = 0.0
    drawdown_usd = 0.0

    for p in open_positions.values():
        principal_usd += max(0.0, (p.entry_price - p.current_stop) * p.shares)
        mark_px = float(p.metadata.get("last_mark_price") or p.highest_price or p.entry_price)
        drawdown_usd += max(0.0, (mark_px - p.current_stop) * p.shares)

    in_flight_risk = 0.0
    for o in active_buys:
        rem_sh = max(0.0, float(o.get("shares") or 0.0) - float(o.get("filled_shares") or 0.0))
        if rem_sh <= 0:
            continue
        try:
            p_meta = json.loads(o.get("plan_json") or "{}")
        except (TypeError, ValueError):
            p_meta = {}
        rps = float(p_meta.get("risk_per_share") or 0.0)
        if rps <= 0:
            init_stop = float(p_meta.get("initial_stop") or 0.0)
            lim_px = float(o.get("limit_price") or 0.0)
            rps = max(0.0, lim_px - init_stop) if init_stop > 0 else lim_px * 0.03
        in_flight_risk += rem_sh * rps

    tot_principal = principal_usd + in_flight_risk
    tot_drawdown = drawdown_usd + in_flight_risk
    return PortfolioHeatSnapshot(
        principal_heat_usd=round(tot_principal, 2),
        principal_heat_pct=round((tot_principal / eq) * 100.0, 2),
        open_drawdown_heat_usd=round(tot_drawdown, 2),
        open_drawdown_heat_pct=round((tot_drawdown / eq) * 100.0, 2),
        in_flight_risk_usd=round(in_flight_risk, 2),
    )


class MandateRiskGate:
    """
    Isolated Fail-Closed Pre-Trade Risk Gate (inspired by HKUDS/Vibe-Trading & Freqtrade):
      1. Global Kill-Switch check (persisted in SQLite across restarts)
      2. Runaway Loop Breaker (if orders in last 60s >= max_orders_per_minute -> auto-latch HALT)
      3. Daily Realized Loss Circuit Breaker (-2.0% / tier max_daily_loss_pct, anchored to ET_TZ)
      4. `StoplossGuard` Whipsaw Cooldown (>= 3 distinct `(symbol, entry_time)` stops in 10 ET trading
         days after `cooldown_triggered_at` watermark -> 5 ET trading-day hard entry cooldown)
      5. Daily & Weekly Trade Frequency Caps + Duplicate / Slot Locks (including in-flight orders)
      6. Portfolio Heat Gate (Principal Heat & Open-Equity Drawdown Heat) + Single-Equity (<=25%)
         / Sector_ETF (<=35%) / Sector Concentration Caps
      7. 60-Day Pairwise Return Correlation Guard (`max_pair_corr <= 0.75` vs `open | pending_buys`)
    """

    def __init__(self, store: StateStore, daemon_cfg: DaemonConfig) -> None:
        self.store = store
        self.daemon_cfg = daemon_cfg

    def evaluate_stoploss_guard(self, ref_now: datetime) -> tuple[bool, str, Dict[str, Any]]:
        cfg = self.daemon_cfg.stoploss_guard
        if not cfg.enabled:
            return True, "DISABLED", {"active": False}

        today_et = ref_now.astimezone(ET_TZ).date()
        state = self.store.get_flag("stoploss_guard_state") or {}
        cooldown_until_str = state.get("cooldown_until_date")
        watermark_iso = str(state.get("cooldown_triggered_at") or "")

        if cooldown_until_str:
            until_d = date.fromisoformat(str(cooldown_until_str))
            if today_et <= until_d:
                return (
                    False,
                    f"StoplossGuard cooldown active through {until_d.isoformat()} ET",
                    {
                        "active": True,
                        "cooldown_until_date": until_d.isoformat(),
                        "cooldown_triggered_at": watermark_iso,
                    },
                )

        # Query distinct `(symbol, entry_time)` INITIAL_ATR_STOP episodes strictly after watermark!
        episodes = self.store.list_stop_out_episodes_since(after_iso=watermark_iso)
        recent_eps: List[Dict[str, Any]] = []
        for ep in episodes:
            exit_dt = datetime.fromisoformat(str(ep["exit_time"]))
            if exit_dt.tzinfo is None:
                exit_dt = exit_dt.replace(tzinfo=timezone.utc)
            exit_d_et = exit_dt.astimezone(ET_TZ).date()
            if exit_d_et <= today_et and count_trading_days_between(exit_d_et, today_et) <= cfg.lookback_trading_days:
                recent_eps.append(ep)

        if len(recent_eps) >= cfg.max_stops_in_window:
            cooldown_until = add_trading_days(today_et, cfg.cooldown_trading_days)
            # Advance watermark past all consumed episodes so Day 6 never re-triggers on prior stops!
            new_watermark = max(str(ep["exit_time"]) for ep in recent_eps)
            new_state = {
                "active": True,
                "cooldown_until_date": cooldown_until.isoformat(),
                "cooldown_triggered_at": new_watermark,
                "triggered_episodes": len(recent_eps),
            }
            self.store.set_flag("stoploss_guard_state", new_state)
            return (
                False,
                f"StoplossGuard triggered ({len(recent_eps)} distinct stops in {cfg.lookback_trading_days}td) "
                f"-> hard cooldown through {cooldown_until.isoformat()} ET",
                new_state,
            )

        return (
            True,
            "OK",
            {
                "active": False,
                "recent_stop_episodes": len(recent_eps),
                "cooldown_triggered_at": watermark_iso,
            },
        )

    def get_portfolio_heat_snapshot(
        self,
        open_positions: Dict[str, OpenPosition],
        account_equity_usd: float,
    ) -> PortfolioHeatSnapshot:
        active_buys = self.store.get_active_orders(side=OrderSide.BUY)
        return compute_portfolio_heat(open_positions, active_buys, account_equity_usd)

    def check_entry(
        self,
        plan: PositionPlan,
        tier: TierProfile,
        account_equity_usd: float,
        open_positions: Dict[str, OpenPosition],
        now: datetime | None = None,
        daily_bars_map: Optional[Mapping[str, Sequence[Bar]]] = None,
        as_of_date: Optional[date] = None,
        extra_pending_symbols: Optional[Iterable[str]] = None,
        enforce_swing_heat: bool = False,
        corr_lookback_days: int = 60,
    ) -> RiskDecision:
        ref_now = now or datetime.now(timezone.utc)
        if ref_now.tzinfo is None:
            ref_now = ref_now.replace(tzinfo=timezone.utc)

        # 1. Global Kill-Switch
        halt = self.store.get_halt_state()
        if halt.get("halted"):
            return RiskDecision(False, f"Global Kill-Switch HALTED: {halt.get('reason')}")

        # 2. Runaway Order Burst Circuit Breaker (1-minute window)
        one_min_ago = (ref_now - timedelta(seconds=60)).isoformat()
        recent_burst = self.store.count_orders_since(one_min_ago)
        if recent_burst >= self.daemon_cfg.max_orders_per_minute:
            msg = f"Auto-HALT: {recent_burst} orders within 60s exceeded limit ({self.daemon_cfg.max_orders_per_minute})"
            self.store.set_halted(True, msg)
            return RiskDecision(False, msg)

        # 3. Daily Max Drawdown Circuit Breaker (anchored to ET_TZ trading session midnight)
        et_today = ref_now.astimezone(ET_TZ).date()
        et_day_start_utc = datetime.combine(et_today, time(0, 0), tzinfo=ET_TZ).astimezone(timezone.utc)
        day_start = et_day_start_utc.isoformat()
        daily_pnl = self.store.realized_pnl_since(day_start)
        max_loss_usd = -abs(account_equity_usd * tier.max_daily_loss_pct)
        if daily_pnl <= max_loss_usd:
            return RiskDecision(
                False,
                f"Daily loss circuit breaker active (${daily_pnl:.2f} <= ${max_loss_usd:.2f})",
            )

        # 4. StoplossGuard Whipsaw Cooldown (distinct episodes + watermark)
        sl_ok, sl_reason, _ = self.evaluate_stoploss_guard(ref_now)
        if not sl_ok:
            return RiskDecision(False, sl_reason)

        # 5. Daily & Weekly Trade Frequency Caps (0-fill expired orders do not burn cap)
        buys_today = self.store.count_trade_budget_orders_since(day_start, side=OrderSide.BUY)
        if buys_today >= tier.max_trades_per_day:
            return RiskDecision(
                False,
                f"Daily trade frequency cap reached ({buys_today}/{tier.max_trades_per_day})",
            )

        week_start = (ref_now - timedelta(days=7)).isoformat()
        buys_week = self.store.count_trade_budget_orders_since(week_start, side=OrderSide.BUY)
        if buys_week >= tier.max_trades_per_week:
            return RiskDecision(
                False,
                f"Weekly trade frequency cap reached ({buys_week}/{tier.max_trades_per_week}) to preserve fee budget",
            )

        # 6. Position Slot & Duplicate Symbol Check (locks in-flight SUBMITTED/PARTIAL_FILLED buy orders!)
        active_buys = self.store.get_active_orders(side=OrderSide.BUY)
        pending_buy_symbols = {str(o["symbol"]).upper() for o in active_buys}
        if extra_pending_symbols:
            pending_buy_symbols |= {s.upper() for s in extra_pending_symbols}

        if plan.symbol in open_positions:
            return RiskDecision(False, f"Position already open in {plan.symbol}")
        if plan.symbol.upper() in pending_buy_symbols:
            return RiskDecision(
                False, f"In-flight BUY order already pending/submitted in {plan.symbol}"
            )

        occupied_symbols = {s.upper() for s in open_positions.keys()} | pending_buy_symbols
        if len(occupied_symbols) >= tier.max_concurrent_positions:
            return RiskDecision(
                False,
                f"Max concurrent slots reached ({len(occupied_symbols)}/{tier.max_concurrent_positions} including in-flight orders)",
            )

        # 7. Portfolio Heat & Single-Name / Sector Concentration Caps
        eq = max(1.0, account_equity_usd)
        if enforce_swing_heat or tier.enforce_swing_caps:
            is_etf = plan.sector.endswith("_ETF")
            single_cap = (
                min(tier.max_position_pct, tier.max_sector_etf_pct if is_etf else tier.max_single_equity_pct)
                if (tier.enforce_swing_caps or tier.max_concurrent_positions >= 3)
                else tier.max_position_pct
            )
            if (plan.notional_usd / eq) > single_cap + 1e-6:
                return RiskDecision(
                    False,
                    f"Single-asset cap exceeded for {plan.symbol} ({plan.notional_usd/eq*100:.1f}% > {single_cap*100:.0f}%)",
                )

            heat = compute_portfolio_heat(open_positions, active_buys, eq)
            proj_principal_pct = (heat.principal_heat_usd + plan.total_risk_usd) / eq
            if proj_principal_pct > tier.max_portfolio_heat_pct + 1e-6:
                return RiskDecision(
                    False,
                    f"Portfolio Principal Heat cap exceeded ({proj_principal_pct*100:.2f}% > {tier.max_portfolio_heat_pct*100:.1f}%)",
                )
            proj_dd_pct = (heat.open_drawdown_heat_usd + plan.total_risk_usd) / eq
            if proj_dd_pct > tier.max_drawdown_heat_pct + 1e-6:
                return RiskDecision(
                    False,
                    f"Portfolio Open-Equity Heat cap exceeded ({proj_dd_pct*100:.2f}% > {tier.max_drawdown_heat_pct*100:.1f}%)",
                )

        sector_notional = sum(
            p.shares * p.entry_price
            for p in open_positions.values()
            if p.sector == plan.sector
        )
        for o in active_buys:
            try:
                p_meta = json.loads(o.get("plan_json") or "{}")
            except (TypeError, ValueError):
                p_meta = {}
            if p_meta.get("sector") == plan.sector:
                rem_sh = max(
                    0.0, float(o.get("shares") or 0.0) - float(o.get("filled_shares") or 0.0)
                )
                sector_notional += rem_sh * float(o.get("limit_price") or 0.0)

        projected_sector_pct = (sector_notional + plan.notional_usd) / eq
        if projected_sector_pct > tier.max_sector_weight_pct + 1e-6:
            return RiskDecision(
                False,
                f"Sector '{plan.sector}' cap exceeded ({projected_sector_pct*100:.1f}% > {tier.max_sector_weight_pct*100:.0f}%)",
            )

        # 8. 60-Day Pairwise Return Correlation Guard against `open_positions | pending_buy_symbols`
        if daily_bars_map and occupied_symbols:
            eval_d = as_of_date or et_today
            max_corr, peer_sym = compute_max_pairwise_correlation(
                candidate_symbol=plan.symbol,
                comparison_symbols=occupied_symbols,
                daily_bars_map=daily_bars_map,
                as_of_date=eval_d,
                lookback_days=corr_lookback_days,
            )
            if max_corr is not None and max_corr > tier.max_pair_corr:
                return RiskDecision(
                    False,
                    f"60d correlation guard rejected {plan.symbol}: corr={max_corr:.2f} with {peer_sym} > {tier.max_pair_corr:.2f}",
                )

        return RiskDecision(True, "APPROVED")
