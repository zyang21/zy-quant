from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict

from zy_quant.config import DaemonConfig, TierProfile
from zy_quant.models import OpenPosition, OrderSide, PositionPlan
from zy_quant.storage.state_store import StateStore


@dataclass
class RiskDecision:
    allowed: bool
    reason: str


class MandateRiskGate:
    """
    Isolated Fail-Closed Pre-Trade Risk Gate (inspired by HKUDS/Vibe-Trading mandate governance):
      1. Global Kill-Switch check (persisted in SQLite across restarts)
      2. Runaway Loop Breaker (if orders in last 60s >= max_orders_per_minute -> auto-latch HALT)
      3. Daily Realized Loss Circuit Breaker (-2.0% / tier max_daily_loss_pct)
      4. Daily & Weekly Trade Frequency Caps (protects $1k/$10k accounts from fee bleed)
      5. Max Concurrent Positions & Duplicate Symbol Guard
      6. Sector Concentration Cap (e.g. <= 40% in Tier 3 $100k)
    """

    def __init__(self, store: StateStore, daemon_cfg: DaemonConfig) -> None:
        self.store = store
        self.daemon_cfg = daemon_cfg

    def check_entry(
        self,
        plan: PositionPlan,
        tier: TierProfile,
        account_equity_usd: float,
        open_positions: Dict[str, OpenPosition],
        now: datetime | None = None,
    ) -> RiskDecision:
        ref_now = now or datetime.now(timezone.utc)

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

        # 3. Daily Max Drawdown Circuit Breaker
        day_start = ref_now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        daily_pnl = self.store.realized_pnl_since(day_start)
        max_loss_usd = -abs(account_equity_usd * tier.max_daily_loss_pct)
        if daily_pnl <= max_loss_usd:
            return RiskDecision(
                False,
                f"Daily loss circuit breaker active (${daily_pnl:.2f} <= ${max_loss_usd:.2f})",
            )

        # 4. Daily & Weekly Trade Frequency Caps (anti-fee-churn)
        buys_today = self.store.count_orders_since(day_start, side=OrderSide.BUY)
        if buys_today >= tier.max_trades_per_day:
            return RiskDecision(
                False,
                f"Daily trade frequency cap reached ({buys_today}/{tier.max_trades_per_day})",
            )

        week_start = (ref_now - timedelta(days=7)).isoformat()
        buys_week = self.store.count_orders_since(week_start, side=OrderSide.BUY)
        if buys_week >= tier.max_trades_per_week:
            return RiskDecision(
                False,
                f"Weekly trade frequency cap reached ({buys_week}/{tier.max_trades_per_week}) to preserve fee budget",
            )

        # 5. Position Slot & Duplicate Symbol Check
        if plan.symbol in open_positions:
            return RiskDecision(False, f"Position already open in {plan.symbol}")
        if len(open_positions) >= tier.max_concurrent_positions:
            return RiskDecision(
                False,
                f"Max concurrent positions reached ({len(open_positions)}/{tier.max_concurrent_positions})",
            )

        # 6. Sector Concentration Cap (vital for $100k multi-position portfolio)
        sector_notional = sum(
            p.shares * p.entry_price
            for p in open_positions.values()
            if p.sector == plan.sector
        )
        projected_sector_pct = (sector_notional + plan.notional_usd) / max(1.0, account_equity_usd)
        if projected_sector_pct > tier.max_sector_weight_pct + 1e-6:
            return RiskDecision(
                False,
                f"Sector '{plan.sector}' cap exceeded ({projected_sector_pct*100:.1f}% > {tier.max_sector_weight_pct*100:.0f}%)",
            )

        return RiskDecision(True, "APPROVED")
