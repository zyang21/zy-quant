from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Tuple

from zy_quant.config import ExitModelsConfig, TierProfile
from zy_quant.fees import FutuUSFeeModel
from zy_quant.models import (
    Bar,
    EntrySignal,
    ExitReason,
    OpenPosition,
    PositionPlan,
)


@dataclass
class ExitAction:
    should_exit: bool
    shares_to_sell: float
    exit_price: float
    reason: Optional[ExitReason]
    updated_stop: float
    note: str = ""


class ScientificExitAndSizingEngine:
    """
    Step 4: Mathematical Position Sizing & Exit Engine
    Implements the 4 quantitative models:
      1. Wilder ATR Stop: S_init = P_entry - k_sl * ATR_14 (capped at max_hard_stop_pct)
      2. Chandelier Trailing Exit (Chuck LeBeau):
         S_trail(t) = max_{tau <= t}(H_tau) - k_trail * ATR_14(t), with monotonic upward ratchet
         + Breakeven-plus-fees lift once unrealized >= +1.5R
      3. Triple-Barrier Time Stop (Lopez de Prado):
         Exit stagnant trades if bars_held >= time_stop_bars_15m and unrealized < +0.30R
      4. Positive Expectancy & Fixed-Fractional Sizing:
         E = p * R - (1 - p) * 1 - (RoundTripFriction / TotalRiskUSD) > min_expectancy_r
    """

    def __init__(self, cfg: ExitModelsConfig, fee_model: FutuUSFeeModel) -> None:
        self.cfg = cfg
        self.fee_model = fee_model

    def build_position_plan(
        self,
        signal: EntrySignal,
        account_equity_usd: float,
        available_cash_usd: float,
        tier: TierProfile,
        position_scale: float = 1.0,
    ) -> Tuple[Optional[PositionPlan], str]:
        px = signal.entry_price
        if px <= 0 or signal.atr_14 <= 0:
            return None, "Invalid entry price or ATR"

        scale = max(0.25, min(1.0, position_scale))

        # 1. Wilder ATR Initial Stop Distance with noise-band floor (min_stop_pct) & safety ceiling
        raw_stop_dist = max(self.cfg.atr_stop_multiplier * signal.atr_14, px * self.cfg.min_stop_pct)
        max_stop_dist = px * self.cfg.max_hard_stop_pct
        risk_per_share = min(raw_stop_dist, max_stop_dist)
        initial_stop = round(px - risk_per_share, 2)
        risk_per_share = max(0.01, px - initial_stop)

        # Expected multi-bar reward based strictly on actual ATR volatility (never inflated by min_stop_pct)
        expected_move_usd = self.cfg.expected_reward_atr_multiple * signal.atr_14
        expected_move_pct = expected_move_usd / px
        if expected_move_pct < tier.min_expected_move_pct:
            return (
                None,
                f"Expected move {expected_move_pct*100:.2f}% < tier minimum {tier.min_expected_move_pct*100:.2f}%",
            )

        # 2. Fixed-Fractional Risk Sizing scaled by Regime Conviction (`scale`)
        max_risk_budget_usd = account_equity_usd * tier.risk_per_trade_pct * scale
        max_notional_usd = min(
            available_cash_usd,
            account_equity_usd * tier.max_position_pct * scale,
        )

        shares_by_risk = max_risk_budget_usd / risk_per_share
        shares_by_notional = max_notional_usd / px
        raw_shares = min(shares_by_risk, shares_by_notional)

        if raw_shares >= 1.0:
            shares = float(math.floor(raw_shares))
        elif tier.allow_fractional_shares and raw_shares * px >= tier.min_order_notional_usd:
            # Futu <1 share fractional optimization for Tier 1 ($1,000 starter account on high-priced leaders)
            shares = round(min(0.99, raw_shares), 4)
        else:
            return None, f"Insufficient buying power for >=1 share or min notional (${tier.min_order_notional_usd:.0f})"

        notional_usd = shares * px
        if notional_usd < tier.min_order_notional_usd:
            return (
                None,
                f"Order notional ${notional_usd:.2f} < tier floor ${tier.min_order_notional_usd:.2f}",
            )

        # 3. Exact Futu Round-Trip Friction & Edge-to-Fee Ratio Check
        projected_target_px = px + expected_move_usd
        rt = self.fee_model.estimate_round_trip(
            shares=shares,
            entry_price=px,
            exit_price=projected_target_px,
            scale_out=tier.enable_scale_out,
        )
        friction_usd = rt["total_friction_usd"]
        expected_gross_profit_usd = expected_move_usd * shares

        if expected_gross_profit_usd < tier.edge_to_fee_multiple * friction_usd:
            return (
                None,
                f"Edge-to-fee gate rejected: expected gross ${expected_gross_profit_usd:.2f} "
                f"< {tier.edge_to_fee_multiple:.1f}x friction (${friction_usd:.2f})",
            )

        # 4. Mathematical Expectancy Formula: E = p*R - (1-p)*1 - (Friction / Risk_USD)
        total_risk_usd = max(0.01, risk_per_share * shares)
        reward_r = expected_move_usd / risk_per_share
        p = self.cfg.assumed_base_win_rate
        expectancy_r = (p * reward_r) - ((1.0 - p) * 1.0) - (friction_usd / total_risk_usd)

        if expectancy_r < self.cfg.min_expectancy_r:
            return (
                None,
                f"Mathematical expectancy {expectancy_r:.2f}R < required +{self.cfg.min_expectancy_r:.2f}R",
            )

        limit_px = round(px * (1.0 + tier.order_protection_bps / 10000.0), 2)
        return (
            PositionPlan(
                symbol=signal.symbol,
                sector=signal.sector,
                shares=shares,
                entry_price=round(px, 2),
                limit_price=limit_px,
                initial_stop=initial_stop,
                risk_per_share=round(risk_per_share, 4),
                total_risk_usd=round(total_risk_usd, 2),
                notional_usd=round(notional_usd, 2),
                estimated_round_trip_fee_usd=round(friction_usd, 2),
                expected_reward_r=round(reward_r, 2),
                expectancy_r=round(expectancy_r, 3),
                setup=signal.setup,
            ),
            "APPROVED",
        )

    def step_position(
        self,
        pos: OpenPosition,
        bar: Bar,
        atr_14: float,
        tier: TierProfile,
        is_new_bar: bool = True,
        is_eod_window: bool = False,
    ) -> ExitAction:
        """
        Updates ratcheted stop and checks all barriers + Futu HK T+0 EOD/Friday shield + partial scale-out.
        Modifies `pos` in place and returns an `ExitAction`.
        """
        if is_new_bar:
            pos.bars_held_15m += 1

        # 1. Lower Barrier Check FIRST (conservative intrabar assumption: check low against active stop)
        if bar.low <= pos.current_stop:
            fill_px = min(bar.open, pos.current_stop) if bar.open < pos.current_stop else pos.current_stop
            if pos.current_stop > pos.entry_price:
                locked_r = (pos.current_stop - pos.entry_price) / max(0.01, pos.risk_per_share)
                reason = (
                    ExitReason.CHANDELIER_TRAIL
                    if locked_r >= 0.25 or (pos.highest_price - pos.entry_price) >= 1.5 * pos.risk_per_share
                    else ExitReason.BREAKEVEN_STOP
                )
            else:
                reason = ExitReason.INITIAL_ATR_STOP
            return ExitAction(
                should_exit=True,
                shares_to_sell=pos.shares,
                exit_price=round(fill_px, 2),
                reason=reason,
                updated_stop=pos.current_stop,
                note=f"Stop barrier hit at ${fill_px:.2f} ({reason.value})",
            )

        # Update peak price reached since entry
        if bar.high > pos.highest_price:
            pos.highest_price = bar.high

        peak_r = (pos.highest_price - pos.entry_price) / max(0.01, pos.risk_per_share)
        curr_r = (bar.close - pos.entry_price) / max(0.01, pos.risk_per_share)

        # 2. Partial Scale-Out (Tier 2 $10k & Tier 3 $100k on Futu HK):
        # Lock in 50% at scale_out_target_r when half-lot >= min_scale_out_notional_usd
        if (
            tier.enable_scale_out
            and not pos.scaled_out
            and pos.shares >= 2.0
            and peak_r >= tier.scale_out_target_r
        ):
            target_px = round(pos.entry_price + tier.scale_out_target_r * pos.risk_per_share, 2)
            sell_qty = float(math.floor(pos.shares * tier.scale_out_fraction))
            if sell_qty >= 1.0 and (sell_qty * target_px) >= tier.min_scale_out_notional_usd:
                pos.scaled_out = True
                # Immediately arm breakeven stop on remaining shares
                rt_per_share = (
                    self.fee_model.estimate_round_trip(pos.initial_shares, pos.entry_price)["total_friction_usd"]
                    / pos.initial_shares
                )
                be_stop = round(pos.entry_price + rt_per_share + 0.02, 2)
                pos.current_stop = max(pos.current_stop, be_stop)
                pos.breakeven_armed = True
                return ExitAction(
                    should_exit=True,
                    shares_to_sell=sell_qty,
                    exit_price=max(target_px, round(bar.open, 2) if bar.open > target_px else target_px),
                    reason=ExitReason.SCALE_OUT_2R,
                    updated_stop=pos.current_stop,
                    note=f"Tier scale-out {sell_qty:.0f} sh at +{tier.scale_out_target_r:.1f}R (${target_px:.2f})",
                )

        # 3. Breakeven + Fee Lift once peak unrealized >= breakeven_trigger_r (+1.5R)
        if peak_r >= self.cfg.breakeven_trigger_r:
            rt_fee = self.fee_model.estimate_round_trip(pos.shares, pos.entry_price)["total_friction_usd"]
            be_px = round(pos.entry_price + (rt_fee / max(0.01, pos.shares)) + 0.02, 2)
            if be_px < bar.close:
                pos.current_stop = max(pos.current_stop, be_px)
                pos.breakeven_armed = True

        # 4. Progressive Profit-Floor + Tightened Chandelier Ratchet (monotonic upward)
        if pos.breakeven_armed and atr_14 > 0:
            chandelier_stop = pos.highest_price - self.cfg.chandelier_atr_multiplier * atr_14
            if peak_r >= self.cfg.profit_lock_trigger_r:
                # Guarantee at least `profit_lock_floor_ratio` (e.g. 55%) of peak open gain is retained!
                floor_stop = pos.entry_price + self.cfg.profit_lock_floor_ratio * (
                    pos.highest_price - pos.entry_price
                )
                candidate_stop = round(max(chandelier_stop, floor_stop), 2)
            else:
                candidate_stop = round(chandelier_stop, 2)

            if candidate_stop < bar.close:
                pos.current_stop = max(pos.current_stop, candidate_stop)

        # 5. Futu HK Infinite T+0 Conditional EOD & Weekend Shield (final 30m of session)
        if is_eod_window and tier.enable_t0_eod_exit and not pos.breakeven_armed:
            opened_today = pos.entry_time.date() == bar.timestamp.date()
            if opened_today and tier.eod_weak_exit_below_r is not None and curr_r < tier.eod_weak_exit_below_r:
                return ExitAction(
                    should_exit=True,
                    shares_to_sell=pos.shares,
                    exit_price=round(bar.close, 2),
                    reason=ExitReason.EOD_T0_FLATTEN,
                    updated_stop=pos.current_stop,
                    note=(
                        f"Futu HK T+0 EOD exit: day-0 setup at {curr_r:+.2f}R "
                        f"(< {tier.eod_weak_exit_below_r:+.2f}R overnight threshold)"
                    ),
                )

            is_friday = bar.timestamp.weekday() == 4
            if is_friday and tier.friday_min_cushion_r is not None and curr_r < tier.friday_min_cushion_r:
                return ExitAction(
                    should_exit=True,
                    shares_to_sell=pos.shares,
                    exit_price=round(bar.close, 2),
                    reason=ExitReason.EOD_T0_FLATTEN,
                    updated_stop=pos.current_stop,
                    note=(
                        f"Futu HK Friday weekend shield: {curr_r:+.2f}R "
                        f"(< {tier.friday_min_cushion_r:+.2f}R weekend cushion)"
                    ),
                )

        # 6. Triple-Barrier Vertical Time Stop (exit dead-money trades early to free capital)
        if (
            not pos.breakeven_armed
            and pos.bars_held_15m >= self.cfg.time_stop_bars_15m
            and curr_r < self.cfg.time_stop_min_progress_r
        ):
            return ExitAction(
                should_exit=True,
                shares_to_sell=pos.shares,
                exit_price=round(bar.close, 2),
                reason=ExitReason.TIME_BARRIER,
                updated_stop=pos.current_stop,
                note=(
                    f"Time barrier: {pos.bars_held_15m} bars elapsed with only {curr_r:+.2f}R "
                    f"(< +{self.cfg.time_stop_min_progress_r:.2f}R)"
                ),
            )

        return ExitAction(
            should_exit=False,
            shares_to_sell=0.0,
            exit_price=0.0,
            reason=None,
            updated_stop=pos.current_stop,
        )
