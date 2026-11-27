from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple

from zy_quant.config import ExitModelsConfig, TierProfile
from zy_quant.fees import FutuUSFeeModel
from zy_quant.intel.market_hours import ET_TZ, add_trading_days
from zy_quant.models import (
    Bar,
    EarningsEvent,
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
            entry_d = (
                pos.entry_time.date()
                if pos.entry_time.tzinfo is None
                else pos.entry_time.astimezone(ET_TZ).date()
            )
            bar_d = (
                bar.timestamp.date()
                if bar.timestamp.tzinfo is None
                else bar.timestamp.astimezone(ET_TZ).date()
            )
            opened_today = entry_d == bar_d
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

            is_friday = bar_d.weekday() == 4
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

    def build_swing_position_plan(
        self,
        signal: EntrySignal,
        account_equity_usd: float,
        available_cash_usd: float,
        tier: TierProfile,
        position_scale: float = 1.0,
    ) -> Tuple[Optional[PositionPlan], str]:
        """
        Daily Swing Sizing Engine (`swing_1d`):
          - Uses `initial_atr_1d_multiplier` (~2.2x 1d ATR) and `swing_max_hard_stop_pct` (9.0%)
            so daily stops are never silently clamped at 4.0%
          - Enforces single-equity (`<= 25%`) vs diversified `Sector_ETF` (`<= 35%`) notional caps
        """
        px = signal.entry_price
        if px <= 0 or signal.atr_14 <= 0:
            return None, "Invalid entry price or 1d ATR"

        scale = max(0.25, min(1.0, position_scale))
        min_floor = max(self.cfg.min_stop_pct, 0.025)
        raw_stop_dist = max(self.cfg.initial_atr_1d_multiplier * signal.atr_14, px * min_floor)
        max_stop_dist = px * self.cfg.swing_max_hard_stop_pct
        risk_per_share = min(raw_stop_dist, max_stop_dist)
        initial_stop = round(px - risk_per_share, 2)
        risk_per_share = max(0.01, px - initial_stop)

        expected_move_usd = max(5.0, self.cfg.chandelier_atr_1d_multiplier * 1.35) * signal.atr_14
        is_etf = signal.sector.endswith("_ETF")
        single_cap_pct = (
            min(tier.max_position_pct, tier.max_sector_etf_pct if is_etf else tier.max_single_equity_pct)
            if tier.enforce_swing_caps or tier.max_concurrent_positions >= 3
            else tier.max_position_pct
        )
        max_risk_budget_usd = account_equity_usd * tier.risk_per_trade_pct * scale
        max_notional_usd = min(available_cash_usd, account_equity_usd * single_cap_pct * scale)

        raw_shares = min(max_risk_budget_usd / risk_per_share, max_notional_usd / px)
        if raw_shares >= 1.0:
            shares = float(math.floor(raw_shares))
        elif tier.allow_fractional_shares and raw_shares * px >= min(tier.min_order_notional_usd, 450.0):
            shares = round(min(0.99, raw_shares), 4)
        else:
            return None, "Insufficient buying power for >=1 share or fractional swing lot"

        notional_usd = shares * px
        eff_min_notional = (
            min(tier.min_order_notional_usd, account_equity_usd * single_cap_pct * 0.65)
            if account_equity_usd >= 5000.0
            else tier.min_order_notional_usd
        )
        if notional_usd < eff_min_notional:
            return None, f"Swing notional ${notional_usd:.2f} < floor ${eff_min_notional:.2f}"

        rt = self.fee_model.estimate_round_trip(
            shares=shares,
            entry_price=px,
            exit_price=px + expected_move_usd,
            scale_out=False,
        )
        friction_usd = rt["total_friction_usd"]
        if expected_move_usd * shares < tier.edge_to_fee_multiple * friction_usd:
            return None, "Swing edge-to-fee gate rejected"

        total_risk_usd = max(0.01, risk_per_share * shares)
        reward_r = expected_move_usd / risk_per_share
        p = self.cfg.assumed_base_win_rate
        expectancy_r = (p * reward_r) - ((1.0 - p) * 1.0) - (friction_usd / total_risk_usd)
        if expectancy_r < self.cfg.min_expectancy_r:
            return None, f"Swing expectancy {expectancy_r:.2f}R < +{self.cfg.min_expectancy_r:.2f}R"

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

    def step_daily_position(
        self,
        pos: OpenPosition,
        daily_bar: Bar,
        atr_1d: float,
        tier: TierProfile,
        upcoming_earnings: Optional[EarningsEvent] = None,
        is_new_day: bool = True,
    ) -> ExitAction:
        """
        Daily Multi-Week Swing Barrier Evaluator (Qlib `TopkDropout` Buffer-Hold Discipline):
          1. Conservative Gap-Aware Stop Check (`fill_px = min(open, current_stop)`)
          2. `BMO` (`T-1` close) vs `AMC` (`T-0` close) 3-Tier Pre-Earnings Profit-Cushion Rule:
             - `< +1.0R` -> Exit 100%
             - `+1.0R <= curr_r < +2.5R` -> Scale out 50% (with whole-share/notional guard) & arm breakeven
             - `>= +2.5R` -> Hold 100% on house money
          3. Breakeven Lift (`+1.5R`) + `1d` ATR Chandelier Upward Ratchet
          4. Multi-Week Daily Time Stop (`time_stop_trading_days: 14` if `< +0.25R`)
        """
        if is_new_day:
            pos.days_held += 1

        if daily_bar.low <= pos.current_stop:
            fill_px = (
                min(daily_bar.open, pos.current_stop)
                if daily_bar.open < pos.current_stop
                else pos.current_stop
            )
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
                note=f"Daily stop hit at ${fill_px:.2f} ({reason.value})",
            )

        if daily_bar.high > pos.highest_price:
            pos.highest_price = daily_bar.high

        peak_r = (pos.highest_price - pos.entry_price) / max(0.01, pos.risk_per_share)
        curr_r = (daily_bar.close - pos.entry_price) / max(0.01, pos.risk_per_share)
        bar_d = (
            daily_bar.timestamp.date()
            if daily_bar.timestamp.tzinfo is None
            else daily_bar.timestamp.astimezone(ET_TZ).date()
        )

        # Check BMO (act on T-1 close) vs AMC (act on T-0 close) Pre-Earnings Cushion
        if upcoming_earnings is not None:
            rep_d = upcoming_earnings.report_date
            timing = (upcoming_earnings.timing or "UNKNOWN").upper()
            action_day = rep_d if timing == "AMC" else add_trading_days(rep_d, -1)
            rep_iso = rep_d.isoformat()
            if bar_d == action_day and pos.earnings_cushion_applied_date != rep_iso:
                pos.earnings_cushion_applied_date = rep_iso
                if curr_r < 1.0:
                    return ExitAction(
                        should_exit=True,
                        shares_to_sell=pos.shares,
                        exit_price=round(daily_bar.close, 2),
                        reason=ExitReason.PRE_EARNINGS_CUSHION,
                        updated_stop=pos.current_stop,
                        note=f"Pre-earnings ({timing} {rep_iso}) 100% exit: cushion {curr_r:+.2f}R < +1.00R",
                    )
                if curr_r < 2.5:
                    sell_qty = float(math.floor(pos.shares * 0.50))
                    if sell_qty >= 1.0 and (sell_qty * daily_bar.close) >= min(1000.0, tier.min_scale_out_notional_usd):
                        rt_ps = (
                            self.fee_model.estimate_round_trip(pos.initial_shares, pos.entry_price)["total_friction_usd"]
                            / max(1e-6, pos.initial_shares)
                        )
                        pos.current_stop = max(pos.current_stop, round(pos.entry_price + rt_ps + 0.02, 2))
                        pos.breakeven_armed = True
                        pos.scaled_out = True
                        return ExitAction(
                            should_exit=True,
                            shares_to_sell=sell_qty,
                            exit_price=round(daily_bar.close, 2),
                            reason=ExitReason.PRE_EARNINGS_CUSHION,
                            updated_stop=pos.current_stop,
                            note=f"Pre-earnings ({timing} {rep_iso}) 50% house-money scale-out at {curr_r:+.2f}R",
                        )
                    if curr_r < 1.5:
                        return ExitAction(
                            should_exit=True,
                            shares_to_sell=pos.shares,
                            exit_price=round(daily_bar.close, 2),
                            reason=ExitReason.PRE_EARNINGS_CUSHION,
                            updated_stop=pos.current_stop,
                            note=f"Pre-earnings ({timing} {rep_iso}) small-lot exit at {curr_r:+.2f}R",
                        )

        # Breakeven + Chandelier 1d ATR upward ratchet
        if peak_r >= self.cfg.breakeven_trigger_r:
            rt_fee = self.fee_model.estimate_round_trip(pos.shares, pos.entry_price)["total_friction_usd"]
            be_px = round(pos.entry_price + (rt_fee / max(0.01, pos.shares)) + 0.02, 2)
            if be_px < daily_bar.close:
                pos.current_stop = max(pos.current_stop, be_px)
                pos.breakeven_armed = True

        if pos.breakeven_armed and atr_1d > 0:
            chand_stop = round(pos.highest_price - self.cfg.chandelier_atr_1d_multiplier * atr_1d, 2)
            if peak_r >= 2.2:
                floor_stop = round(
                    pos.entry_price + self.cfg.profit_lock_floor_ratio * (pos.highest_price - pos.entry_price),
                    2,
                )
                chand_stop = max(chand_stop, floor_stop)
            if chand_stop < daily_bar.close:
                pos.current_stop = max(pos.current_stop, chand_stop)

        # Multi-Week Daily Time Barrier (`time_stop_trading_days` ~ 14 trading days)
        if (
            not pos.breakeven_armed
            and pos.days_held >= self.cfg.time_stop_trading_days
            and curr_r < 0.25
        ):
            return ExitAction(
                should_exit=True,
                shares_to_sell=pos.shares,
                exit_price=round(daily_bar.close, 2),
                reason=ExitReason.TIME_BARRIER,
                updated_stop=pos.current_stop,
                note=f"Daily time stop: {pos.days_held}d held with {curr_r:+.2f}R (< +0.25R)",
            )

        return ExitAction(
            should_exit=False,
            shares_to_sell=0.0,
            exit_price=0.0,
            reason=None,
            updated_stop=pos.current_stop,
        )
