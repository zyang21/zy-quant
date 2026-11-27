from __future__ import annotations

from datetime import date
from typing import List, Mapping, Optional, Sequence

from zy_quant.config import EntryConfig, RegimeConfig, SwingPipelineConfig
from zy_quant.intel.market_hours import (
    ET_TZ,
    add_trading_days,
    count_trading_days_between,
    daily_bar_session_date,
)
from zy_quant.models import (
    Bar,
    CapitalFlowSnapshot,
    EarningsEvent,
    EntrySignal,
    RegimeLight,
    RegimeSnapshot,
    SetupType,
)
from zy_quant.strategy.indicators import (
    compute_ema_series,
    compute_rvol_series,
    compute_sma,
    compute_wilder_atr_series,
)


def _bar_et_date(b: Bar) -> date:
    return daily_bar_session_date(b.timestamp)


def build_strictly_causal_daily_series(
    daily_bars: Sequence[Bar],
    as_of_date: date,
    partial_today_bar: Optional[Bar] = None,
    include_completed_as_of: bool = False,
) -> List[Bar]:
    """
    Zero-lookahead causal daily series builder:
      - Default (`include_completed_as_of=False`, used in live 15:40 ET Phase 3 & Hybrid Clock):
        retains ONLY `bar_date < as_of_date` (`bisect_left` semantics!) and appends `partial_today_bar`
        built strictly from `[09:30 ET .. decision_ts]`. Mutating `as_of_date`'s 16:00 ET `1d` bar
        has ZERO effect!
      - `include_completed_as_of=True` (used ONLY at `as_of_date` post-close when generating
        orders queued for `T+1` next-day open in pure `1d` backtesting): retains `bar_date <= as_of_date`.
    """
    if include_completed_as_of:
        base = [b for b in daily_bars if _bar_et_date(b) <= as_of_date]
    else:
        base = [b for b in daily_bars if _bar_et_date(b) < as_of_date]
        if partial_today_bar is not None and _bar_et_date(partial_today_bar) == as_of_date:
            base.append(partial_today_bar)
    return base


class DailySwingSignalEngine:
    """
    Multi-Week Institutional Daily Swing Signal Engine (`swing_1d`):
      1. `DAILY_PULLBACK_EMA21`: Stage-2 uptrend (`Close > SMA50`, `EMA21 >= SMA50 * 0.99`),
         orderly 2-8d pullback testing rising `EMA21` on contracted volume (`<= 0.95x`),
         followed by bullish reversal close reclaiming above `EMA9`.
      2. `VCP_PIVOT_BREAKOUT`: Volatility contraction (`ATR_5d / ATR_20d <= 0.88`) breaking
         above prior 20-day pivot high on institutional volume expansion (`RVOL_1d >= 1.30x`).
      3. `DONCHIAN_20D_RS`: Top RS leader (`RS_60d vs QQQ >= +6%`) breaking 20-day closing high.
      4. `PEAD_CONTINUATION`: Post-Earnings Announcement Drift (`T+1..T+5` trading days after
         earnings gap-up) holding above earnings-day low and reclaiming post-earnings high.
    """

    mode = "swing_1d"

    def __init__(
        self,
        entry_cfg: EntryConfig,
        regime_cfg: RegimeConfig,
        swing_cfg: Optional[SwingPipelineConfig] = None,
    ) -> None:
        from zy_quant.intel.regime import DailyRegimeDetector

        self.entry_cfg = entry_cfg
        self.swing_cfg = swing_cfg or SwingPipelineConfig()
        self.regime_detector = DailyRegimeDetector(regime_cfg, self.swing_cfg)

    def evaluate_regime(
        self,
        qqq_daily: Sequence[Bar],
        spy_daily: Sequence[Bar],
        universe_daily: Optional[Mapping[str, Sequence[Bar]]] = None,
        qqq_intraday_15m: Optional[Sequence[Bar]] = None,
        as_of_date: Optional[date] = None,
        qqq_partial_bar: Optional[Bar] = None,
        spy_partial_bar: Optional[Bar] = None,
        universe_partial_bars: Optional[Mapping[str, Optional[Bar]]] = None,
        vix_level: Optional[float] = None,
        include_completed_as_of: bool = False,
    ) -> RegimeSnapshot:
        return self.regime_detector.evaluate_daily(
            qqq_daily=qqq_daily,
            spy_daily=spy_daily,
            universe_daily=universe_daily,
            as_of_date=as_of_date,
            qqq_partial_bar=qqq_partial_bar,
            spy_partial_bar=spy_partial_bar,
            universe_partial_bars=universe_partial_bars,
            vix_level=vix_level,
            include_completed_as_of=include_completed_as_of,
        )

    def evaluate_entry(
        self,
        symbol: str,
        sector: str,
        regime_light: RegimeLight,
        watchlist_score: float,
        stock_daily_bars: Sequence[Bar],
        qqq_daily_bars: Sequence[Bar],
        as_of_date: date,
        partial_today_bar: Optional[Bar] = None,
        qqq_partial_bar: Optional[Bar] = None,
        bars_15m: Optional[Sequence[Bar]] = None,
        capital_flow: Optional[CapitalFlowSnapshot] = None,
        recent_earnings: Optional[EarningsEvent] = None,
        include_completed_as_of: bool = False,
    ) -> Optional[EntrySignal]:
        if regime_light == RegimeLight.RED:
            return None

        s_bars = build_strictly_causal_daily_series(
            stock_daily_bars,
            as_of_date=as_of_date,
            partial_today_bar=partial_today_bar,
            include_completed_as_of=include_completed_as_of,
        )
        q_bars = build_strictly_causal_daily_series(
            qqq_daily_bars,
            as_of_date=as_of_date,
            partial_today_bar=qqq_partial_bar,
            include_completed_as_of=include_completed_as_of,
        )

        if len(s_bars) < 55 or len(q_bars) < 25:
            return None

        # Pre-earnings entry buffer veto (strictly `<` 10 trading days or `<` 14 calendar days)
        if recent_earnings is not None and recent_earnings.report_date >= as_of_date:
            cal_days = (recent_earnings.report_date - as_of_date).days
            trd_days = count_trading_days_between(as_of_date, recent_earnings.report_date)
            if (
                cal_days < self.swing_cfg.pre_earnings_buffer_calendar_days
                or trd_days < self.swing_cfg.pre_earnings_buffer_trading_days
            ):
                return None

        closes = [b.close for b in s_bars]
        curr = s_bars[-1]
        prev = s_bars[-2]
        sma50 = compute_sma(closes, 50)
        sma200 = compute_sma(closes, min(200, len(closes)))
        ema9 = compute_ema_series(closes, 9)[-1]
        ema21_series = compute_ema_series(closes, 21)
        ema21 = ema21_series[-1]
        atrs_14 = compute_wilder_atr_series(s_bars, 14)
        atr_now = atrs_14[-1]
        rvols = compute_rvol_series(s_bars, lookback=20)
        rvol_now = rvols[-1]

        # Stage-2 alignment check: stock must hold above rising/supportive SMA50 & SMA200
        if curr.close < sma50 or ema21 < sma50 * 0.985 or curr.close < sma200 * 0.98:
            return None

        # Relative strength vs QQQ (20d & 60d)
        s_ret_20 = (curr.close - s_bars[-21].close) / max(0.01, s_bars[-21].close)
        q_lb_20 = min(20, len(q_bars) - 1)
        q_ret_20 = (q_bars[-1].close - q_bars[-1 - q_lb_20].close) / max(0.01, q_bars[-1 - q_lb_20].close)
        rs_20 = s_ret_20 - q_ret_20

        s_lb_60 = min(60, len(s_bars) - 1)
        q_lb_60 = min(60, len(q_bars) - 1)
        rs_60 = (
            (curr.close - s_bars[-1 - s_lb_60].close) / max(0.01, s_bars[-1 - s_lb_60].close)
        ) - (
            (q_bars[-1].close - q_bars[-1 - q_lb_60].close) / max(0.01, q_bars[-1 - q_lb_60].close)
        )

        # In YELLOW caution regime, restrict strictly to independent alpha leaders beating QQQ >= +3%
        if regime_light == RegimeLight.YELLOW and rs_20 < max(0.03, self.entry_cfg.yellow_min_rs_outperformance):
            return None

        if capital_flow is not None and capital_flow.big_order_net_inflow < 0:
            return None

        # --- Setup 4: PEAD Continuation (T+1..T+5 trading days after earnings reaction gap-up) ---
        if recent_earnings is not None:
            # AMC earnings react on the next NYSE trading day (`T+1` open); BMO reacts on `report_date`
            reaction_date = (
                add_trading_days(recent_earnings.report_date, 1)
                if (recent_earnings.timing or "").upper() == "AMC"
                else recent_earnings.report_date
            )
            if reaction_date < as_of_date:
                post_days = count_trading_days_between(reaction_date, as_of_date)
                if 1 <= post_days <= 5:
                    earn_bars = [b for b in s_bars[-8:] if _bar_et_date(b) >= reaction_date]
                    if len(earn_bars) >= 2:
                        earn_day_bar = earn_bars[0]
                        prior_earn_idx = len(s_bars) - len(earn_bars) - 1
                        pre_earn_close = s_bars[max(0, prior_earn_idx)].close
                        gapped_up = earn_day_bar.close > pre_earn_close * 1.03
                        held_gap_low = all(b.low >= earn_day_bar.low * 0.995 for b in earn_bars[1:])
                        post_high = max(b.high for b in earn_bars[:-1])
                        if gapped_up and held_gap_low and curr.close > post_high and rvol_now >= 1.15:
                            return EntrySignal(
                                symbol=symbol,
                                sector=sector,
                                setup=SetupType.PEAD_CONTINUATION,
                                timestamp=curr.timestamp,
                                entry_price=curr.close,
                                atr_14=atr_now,
                                vwap=ema21,
                                rvol=round(rvol_now, 2),
                                watchlist_score=watchlist_score,
                                rationale=(
                                    f"[{regime_light.value}] PEAD T+{post_days} breakout above ${post_high:.2f}; "
                                    f"1d ATR=${atr_now:.2f}"
                                ),
                            )

        # --- Setup 1: VCP Volatility-Contraction 20-Day Pivot Breakout ---
        prior_20 = s_bars[-21:-1]
        pivot_high_20 = max(b.high for b in prior_20)
        atr_5 = sum(b.high - b.low for b in s_bars[-6:-1]) / 5.0
        atr_20 = sum(b.high - b.low for b in prior_20) / 20.0
        vcp_tight = atr_5 <= atr_20 * 0.90
        strong_close_in_range = (curr.close - curr.low) >= 0.60 * max(0.01, curr.high - curr.low)

        if (
            vcp_tight
            and curr.close > pivot_high_20
            and prev.close <= pivot_high_20
            and rvol_now >= 1.30
            and strong_close_in_range
            and rs_20 >= 0.0
        ):
            return EntrySignal(
                symbol=symbol,
                sector=sector,
                setup=SetupType.VCP_PIVOT_BREAKOUT,
                timestamp=curr.timestamp,
                entry_price=curr.close,
                atr_14=atr_now,
                vwap=ema21,
                rvol=round(rvol_now, 2),
                watchlist_score=watchlist_score,
                rationale=(
                    f"[{regime_light.value}] Daily VCP 20d breakout > ${pivot_high_20:.2f} "
                    f"(ATR5/20={atr_5/max(0.01,atr_20):.2f}, RVOL={rvol_now:.2f}x)"
                ),
            )

        # --- Setup 2: Daily EMA21 Institutional Pullback & Reclaim ---
        pulled_back_near_ema21 = (
            abs(prev.low - ema21_series[-2]) <= 0.60 * atr_now
            or abs(curr.low - ema21) <= 0.50 * atr_now
        )
        vol_contracted_on_dip = min(rvols[-2], rvols[-3]) <= 0.95
        bullish_reclaim = (
            curr.close > curr.open
            and curr.close > prev.high
            and curr.close >= ema9
            and curr.close >= ema21
            and ema21 > ema21_series[-6]
        )
        if pulled_back_near_ema21 and vol_contracted_on_dip and bullish_reclaim and rs_20 >= 0.01:
            return EntrySignal(
                symbol=symbol,
                sector=sector,
                setup=SetupType.DAILY_PULLBACK_EMA21,
                timestamp=curr.timestamp,
                entry_price=curr.close,
                atr_14=atr_now,
                vwap=ema21,
                rvol=round(rvol_now, 2),
                watchlist_score=watchlist_score,
                rationale=(
                    f"[{regime_light.value}] Daily EMA21 pullback reclaim @ ${curr.close:.2f} "
                    f"(RS20={rs_20*100:+.1f}%, 1d ATR=${atr_now:.2f})"
                ),
            )

        # --- Setup 3: 20-Day Donchian Relative-Strength Breakout ---
        highest_close_20 = max(b.close for b in prior_20)
        if (
            rs_60 >= 0.06
            and rs_20 >= 0.02
            and curr.close > highest_close_20
            and prev.close <= highest_close_20
            and rvol_now >= 1.20
            and strong_close_in_range
        ):
            return EntrySignal(
                symbol=symbol,
                sector=sector,
                setup=SetupType.DONCHIAN_20D_RS,
                timestamp=curr.timestamp,
                entry_price=curr.close,
                atr_14=atr_now,
                vwap=ema21,
                rvol=round(rvol_now, 2),
                watchlist_score=watchlist_score,
                rationale=(
                    f"[{regime_light.value}] 20d Donchian RS breakout > ${highest_close_20:.2f} "
                    f"(RS60={rs_60*100:+.1f}%, RVOL={rvol_now:.2f}x)"
                ),
            )

        return None
