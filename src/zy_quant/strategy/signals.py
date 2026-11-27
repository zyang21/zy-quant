from __future__ import annotations

from datetime import date
from typing import Mapping, Optional, Sequence

from zy_quant.config import EntryConfig, RegimeConfig
from zy_quant.intel.market_hours import ET_TZ
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
    compute_session_vwap,
    compute_sma,
    compute_wilder_atr_series,
)


class IntradaySignalEngine:
    """
    Step 3: 15-Minute Institutional Trigger Engine (Graduated Regime Governance)
    - RED regime: Strictly blocked.
    - YELLOW regime: Unlocks Selective Alpha Leaders ONLY — stock must be above its own
      20-day SMA AND outperforming QQQ over the past 20 trading days by >= `yellow_min_rs_outperformance`.
    - GREEN regime: Unlocks stocks trading above their own 20-day SMA.
    - Prime Window Filter: Blocks late-afternoon (>14:30 ET) entries to avoid overnight gap traps.
    """

    mode = "intraday_15m"

    def __init__(self, cfg: EntryConfig, regime_cfg: Optional[RegimeConfig] = None) -> None:
        from zy_quant.intel.regime import MarketRegimeDetector

        self.cfg = cfg
        self._regime_detector = MarketRegimeDetector(regime_cfg or RegimeConfig())

    def _passes_daily_trend_and_regime(
        self,
        curr_price: float,
        session_day: object,
        regime_light: RegimeLight,
        stock_daily_bars: Optional[Sequence[Bar]],
        qqq_daily_bars: Optional[Sequence[Bar]],
        qqq_curr_price: Optional[float] = None,
    ) -> bool:
        if regime_light == RegimeLight.RED:
            return False

        prior_stock_d = (
            [
                b
                for b in stock_daily_bars
                if (b.timestamp.date() if b.timestamp.tzinfo is None else b.timestamp.astimezone(ET_TZ).date())
                < session_day  # type: ignore[operator]
            ]
            if stock_daily_bars
            else []
        )
        prior_qqq_d = (
            [
                b
                for b in qqq_daily_bars
                if (b.timestamp.date() if b.timestamp.tzinfo is None else b.timestamp.astimezone(ET_TZ).date())
                < session_day  # type: ignore[operator]
            ]
            if qqq_daily_bars
            else []
        )

        if len(prior_stock_d) >= 15:
            d_closes = [b.close for b in prior_stock_d]
            sma20 = compute_sma(d_closes, 20)
            if self.cfg.require_stock_above_sma20 and curr_price < sma20:
                return False

            if regime_light == RegimeLight.YELLOW:
                # Require independent Relative Strength vs QQQ over ~20 completed trading days
                if len(prior_qqq_d) < 15 or len(prior_stock_d) < 20:
                    return False
                s_lb = min(20, len(prior_stock_d))
                q_lb = min(20, len(prior_qqq_d))
                q_now = qqq_curr_price if (qqq_curr_price and qqq_curr_price > 0) else prior_qqq_d[-1].close
                s_ret = (curr_price - prior_stock_d[-s_lb].close) / max(0.01, prior_stock_d[-s_lb].close)
                q_ret = (q_now - prior_qqq_d[-q_lb].close) / max(0.01, prior_qqq_d[-q_lb].close)
                if (s_ret - q_ret) < self.cfg.yellow_min_rs_outperformance:
                    return False
        elif regime_light == RegimeLight.YELLOW:
            # Without daily history to prove independent leadership, skip YELLOW entries
            return False

        return True

    def evaluate_15m(
        self,
        symbol: str,
        sector: str,
        bars_15m: Sequence[Bar],
        regime_light: RegimeLight,
        watchlist_score: float,
        capital_flow: Optional[CapitalFlowSnapshot] = None,
        stock_daily_bars: Optional[Sequence[Bar]] = None,
        qqq_daily_bars: Optional[Sequence[Bar]] = None,
        qqq_curr_price: Optional[float] = None,
    ) -> Optional[EntrySignal]:
        if len(bars_15m) < max(self.cfg.atr_period + 2, self.cfg.ema_trend_period):
            return None

        curr = bars_15m[-1]
        curr_et_day = curr.timestamp.date() if curr.timestamp.tzinfo is None else curr.timestamp.astimezone(ET_TZ).date()
        if not self._passes_daily_trend_and_regime(
            curr.close,
            curr_et_day,
            regime_light,
            stock_daily_bars,
            qqq_daily_bars,
            qqq_curr_price=qqq_curr_price,
        ):
            return None

        prev = bars_15m[-2]
        closes = [b.close for b in bars_15m]
        vwaps = compute_session_vwap(bars_15m)
        atrs = compute_wilder_atr_series(bars_15m, self.cfg.atr_period)
        rvols = compute_rvol_series(bars_15m, lookback=20)
        ema_fast = compute_ema_series(closes, self.cfg.ema_fast_period)
        ema_trend = compute_ema_series(closes, self.cfg.ema_trend_period)

        vwap_now = vwaps[-1]
        atr_now = atrs[-1]
        rvol_now = rvols[-1]
        ema9_now = ema_fast[-1]
        ema21_now = ema_trend[-1]

        # Partition today's session bars (26 x 15m bars in regular US session 09:30 - 16:00 ET)
        session_day = curr.timestamp.date()
        session_indices = [i for i, b in enumerate(bars_15m) if b.timestamp.date() == session_day]
        session_bar_num = len(session_indices)
        if session_bar_num < 3:
            return None

        # Prime Institutional Entry Window: bars #3..#20 of session (10:00 - 14:30 ET)
        # Avoids chaotic opening 15m and late-day >14:30 entries into overnight gap risk!
        max_session_bar = 2 + ((self.cfg.entry_end_hour_et - 10) * 4) + (self.cfg.entry_end_minute_et // 15)
        if session_bar_num > max_session_bar:
            return None

        # Big-order flow gate
        if capital_flow is not None and capital_flow.big_order_net_inflow < 0:
            return None

        # --- Setup A: VWAP Institutional Pullback (in confirmed 15m uptrend EMA9 >= EMA21) ---
        if self.cfg.vwap_pullback.enabled and session_bar_num >= 4:
            vp = self.cfg.vwap_pullback
            early_rvols = [rvols[idx] for idx in session_indices[:-2]]
            had_morning_impulse = max(early_rvols, default=0.0) >= vp.min_morning_rvol
            pullback_vol_contracted = rvols[-2] <= vp.max_pullback_rvol or rvol_now <= vp.max_pullback_rvol
            tested_vwap = (
                abs(curr.low - vwap_now) <= vp.vwap_touch_band_atr * atr_now
                or abs(prev.low - vwaps[-2]) <= vp.vwap_touch_band_atr * atr_now
            )
            bullish_reclaim = (
                curr.close > curr.open
                and curr.close > vwap_now
                and curr.close >= ema21_now
                and ema9_now >= ema21_now * 0.999
            )
            if had_morning_impulse and pullback_vol_contracted and tested_vwap and bullish_reclaim:
                return EntrySignal(
                    symbol=symbol,
                    sector=sector,
                    setup=SetupType.VWAP_PULLBACK,
                    timestamp=curr.timestamp,
                    entry_price=curr.close,
                    atr_14=atr_now,
                    vwap=vwap_now,
                    rvol=round(rvol_now, 2),
                    watchlist_score=watchlist_score,
                    rationale=(
                        f"[{regime_light.value}] VWAP Pullback reclaim above ${vwap_now:.2f} "
                        f"after {max(early_rvols):.1f}x impulse; ATR14=${atr_now:.2f}"
                    ),
                )

        # --- Setup B: 30-Minute Opening Range Breakout (ORB) ---
        if self.cfg.opening_range_breakout.enabled:
            orb = self.cfg.opening_range_breakout
            orb_bars_count = max(1, orb.orb_minutes // 15)
            if orb_bars_count < session_bar_num <= orb_bars_count + 8:
                orb_slice = [bars_15m[idx] for idx in session_indices[:orb_bars_count]]
                orb_high = max(b.high for b in orb_slice)
                prior_post_orb = [
                    bars_15m[idx] for idx in session_indices[orb_bars_count:-1]
                ]
                fresh_breakout = all(b.close <= orb_high for b in prior_post_orb)
                flow_ok = True
                if orb.require_positive_big_order_flow and capital_flow is not None:
                    flow_ok = capital_flow.big_order_net_inflow > 0

                if (
                    fresh_breakout
                    and curr.close > orb_high
                    and curr.close > vwap_now
                    and ema9_now >= ema21_now * 0.999
                    and rvol_now >= orb.min_breakout_rvol
                    and flow_ok
                ):
                    return EntrySignal(
                        symbol=symbol,
                        sector=sector,
                        setup=SetupType.ORB_30M,
                        timestamp=curr.timestamp,
                        entry_price=curr.close,
                        atr_14=atr_now,
                        vwap=vwap_now,
                        rvol=round(rvol_now, 2),
                        watchlist_score=watchlist_score,
                        rationale=(
                            f"[{regime_light.value}] 30m ORB breakout above ${orb_high:.2f} "
                            f"on {rvol_now:.2f}x RVOL; ATR14=${atr_now:.2f}"
                        ),
                    )

        return None

    # --- StrategyPipelineProtocol Adapter ---
    def evaluate_regime(
        self,
        qqq_daily: Sequence[Bar],
        spy_daily: Sequence[Bar],
        universe_daily: Optional[Mapping[str, Sequence[Bar]]] = None,
        qqq_intraday_15m: Optional[Sequence[Bar]] = None,
        as_of_date: Optional[date] = None,
        qqq_partial_bar: Optional[Bar] = None,
        spy_partial_bar: Optional[Bar] = None,
        vix_level: Optional[float] = None,
    ) -> RegimeSnapshot:
        return self._regime_detector.evaluate(
            qqq_intraday_15m=qqq_intraday_15m or [],
            qqq_daily=qqq_daily,
            spy_daily=spy_daily,
            vix_level=vix_level or 18.0,
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
        bars_15m: Optional[Sequence[Bar]] = None,
        capital_flow: Optional[CapitalFlowSnapshot] = None,
        recent_earnings: Optional[EarningsEvent] = None,
    ) -> Optional[EntrySignal]:
        if not bars_15m:
            return None
        return self.evaluate_15m(
            symbol=symbol,
            sector=sector,
            bars_15m=bars_15m,
            regime_light=regime_light,
            watchlist_score=watchlist_score,
            capital_flow=capital_flow,
            stock_daily_bars=stock_daily_bars,
            qqq_daily_bars=qqq_daily_bars,
        )
