from __future__ import annotations

from typing import Optional, Sequence

from zy_quant.config import EntryConfig
from zy_quant.models import (
    Bar,
    CapitalFlowSnapshot,
    EntrySignal,
    RegimeLight,
    SetupType,
)
from zy_quant.strategy.indicators import (
    compute_ema_series,
    compute_rvol_series,
    compute_session_vwap,
    compute_wilder_atr_series,
)


class IntradaySignalEngine:
    """
    Step 3: 15-Minute Trigger Engine (Active ONLY when Market Regime == GREEN)
    Evaluates two institutional momentum setups:
    - Setup A (VWAP_PULLBACK): Strong morning impulse (RVOL >= 1.5x), followed by low-volume
      pullback testing session VWAP within `vwap_touch_band_atr * ATR` and closing bullishly above VWAP & EMA21.
    - Setup B (ORB_30M): Clean breakout above the first 30-minute Opening Range High on RVOL >= 1.8x
      with positive institutional big-order net inflow.
    """

    def __init__(self, cfg: EntryConfig) -> None:
        self.cfg = cfg

    def evaluate_15m(
        self,
        symbol: str,
        sector: str,
        bars_15m: Sequence[Bar],
        regime_light: RegimeLight,
        watchlist_score: float,
        capital_flow: Optional[CapitalFlowSnapshot] = None,
    ) -> Optional[EntrySignal]:
        if regime_light != RegimeLight.GREEN:
            return None
        if len(bars_15m) < max(self.cfg.atr_period + 2, self.cfg.ema_trend_period):
            return None

        curr = bars_15m[-1]
        prev = bars_15m[-2]
        closes = [b.close for b in bars_15m]
        vwaps = compute_session_vwap(bars_15m)
        atrs = compute_wilder_atr_series(bars_15m, self.cfg.atr_period)
        rvols = compute_rvol_series(bars_15m, lookback=20)
        ema_trend = compute_ema_series(closes, self.cfg.ema_trend_period)

        vwap_now = vwaps[-1]
        atr_now = atrs[-1]
        rvol_now = rvols[-1]
        ema21_now = ema_trend[-1]

        # Partition today's session bars
        session_day = curr.timestamp.date()
        session_indices = [i for i, b in enumerate(bars_15m) if b.timestamp.date() == session_day]
        if len(session_indices) < 3:
            return None

        # Big-order flow gate (if live Futu flow is available, block when institutions are net dumping)
        if capital_flow is not None and capital_flow.big_order_net_inflow < 0:
            return None

        # --- Setup A: VWAP Pullback ---
        if self.cfg.vwap_pullback.enabled and len(session_indices) >= 4:
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
                        f"VWAP Pullback reclaim above ${vwap_now:.2f} after {max(early_rvols):.1f}x impulse; "
                        f"ATR14=${atr_now:.2f}"
                    ),
                )

        # --- Setup B: 30-Minute Opening Range Breakout (ORB) ---
        if self.cfg.opening_range_breakout.enabled:
            orb = self.cfg.opening_range_breakout
            orb_bars_count = max(1, orb.orb_minutes // 15)
            if len(session_indices) > orb_bars_count and len(session_indices) <= orb_bars_count + 8:
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
                            f"30m ORB breakout above ${orb_high:.2f} on {rvol_now:.2f}x RVOL; "
                            f"ATR14=${atr_now:.2f}"
                        ),
                    )

        return None
