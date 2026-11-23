from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from zy_quant.config import RegimeConfig
from zy_quant.models import Bar, RegimeLight, RegimeSnapshot
from zy_quant.strategy.indicators import compute_session_vwap, compute_sma


class MarketRegimeDetector:
    """
    Step 1: Market Regime Traffic Light (SPY / QQQ + VIX)
    - GREEN: QQQ >= session VWAP, QQQ >= 20-day SMA, VIX < yellow_threshold, no sharp dump
        -> Unlocks 15m entry scanner (scan cadence = 15m)
    - YELLOW / RED: Locks all new buy orders (0 trades, 0 fee churn),
        switches empty-position daemon cadence to 2 hours (120m) to score Relative Strength & refresh watchlist.
    """

    def __init__(self, cfg: RegimeConfig) -> None:
        self.cfg = cfg

    def evaluate(
        self,
        qqq_intraday_15m: Sequence[Bar],
        qqq_daily: Sequence[Bar],
        spy_daily: Sequence[Bar],
        vix_level: float = 18.0,
    ) -> RegimeSnapshot:
        now = datetime.now(timezone.utc)
        if not qqq_intraday_15m or not qqq_daily:
            return RegimeSnapshot(
                light=RegimeLight.RED,
                qqq_price=0.0,
                qqq_vwap=0.0,
                qqq_sma20=0.0,
                qqq_intraday_return=0.0,
                spy_price=0.0,
                spy_sma20=0.0,
                vix_level=vix_level,
                recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
                reason="Insufficient benchmark market data — fail-closed RED",
                evaluated_at=now,
            )

        qqq_px = qqq_intraday_15m[-1].close
        vwap_series = compute_session_vwap(qqq_intraday_15m)
        qqq_vwap = vwap_series[-1] if vwap_series else qqq_px

        qqq_closes_d = [b.close for b in qqq_daily]
        qqq_sma20 = compute_sma(qqq_closes_d, self.cfg.daily_sma_period)

        spy_closes_d = [b.close for b in spy_daily] if spy_daily else [qqq_px]
        spy_px = spy_closes_d[-1]
        spy_sma20 = compute_sma(spy_closes_d, self.cfg.daily_sma_period)

        # Compute today's intraday return from today's first bar open (or prior daily close)
        prev_close = qqq_daily[-2].close if len(qqq_daily) >= 2 else qqq_intraday_15m[0].open
        intraday_ret = (qqq_px - prev_close) / prev_close if prev_close > 0 else 0.0

        # Hard RED triggers
        if vix_level >= self.cfg.vix_red_threshold:
            return RegimeSnapshot(
                light=RegimeLight.RED,
                qqq_price=qqq_px,
                qqq_vwap=qqq_vwap,
                qqq_sma20=qqq_sma20,
                qqq_intraday_return=intraday_ret,
                spy_price=spy_px,
                spy_sma20=spy_sma20,
                vix_level=vix_level,
                recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
                reason=f"VIX panic ({vix_level:.1f} >= {self.cfg.vix_red_threshold}) -> Rest & 2h RS scan",
                evaluated_at=now,
            )

        if intraday_ret <= self.cfg.sharp_intraday_drop_pct:
            return RegimeSnapshot(
                light=RegimeLight.RED,
                qqq_price=qqq_px,
                qqq_vwap=qqq_vwap,
                qqq_sma20=qqq_sma20,
                qqq_intraday_return=intraday_ret,
                spy_price=spy_px,
                spy_sma20=spy_sma20,
                vix_level=vix_level,
                recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
                reason=f"QQQ sharp drop ({intraday_ret*100:.2f}%) -> Rest & 2h RS watchlist refresh",
                evaluated_at=now,
            )

        if qqq_px < qqq_vwap and qqq_px < qqq_sma20:
            return RegimeSnapshot(
                light=RegimeLight.RED,
                qqq_price=qqq_px,
                qqq_vwap=qqq_vwap,
                qqq_sma20=qqq_sma20,
                qqq_intraday_return=intraday_ret,
                spy_price=spy_px,
                spy_sma20=spy_sma20,
                vix_level=vix_level,
                recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
                reason="QQQ below both session VWAP and 20-day SMA -> Bearish rest mode",
                evaluated_at=now,
            )

        # GREEN check: QQQ above session VWAP + above SMA20 + SPY supportive + VIX calm
        if (
            qqq_px >= qqq_vwap
            and qqq_px >= qqq_sma20
            and vix_level < self.cfg.vix_yellow_threshold
        ):
            return RegimeSnapshot(
                light=RegimeLight.GREEN,
                qqq_price=qqq_px,
                qqq_vwap=qqq_vwap,
                qqq_sma20=qqq_sma20,
                qqq_intraday_return=intraday_ret,
                spy_price=spy_px,
                spy_sma20=spy_sma20,
                vix_level=vix_level,
                recommended_scan_minutes=self.cfg.green_scan_interval_minutes,
                reason="QQQ holding above VWAP & 20d SMA with calm VIX -> Long setups unlocked",
                evaluated_at=now,
            )

        # Otherwise YELLOW caution
        return RegimeSnapshot(
            light=RegimeLight.YELLOW,
            qqq_price=qqq_px,
            qqq_vwap=qqq_vwap,
            qqq_sma20=qqq_sma20,
            qqq_intraday_return=intraday_ret,
            spy_price=spy_px,
            spy_sma20=spy_sma20,
            vix_level=vix_level,
            recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
            reason="Mixed index regime (QQQ split between VWAP/SMA20 or elevated VIX) -> Hold stops, 2h watchlist scan",
            evaluated_at=now,
        )
