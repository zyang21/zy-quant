from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Mapping, Optional, Sequence

from zy_quant.config import RegimeConfig, SwingPipelineConfig
from zy_quant.intel.market_hours import ET_TZ
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

        # Compute today's intraday return against prior completed daily close (anchored to ET_TZ)
        last_ts = qqq_intraday_15m[-1].timestamp
        session_d = last_ts.date() if last_ts.tzinfo is None else last_ts.astimezone(ET_TZ).date()
        last_d_ts = qqq_daily[-1].timestamp
        last_d_date = last_d_ts.date() if last_d_ts.tzinfo is None else last_d_ts.astimezone(ET_TZ).date()
        if last_d_date < session_d:
            prev_close = qqq_daily[-1].close
        elif len(qqq_daily) >= 2:
            prev_close = qqq_daily[-2].close
        else:
            prev_close = qqq_intraday_15m[0].open
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
            recommended_scan_minutes=self.cfg.yellow_watchlist_interval_minutes,
            reason="Mixed index regime (QQQ split between VWAP/SMA20 or elevated VIX) -> Hold stops (3m), 45m watchlist scan",
            evaluated_at=now,
        )


class DailyRegimeDetector:
    """
    Multi-Week Swing Macro & Market-Breadth Regime Detector (`QQQ / SPY` vs `SMA50 / SMA200`
    + 100-stock Universe Breadth `% > SMA50` + annualized 20-day realized volatility):
      - Strictly causal: slices daily bars `< as_of_date` (`bisect_left` semantics) + optional
        `partial_1d_bar` built from `[09:30 ET .. decision_ts]`.
    """

    def __init__(
        self,
        regime_cfg: RegimeConfig,
        swing_cfg: Optional[SwingPipelineConfig] = None,
    ) -> None:
        self.cfg = regime_cfg
        self.swing_cfg = swing_cfg or SwingPipelineConfig()

    @staticmethod
    def _et_date(b: Bar) -> date:
        return b.timestamp.date() if b.timestamp.tzinfo is None else b.timestamp.astimezone(ET_TZ).date()

    def _slice_causal(
        self,
        bars: Sequence[Bar],
        as_of_date: Optional[date],
        partial_bar: Optional[Bar],
        include_completed_as_of: bool,
    ) -> list[Bar]:
        if as_of_date is None:
            return list(bars)
        if include_completed_as_of:
            return [b for b in bars if self._et_date(b) <= as_of_date]
        out = [b for b in bars if self._et_date(b) < as_of_date]
        if partial_bar is not None and self._et_date(partial_bar) == as_of_date:
            out.append(partial_bar)
        return out

    def evaluate_daily(
        self,
        qqq_daily: Sequence[Bar],
        spy_daily: Sequence[Bar],
        universe_daily: Optional[Mapping[str, Sequence[Bar]]] = None,
        as_of_date: Optional[date] = None,
        qqq_partial_bar: Optional[Bar] = None,
        spy_partial_bar: Optional[Bar] = None,
        vix_level: Optional[float] = None,
        include_completed_as_of: bool = False,
    ) -> RegimeSnapshot:
        now = datetime.now(timezone.utc)
        q_bars = self._slice_causal(qqq_daily, as_of_date, qqq_partial_bar, include_completed_as_of)
        s_bars = self._slice_causal(spy_daily, as_of_date, spy_partial_bar, include_completed_as_of)

        if len(q_bars) < 20:
            return RegimeSnapshot(
                light=RegimeLight.RED,
                qqq_price=0.0,
                qqq_vwap=0.0,
                qqq_sma20=0.0,
                qqq_intraday_return=0.0,
                spy_price=0.0,
                spy_sma20=0.0,
                vix_level=vix_level or 20.0,
                recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
                reason="Insufficient daily benchmark history — fail-closed RED",
                evaluated_at=now,
            )

        q_closes = [b.close for b in q_bars]
        q_px = q_closes[-1]
        q_sma20 = compute_sma(q_closes, 20)
        q_sma50 = compute_sma(q_closes, self.swing_cfg.daily_sma_fast)
        q_sma200 = compute_sma(q_closes, min(self.swing_cfg.daily_sma_slow, len(q_closes)))
        prev_c = q_closes[-2] if len(q_closes) >= 2 else q_bars[-1].open
        day_ret = (q_px - prev_c) / max(0.01, prev_c)

        s_closes = [b.close for b in s_bars] if s_bars else q_closes
        s_px = s_closes[-1]
        s_sma20 = compute_sma(s_closes, 20)
        s_sma50 = compute_sma(s_closes, self.swing_cfg.daily_sma_fast)
        s_sma200 = compute_sma(s_closes, min(self.swing_cfg.daily_sma_slow, len(s_closes)))

        breadth_pct = 55.0
        if universe_daily:
            above_50 = 0
            counted = 0
            for sym, ubars in universe_daily.items():
                if sym in ("QQQ", "SPY"):
                    continue
                u_slice = self._slice_causal(ubars, as_of_date, None, include_completed_as_of)
                if len(u_slice) >= 25:
                    counted += 1
                    u_c = [x.close for x in u_slice]
                    if u_c[-1] >= compute_sma(u_c, min(50, len(u_c))):
                        above_50 += 1
            if counted >= 5:
                breadth_pct = round((above_50 / float(counted)) * 100.0, 1)

        if vix_level is not None and vix_level > 0:
            vol_est = vix_level
        else:
            vol_est = 18.0
            if len(q_closes) >= 21:
                rets = [
                    math.log(q_closes[i] / q_closes[i - 1])
                    for i in range(len(q_closes) - 20, len(q_closes))
                    if q_closes[i - 1] > 0
                ]
                if len(rets) >= 10:
                    m_r = sum(rets) / len(rets)
                    var_r = sum((r - m_r) ** 2 for r in rets) / (len(rets) - 1)
                    vol_est = round(max(11.0, min(80.0, math.sqrt(var_r * 252.0) * 100.0 * 1.10)), 2)

        if vol_est >= self.cfg.vix_red_threshold or (
            q_px < q_sma200 and s_px < s_sma200 and breadth_pct < self.swing_cfg.breadth_red_pct
        ):
            return RegimeSnapshot(
                light=RegimeLight.RED,
                qqq_price=q_px,
                qqq_vwap=q_sma20,
                qqq_sma20=q_sma20,
                qqq_intraday_return=day_ret,
                spy_price=s_px,
                spy_sma20=s_sma20,
                vix_level=vol_est,
                recommended_scan_minutes=self.cfg.rest_watchlist_interval_minutes,
                reason=(
                    f"Bearish macro regime (QQQ ${q_px:.2f} < SMA200 ${q_sma200:.2f}, "
                    f"Breadth {breadth_pct:.0f}%, Vol {vol_est:.1f}) -> Cash defense"
                ),
                evaluated_at=now,
                qqq_sma50=round(q_sma50, 2),
                qqq_sma200=round(q_sma200, 2),
                spy_sma50=round(s_sma50, 2),
                spy_sma200=round(s_sma200, 2),
                breadth_above_sma50_pct=breadth_pct,
            )

        if (
            q_px >= q_sma50
            and (q_sma50 >= q_sma200 * 0.995 or breadth_pct >= self.swing_cfg.breadth_green_pct)
            and vol_est < self.cfg.vix_yellow_threshold
        ):
            return RegimeSnapshot(
                light=RegimeLight.GREEN,
                qqq_price=q_px,
                qqq_vwap=q_sma20,
                qqq_sma20=q_sma20,
                qqq_intraday_return=day_ret,
                spy_price=s_px,
                spy_sma20=s_sma20,
                vix_level=vol_est,
                recommended_scan_minutes=self.cfg.green_scan_interval_minutes,
                reason=(
                    f"Bullish swing regime (QQQ >= SMA50 ${q_sma50:.2f}, "
                    f"Breadth {breadth_pct:.0f}% > SMA50, Vol {vol_est:.1f}) -> Full swing capacity"
                ),
                evaluated_at=now,
                qqq_sma50=round(q_sma50, 2),
                qqq_sma200=round(q_sma200, 2),
                spy_sma50=round(s_sma50, 2),
                spy_sma200=round(s_sma200, 2),
                breadth_above_sma50_pct=breadth_pct,
            )

        return RegimeSnapshot(
            light=RegimeLight.YELLOW,
            qqq_price=q_px,
            qqq_vwap=q_sma20,
            qqq_sma20=q_sma20,
            qqq_intraday_return=day_ret,
            spy_price=s_px,
            spy_sma20=s_sma20,
            vix_level=vol_est,
            recommended_scan_minutes=self.cfg.yellow_watchlist_interval_minutes,
            reason=(
                f"Selective rotation regime (QQQ ${q_px:.2f} vs SMA50 ${q_sma50:.2f}, "
                f"Breadth {breadth_pct:.0f}%) -> Independent RS leaders only"
            ),
            evaluated_at=now,
            qqq_sma50=round(q_sma50, 2),
            qqq_sma200=round(q_sma200, 2),
            spy_sma50=round(s_sma50, 2),
            spy_sma200=round(s_sma200, 2),
            breadth_above_sma50_pct=breadth_pct,
        )
