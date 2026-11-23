from __future__ import annotations

from typing import List, Sequence

from zy_quant.models import Bar


def compute_sma(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0
    window = values[-period:] if len(values) >= period else values
    return sum(window) / float(len(window))


def compute_ema_series(values: Sequence[float], period: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(alpha * float(v) + (1.0 - alpha) * out[-1])
    return out


def compute_session_vwap(bars: Sequence[Bar]) -> List[float]:
    """Computes cumulative Typical-Price Volume-Weighted Average Price, resetting each trading day."""
    out: List[float] = []
    cum_pv = 0.0
    cum_vol = 0.0
    current_day = None

    for b in bars:
        b_day = b.timestamp.date()
        if current_day != b_day:
            current_day = b_day
            cum_pv = 0.0
            cum_vol = 0.0
        typical_px = (b.high + b.low + b.close) / 3.0
        vol = max(1.0, b.volume)
        cum_pv += typical_px * vol
        cum_vol += vol
        out.append(cum_pv / cum_vol)
    return out


def compute_wilder_atr_series(bars: Sequence[Bar], period: int = 14) -> List[float]:
    """Wilder's smoothed Average True Range (ATR_14)."""
    if not bars:
        return []
    trs: List[float] = []
    for i, b in enumerate(bars):
        if i == 0:
            tr = b.high - b.low
        else:
            prev_c = bars[i - 1].close
            tr = max(b.high - b.low, abs(b.high - prev_c), abs(b.low - prev_c))
        trs.append(max(0.01, tr))

    atrs: List[float] = []
    for i, tr in enumerate(trs):
        if i < period:
            atrs.append(sum(trs[: i + 1]) / float(i + 1))
        else:
            prev_atr = atrs[-1]
            atrs.append(((prev_atr * (period - 1)) + tr) / float(period))
    return atrs


def compute_rvol_series(bars: Sequence[Bar], lookback: int = 20) -> List[float]:
    """Relative Volume vs rolling `lookback`-bar average volume."""
    out: List[float] = []
    vols = [max(1.0, b.volume) for b in bars]
    for i, v in enumerate(vols):
        if i == 0:
            out.append(1.0)
        else:
            start = max(0, i - lookback)
            avg_v = sum(vols[start:i]) / float(i - start)
            out.append(v / avg_v if avg_v > 0 else 1.0)
    return out
