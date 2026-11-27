from __future__ import annotations

import math
from bisect import bisect_left
from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from zy_quant.intel.market_hours import ET_TZ
from zy_quant.models import Bar


def _bar_et_date(b: Bar) -> date:
    ts = b.timestamp
    return ts.date() if ts.tzinfo is None else ts.astimezone(ET_TZ).date()


def _extract_causal_daily_returns(
    bars: Sequence[Bar],
    as_of_date: date,
    lookback_days: int = 60,
) -> Dict[date, float]:
    """
    Slices strictly completed daily bars `< as_of_date` via `bisect_left` (zero look-ahead of
    `as_of_date`'s 16:00 ET close) and computes up to `lookback_days` log returns keyed by ET date.
    """
    if len(bars) < 3:
        return {}
    dates = [_bar_et_date(b) for b in bars]
    cut = bisect_left(dates, as_of_date)
    if cut < 3:
        return {}
    start = max(0, cut - (lookback_days + 1))
    sliced = bars[start:cut]
    s_dates = dates[start:cut]

    rets: Dict[date, float] = {}
    for i in range(1, len(sliced)):
        prev_c = sliced[i - 1].close
        curr_c = sliced[i].close
        if prev_c > 0 and curr_c > 0:
            rets[s_dates[i]] = math.log(curr_c / prev_c)
    return rets


def compute_pearson_corr(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 20 or len(ys) != n:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    denom = math.sqrt(vx * vy)
    if denom <= 1e-12:
        return None
    return max(-1.0, min(1.0, cov / denom))


def compute_max_pairwise_correlation(
    candidate_symbol: str,
    comparison_symbols: Iterable[str],
    daily_bars_map: Mapping[str, Sequence[Bar]],
    as_of_date: date,
    lookback_days: int = 60,
    min_overlap_days: int = 25,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Shared pure correlation evaluator (used identically by `MandateRiskGate` and `BacktestEngine`):
      - Inner-joins strictly completed `[T-lookback .. T-1]` `ET_TZ` daily returns (`bisect_left`)
      - Compares `candidate_symbol` against `open_positions | pending_buy_symbols` so correlated
        names (e.g., `NVDA + AVGO + SMH`) cannot crowd in the same `15:40 ET` batch.
    """
    cand = candidate_symbol.upper()
    cand_bars = daily_bars_map.get(cand)
    if not cand_bars:
        return None, None

    cand_rets = _extract_causal_daily_returns(cand_bars, as_of_date, lookback_days)
    if len(cand_rets) < min_overlap_days:
        return None, None

    max_corr: Optional[float] = None
    max_peer: Optional[str] = None
    for peer_raw in comparison_symbols:
        peer = peer_raw.upper()
        if peer == cand:
            continue
        peer_bars = daily_bars_map.get(peer)
        if not peer_bars:
            continue
        peer_rets = _extract_causal_daily_returns(peer_bars, as_of_date, lookback_days)
        common_dates = sorted(cand_rets.keys() & peer_rets.keys())[-lookback_days:]
        if len(common_dates) < min_overlap_days:
            continue
        xs = [cand_rets[d] for d in common_dates]
        ys = [peer_rets[d] for d in common_dates]
        corr = compute_pearson_corr(xs, ys)
        if corr is not None and (max_corr is None or corr > max_corr):
            max_corr = round(corr, 4)
            max_peer = peer

    return max_corr, max_peer
