from __future__ import annotations

from datetime import date
from typing import Mapping, Optional, Protocol, Sequence, runtime_checkable

from zy_quant.models import (
    Bar,
    CapitalFlowSnapshot,
    EarningsEvent,
    EntrySignal,
    RegimeLight,
    RegimeSnapshot,
)


@runtime_checkable
class StrategyPipelineProtocol(Protocol):
    """
    Timeframe-agnostic strategy signal & regime interface decoupling `TradingDaemon`
    and `BacktestEngine` across `"swing_1d"` and `"intraday_15m"` modes.
    """

    @property
    def mode(self) -> str: ...

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
    ) -> RegimeSnapshot: ...

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
    ) -> Optional[EntrySignal]: ...
