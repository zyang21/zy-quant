from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class RegimeLight(str, Enum):
    GREEN = "GREEN"    # Bullish trend: QQQ > VWAP & > SMA20, VIX calm -> entries enabled (15m loop)
    YELLOW = "YELLOW"  # Mixed/Caution: hold existing w/ stops, block new entries (2h watchlist RS scan)
    RED = "RED"        # Bearish/Panic: lock new buys -> 0 new trades, 0 churn fees (2h watchlist RS scan)


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class SetupType(str, Enum):
    VWAP_PULLBACK = "VWAP_PULLBACK"
    ORB_30M = "ORB_30M"


class ExitReason(str, Enum):
    INITIAL_ATR_STOP = "INITIAL_ATR_STOP"
    BREAKEVEN_STOP = "BREAKEVEN_STOP"
    CHANDELIER_TRAIL = "CHANDELIER_TRAIL"
    TIME_BARRIER = "TIME_BARRIER"
    SCALE_OUT_2R = "SCALE_OUT_2R"
    KILL_SWITCH = "KILL_SWITCH"


@dataclass
class Bar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class QuoteSnapshot:
    symbol: str
    last_price: float
    bid: float
    ask: float
    avg_daily_dollar_vol: float
    timestamp: datetime

    @property
    def spread_bps(self) -> float:
        mid = 0.5 * (self.bid + self.ask)
        if mid <= 0:
            return 0.0
        return ((self.ask - self.bid) / mid) * 10000.0


@dataclass
class CapitalFlowSnapshot:
    """Real-time institutional capital flow from Futu get_capital_distribution()."""

    symbol: str
    super_large_inflow: float
    super_large_outflow: float
    large_inflow: float
    large_outflow: float
    medium_net: float = 0.0
    small_net: float = 0.0

    @property
    def big_order_net_inflow(self) -> float:
        return (self.super_large_inflow - self.super_large_outflow) + (
            self.large_inflow - self.large_outflow
        )

    @property
    def big_order_inflow_ratio(self) -> float:
        gross = (
            self.super_large_inflow
            + self.super_large_outflow
            + self.large_inflow
            + self.large_outflow
        )
        if gross <= 0:
            return 0.0
        return self.big_order_net_inflow / gross


@dataclass
class InsiderActivity:
    symbol: str
    open_market_buy_count_14d: int = 0
    net_shares_bought_14d: float = 0.0
    latest_filing_date: Optional[str] = None


@dataclass
class RegimeSnapshot:
    light: RegimeLight
    qqq_price: float
    qqq_vwap: float
    qqq_sma20: float
    qqq_intraday_return: float
    spy_price: float
    spy_sma20: float
    vix_level: float
    recommended_scan_minutes: int
    reason: str
    evaluated_at: datetime


@dataclass
class WatchlistCandidate:
    symbol: str
    sector: str
    price: float
    total_score: float
    passed_hard_filters: bool
    filter_reason: str
    etf_component_score: float
    institutional_component_score: float
    stability_rs_component_score: float
    big_flow_component_score: float
    insider_component_score: float
    relative_strength_vs_qqq: float
    big_order_net_usd: float
    updated_at: datetime


@dataclass
class EntrySignal:
    symbol: str
    sector: str
    setup: SetupType
    timestamp: datetime
    entry_price: float
    atr_14: float
    vwap: float
    rvol: float
    watchlist_score: float
    rationale: str


@dataclass
class PositionPlan:
    symbol: str
    sector: str
    shares: float
    entry_price: float
    limit_price: float
    initial_stop: float
    risk_per_share: float
    total_risk_usd: float
    notional_usd: float
    estimated_round_trip_fee_usd: float
    expected_reward_r: float
    expectancy_r: float
    setup: SetupType


@dataclass
class OpenPosition:
    symbol: str
    sector: str
    shares: float
    initial_shares: float
    entry_price: float
    entry_time: datetime
    initial_stop: float
    current_stop: float
    highest_price: float
    risk_per_share: float
    bars_held_15m: int = 0
    breakeven_armed: bool = False
    scaled_out: bool = False
    broker_stop_order_id: Optional[str] = None
    setup: str = "VWAP_PULLBACK"
    metadata: dict = field(default_factory=dict)
