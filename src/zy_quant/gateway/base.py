from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from zy_quant.models import (
    Bar,
    BrokerOrderUpdate,
    CapitalFlowSnapshot,
    OrderSide,
    QuoteSnapshot,
)


class MarketDataAdapter(ABC):
    @abstractmethod
    def get_bars(self, symbol: str, timeframe: str = "15m", days: int = 30) -> List[Bar]:
        raise NotImplementedError

    @abstractmethod
    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        raise NotImplementedError

    @abstractmethod
    def get_capital_flow(self, symbol: str) -> Optional[CapitalFlowSnapshot]:
        raise NotImplementedError

    @abstractmethod
    def get_vix_level(self) -> float:
        raise NotImplementedError


class BrokerExecutionAdapter(ABC):
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_account_summary(self) -> Dict[str, float]:
        raise NotImplementedError

    @abstractmethod
    def get_open_positions(self) -> Dict[str, Dict[str, float]]:
        raise NotImplementedError

    @abstractmethod
    def place_limit_order(
        self, symbol: str, side: OrderSide, shares: float, limit_price: float, remark: str = ""
    ) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def place_or_update_protective_stop(
        self,
        symbol: str,
        shares: float,
        stop_price: float,
        existing_order_id: Optional[str] = None,
    ) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    def has_target_account(self) -> bool:
        return False

    def register_order_callback(self, callback: Callable[[BrokerOrderUpdate], Any]) -> None:
        return None

    def sync_open_orders(self, refresh_cache: bool = False) -> List[BrokerOrderUpdate]:
        return []

    def query_order_fee(self, order_id: str) -> Optional[float]:
        return None
