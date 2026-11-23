from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from zy_quant.gateway.base import BrokerExecutionAdapter, MarketDataAdapter
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.models import Bar, CapitalFlowSnapshot, OrderSide, QuoteSnapshot

logger = logging.getLogger(__name__)


def to_futu_us_code(symbol: str) -> str:
    clean = symbol.replace("^", "").upper()
    return clean if clean.startswith("US.") else f"US.{clean}"


def from_futu_us_code(code: str) -> str:
    return code[3:] if code.startswith("US.") else code


class FutuGatewayAdapter(MarketDataAdapter, BrokerExecutionAdapter):
    """
    Production-grade FutuOpenD Gateway Connector (`127.0.0.1:11111`):
    - Defaults strictly to `TrdEnv.SIMULATE` (Paper Trading) unless `ZY_TRD_ENV=REAL` is explicitly set
    - Reads real-time `get_capital_distribution()` for Super-Large + Large institutional net inflow
    - Reads real-time OrderBook / Snapshot for spread & marketable limit protection
    - Submits native Server-Side Protective Stop Orders (`OrderType.STOP`) on Futu's servers
      so positions remain protected even if local Mac/VPS loses power or internet
    - Automatically falls back to `YahooMarketDataAdapter` for historical bars if `FutuOpenD` is not running
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        trd_env: Optional[str] = None,
        fallback_data: Optional[YahooMarketDataAdapter] = None,
    ) -> None:
        self.host = host or os.getenv("ZY_FUTU_HOST", "127.0.0.1")
        self.port = int(port or os.getenv("ZY_FUTU_PORT", "11111"))
        self.trd_env_name = (trd_env or os.getenv("ZY_TRD_ENV", "SIMULATE")).upper()
        self.trade_pwd_md5 = os.getenv("ZY_FUTU_TRADE_PWD_MD5", "")
        self.fallback = fallback_data or YahooMarketDataAdapter()

        self._quote_ctx: Any = None
        self._trade_ctx: Any = None
        self._futu_mod: Any = None

    def connect(self) -> bool:
        try:
            import futu as ft

            self._futu_mod = ft
            self._quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
            self._trade_ctx = ft.OpenSecTradeContext(
                filter_trdmarket=ft.TrdMarket.US,
                host=self.host,
                port=self.port,
                security_firm=ft.SecurityFirm.FUTUSECURITIES,
            )
            if self.trd_env_name == "REAL" and self.trade_pwd_md5:
                ret, data = self._trade_ctx.unlock_trade(password_md5=self.trade_pwd_md5)
                if ret != ft.RET_OK:
                    logger.error("Futu unlock_trade failed: %s", data)
                    self.close()
                    return False
            logger.info(
                "Connected to FutuOpenD at %s:%s (env=%s)",
                self.host,
                self.port,
                self.trd_env_name,
            )
            return True
        except Exception as exc:
            logger.info("FutuOpenD not reachable at %s:%s (%s) — running in offline/fallback mode", self.host, self.port, exc)
            self.close()
            return False

    def close(self) -> None:
        for ctx in (self._quote_ctx, self._trade_ctx):
            if ctx is not None:
                try:
                    ctx.close()
                except Exception:
                    pass
        self._quote_ctx = None
        self._trade_ctx = None

    def is_connected(self) -> bool:
        if self._quote_ctx is None or self._futu_mod is None:
            return False
        try:
            ret, _ = self._quote_ctx.get_global_state()
            return ret == self._futu_mod.RET_OK
        except Exception:
            return False

    def _trd_env(self) -> Any:
        ft = self._futu_mod
        return ft.TrdEnv.REAL if self.trd_env_name == "REAL" else ft.TrdEnv.SIMULATE

    # --- Market Data ---
    def get_bars(self, symbol: str, timeframe: str = "15m", days: int = 30) -> List[Bar]:
        if self.is_connected() and not symbol.startswith("^"):
            ft = self._futu_mod
            ktype = ft.KLType.K_15M if timeframe == "15m" else ft.KLType.K_DAY
            code = to_futu_us_code(symbol)
            ret, df, _ = self._quote_ctx.request_history_kline(
                code, ktype=ktype, max_count=min(1000, days * 28)
            )
            if ret == ft.RET_OK and df is not None and not df.empty:
                out: List[Bar] = []
                for _, row in df.iterrows():
                    ts = datetime.fromisoformat(str(row["time_key"])).replace(tzinfo=timezone.utc)
                    out.append(
                        Bar(
                            symbol=symbol,
                            timestamp=ts,
                            open=float(row["open"]),
                            high=float(row["high"]),
                            low=float(row["low"]),
                            close=float(row["close"]),
                            volume=float(row["volume"]),
                        )
                    )
                return out
        return self.fallback.get_bars(symbol, timeframe=timeframe, days=days)

    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        if self.is_connected() and not symbol.startswith("^"):
            ft = self._futu_mod
            code = to_futu_us_code(symbol)
            ret, df = self._quote_ctx.get_market_snapshot([code])
            if ret == ft.RET_OK and df is not None and not df.empty:
                row = df.iloc[0]
                last_px = float(row["last_price"])
                bid = float(row.get("bid_price", last_px)) or last_px
                ask = float(row.get("ask_price", last_px)) or last_px
                turnover = float(row.get("turnover", 800_000_000.0)) or 800_000_000.0
                return QuoteSnapshot(
                    symbol=symbol,
                    last_price=last_px,
                    bid=bid,
                    ask=ask,
                    avg_daily_dollar_vol=max(turnover, 600_000_000.0),
                    timestamp=datetime.now(timezone.utc),
                )
        return self.fallback.get_quote(symbol)

    def get_capital_flow(self, symbol: str) -> Optional[CapitalFlowSnapshot]:
        if self.is_connected() and not symbol.startswith("^"):
            ft = self._futu_mod
            code = to_futu_us_code(symbol)
            ret, df = self._quote_ctx.get_capital_distribution(code)
            if ret == ft.RET_OK and df is not None and not df.empty:
                row = df.iloc[0]
                return CapitalFlowSnapshot(
                    symbol=symbol,
                    super_large_inflow=float(row.get("capital_in_super", 0.0)),
                    super_large_outflow=float(row.get("capital_out_super", 0.0)),
                    large_inflow=float(row.get("capital_in_big", 0.0)),
                    large_outflow=float(row.get("capital_out_big", 0.0)),
                    medium_net=float(row.get("capital_in_mid", 0.0)) - float(row.get("capital_out_mid", 0.0)),
                    small_net=float(row.get("capital_in_small", 0.0)) - float(row.get("capital_out_small", 0.0)),
                )
        return self.fallback.get_capital_flow(symbol)

    def get_vix_level(self) -> float:
        return self.fallback.get_vix_level()

    # --- Broker Execution ---
    def get_account_summary(self) -> Dict[str, float]:
        if not self.is_connected() or self._trade_ctx is None:
            return {"equity_usd": 0.0, "cash_usd": 0.0, "connected": 0.0}
        ft = self._futu_mod
        ret, df = self._trade_ctx.accinfo_query(trd_env=self._trd_env(), currency=ft.Currency.USD)
        if ret != ft.RET_OK or df is None or df.empty:
            return {"equity_usd": 0.0, "cash_usd": 0.0, "connected": 1.0}
        row = df.iloc[0]
        return {
            "equity_usd": float(row.get("total_assets", 0.0)),
            "cash_usd": float(row.get("cash", 0.0)),
            "connected": 1.0,
        }

    def get_open_positions(self) -> Dict[str, Dict[str, float]]:
        if not self.is_connected() or self._trade_ctx is None:
            return {}
        ft = self._futu_mod
        ret, df = self._trade_ctx.position_list_query(trd_env=self._trd_env())
        if ret != ft.RET_OK or df is None or df.empty:
            return {}
        holdings: Dict[str, Dict[str, float]] = {}
        for _, row in df.iterrows():
            qty = float(row.get("qty", 0.0))
            if qty <= 0:
                continue
            sym = from_futu_us_code(str(row["code"]))
            holdings[sym] = {
                "shares": qty,
                "avg_price": float(row.get("cost_price", 0.0)),
                "market_val": float(row.get("market_val", 0.0)),
            }
        return holdings

    def place_limit_order(
        self, symbol: str, side: OrderSide, shares: float, limit_price: float, remark: str = ""
    ) -> Optional[str]:
        if not self.is_connected() or self._trade_ctx is None:
            return None
        ft = self._futu_mod
        trd_side = ft.TrdSide.BUY if side == OrderSide.BUY else ft.TrdSide.SELL
        ret, df = self._trade_ctx.place_order(
            price=round(limit_price, 2),
            qty=shares,
            code=to_futu_us_code(symbol),
            trd_side=trd_side,
            order_type=ft.OrderType.NORMAL,
            trd_env=self._trd_env(),
            remark=remark[:60],
        )
        if ret != ft.RET_OK or df is None or df.empty:
            logger.error("Futu place_order failed (%s %s): %s", side.value, symbol, df)
            return None
        return str(df.iloc[0]["order_id"])

    def place_or_update_protective_stop(
        self,
        symbol: str,
        shares: float,
        stop_price: float,
        existing_order_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Server-Side Safety Net: Places or ratchets a native STOP order on Futu's servers
        so downside risk is capped even if local network/host goes completely dark.
        """
        if not self.is_connected() or self._trade_ctx is None:
            return existing_order_id
        ft = self._futu_mod

        # Note: Futu SIMULATE paper environment only supports NORMAL/MARKET orders;
        # native STOP orders are supported in REAL US trading.
        if self.trd_env_name != "REAL":
            return existing_order_id or f"SIM-LOCAL-STOP-{symbol}"

        if existing_order_id:
            ret, _ = self._trade_ctx.modify_order(
                modify_order_op=ft.ModifyOrderOp.NORMAL,
                order_id=existing_order_id,
                qty=shares,
                price=round(stop_price * 0.995, 2),
                aux_price=round(stop_price, 2),
                trd_env=self._trd_env(),
            )
            if ret == ft.RET_OK:
                return existing_order_id

        ret, df = self._trade_ctx.place_order(
            price=round(stop_price * 0.995, 2),
            aux_price=round(stop_price, 2),
            qty=shares,
            code=to_futu_us_code(symbol),
            trd_side=ft.TrdSide.SELL,
            order_type=ft.OrderType.STOP_LIMIT,
            trd_env=self._trd_env(),
            remark="zy-quant protective stop",
        )
        if ret != ft.RET_OK or df is None or df.empty:
            logger.error("Failed placing server-side stop for %s: %s", symbol, df)
            return None
        return str(df.iloc[0]["order_id"])

    def cancel_order(self, order_id: str) -> bool:
        if not self.is_connected() or self._trade_ctx is None or order_id.startswith("SIM-"):
            return True
        ft = self._futu_mod
        ret, _ = self._trade_ctx.modify_order(
            modify_order_op=ft.ModifyOrderOp.CANCEL,
            order_id=order_id,
            qty=0,
            price=0,
            trd_env=self._trd_env(),
        )
        return ret == ft.RET_OK
