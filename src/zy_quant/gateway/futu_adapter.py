from __future__ import annotations

import logging
import math
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from zy_quant.config import FutuConnectionConfig
from zy_quant.gateway.base import BrokerExecutionAdapter, MarketDataAdapter
from zy_quant.gateway.yfinance_adapter import YahooMarketDataAdapter
from zy_quant.models import (
    Bar,
    BrokerOrderUpdate,
    CapitalFlowSnapshot,
    OrderSide,
    OrderStatus,
    QuoteSnapshot,
)

logger = logging.getLogger(__name__)


def to_futu_us_code(symbol: str) -> str:
    clean = symbol.replace("^", "").upper()
    return clean if clean.startswith("US.") else f"US.{clean}"


def from_futu_us_code(code: str) -> str:
    return code[3:] if code.startswith("US.") else code


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val in (None, "N/A", ""):
        return default
    try:
        f = float(val)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def map_futu_order_status(raw_status: str, dealt_qty: float = 0.0) -> OrderStatus:
    """
    Maps Futu v10.12 OrderStatus strings into internal OrderStatus lifecycle states.
    Note: CANCELLED_PART occurs when a partial fill's remaining open shares are cancelled;
    returning terminal CANCELLED with filled_shares > 0 lets the daemon lock in exact dealt_qty.
    """
    st = (raw_status or "").strip().upper()
    if st == "FILLED_ALL":
        return OrderStatus.FILLED
    if st in ("FILLED_PART", "CANCELLING_PART"):
        return OrderStatus.PARTIAL_FILLED
    if st in ("WAITING_SUBMIT", "SUBMITTING", "SUBMITTED", "UNSUBMITTED", "CANCELLING_ALL"):
        return OrderStatus.PARTIAL_FILLED if dealt_qty > 0 else OrderStatus.SUBMITTED
    if st in ("CANCELLED_ALL", "CANCELLED_PART", "DELETED", "FILL_CANCELLED"):
        return OrderStatus.CANCELLED
    if st == "TIMEOUT":
        return OrderStatus.EXPIRED
    if st in ("FAILED", "SUBMIT_FAILED", "DISABLED"):
        return OrderStatus.FAILED
    return OrderStatus.SUBMITTED


def parse_futu_order_row(row: Any) -> BrokerOrderUpdate:
    dealt_qty = _safe_float(row.get("dealt_qty"), 0.0)
    dealt_avg = _safe_float(row.get("dealt_avg_price"), 0.0)
    raw_status = str(row.get("order_status", "") or "SUBMITTED").strip().upper()
    side_str = str(row.get("trd_side", "") or "BUY").strip().upper()
    side = OrderSide.SELL if side_str == "SELL" else OrderSide.BUY
    remark_val = row.get("remark", "")
    err_val = row.get("last_err_msg", "")
    upd_val = row.get("updated_time", "")
    return BrokerOrderUpdate(
        broker_order_id=str(row.get("order_id", "") or ""),
        symbol=from_futu_us_code(str(row.get("code", "") or "")),
        side=side,
        order_type=str(row.get("order_type", "") or "NORMAL"),
        status=map_futu_order_status(raw_status, dealt_qty),
        raw_status=raw_status,
        qty=_safe_float(row.get("qty"), 0.0),
        price=_safe_float(row.get("price"), 0.0),
        filled_shares=dealt_qty,
        avg_fill_price=round(dealt_avg, 4) if dealt_avg > 0 else None,
        remark="" if remark_val in (None, "N/A") else str(remark_val),
        last_err_msg="" if err_val in (None, "N/A") else str(err_val),
        updated_time="" if upd_val in (None, "N/A") else str(upd_val),
    )


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
        api_token: Optional[str] = None,
        futu_cfg: Optional[FutuConnectionConfig] = None,
        allow_synthetic_fallback: bool = False,
        fallback_data: Optional[YahooMarketDataAdapter] = None,
    ) -> None:
        cfg = futu_cfg or FutuConnectionConfig()
        self.host = host or os.getenv("ZY_FUTU_HOST") or cfg.host
        self.port = int(port or os.getenv("ZY_FUTU_PORT") or cfg.port)
        self.trd_env_name = (trd_env or os.getenv("ZY_TRD_ENV") or cfg.trd_env).upper()
        self.api_token = (
            api_token
            if api_token is not None
            else (cfg.api_token or os.getenv("ZY_FUTU_API_TOKEN", ""))
        )
        self.trade_pwd_md5 = cfg.trade_pwd_md5 or os.getenv("ZY_FUTU_TRADE_PWD_MD5", "")
        self.fallback = fallback_data or YahooMarketDataAdapter(
            allow_synthetic_fallback=allow_synthetic_fallback
        )

        self._quote_ctx: Any = None
        self._trade_ctx: Any = None
        self._futu_mod: Any = None
        self._rpc_lock = threading.RLock()
        self._cap_dist_supported: bool = True
        self._order_fee_supported: bool = True
        self._order_callbacks: List[Callable[[BrokerOrderUpdate], Any]] = []
        self._order_handler: Any = None
        self._kline_cache: Dict[tuple[str, str], tuple[float, int, List[Bar]]] = {}
        self._last_kline_rpc_mono: float = 0.0
        self._last_order_refresh_mono: float = 0.0
        self.last_order_sync_ok: bool = False
        self.last_position_sync_ok: bool = False
        self.last_error: str = "Not connected yet — paste Futubull OpenAPI Key (连接密钥) and click Connect"

    def register_order_callback(self, callback: Callable[[BrokerOrderUpdate], Any]) -> None:
        if callback not in self._order_callbacks:
            self._order_callbacks.append(callback)

    def _dispatch_order_update(self, update: BrokerOrderUpdate) -> None:
        for cb in list(self._order_callbacks):
            try:
                cb(update)
            except Exception as exc:
                logger.exception("Error in order update callback for %s: %s", update.broker_order_id, exc)

    def reconfigure(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        trd_env: Optional[str] = None,
        api_token: Optional[str] = None,
    ) -> bool:
        self.close()
        if host:
            self.host = host.strip()
        if port:
            self.port = int(port)
        if trd_env:
            self.trd_env_name = trd_env.strip().upper()
        if api_token is not None:
            self.api_token = api_token.strip()
        return self.connect()

    def _tcp_reachable(self, timeout: float = 0.8) -> bool:
        try:
            with socket.create_connection((self.host, self.port), timeout=timeout):
                return True
        except OSError as exc:
            self.last_error = (
                f"Cannot reach OpenAPI port {self.host}:{self.port} ({exc}). "
                f"Enable OpenAPI in 富途牛牛 -> Settings -> OpenAPI (or launch FutuOpenD)."
            )
            return False

    def connect(self) -> bool:
        self.close()
        if not self._tcp_reachable():
            return False
        try:
            import futu as ft
            from futu.common.open_context_base import ContextStatus

            ft.SysConfig.set_all_thread_daemon(True)
            ft.SysConfig.set_client_info("zy-quant", 1)
            ft.SysConfig.set_token(self.api_token)

            self._futu_mod = ft
            # Use is_async_connect=True so an Unauthorized/missing token fails fast in <2s
            # instead of hanging forever inside futu-api's synchronous infinite retry loop.
            self._quote_ctx = ft.OpenQuoteContext(
                host=self.host, port=self.port, is_async_connect=True
            )
            deadline = time.monotonic() + 2.2
            ready = False
            while time.monotonic() < deadline:
                if getattr(self._quote_ctx, "_status", None) == ContextStatus.READY:
                    ready = True
                    break
                time.sleep(0.1)

            if not ready:
                self.last_error = (
                    f"Port {self.host}:{self.port} rejected InitConnect (Unauthorized). "
                    f"Open 富途牛牛 -> Settings (设置) -> OpenAPI, copy the API Key / 连接密钥, and paste it in the Token field!"
                )
                logger.warning(self.last_error)
                self.close()
                return False

            self._trade_ctx = ft.OpenSecTradeContext(
                filter_trdmarket=ft.TrdMarket.US,
                host=self.host,
                port=self.port,
                security_firm=ft.SecurityFirm.NONE,
            )
            self._attach_order_push_handler(ft)
            if self.trd_env_name == "REAL" and self.trade_pwd_md5:
                ret, data = self._trade_ctx.unlock_trade(password_md5=self.trade_pwd_md5)
                if ret != ft.RET_OK:
                    self.last_error = f"Futu unlock_trade failed: {data}"
                    logger.error(self.last_error)
                    self.close()
                    return False
            self.last_error = ""
            logger.info(
                "Connected to Futu OpenAPI at %s:%s (env=%s)",
                self.host,
                self.port,
                self.trd_env_name,
            )
            return True
        except Exception as exc:
            self.last_error = f"Futu OpenAPI handshake failed at {self.host}:{self.port}: {exc}"
            logger.info("%s — running in offline/fallback mode", self.last_error)
            self.close()
            return False

    def _attach_order_push_handler(self, ft: Any) -> None:
        if self._trade_ctx is None:
            return
        adapter_ref = self

        class ZyTradeOrderHandler(ft.TradeOrderHandlerBase):
            """Real-time Futu order lifecycle push handler (works in both SIMULATE and REAL)."""

            def on_recv_rsp(self, rsp_pb: Any) -> Any:
                ret_code, content = super().on_recv_rsp(rsp_pb)
                if ret_code != ft.RET_OK or content is None or getattr(content, "empty", True):
                    return ret_code, content
                for _, row in content.iterrows():
                    row_env = str(row.get("trd_env", "") or "").strip().upper()
                    if row_env and row_env != adapter_ref.trd_env_name:
                        continue
                    upd = parse_futu_order_row(row)
                    if upd.broker_order_id:
                        adapter_ref._dispatch_order_update(upd)
                return ret_code, content

        self._order_handler = ZyTradeOrderHandler()
        self._trade_ctx.set_handler(self._order_handler)

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
            from futu.common.open_context_base import ContextStatus

            return getattr(self._quote_ctx, "_status", None) == ContextStatus.READY
        except Exception:
            return False

    def _trd_env(self) -> Any:
        ft = self._futu_mod
        return ft.TrdEnv.REAL if self.trd_env_name == "REAL" else ft.TrdEnv.SIMULATE

    # --- Market Data ---
    def get_bars(self, symbol: str, timeframe: str = "15m", days: int = 30) -> List[Bar]:
        if self.is_connected() and not symbol.startswith("^"):
            cal_days = max(8, int(days * 1.6) + 4) if timeframe == "15m" else max(45, int(days * 1.6) + 10)
            cache_key = (symbol, timeframe)
            now_mono = time.monotonic()
            ttl = 300.0 if days >= 10 else 45.0

            cached = self._kline_cache.get(cache_key)
            if cached and (now_mono - cached[0]) < ttl and cached[1] >= cal_days:
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=cal_days + 1)
                return [b for b in cached[2] if b.timestamp >= cutoff_dt]

            ft = self._futu_mod
            ktype = ft.KLType.K_15M if timeframe == "15m" else ft.KLType.K_DAY
            code = to_futu_us_code(symbol)
            start_date = (datetime.now(timezone.utc) - timedelta(days=cal_days)).strftime("%Y-%m-%d")

            frames = []
            page_key = None
            with self._rpc_lock:
                for _ in range(4):
                    # Pace K-line RPCs under Futu's 60 req / 30s quota (~0.52s min spacing)
                    elapsed = time.monotonic() - self._last_kline_rpc_mono
                    if elapsed < 0.52:
                        time.sleep(0.52 - elapsed)
                    self._last_kline_rpc_mono = time.monotonic()

                    ret, df, page_key = self._quote_ctx.request_history_kline(
                        code, start=start_date, ktype=ktype, max_count=1000, page_req_key=page_key
                    )
                    if ret != ft.RET_OK and isinstance(df, str) and any(
                        kw in df.lower() for kw in ("freq", "频繁", "limit", "too many")
                    ):
                        time.sleep(2.0)
                        self._last_kline_rpc_mono = time.monotonic()
                        ret, df, page_key = self._quote_ctx.request_history_kline(
                            code, start=start_date, ktype=ktype, max_count=1000, page_req_key=page_key
                        )
                    if ret != ft.RET_OK or df is None or isinstance(df, str) or df.empty:
                        break
                    frames.append(df)
                    if page_key is None:
                        break

            if frames:
                out: List[Bar] = []
                for frame in frames:
                    for _, row in frame.iterrows():
                        ts_str = str(row["time_key"]).replace(" ", "T")
                        parsed_dt = datetime.fromisoformat(ts_str)
                        if timeframe == "1d" and parsed_dt.hour == 0 and parsed_dt.minute == 0:
                            # Anchor daily bars at 12:00 UTC (07:00/08:00 ET) so UTC and ET_TZ dates match identically
                            ts = parsed_dt.replace(hour=12, tzinfo=timezone.utc)
                        else:
                            ts = parsed_dt.replace(tzinfo=timezone.utc)
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
                self._kline_cache[cache_key] = (time.monotonic(), cal_days, out)
                return out
            # Never contaminate a connected Futu session with fake synthetic bars
            logger.warning(
                "[DATA WARNING] Futu OpenAPI returned empty KLine for %s (%s); skipping fallback contamination",
                symbol,
                timeframe,
            )
            return cached[2] if cached else []
        logger.warning(
            "[FALLBACK ALERT] Futu OpenAPI disconnected — entering fallback data adapter for %s (%s)",
            symbol,
            timeframe,
        )
        return self.fallback.get_bars(symbol, timeframe=timeframe, days=days)

    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        """
        Uses fast K_DAY bars (0.15s on Futubull Desktop Built-In OpenAPIHost, where
        `get_market_snapshot` times out for 12.9s when LV1 snapshot streaming isn't subscribed).
        """
        bars_1d = self.get_bars(symbol, timeframe="1d", days=25)
        if bars_1d:
            last = bars_1d[-1]
            avg_dvol = sum(b.close * b.volume for b in bars_1d[-20:]) / float(min(20, len(bars_1d)))
            half_spread = max(0.01, round(last.close * 0.00015, 2))
            return QuoteSnapshot(
                symbol=symbol,
                last_price=round(last.close, 2),
                bid=round(last.close - half_spread, 2),
                ask=round(last.close + half_spread, 2),
                avg_daily_dollar_vol=max(avg_dvol, 600_000_000.0),
                timestamp=last.timestamp,
            )
        return self.fallback.get_quote(symbol)

    def get_capital_flow(self, symbol: str) -> Optional[CapitalFlowSnapshot]:
        if self.is_connected() and not symbol.startswith("^"):
            if self._cap_dist_supported:
                ft = self._futu_mod
                code = to_futu_us_code(symbol)
                with self._rpc_lock:
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
                if isinstance(df, str) and "Unknown protocol ID" in df:
                    # Futubull Desktop built-in OpenAPIHost omits protocol 3212; avoid repeating unsupported RPC
                    self._cap_dist_supported = False

            # Compute block dollar flow directly from Futu's real-time 15m K-lines
            bars_15m = self.get_bars(symbol, timeframe="15m", days=3)
            if bars_15m:
                recent = bars_15m[-16:]
                inflow = sum(b.close * b.volume * 0.35 for b in recent if b.close >= b.open)
                outflow = sum(b.close * b.volume * 0.35 for b in recent if b.close < b.open)
                return CapitalFlowSnapshot(
                    symbol=symbol,
                    super_large_inflow=inflow * 0.45,
                    super_large_outflow=outflow * 0.45,
                    large_inflow=inflow * 0.55,
                    large_outflow=outflow * 0.55,
                )
        return self.fallback.get_capital_flow(symbol)

    def get_vix_level(self) -> float:
        if self.is_connected():
            # Compute annualized 10-day realized volatility of QQQ directly from cached Futu K-lines
            # so we never block on Yahoo Finance ^VIX when Yahoo is unreachable on local network!
            qqq_d = self.get_bars("QQQ", timeframe="1d", days=20)
            if len(qqq_d) >= 11:
                import math

                rets = [
                    math.log(qqq_d[i].close / qqq_d[i - 1].close)
                    for i in range(len(qqq_d) - 10, len(qqq_d))
                    if qqq_d[i - 1].close > 0
                ]
                if len(rets) >= 5:
                    mean_r = sum(rets) / len(rets)
                    var_r = sum((r - mean_r) ** 2 for r in rets) / max(1, len(rets) - 1)
                    hv_annual = math.sqrt(var_r * 252.0) * 100.0
                    return round(max(11.0, min(80.0, hv_annual * 1.15)), 2)
        return self.fallback.get_vix_level()

    # --- Broker Execution ---
    def _select_acc_id(self, env_name: Optional[str] = None) -> int:
        if not self.is_connected() or self._trade_ctx is None:
            return 0
        target_env = env_name or self.trd_env_name
        ft = self._futu_mod
        with self._rpc_lock:
            ret, df = self._trade_ctx.get_acc_list()
        if ret != ft.RET_OK or df is None or df.empty:
            return 0
        env_rows = df[df["trd_env"] == target_env]
        if env_rows.empty:
            return 0
        if target_env == "SIMULATE" and "sim_acc_type" in env_rows.columns:
            stock_sim = env_rows[env_rows["sim_acc_type"] == "STOCK"]
            if not stock_sim.empty:
                return int(stock_sim.iloc[0]["acc_id"])
        return int(env_rows.iloc[0]["acc_id"])

    def has_target_account(self) -> bool:
        return self.is_connected() and self._select_acc_id(self.trd_env_name) != 0

    def sync_open_orders(self, refresh_cache: bool = False) -> List[BrokerOrderUpdate]:
        """
        Pull-based order reconciliation fallback via Futu order_list_query().
        Tracks `last_order_sync_ok` so callers never mistake an RPC/rate-limit error for an empty order book.
        """
        if not self.is_connected() or self._trade_ctx is None:
            self.last_order_sync_ok = False
            return []
        acc_id = self._select_acc_id(self.trd_env_name)
        if acc_id == 0:
            self.last_order_sync_ok = False
            return []
        ft = self._futu_mod
        now_mono = time.monotonic()
        eff_refresh = bool(refresh_cache and (now_mono - self._last_order_refresh_mono) >= 2.0)
        with self._rpc_lock:
            ret, df = self._trade_ctx.order_list_query(
                trd_env=self._trd_env(),
                acc_id=acc_id,
                refresh_cache=eff_refresh,
            )
        if ret != ft.RET_OK or df is None or isinstance(df, str):
            self.last_order_sync_ok = False
            logger.warning("Futu order_list_query failed (ret=%s): %s", ret, df)
            return []
        self.last_order_sync_ok = True
        if eff_refresh:
            self._last_order_refresh_mono = now_mono
        if df.empty:
            return []
        updates: List[BrokerOrderUpdate] = []
        for _, row in df.iterrows():
            upd = parse_futu_order_row(row)
            if upd.broker_order_id:
                updates.append(upd)
        return updates

    def query_order_fee(self, order_id: str) -> Optional[float]:
        """
        Queries exact broker billing fee via order_fee_query() in REAL mode.
        Gracefully returns None (triggering FutuUSFeeModel fallback) in SIMULATE mode,
        when Built-In OpenAPIHost returns 'Unknown protocol ID', or before clearing settles.
        """
        if (
            not self.is_connected()
            or self._trade_ctx is None
            or not self._order_fee_supported
            or self.trd_env_name != "REAL"
            or not order_id
            or order_id.startswith(("PAPER-", "SIM-"))
        ):
            return None
        acc_id = self._select_acc_id("REAL")
        if acc_id == 0:
            return None
        ft = self._futu_mod
        with self._rpc_lock:
            ret, df = self._trade_ctx.order_fee_query(
                order_id_list=[str(order_id)],
                trd_env=self._trd_env(),
                acc_id=acc_id,
            )
        if ret != ft.RET_OK:
            if isinstance(df, str) and "unknown protocol id" in df.lower():
                logger.warning(
                    "Futu host does not support order_fee_query protocol; falling back to FutuUSFeeModel"
                )
                self._order_fee_supported = False
            return None
        if df is None or df.empty:
            return None
        fee_val = _safe_float(df.iloc[0].get("fee_amount"), -1.0)
        return round(fee_val, 4) if fee_val > 0.0 else None

    def get_account_summary(self) -> Dict[str, Any]:
        if not self.is_connected() or self._trade_ctx is None:
            return {
                "equity_usd": 0.0,
                "cash_usd": 0.0,
                "market_val_usd": 0.0,
                "buying_power_usd": 0.0,
                "connected": 0.0,
                "note": "",
            }
        ft = self._futu_mod
        acc_id = self._select_acc_id(self.trd_env_name)
        query_env = self._trd_env()
        note = ""

        # Strict environment isolation: when in SIMULATE mode, NEVER read or expose REAL account funds.
        if acc_id == 0:
            return {
                "equity_usd": 0.0,
                "cash_usd": 0.0,
                "market_val_usd": 0.0,
                "buying_power_usd": 0.0,
                "connected": 1.0,
                "note": "Virtual Paper Ledger",
            }

        with self._rpc_lock:
            ret, df = self._trade_ctx.accinfo_query(
                trd_env=query_env, acc_id=acc_id, currency=ft.Currency.USD
            )
            queried_usd_ok = ret == ft.RET_OK and df is not None and not isinstance(df, str) and not df.empty
            if not queried_usd_ok:
                ret, df = self._trade_ctx.accinfo_query(trd_env=query_env, acc_id=acc_id)
        if ret != ft.RET_OK or df is None or isinstance(df, str) or df.empty:
            return {
                "equity_usd": 0.0,
                "cash_usd": 0.0,
                "market_val_usd": 0.0,
                "buying_power_usd": 0.0,
                "connected": 1.0,
                "note": note,
            }
        row = df.iloc[0]

        def _f(val: Any) -> float:
            try:
                return float(val) if val not in (None, "N/A", "") else 0.0
            except (TypeError, ValueError):
                return 0.0

        row_ccy = str(row.get("currency") or "").strip().upper()
        is_usd_denominated = queried_usd_ok or row_ccy == "USD"
        if is_usd_denominated:
            equity = _f(row.get("total_assets")) or _f(row.get("usd_assets"))
            has_us_cash_field = "us_cash" in row and row.get("us_cash") not in (None, "N/A", "")
            raw_cash = _f(row.get("us_cash")) if has_us_cash_field else _f(row.get("cash"))
            fund_assets = _f(row.get("fund_assets"))
            market_val = _f(row.get("market_val"))
            buying_power = _f(row.get("power"))
        else:
            # Fallback non-USD row (e.g. HKD unified summary): strictly read explicit US-dollar columns only!
            equity = _f(row.get("usd_assets"))
            raw_cash = _f(row.get("us_cash"))
            fund_assets = 0.0
            market_val = _f(row.get("us_market_val"))
            buying_power = 0.0

        # Futu HK Cash Plus (USD MMF auto-sweep) parks idle USD in `fund_assets` with `us_cash == 0`;
        # floor at $0 and cap at non-leveraged `equity - market_val` so MARGIN `power` never buys on leverage!
        liquid_cash = max(0.0, raw_cash + fund_assets)
        if equity > 0:
            liquid_cash = min(liquid_cash, max(0.0, equity - market_val))
        if buying_power > 0:
            liquid_cash = min(liquid_cash, buying_power)
        return {
            "equity_usd": equity,
            "cash_usd": liquid_cash,
            "raw_cash_usd": raw_cash,
            "fund_assets_usd": fund_assets,
            "market_val_usd": market_val,
            "buying_power_usd": buying_power,
            "connected": 1.0,
            "note": note,
        }

    def get_broker_status(self) -> Dict[str, Any]:
        connected = self.is_connected()
        accounts: List[Dict[str, Any]] = []
        acct_summary = self.get_account_summary() if connected else {
            "equity_usd": 0.0,
            "cash_usd": 0.0,
            "market_val_usd": 0.0,
            "buying_power_usd": 0.0,
            "connected": 0.0,
        }
        if connected and self._trade_ctx is not None:
            ft = self._futu_mod
            ret, df = self._trade_ctx.get_acc_list()
            if ret == ft.RET_OK and df is not None and not df.empty:
                for _, row in df.iterrows():
                    accounts.append(
                        {
                            "acc_id": str(row.get("acc_id", "")),
                            "trd_env": str(row.get("trd_env", "")),
                            "acc_type": str(row.get("acc_type", "")),
                            "sim_acc_type": str(row.get("sim_acc_type", "")),
                        }
                    )
        return {
            "connected": connected,
            "host": self.host,
            "port": self.port,
            "trd_env": self.trd_env_name,
            "data_source": "Futu OpenAPI Real-Time" if connected else "Yahoo Finance (Offline Fallback)",
            "last_error": self.last_error,
            "account_summary": acct_summary,
            "accounts": accounts,
        }

    def get_open_positions(self) -> Dict[str, Dict[str, float]]:
        if not self.is_connected() or self._trade_ctx is None:
            self.last_position_sync_ok = False
            return {}
        ft = self._futu_mod
        acc_id = self._select_acc_id()
        if acc_id == 0:
            self.last_position_sync_ok = False
            return {}
        with self._rpc_lock:
            ret, df = self._trade_ctx.position_list_query(trd_env=self._trd_env(), acc_id=acc_id)
        if ret != ft.RET_OK or df is None or isinstance(df, str):
            self.last_position_sync_ok = False
            logger.warning("Futu position_list_query failed (ret=%s): %s", ret, df)
            return {}
        self.last_position_sync_ok = True
        if df.empty:
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
        acc_id = self._select_acc_id(self.trd_env_name)
        if acc_id == 0:
            return None
        ft = self._futu_mod
        trd_side = ft.TrdSide.BUY if side == OrderSide.BUY else ft.TrdSide.SELL
        with self._rpc_lock:
            ret, df = self._trade_ctx.place_order(
                price=round(limit_price, 2),
                qty=shares,
                code=to_futu_us_code(symbol),
                trd_side=trd_side,
                order_type=ft.OrderType.NORMAL,
                trd_env=self._trd_env(),
                acc_id=acc_id,
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
        order_mode: str = "STOP",
    ) -> Optional[str]:
        """
        Server-Side Safety Net (`TimeInForce.GTC` + `fill_outside_rth=False`):
        - Guards `< 1.0` fractional positions (`SIM-CLIENT-FRAC-STOP-{symbol}`), since Futu US
          rejects server-side `STOP` and `GTC` orders on fractional lots.
        - For whole-share (`>= 1.0`) positions in `REAL` mode, places/modifies a persistent
          `TimeInForce.GTC` protective stop (`OrderType.STOP` default so overnight gap-downs never
          leave `STOP_LIMIT` orders stranded unfilled; `OrderType.STOP_LIMIT` also supported).
        """
        if shares <= 0:
            return None
        # Fractional share guard (< 1.0 sh): route strictly through client-side RTH stop monitor
        if shares < 1.0:
            if (
                existing_order_id
                and not existing_order_id.startswith(("SIM-", "PAPER-"))
                and self.is_connected()
            ):
                self.cancel_order(existing_order_id)
            return f"SIM-CLIENT-FRAC-STOP-{symbol}"

        whole_shares = float(math.floor(shares)) if shares != math.floor(shares) else float(shares)

        # Note: Futu SIMULATE / offline PAPER-LOCAL environment only supports NORMAL/MARKET orders;
        # native GTC STOP orders are supported in REAL US trading.
        if self.trd_env_name != "REAL":
            return existing_order_id or f"SIM-LOCAL-STOP-{symbol}"

        if not self.is_connected() or self._trade_ctx is None:
            return existing_order_id
        ft = self._futu_mod

        acc_id = self._select_acc_id("REAL")
        if acc_id == 0:
            return existing_order_id

        use_stop_market = order_mode.strip().upper() != "STOP_LIMIT"
        ord_type = ft.OrderType.STOP if use_stop_market else ft.OrderType.STOP_LIMIT
        lim_px = round(stop_price, 2) if use_stop_market else round(stop_price * 0.995, 2)
        trig_px = round(stop_price, 2)

        if existing_order_id and not existing_order_id.startswith(("SIM-", "PAPER-")):
            with self._rpc_lock:
                ret, _ = self._trade_ctx.modify_order(
                    modify_order_op=ft.ModifyOrderOp.NORMAL,
                    order_id=existing_order_id,
                    qty=whole_shares,
                    price=lim_px,
                    aux_price=trig_px,
                    trd_env=self._trd_env(),
                    acc_id=acc_id,
                )
            if ret == ft.RET_OK:
                return existing_order_id

            # Short-flip prevention: NEVER submit a second GTC STOP while `existing_order_id` may still be live!
            open_map = {u.broker_order_id: u for u in self.sync_open_orders(refresh_cache=True)}
            prev_ord = open_map.get(existing_order_id)
            if prev_ord is None or prev_ord.status.is_active:
                if not self.cancel_order(existing_order_id):
                    logger.error(
                        "Refusing to place duplicate GTC STOP for %s while existing stop %s could not be cancelled",
                        symbol,
                        existing_order_id,
                    )
                    return existing_order_id

        with self._rpc_lock:
            ret, df = self._trade_ctx.place_order(
                price=lim_px,
                aux_price=trig_px,
                qty=whole_shares,
                code=to_futu_us_code(symbol),
                trd_side=ft.TrdSide.SELL,
                order_type=ord_type,
                time_in_force=ft.TimeInForce.GTC,
                fill_outside_rth=False,
                trd_env=self._trd_env(),
                acc_id=acc_id,
                remark="zy-quant protective GTC stop",
            )
        if ret != ft.RET_OK or df is None or isinstance(df, str) or df.empty:
            logger.error("Failed placing server-side GTC stop for %s: %s", symbol, df)
            return None
        return str(df.iloc[0]["order_id"])

    def cancel_order(self, order_id: str) -> bool:
        if (
            not self.is_connected()
            or self._trade_ctx is None
            or not order_id
            or order_id.startswith(("SIM-", "PAPER-"))
        ):
            return True
        acc_id = self._select_acc_id(self.trd_env_name)
        ft = self._futu_mod
        with self._rpc_lock:
            ret, _ = self._trade_ctx.modify_order(
                modify_order_op=ft.ModifyOrderOp.CANCEL,
                order_id=order_id,
                qty=0,
                price=0,
                trd_env=self._trd_env(),
                acc_id=acc_id,
            )
        return ret == ft.RET_OK
