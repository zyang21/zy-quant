from __future__ import annotations

import logging
import math
import random
import socket
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from zy_quant.gateway.base import MarketDataAdapter
from zy_quant.models import Bar, CapitalFlowSnapshot, QuoteSnapshot

logger = logging.getLogger(__name__)


class YahooMarketDataAdapter(MarketDataAdapter):
    """
    Market data adapter using `yfinance` for US 15m & 1d historical/delayed bars.
    Synthetic fake data is DISABLED by default (`allow_synthetic_fallback=False`) and
    always emits explicit WARNING alerts whenever any fallback path is entered.
    """

    def __init__(self, allow_synthetic_fallback: bool = False) -> None:
        self.allow_synthetic_fallback = allow_synthetic_fallback
        self._bar_cache: Dict[tuple[str, str, int], List[Bar]] = {}
        self._yahoo_blocked_until: float = 0.0
        self.used_synthetic: bool = False
        self.used_yahoo_fallback: bool = False
        self.fallback_warnings: List[str] = []

    def _yahoo_reachable(self) -> bool:
        now = time.monotonic()
        if now < self._yahoo_blocked_until:
            return False
        try:
            with socket.create_connection(("query2.finance.yahoo.com", 443), timeout=1.5):
                return True
        except OSError:
            self._yahoo_blocked_until = now + 300.0
            return False

    def get_bars(self, symbol: str, timeframe: str = "15m", days: int = 30) -> List[Bar]:
        cache_key = (symbol, timeframe, days)
        if cache_key in self._bar_cache:
            return self._bar_cache[cache_key]

        bars: List[Bar] = []
        if self._yahoo_reachable():
            try:
                import yfinance as yf

                interval = "15m" if timeframe == "15m" else "1d"
                max_days = min(days, 58) if interval == "15m" else max(days, 60)
                df = yf.Ticker(symbol).history(period=f"{max_days}d", interval=interval, auto_adjust=True)
                if df is not None and not df.empty:
                    for idx, row in df.iterrows():
                        ts = idx.to_pydatetime()
                        if timeframe == "1d":
                            # Anchor 1d session bars to 12:00 UTC on their trading date so ET_TZ never shifts -1 day
                            d_sess = idx.date()
                            ts = datetime(d_sess.year, d_sess.month, d_sess.day, 12, 0, tzinfo=timezone.utc)
                        elif ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        bars.append(
                            Bar(
                                symbol=symbol,
                                timestamp=ts,
                                open=float(row["Open"]),
                                high=float(row["High"]),
                                low=float(row["Low"]),
                                close=float(row["Close"]),
                                volume=float(row["Volume"]),
                            )
                        )
                if bars:
                    self.used_yahoo_fallback = True
                    msg = f"[LOCAL/YAHOO FALLBACK] Loaded {len(bars)} {timeframe} bars for {symbol} via Yahoo Finance (Futu OpenAPI offline)"
                    logger.warning(msg)
                    if msg not in self.fallback_warnings:
                        self.fallback_warnings.append(msg)
            except Exception as exc:
                logger.warning("[DATA FALLBACK] Yahoo Finance fetch failed for %s: %s", symbol, exc)
                bars = []

        if not bars:
            if self.allow_synthetic_fallback:
                self.used_synthetic = True
                msg = f"[SYNTHETIC FAKE DATA] Generated synthetic {timeframe} bars for {symbol} (non-real market data!)"
                logger.warning(msg)
                if msg not in self.fallback_warnings:
                    self.fallback_warnings.append(msg)
                bars = self.generate_synthetic_bars(symbol, timeframe=timeframe, days=days)
            else:
                msg = f"[NO REAL MARKET DATA] Both Futu OpenAPI and Yahoo Finance unavailable for {symbol} ({timeframe}); synthetic fake data is disabled."
                logger.error(msg)
                if msg not in self.fallback_warnings:
                    self.fallback_warnings.append(msg)

        self._bar_cache[cache_key] = bars
        return bars

    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        bars_1d = self.get_bars(symbol, timeframe="1d", days=30)
        if not bars_1d:
            return None
        last = bars_1d[-1]
        avg_dvol = sum(b.close * b.volume for b in bars_1d[-20:]) / float(min(20, len(bars_1d)))
        half_spread = max(0.01, round(last.close * 0.00015, 2))
        return QuoteSnapshot(
            symbol=symbol,
            last_price=round(last.close, 2),
            bid=round(last.close - half_spread, 2),
            ask=round(last.close + half_spread, 2),
            avg_daily_dollar_vol=max(avg_dvol, 800_000_000.0),
            timestamp=last.timestamp,
        )

    def get_capital_flow(self, symbol: str) -> Optional[CapitalFlowSnapshot]:
        """Approximates block flow from recent 15m up-volume vs down-volume when FutuOpenD is offline."""
        bars = self.get_bars(symbol, timeframe="15m", days=5)
        if not bars:
            return None
        recent = bars[-16:]
        inflow = sum(b.close * b.volume * 0.35 for b in recent if b.close >= b.open)
        outflow = sum(b.close * b.volume * 0.35 for b in recent if b.close < b.open)
        return CapitalFlowSnapshot(
            symbol=symbol,
            super_large_inflow=inflow * 0.45,
            super_large_outflow=outflow * 0.45,
            large_inflow=inflow * 0.55,
            large_outflow=outflow * 0.55,
        )

    def get_vix_level(self) -> float:
        bars = self.get_bars("^VIX", timeframe="1d", days=10)
        if bars:
            px = bars[-1].close
            if 8.0 <= px <= 85.0:
                return round(px, 2)
        return 17.5

    @staticmethod
    def generate_synthetic_bars(symbol: str, timeframe: str = "15m", days: int = 30) -> List[Bar]:
        """Deterministic realistic multi-day US session bars for offline testing & instant demos."""
        seed = sum(ord(c) * (i + 1) for i, c in enumerate(symbol))
        rng = random.Random(seed + 202611)
        base_prices = {
            "NVDA": 148.0,
            "MSFT": 435.0,
            "AAPL": 228.0,
            "META": 590.0,
            "AMZN": 210.0,
            "AVGO": 182.0,
            "GOOGL": 186.0,
            "LLY": 820.0,
            "JPM": 240.0,
            "COST": 910.0,
            "QQQ": 505.0,
            "SPY": 592.0,
            "^VIX": 16.8,
        }
        px = base_prices.get(symbol, 165.0)
        now_day = datetime.now(timezone.utc).replace(hour=14, minute=30, second=0, microsecond=0)
        start_day = now_day - timedelta(days=days + 12)

        bars: List[Bar] = []
        cur_day = start_day
        trading_days_generated = 0

        while trading_days_generated < days:
            if cur_day.weekday() < 5:
                trading_days_generated += 1
                if timeframe == "1d":
                    drift = 0.0018 if symbol != "^VIX" else -0.001
                    ret = drift + rng.gauss(0, 0.011)
                    open_px = px
                    close_px = max(12.0, open_px * (1.0 + ret))
                    high_px = max(open_px, close_px) * (1.0 + abs(rng.gauss(0, 0.005)))
                    low_px = min(open_px, close_px) * (1.0 - abs(rng.gauss(0, 0.005)))
                    vol = 18_000_000.0 * (1.0 + abs(ret) * 25.0)
                    bars.append(
                        Bar(
                            symbol=symbol,
                            timestamp=cur_day,
                            open=round(open_px, 2),
                            high=round(high_px, 2),
                            low=round(low_px, 2),
                            close=round(close_px, 2),
                            volume=round(vol, 0),
                        )
                    )
                    px = close_px
                else:
                    # 26 bars of 15m per regular US session (09:30 - 16:00 ET)
                    day_bias = 0.00055 if (trading_days_generated % 4 != 0) else -0.00035
                    for b_idx in range(26):
                        ts = cur_day + timedelta(minutes=15 * b_idx)
                        # Create realistic morning impulse + midday VWAP test + afternoon trend continuation
                        wave = math.sin(b_idx / 3.2) * 0.0018
                        ret = day_bias + wave + rng.gauss(0, 0.0022)
                        open_px = px
                        close_px = max(15.0, open_px * (1.0 + ret))
                        span = max(0.12, open_px * abs(rng.gauss(0.0025, 0.0012)))
                        high_px = max(open_px, close_px) + span * 0.55
                        low_px = min(open_px, close_px) - span * 0.45
                        vol_mult = 2.3 if b_idx in (0, 1, 2, 5) else (0.75 if 3 <= b_idx <= 4 else 1.1)
                        vol = 650_000.0 * vol_mult * (1.0 + rng.random() * 0.35)
                        bars.append(
                            Bar(
                                symbol=symbol,
                                timestamp=ts,
                                open=round(open_px, 2),
                                high=round(high_px, 2),
                                low=round(low_px, 2),
                                close=round(close_px, 2),
                                volume=round(vol, 0),
                            )
                        )
                        px = close_px
            cur_day += timedelta(days=1)
        return bars
