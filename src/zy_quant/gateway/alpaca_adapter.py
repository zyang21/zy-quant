from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

import httpx

from zy_quant.config import AlpacaDataConfig
from zy_quant.gateway.base import MarketDataAdapter
from zy_quant.intel.market_hours import is_trading_day, session_close_time
from zy_quant.models import Bar, CapitalFlowSnapshot, QuoteSnapshot
from zy_quant.storage.bar_cache import HistoricalBarCache

logger = logging.getLogger(__name__)
ET_TZ = ZoneInfo("America/New_York")


def _parse_iso_utc(ts_str: str) -> datetime:
    clean = ts_str.strip()
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    dt = datetime.fromisoformat(clean)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_us_regular_trading_bar(ts_utc: datetime) -> bool:
    """Filters 15m bars strictly to US Regular Trading Hours (09:30 <= ET < close, respecting 13:00 early closes)."""
    et = ts_utc.astimezone(ET_TZ)
    if et.weekday() >= 5:
        return False
    close_t = session_close_time(et.date())
    hm = (et.hour, et.minute)
    return (9, 30) <= hm < (close_t.hour, close_t.minute)


def latest_expected_rth_bar_utc(ref_utc: datetime) -> datetime:
    """
    Returns the timestamp of the latest completed regular US trading session close <= `ref_utc`.
    Prevents redundant network re-fetches on evenings, weekends, and US holidays (e.g. Thanksgiving)
    when SQLite BarCache already holds up to the latest market close!
    """
    et = ref_utc.astimezone(ET_TZ)
    d = et.date()
    if is_trading_day(d):
        open_dt = et.replace(hour=9, minute=30, second=0, microsecond=0)
        close_t = session_close_time(d)
        close_dt = et.replace(hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0)
        if et >= close_dt:
            return (close_dt - timedelta(minutes=15)).astimezone(timezone.utc)
        if et >= open_dt + timedelta(minutes=15):
            return (et - timedelta(minutes=20)).astimezone(timezone.utc)
        d = d - timedelta(days=1)

    for _ in range(10):
        if is_trading_day(d):
            close_t = session_close_time(d)
            prev_close = datetime(
                d.year, d.month, d.day, close_t.hour, close_t.minute, 0, tzinfo=ET_TZ
            )
            return (prev_close - timedelta(minutes=15)).astimezone(timezone.utc)
        d = d - timedelta(days=1)
    return ref_utc - timedelta(days=1)


class AlpacaMarketDataAdapter(MarketDataAdapter):
    """
    High-throughput US Equities Market Data Adapter powered by Alpaca Market Data API v2
    + persistent SQLite WAL HistoricalBarCache:
      - Scans 100+ symbols in < 1.0s via single-request batch `/v2/stocks/snapshots` (zero Futu quota)
      - Fetches split-adjusted multi-year (2016–present) `15m` (strictly RTH) & `1d` bars with
        automatic `next_page_token` pagination and local SQLite caching
      - Automatically uses full consolidated `sip` volume on historical windows (>= 16m old) and
        gracefully falls back to free real-time `iex` feed on live intraday windows
      - Falls back transparently to secondary adapter (Futu / Yahoo) when API keys are unconfigured
    """

    def __init__(
        self,
        alpaca_cfg: Optional[AlpacaDataConfig] = None,
        bar_cache: Optional[HistoricalBarCache] = None,
        fallback: Optional[MarketDataAdapter] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        cfg = alpaca_cfg or AlpacaDataConfig()
        self.enabled = cfg.enabled
        self.api_key = (cfg.api_key or "").strip()
        self.secret_key = (cfg.secret_key or "").strip()
        self.feed = (cfg.feed or "iex").strip().lower()
        self.base_url = (cfg.base_url or "https://data.alpaca.markets").rstrip("/")
        self.bar_cache = bar_cache
        self.fallback = fallback
        self._client = http_client
        self._owned_client: Optional[httpx.Client] = None
        self.last_error: str = ""
        self.used_alpaca: bool = False
        self.last_prev_closes: Dict[str, float] = {}
        self.sip_iex_vol_ratios: Dict[str, float] = {}
        self.last_bars_feed: Dict[tuple[str, str], str] = {}
        self._last_http_feed_used: str = self.feed

    def _get_http_client(self) -> httpx.Client:
        if self._client is not None:
            return self._client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = httpx.Client(
                timeout=20.0,
                limits=httpx.Limits(max_keepalive_connections=8, max_connections=12),
            )
        return self._owned_client

    def close(self) -> None:
        if self._owned_client is not None and not self._owned_client.is_closed:
            self._owned_client.close()
            self._owned_client = None

    def has_credentials(self) -> bool:
        return bool(self.enabled and self.api_key and self.secret_key)

    def _headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Accept": "application/json",
        }

    def _request_json(
        self, path: str, params: Dict[str, Any], timeout: float = 15.0
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        client = self._get_http_client()
        try:
            resp = client.get(url, params=params, headers=self._headers(), timeout=timeout)

            if resp.status_code == 200:
                self.last_error = ""
                self.used_alpaca = True
                self._last_http_feed_used = str(params.get("feed") or self.feed)
                return resp.json()

            # Free tier restricts real-time (<15m) SIP; transparently retry with 'iex'
            if resp.status_code in (403, 422) and params.get("feed") == "sip":
                retry_params = dict(params, feed="iex")
                resp2 = client.get(
                    url, params=retry_params, headers=self._headers(), timeout=timeout
                )
                if resp2.status_code == 200:
                    self.last_error = ""
                    self.used_alpaca = True
                    self._last_http_feed_used = "iex"
                    return resp2.json()

            self.last_error = f"Alpaca HTTP {resp.status_code}: {resp.text[:180]}"
            logger.warning("[ALPACA API] %s on %s", self.last_error, path)
            return None
        except Exception as exc:
            self.last_error = f"Alpaca network error: {exc}"
            logger.warning("[ALPACA API] Request failed on %s: %s", path, exc)
            return None

    def test_connection(self, sample_symbols: Sequence[str] = ("SPY", "QQQ", "NVDA")) -> Dict[str, Any]:
        """Verifies Alpaca credentials, endpoint reachability, batch snapshot latency, and bar retrieval."""
        t0 = time.perf_counter()
        if not self.has_credentials():
            return {
                "ok": False,
                "configured": False,
                "endpoint": self.base_url,
                "feed": self.feed,
                "latency_ms": 0.0,
                "error": (
                    "Alpaca API keys not configured yet. Add `alpaca.api_key` and "
                    "`alpaca.secret_key` in gitignored `config/settings.local.json`."
                ),
                "snapshots_count": 0,
                "sample_bars_count": 0,
            }

        snaps = self.get_snapshots_batch(sample_symbols)
        bars_qqq = self.get_bars("QQQ", timeframe="15m", days=5) if snaps else []
        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        ok = len(snaps) > 0 and len(bars_qqq) > 0
        return {
            "ok": ok,
            "configured": True,
            "endpoint": self.base_url,
            "feed": self.feed,
            "latency_ms": latency_ms,
            "error": "" if ok else (self.last_error or "Empty response from Alpaca"),
            "snapshots_count": len(snaps),
            "sample_quotes": {
                s: {"price": q.last_price, "spread_bps": round(q.spread_bps, 2)}
                for s, q in snaps.items()
            },
            "sample_bars_count": len(bars_qqq),
            "cache_stats": self.bar_cache.stats() if self.bar_cache else {},
        }

    # --- Batch Multi-Symbol APIs (100-stock scanning & multi-year backtesting) ---
    def get_snapshots_batch(
        self, symbols: Sequence[str], chunk_size: int = 100
    ) -> Dict[str, QuoteSnapshot]:
        """
        Fetches real-time quotes + daily dollar volume for 100+ symbols in 1-2 HTTP requests.
        """
        clean_syms = [s.strip().upper() for s in symbols if s and not s.startswith("^")]
        if not clean_syms or not self.has_credentials():
            return {}

        out: Dict[str, QuoteSnapshot] = {}
        # Pre-load cached 20d SIP dollar volume if available in local bar_cache
        adv_map: Dict[str, float] = {}
        if self.bar_cache is not None:
            start_35d = datetime.now(timezone.utc) - timedelta(days=38)
            cached_1d = self.bar_cache.get_bars_batch(clean_syms, "1d", start_dt=start_35d)
            for sym, blist in cached_1d.items():
                if blist:
                    recent = blist[-20:]
                    adv_map[sym] = sum(b.close * b.volume for b in recent) / len(recent)

        for idx in range(0, len(clean_syms), chunk_size):
            batch = clean_syms[idx : idx + chunk_size]
            payload = self._request_json(
                "/v2/stocks/snapshots",
                params={"symbols": ",".join(batch), "feed": self.feed},
            )
            if not payload:
                continue
            snap_dict = payload.get("snapshots", payload)
            if not isinstance(snap_dict, dict):
                continue

            for sym, item in snap_dict.items():
                if not isinstance(item, dict):
                    continue
                trade = item.get("latestTrade") or {}
                quote = item.get("latestQuote") or {}
                d_bar = item.get("dailyBar") or {}
                p_bar = item.get("prevDailyBar") or {}

                last_px = float(
                    trade.get("p") or d_bar.get("c") or p_bar.get("c") or 0.0
                )
                if last_px <= 0:
                    continue
                prev_c = float(p_bar.get("c") or 0.0)
                if prev_c > 0:
                    self.last_prev_closes[sym.upper()] = prev_c

                raw_bid = float(quote.get("bp") or 0.0)
                raw_ask = float(quote.get("ap") or 0.0)
                # IEX stub quotes occasionally show wide off-market stubs when book is thin;
                # clamp unrealistic (>50 bps) IEX-only stub spreads on mega-caps if needed
                if raw_bid > 0 and raw_ask >= raw_bid and ((raw_ask - raw_bid) / last_px) <= 0.005:
                    bid_px, ask_px = raw_bid, raw_ask
                else:
                    half = max(0.01, round(last_px * 0.00015, 2))
                    bid_px, ask_px = round(last_px - half, 2), round(last_px + half, 2)

                # Compute ADV: prefer 20d SIP bar_cache ADV; otherwise scale IEX volume (~2.5% share -> x35)
                if sym in adv_map and adv_map[sym] > 0:
                    adv = adv_map[sym]
                else:
                    ref_vol = max(float(p_bar.get("v") or 0.0), float(d_bar.get("v") or 0.0))
                    vol_mult = 35.0 if self.feed == "iex" else 1.0
                    adv = last_px * ref_vol * vol_mult
                    if adv <= 0:
                        adv = 650_000_000.0

                ts_raw = trade.get("t") or d_bar.get("t") or p_bar.get("t")
                ts = _parse_iso_utc(ts_raw) if isinstance(ts_raw, str) and ts_raw else datetime.now(timezone.utc)

                out[sym.upper()] = QuoteSnapshot(
                    symbol=sym.upper(),
                    last_price=round(last_px, 2),
                    bid=round(bid_px, 2),
                    ask=round(ask_px, 2),
                    avg_daily_dollar_vol=round(adv, 2),
                    timestamp=ts,
                )

        return out

    def get_bars_batch(
        self,
        symbols: Sequence[str],
        timeframe: str = "15m",
        days: int = 30,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        chunk_size: int = 50,
    ) -> Dict[str, List[Bar]]:
        """
        Fetches split-adjusted historical `15m` (RTH) or `1d` bars across arbitrarily long
        multi-year windows (e.g., 2020-2026), reading/writing through local HistoricalBarCache.
        """
        clean_syms = [s.strip().upper() for s in symbols if s and not s.startswith("^")]
        if not clean_syms:
            return {}

        now_utc = datetime.now(timezone.utc)
        eff_end = end_dt or now_utc
        if start_dt is not None:
            eff_start = start_dt
        else:
            cal_days = max(8, int(days * 1.55) + 5) if timeframe == "15m" else max(45, int(days * 1.55) + 12)
            eff_start = eff_end - timedelta(days=cal_days)

        # 1. Partition into Full Fetch vs Head-Delta vs Tail-Delta against SQLite BarCache
        #    Never re-download an existing 12+ month cached body just because warmup shifted start by 12 days!
        full_fetch_syms: List[str] = []
        head_gap_syms: List[str] = []
        head_gap_end: Optional[datetime] = None
        tail_gap_syms: List[str] = []
        tail_gap_start: Optional[datetime] = None

        expected_latest = latest_expected_rth_bar_utc(eff_end)
        min_required_max_ts = (
            expected_latest - timedelta(minutes=30)
            if timeframe == "15m"
            else expected_latest - timedelta(hours=26)
        )

        if self.bar_cache is not None:
            for sym in clean_syms:
                bounds = self.bar_cache.coverage_bounds(sym, timeframe)
                if bounds is None:
                    full_fetch_syms.append(sym)
                    continue
                min_cached, max_cached, _ = bounds
                if min_cached > eff_start + timedelta(days=4):
                    head_gap_syms.append(sym)
                    head_gap_end = min_cached if head_gap_end is None else max(head_gap_end, min_cached)
                if max_cached < min_required_max_ts:
                    tail_gap_syms.append(sym)
                    tail_gap_start = max_cached if tail_gap_start is None else min(tail_gap_start, max_cached)
        else:
            full_fetch_syms = list(clean_syms)

        if not self.has_credentials():
            if self.bar_cache is not None:
                cached_only = self.bar_cache.get_bars_batch(clean_syms, timeframe, eff_start, eff_end)
                return {sym: blist for sym, blist in cached_only.items() if blist}
            return {}

        alpaca_tf = "15Min" if timeframe == "15m" else "1Day"

        def _download_window(
            target_syms: List[str], win_start: datetime, win_end: datetime
        ) -> Dict[str, List[Bar]]:
            if not target_syms or win_end <= win_start:
                return {}
            is_hist = win_end <= (now_utc - timedelta(minutes=17))
            pref_feed = "sip" if (timeframe == "1d" or is_hist) else self.feed
            w_end = (
                min(win_end, now_utc - timedelta(minutes=16))
                if pref_feed == "sip" and end_dt is not None
                else win_end
            )
            s_iso = win_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            e_iso = w_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            eff_chunk = 15 if (timeframe == "15m" and (w_end - win_start).days > 60) else chunk_size
            batches = [
                target_syms[idx : idx + eff_chunk]
                for idx in range(0, len(target_syms), eff_chunk)
            ]

            def _fetch_batch(batch: List[str]) -> Dict[str, List[Bar]]:
                fetched_map: Dict[str, List[Bar]] = {s: [] for s in batch}
                page_token: Optional[str] = None
                for _ in range(500):
                    params: Dict[str, Any] = {
                        "symbols": ",".join(batch),
                        "timeframe": alpaca_tf,
                        "start": s_iso,
                        "end": e_iso,
                        "adjustment": "all",
                        "feed": pref_feed,
                        "limit": 10000,
                    }
                    if page_token:
                        params["page_token"] = page_token
                    payload = self._request_json("/v2/stocks/bars", params=params, timeout=30.0)
                    if not payload:
                        break
                    bars_dict = payload.get("bars") or {}
                    for sym_k, raw_list in bars_dict.items():
                        sym_u = sym_k.upper()
                        if sym_u not in fetched_map or not isinstance(raw_list, list):
                            continue
                        for item in raw_list:
                            ts = _parse_iso_utc(str(item["t"]))
                            if timeframe == "15m" and not is_us_regular_trading_bar(ts):
                                continue
                            fetched_map[sym_u].append(
                                Bar(
                                    symbol=sym_u,
                                    timestamp=ts,
                                    open=float(item["o"]),
                                    high=float(item["h"]),
                                    low=float(item["l"]),
                                    close=float(item["c"]),
                                    volume=float(item["v"]),
                                )
                            )
                    page_token = payload.get("next_page_token")
                    if not page_token:
                        break
                return fetched_map

            if self._client is not None or len(batches) <= 1:
                batch_outputs = [_fetch_batch(b) for b in batches]
            else:
                with ThreadPoolExecutor(max_workers=min(4, len(batches))) as pool:
                    batch_outputs = list(pool.map(_fetch_batch, batches))

            merged_out: Dict[str, List[Bar]] = {}
            new_bars_flat: List[Bar] = []
            actual_feed = self._last_http_feed_used if pref_feed == "sip" else pref_feed
            for batch, fmap in zip(batches, batch_outputs):
                for s in batch:
                    s_bars = sorted(fmap[s], key=lambda b: b.timestamp)
                    if s_bars:
                        merged_out[s] = s_bars
                        new_bars_flat.extend(s_bars)
                        self.last_bars_feed[(s, timeframe)] = actual_feed
            if new_bars_flat and self.bar_cache is not None:
                self.bar_cache.upsert_bars(timeframe, new_bars_flat)
            return merged_out

        # 2. Execute full fetch only for uncached symbols, and surgical delta fetches for head/tail gaps!
        direct_fetched = _download_window(full_fetch_syms, eff_start, eff_end)
        if head_gap_syms and head_gap_end is not None:
            _download_window(head_gap_syms, eff_start, head_gap_end)
        if tail_gap_syms and tail_gap_start is not None:
            # Overlap by 2 days on 1d / 30m on 15m to detect retroactive stock-split / dividend price cliffs!
            overlap_pad = timedelta(days=3) if timeframe == "1d" else timedelta(minutes=30)
            pre_closes: Dict[str, tuple[datetime, float]] = {}
            if self.bar_cache is not None:
                for s in tail_gap_syms:
                    b_cov = self.bar_cache.coverage_bounds(s, timeframe)
                    if b_cov is not None:
                        c_old = self.bar_cache.get_close_at(s, timeframe, b_cov[1])
                        if c_old is not None and c_old > 0:
                            pre_closes[s] = (b_cov[1], c_old)

            tail_fetched = _download_window(tail_gap_syms, tail_gap_start - overlap_pad, eff_end)
            if self.bar_cache is not None and pre_closes:
                resync_syms: List[str] = []
                for s, (anchor_ts, old_c) in pre_closes.items():
                    s_bars = tail_fetched.get(s, [])
                    for nb in s_bars:
                        if int(nb.timestamp.timestamp()) == int(anchor_ts.timestamp()):
                            if abs(nb.close - old_c) / old_c > 0.002:
                                logger.warning(
                                    "[SPLIT/DIVIDEND GUARD] Detected %.2f%% retroactive adjustment on %s (%s); re-syncing cached series",
                                    ((nb.close - old_c) / old_c) * 100.0,
                                    s,
                                    timeframe,
                                )
                                self.bar_cache.delete_symbol(s, timeframe)
                                resync_syms.append(s)
                            break
                if resync_syms:
                    _download_window(resync_syms, eff_start, eff_end)

        if self.bar_cache is None:
            return direct_fetched

        cached_merged = self.bar_cache.get_bars_batch(clean_syms, timeframe, eff_start, eff_end)
        return {sym: blist for sym, blist in cached_merged.items() if blist}

    # --- Single-Symbol MarketDataAdapter Contract ---
    def get_bars(
        self,
        symbol: str,
        timeframe: str = "15m",
        days: int = 30,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> List[Bar]:
        if symbol.startswith("^"):
            return self.fallback.get_bars(symbol, timeframe=timeframe, days=days) if self.fallback else []

        batch = self.get_bars_batch(
            [symbol], timeframe=timeframe, days=days, start_dt=start_dt, end_dt=end_dt
        )
        bars = batch.get(symbol.upper(), [])
        if bars:
            return bars
        if self.fallback is not None:
            fb_bars = self.fallback.get_bars(symbol, timeframe=timeframe, days=days)
            if fb_bars:
                self.last_bars_feed[(symbol.upper(), timeframe)] = "sip_consolidated"
            return fb_bars
        return []

    def get_effective_15m_volume_scale(self, symbol: str) -> float:
        """
        Returns the SIP/IEX volume calibration factor ONLY when the latest 15m bars for `symbol`
        actually came from live `iex` feed; returns `1.0` for cached `sip` or Futu fallback bars!
        """
        sym_u = symbol.upper()
        src_feed = str(self.last_bars_feed.get((sym_u, "15m"), "sip")).lower()
        if src_feed == "iex":
            return float(self.sip_iex_vol_ratios.get(sym_u, 35.0))
        return 1.0

    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        snaps = self.get_snapshots_batch([symbol])
        if symbol.upper() in snaps:
            return snaps[symbol.upper()]
        if self.fallback is not None:
            return self.fallback.get_quote(symbol)
        return None

    def get_capital_flow(self, symbol: str) -> Optional[CapitalFlowSnapshot]:
        bars = self.get_bars(symbol, timeframe="15m", days=3)
        if bars:
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
        if self.fallback is not None:
            return self.fallback.get_capital_flow(symbol)
        return None

    def get_vix_level(self) -> float:
        qqq_d = self.get_bars("QQQ", timeframe="1d", days=22)
        if len(qqq_d) >= 11:
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
        if self.fallback is not None:
            return self.fallback.get_vix_level()
        return 18.0

    def calibrate_sip_iex_volume_ratios(self, symbols: Sequence[str]) -> Dict[str, float]:
        """
        Computes empirical 20-day consolidated SIP / IEX volume multipliers during PRE_OPEN_SYNC
        by comparing historical `sip` 1d volume against `iex` 1d volume per symbol.
        """
        clean = [s.strip().upper() for s in symbols if s and not s.startswith("^")]
        if not clean:
            return {}
        if self.feed != "iex":
            for sym in clean:
                self.sip_iex_vol_ratios[sym] = 1.0
            return dict(self.sip_iex_vol_ratios)

        start_35d = datetime.now(timezone.utc) - timedelta(days=35)
        end_hist = datetime.now(timezone.utc) - timedelta(days=1)
        sip_1d = (
            self.bar_cache.get_bars_batch(clean, "1d", start_dt=start_35d)
            if self.bar_cache is not None
            else {}
        )
        iex_vols: Dict[str, Dict[str, float]] = {}
        if self.has_credentials():
            s_iso = start_35d.strftime("%Y-%m-%dT%H:%M:%SZ")
            e_iso = end_hist.strftime("%Y-%m-%dT%H:%M:%SZ")
            for idx in range(0, len(clean), 50):
                batch = clean[idx : idx + 50]
                payload = self._request_json(
                    "/v2/stocks/bars",
                    params={
                        "symbols": ",".join(batch),
                        "timeframe": "1Day",
                        "start": s_iso,
                        "end": e_iso,
                        "adjustment": "all",
                        "feed": "iex",
                        "limit": 5000,
                    },
                )
                if payload and isinstance(payload.get("bars"), dict):
                    for sym_k, rlist in payload["bars"].items():
                        dmap: Dict[str, float] = {}
                        for item in rlist or []:
                            v = float(item.get("v") or 0.0)
                            if v > 0:
                                dmap[str(item["t"])[:10]] = v
                        iex_vols[sym_k.upper()] = dmap

        for sym in clean:
            s_bars = sip_1d.get(sym, [])
            imap = iex_vols.get(sym, {})
            ratios: List[float] = []
            for b in s_bars[-20:]:
                d_key = b.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d")
                iv = imap.get(d_key, 0.0)
                if iv > 0 and b.volume > 0:
                    ratios.append(b.volume / iv)
            if ratios:
                ratios.sort()
                med = ratios[len(ratios) // 2]
                self.sip_iex_vol_ratios[sym] = round(max(10.0, min(65.0, med)), 2)
            elif sym not in self.sip_iex_vol_ratios:
                self.sip_iex_vol_ratios[sym] = 35.0
        return dict(self.sip_iex_vol_ratios)


def build_causal_partial_1d_bar(
    symbol: str,
    bars_15m_session: Sequence[Bar],
    decision_ts_utc: Optional[datetime] = None,
    volume_scale_mult: float = 1.0,
) -> Optional[Bar]:
    """
    Constructs a strictly causal `partial_1d_bar` from completed `[09:30 ET .. decision_ts]` 15m bars:
      - Zero intrabar look-ahead: 15m bars are stamped at bar OPEN (`09:30, ..., 15:30 ET`),
        so only bars that have FULLY CLOSED (`b.timestamp + 15m <= decision_ts_utc`) and precede
        the final 15-minute closing bar (`< close - 15m`) are admitted!
      - Normalizes partial-day volume by intraday U-curve completion fraction and `volume_scale_mult`.
    """
    if not bars_15m_session:
        return None
    if decision_ts_utc is not None:
        # Enforce full 15m bar completion AND cap at `close - 15m` so the 15:45-16:00 closing bar never leaks
        dec_et = decision_ts_utc.astimezone(ET_TZ)
        close_t = session_close_time(dec_et.date())
        max_eod_cutoff_utc = datetime(
            dec_et.year, dec_et.month, dec_et.day, close_t.hour, close_t.minute, 0, tzinfo=ET_TZ
        ).astimezone(timezone.utc) - timedelta(minutes=15)
        # Note: at `15:45 ET` (`close - 15m`), the `15:30-15:45 ET` bar has just finished (`15:30 + 15m <= 15:45`),
        # while the `15:45-16:00 ET` closing bar (`15:45 + 15m = 16:00 > 15:45`) is strictly excluded!
        eff_cutoff = min(decision_ts_utc, max_eod_cutoff_utc) if dec_et.date() == bars_15m_session[-1].timestamp.astimezone(ET_TZ).date() else decision_ts_utc
        sliced = [b for b in bars_15m_session if (b.timestamp + timedelta(minutes=15)) <= eff_cutoff]
    else:
        sliced = list(bars_15m_session)

    if not sliced:
        return None

    last_ts = sliced[-1].timestamp
    et_last = last_ts.astimezone(ET_TZ)
    session_d = et_last.date()
    if decision_ts_utc is not None and session_d != decision_ts_utc.astimezone(ET_TZ).date():
        return None
    session_bars = [b for b in sliced if b.timestamp.astimezone(ET_TZ).date() == session_d]
    if not session_bars:
        return None

    close_t = session_close_time(session_d)
    total_expected_bars = max(1, ((close_t.hour * 60 + close_t.minute) - (9 * 60 + 30)) // 15)
    elapsed_bars = len(session_bars)
    progress_frac = min(1.0, max(0.20, (elapsed_bars / float(total_expected_bars)) * 0.85))
    raw_vol = sum(max(0.0, b.volume) for b in session_bars)
    projected_full_day_vol = (raw_vol * max(0.01, volume_scale_mult)) / progress_frac

    day_anchor = datetime(session_d.year, session_d.month, session_d.day, 12, 0, tzinfo=timezone.utc)
    return Bar(
        symbol=symbol.upper(),
        timestamp=day_anchor,
        open=session_bars[0].open,
        high=max(b.high for b in session_bars),
        low=min(b.low for b in session_bars),
        close=session_bars[-1].close,
        volume=round(projected_full_day_vol, 2),
    )
