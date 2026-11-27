from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import httpx

from zy_quant.intel.market_hours import ET_TZ
from zy_quant.models import EarningsEvent

logger = logging.getLogger(__name__)


def normalize_nasdaq_earnings_timing(raw_time: str) -> str:
    """
    Maps Nasdaq calendar `time` strings into canonical timing buckets:
      - 'time-pre-market' -> 'BMO' (act on T-1 close)
      - 'time-after-hours' -> 'AMC' (act on T-0 close)
      - 'time-not-supplied' / unknown -> 'UNKNOWN' (fail-safe treated as T-1 close!)
    """
    t = (raw_time or "").strip().lower()
    if "pre-market" in t or t == "bmo":
        return "BMO"
    if "after-hours" in t or t == "amc":
        return "AMC"
    return "UNKNOWN"


class EarningsCalendarFeed:
    """
    Zero-key US Equities Earnings Blackout Calendar (`data/cache/earnings_calendar.json`):
      - Queries Nasdaq public earnings calendar JSON across `[today - 2d .. today + 16d]`
        (covering the full +14 calendar-day / 10 trading-day swing entry buffer!)
      - Preserves `BMO` vs `AMC` timing so `PRE_EARNINGS_CUSHION` acts on `T-1` close for BMO/UNKNOWN
        and `T-0` close for AMC
      - Caches locally for 12 hours so intraday/EOD cycles read in O(1) memory/disk
    """

    NASDAQ_URL = "https://api.nasdaq.com/api/calendar/earnings"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }

    def __init__(
        self,
        cache_path: Optional[Path] = None,
        ttl_hours: int = 12,
        lookahead_days: int = 16,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.cache_path = cache_path
        self.ttl = timedelta(hours=ttl_hours)
        self.lookahead_days = lookahead_days
        self._http_client = http_client
        self._mem_cache: Optional[tuple[datetime, Dict[str, EarningsEvent]]] = None

    def get_upcoming_events(
        self,
        symbols: Optional[Iterable[str]] = None,
        ref_day: Optional[date] = None,
        allow_network: bool = True,
    ) -> Dict[str, EarningsEvent]:
        now = datetime.now(timezone.utc)
        target_set = {s.upper() for s in symbols} if symbols is not None else None

        if self._mem_cache and (now - self._mem_cache[0]) < self.ttl:
            return self._filter(self._mem_cache[1], target_set)

        disk_map = self._read_disk_cache(now)
        if disk_map is not None:
            self._mem_cache = (now, disk_map)
            return self._filter(disk_map, target_set)

        if not allow_network:
            return {}

        today = ref_day or now.astimezone(ET_TZ).date()
        fetched: Dict[str, EarningsEvent] = {}
        any_http_ok = False
        try:
            owned = self._http_client is None
            client = self._http_client or httpx.Client(timeout=6.0, headers=self.HEADERS)
            try:
                # Look back 10 calendar days (~7 trading days) so PEAD T+1..T+5 is reachable!
                for offset in range(-10, self.lookahead_days + 1):
                    q_day = today + timedelta(days=offset)
                    if q_day.weekday() >= 5:
                        continue
                    resp = client.get(self.NASDAQ_URL, params={"date": q_day.isoformat()})
                    if resp.status_code != 200:
                        continue
                    any_http_ok = True
                    rows = ((resp.json().get("data") or {}).get("rows")) or []
                    for r in rows:
                        sym = str(r.get("symbol") or "").strip().upper()
                        if not sym:
                            continue
                        timing = normalize_nasdaq_earnings_timing(str(r.get("time") or ""))
                        ev = EarningsEvent(symbol=sym, report_date=q_day, timing=timing)
                        if sym not in fetched:
                            fetched[sym] = ev
                        else:
                            prev_d = fetched[sym].report_date
                            if (q_day >= today and prev_d < today) or (
                                abs((q_day - today).days) < abs((prev_d - today).days)
                            ):
                                fetched[sym] = ev
            finally:
                if owned:
                    client.close()
        except Exception as exc:
            logger.debug("Earnings calendar network refresh skipped: %s", exc)

        if any_http_ok:
            self._mem_cache = (now, fetched)
            self._write_disk_cache(now, fetched)
            return self._filter(fetched, target_set)

        # Never poison disk/memory cache with `{}` on network failure; fall back to stale disk cache if present!
        stale_disk = self._read_disk_cache(now, ignore_ttl=True)
        if stale_disk is not None:
            return self._filter(stale_disk, target_set)
        return {}

    def get_upcoming_earnings(
        self,
        symbols: Optional[Iterable[str]] = None,
        ref_day: Optional[date] = None,
        allow_network: bool = True,
    ) -> Dict[str, date]:
        """Backward-compatible helper returning `symbol -> report_date`."""
        events = self.get_upcoming_events(symbols=symbols, ref_day=ref_day, allow_network=allow_network)
        return {sym: ev.report_date for sym, ev in events.items()}

    @staticmethod
    def _filter(
        full_map: Dict[str, EarningsEvent], target_set: Optional[set[str]]
    ) -> Dict[str, EarningsEvent]:
        if target_set is None:
            return dict(full_map)
        return {k: v for k, v in full_map.items() if k in target_set}

    def _read_disk_cache(
        self, now: datetime, ignore_ttl: bool = False
    ) -> Optional[Dict[str, EarningsEvent]]:
        if self.cache_path is None or not self.cache_path.exists():
            return None
        try:
            raw = json.loads(self.cache_path.read_text(encoding="utf-8"))
            updated_at = datetime.fromisoformat(str(raw["updated_at"]))
            if not ignore_ttl and (now - updated_at) >= self.ttl:
                return None
            out: Dict[str, EarningsEvent] = {}
            events_raw = raw.get("events")
            if isinstance(events_raw, dict):
                for k, info in events_raw.items():
                    sym = str(k).upper()
                    out[sym] = EarningsEvent(
                        symbol=sym,
                        report_date=date.fromisoformat(str(info["date"])),
                        timing=str(info.get("timing") or "UNKNOWN").upper(),
                    )
                return out
            # Backward compatibility with legacy {"dates": {"NVDA": "2026-11-20"}} cache files
            for k, v in (raw.get("dates") or {}).items():
                sym = str(k).upper()
                out[sym] = EarningsEvent(symbol=sym, report_date=date.fromisoformat(str(v)), timing="UNKNOWN")
            return out
        except Exception:
            return None

    def _write_disk_cache(self, now: datetime, mapping: Dict[str, EarningsEvent]) -> None:
        if self.cache_path is None:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": now.isoformat(),
                "dates": {k: v.report_date.isoformat() for k, v in mapping.items()},
                "events": {
                    k: {"date": v.report_date.isoformat(), "timing": v.timing}
                    for k, v in mapping.items()
                },
            }
            self.cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass
