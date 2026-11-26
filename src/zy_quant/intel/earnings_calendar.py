from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import httpx

from zy_quant.intel.market_hours import ET_TZ

logger = logging.getLogger(__name__)


class EarningsCalendarFeed:
    """
    Zero-key US Equities Earnings Blackout Calendar (`data/cache/earnings_calendar.json`):
      - Queries Nasdaq public earnings calendar JSON across `[today - 2d .. today + 5d]`
      - Caches locally for 12 hours so intraday cycles read in O(1) memory/disk
      - Feeds `next_earnings_date` into `ConservativeWatchlistScorer` to hard-veto entries
        within `+-2` trading days of quarterly earnings announcements
    """

    NASDAQ_URL = "https://api.nasdaq.com/api/calendar/earnings"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }

    def __init__(self, cache_path: Optional[Path] = None, ttl_hours: int = 12) -> None:
        self.cache_path = cache_path
        self.ttl = timedelta(hours=ttl_hours)
        self._mem_cache: Optional[tuple[datetime, Dict[str, date]]] = None

    def get_upcoming_earnings(
        self,
        symbols: Optional[Iterable[str]] = None,
        ref_day: Optional[date] = None,
        allow_network: bool = True,
    ) -> Dict[str, date]:
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
        fetched: Dict[str, date] = {}
        try:
            with httpx.Client(timeout=6.0, headers=self.HEADERS) as client:
                for offset in range(-2, 6):
                    q_day = today + timedelta(days=offset)
                    if q_day.weekday() >= 5:
                        continue
                    resp = client.get(self.NASDAQ_URL, params={"date": q_day.isoformat()})
                    if resp.status_code != 200:
                        continue
                    rows = ((resp.json().get("data") or {}).get("rows")) or []
                    for r in rows:
                        sym = str(r.get("symbol") or "").strip().upper()
                        if sym:
                            # Keep closest earnings date to `today`
                            if sym not in fetched or abs((q_day - today).days) < abs((fetched[sym] - today).days):
                                fetched[sym] = q_day
        except Exception as exc:
            logger.debug("Earnings calendar network refresh skipped: %s", exc)

        self._mem_cache = (now, fetched)
        self._write_disk_cache(now, fetched)
        return self._filter(fetched, target_set)

    @staticmethod
    def _filter(full_map: Dict[str, date], target_set: Optional[set[str]]) -> Dict[str, date]:
        if target_set is None:
            return dict(full_map)
        return {k: v for k, v in full_map.items() if k in target_set}

    def _read_disk_cache(self, now: datetime) -> Optional[Dict[str, date]]:
        if self.cache_path is None or not self.cache_path.exists():
            return None
        try:
            raw = json.loads(self.cache_path.read_text(encoding="utf-8"))
            updated_at = datetime.fromisoformat(str(raw["updated_at"]))
            if (now - updated_at) >= self.ttl:
                return None
            return {
                str(k).upper(): date.fromisoformat(str(v))
                for k, v in (raw.get("dates") or {}).items()
            }
        except Exception:
            return None

    def _write_disk_cache(self, now: datetime, mapping: Dict[str, date]) -> None:
        if self.cache_path is None:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": now.isoformat(),
                "dates": {k: v.isoformat() for k, v in mapping.items()},
            }
            self.cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass
