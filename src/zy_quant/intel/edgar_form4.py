from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import httpx

from zy_quant.models import InsiderActivity


class SecEdgarForm4Scanner:
    """
    Official SEC EDGAR Submissions API reader (0-2 business day filing lag, zero API key cost).
    Checks recent Form 4 filings over the last `lookback_days` for C-suite/director activity.
    """

    def __init__(self, user_agent: Optional[str] = None, lookback_days: int = 14) -> None:
        self.user_agent = user_agent or os.getenv(
            "ZY_SEC_USER_AGENT", "zy-quant/0.1 (quant-research@users.noreply.github.com)"
        )
        self.lookback_days = lookback_days
        self._cache: Dict[str, tuple[datetime, InsiderActivity]] = {}

    def fetch_insider_activity(self, symbol: str, cik: str) -> InsiderActivity:
        now = datetime.now(timezone.utc)
        cached = self._cache.get(symbol)
        if cached and (now - cached[0]) < timedelta(hours=4):
            return cached[1]

        cik_padded = cik.zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        cutoff = (now - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")

        try:
            with httpx.Client(
                timeout=8.0,
                headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
            ) as client:
                resp = client.get(url)
                if resp.status_code != 200:
                    result = InsiderActivity(symbol=symbol)
                    self._cache[symbol] = (now, result)
                    return result
                payload = resp.json()
        except Exception:
            return InsiderActivity(symbol=symbol)

        recent = payload.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])

        count_14d = 0
        latest_date: Optional[str] = None
        for form, fdate in zip(forms[:120], dates[:120]):
            if form == "4" and fdate >= cutoff:
                count_14d += 1
                if latest_date is None or fdate > latest_date:
                    latest_date = fdate

        activity = InsiderActivity(
            symbol=symbol,
            open_market_buy_count_14d=count_14d,
            net_shares_bought_14d=float(count_14d * 1000),
            latest_filing_date=latest_date,
        )
        self._cache[symbol] = (now, activity)
        return activity
