from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

ET_TZ = ZoneInfo("America/New_York")

# Official NYSE / NASDAQ full-day closures (2026 - 2027)
NYSE_HOLIDAYS = {
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Jr. Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (Observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving Day
    date(2026, 12, 25), # Christmas Day
    # 2027
    date(2027, 1, 1),
    date(2027, 1, 18),
    date(2027, 2, 15),
    date(2027, 3, 26),
    date(2027, 5, 31),
    date(2027, 6, 18),
    date(2027, 7, 5),
    date(2027, 9, 6),
    date(2027, 11, 25),
    date(2027, 12, 24),
}

# Official NYSE 13:00 ET early-close sessions (Black Friday & Dec 24)
NYSE_EARLY_CLOSE_DATES = {
    date(2026, 11, 27), # Day after Thanksgiving (closes 13:00 ET)
    date(2026, 12, 24), # Christmas Eve (closes 13:00 ET)
    date(2027, 11, 26),
}


@dataclass(frozen=True)
class MarketSessionStatus:
    is_open: bool
    phase: str  # REGULAR_OPEN | PRE_MARKET | AFTER_HOURS | CLOSED_HOLIDAY_OR_WEEKEND
    et_now_str: str
    next_open_et_str: str
    next_open_local_str: str
    minutes_until_open: int
    message: str


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def session_close_time(d: date) -> time:
    return time(13, 0) if d in NYSE_EARLY_CLOSE_DATES else time(16, 0)


def next_regular_open(now_et: datetime) -> datetime:
    cur_date = now_et.date()
    open_today = datetime.combine(cur_date, time(9, 30), tzinfo=ET_TZ)
    if is_trading_day(cur_date) and now_et < open_today:
        return open_today

    probe = cur_date + timedelta(days=1)
    while not is_trading_day(probe):
        probe += timedelta(days=1)
    return datetime.combine(probe, time(9, 30), tzinfo=ET_TZ)


def get_us_market_session(now_utc: Optional[datetime] = None) -> MarketSessionStatus:
    ref_utc = now_utc or datetime.now(timezone.utc)
    if ref_utc.tzinfo is None:
        ref_utc = ref_utc.replace(tzinfo=timezone.utc)

    now_et = ref_utc.astimezone(ET_TZ)
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    cur_date = now_et.date()
    et_str = now_et.strftime("%Y-%m-%d %H:%M ET")

    if is_trading_day(cur_date):
        rth_open = datetime.combine(cur_date, time(9, 30), tzinfo=ET_TZ)
        rth_close = datetime.combine(cur_date, session_close_time(cur_date), tzinfo=ET_TZ)
        pre_open = datetime.combine(cur_date, time(4, 0), tzinfo=ET_TZ)

        if rth_open <= now_et < rth_close:
            mins_left = max(1, int((rth_close - now_et).total_seconds() // 60))
            close_hm = rth_close.strftime("%H:%M ET")
            return MarketSessionStatus(
                is_open=True,
                phase="REGULAR_OPEN",
                et_now_str=et_str,
                next_open_et_str=et_str,
                next_open_local_str="OPEN NOW",
                minutes_until_open=0,
                message=f"US Market OPEN ({et_str}) · {mins_left}m until {close_hm} close",
            )

        nxt_et = next_regular_open(now_et)
        nxt_local = nxt_et.astimezone(local_tz)
        mins_until = max(1, int((nxt_et - now_et).total_seconds() // 60))
        phase = "PRE_MARKET" if pre_open <= now_et < rth_open else "AFTER_HOURS"
        phase_cn = "盘前待开盘" if phase == "PRE_MARKET" else "已收盘休市"
        return MarketSessionStatus(
            is_open=False,
            phase=phase,
            et_now_str=et_str,
            next_open_et_str=nxt_et.strftime("%Y-%m-%d %H:%M ET"),
            next_open_local_str=nxt_local.strftime("%Y-%m-%d %H:%M"),
            minutes_until_open=mins_until,
            message=(
                f"美股{phase_cn} ({et_str}) — 自动 Scan 已暂停，"
                f"将于 {nxt_local.strftime('%m-%d %H:%M')} (美东 09:30) 自动恢复"
            ),
        )

    nxt_et = next_regular_open(now_et)
    nxt_local = nxt_et.astimezone(local_tz)
    mins_until = max(1, int((nxt_et - now_et).total_seconds() // 60))
    reason_cn = "节假日休市" if cur_date in NYSE_HOLIDAYS else "周末休市"
    return MarketSessionStatus(
        is_open=False,
        phase="CLOSED_HOLIDAY_OR_WEEKEND",
        et_now_str=et_str,
        next_open_et_str=nxt_et.strftime("%Y-%m-%d %H:%M ET"),
        next_open_local_str=nxt_local.strftime("%Y-%m-%d %H:%M"),
        minutes_until_open=mins_until,
        message=(
            f"美股{reason_cn} ({et_str}) — 自动 Scan 已暂停，"
            f"将于 {nxt_local.strftime('%m-%d %H:%M')} (美东 09:30) 自动恢复"
        ),
    )
