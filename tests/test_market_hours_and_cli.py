from datetime import datetime, timezone

from zy_quant.intel.market_hours import get_us_market_session


def test_us_market_hours_open_and_after_hours_and_holiday() -> None:
    # Monday Nov 23, 2026 15:00 UTC = 10:00 AM ET -> OPEN
    s_open = get_us_market_session(datetime(2026, 11, 23, 15, 0, tzinfo=timezone.utc))
    assert s_open.is_open is True
    assert s_open.phase == "REGULAR_OPEN"

    # Tuesday Nov 24, 2026 02:00 UTC = Monday 21:00 ET (10:00 AM Beijing) -> AFTER_HOURS CLOSED
    s_closed = get_us_market_session(datetime(2026, 11, 24, 2, 0, tzinfo=timezone.utc))
    assert s_closed.is_open is False
    assert s_closed.phase == "AFTER_HOURS"
    assert "自动 Scan 已暂停" in s_closed.message

    # Thursday Nov 26, 2026 15:00 UTC = Thanksgiving Holiday -> CLOSED
    s_holiday = get_us_market_session(datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc))
    assert s_holiday.is_open is False
    assert s_holiday.phase == "CLOSED_HOLIDAY_OR_WEEKEND"
