from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from zy_quant.config import AppSettings
from zy_quant.intel.market_hours import get_us_market_session
from zy_quant.models import OrderSide, OrderStatus
from zy_quant.services.analytics import HistoryQueryService, PortfolioAnalyticsService
from zy_quant.services.log_stream import RingBufferLogHandler
from zy_quant.storage.state_store import StateStore
from zy_quant.web.app import app


def test_us_market_hours_open_and_after_hours_and_holiday() -> None:
    s_open = get_us_market_session(datetime(2026, 11, 23, 15, 0, tzinfo=timezone.utc))
    assert s_open.is_open is True
    assert s_open.phase == "REGULAR_OPEN"

    s_closed = get_us_market_session(datetime(2026, 11, 24, 2, 0, tzinfo=timezone.utc))
    assert s_closed.is_open is False
    assert s_closed.phase == "AFTER_HOURS"
    assert "自动 Scan 已暂停" in s_closed.message

    s_holiday = get_us_market_session(datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc))
    assert s_holiday.is_open is False
    assert s_holiday.phase == "CLOSED_HOLIDAY_OR_WEEKEND"


def test_history_query_service_date_window_and_profit_factor(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "hist.db")
    store.record_order_intent("BUY-1", "NVDA", OrderSide.BUY, 10, 140.0, "ORB")
    store.update_order_status("BUY-1", OrderStatus.FILLED, "SIM-1", 140.0, 2.15)

    store.record_closed_trade(
        symbol="NVDA",
        setup="ORB_30M",
        shares=10.0,
        entry_price=140.0,
        exit_price=146.0,
        fee_usd=4.30,
        risk_per_share=2.5,
        exit_reason="CHANDELIER_TRAIL",
        entry_time="2026-11-10T15:00:00+00:00",
        exit_time="2026-11-10T18:00:00+00:00",
    )
    store.record_closed_trade(
        symbol="MSFT",
        setup="VWAP_PULLBACK",
        shares=5.0,
        entry_price=420.0,
        exit_price=417.0,
        fee_usd=4.20,
        risk_per_share=3.0,
        exit_reason="INITIAL_ATR_STOP",
        entry_time="2026-11-20T15:00:00+00:00",
        exit_time="2026-11-20T16:30:00+00:00",
    )

    svc = HistoryQueryService(store)
    all_res = svc.query_history(preset="ALL")
    assert all_res["summary"]["trades_count"] == 2
    assert all_res["summary"]["profit_factor"] > 2.0

    # Date window filter isolating Nov 15 -> Nov 22
    window_res = svc.query_history(start_date="2026-11-15", end_date="2026-11-22")
    assert window_res["summary"]["trades_count"] == 1
    assert window_res["closed_trades"][0]["symbol"] == "MSFT"


def test_sanitized_dashboard_and_log_endpoints_hide_infra_secrets() -> None:
    client = TestClient(app)
    r_dash = client.get("/api/dashboard")
    assert r_dash.status_code == 200
    payload = r_dash.json()

    # Verify zero host/port/api_token leaked in public dashboard JSON
    conn = payload["connection"]
    assert "host" not in conn
    assert "port" not in conn
    assert "api_token" not in conn

    r_hist = client.get("/api/history?preset=30D")
    assert r_hist.status_code == 200
    assert "summary" in r_hist.json()

    r_logs = client.get("/api/logs?since_id=0")
    assert r_logs.status_code == 200
    assert "logs" in r_logs.json()

    # Static atomic modules served properly
    assert client.get("/static/css/tokens.css").status_code == 200
    assert client.get("/static/js/app.js").status_code == 200


def test_separate_simulation_and_real_db_paths() -> None:
    settings = AppSettings.load()
    sim_db = settings.resolve_db_path("SIMULATE")
    real_db = settings.resolve_db_path("REAL")
    assert sim_db.name == "zy_quant_sim.db"
    assert real_db.name == "zy_quant_real.db"
    assert sim_db != real_db
