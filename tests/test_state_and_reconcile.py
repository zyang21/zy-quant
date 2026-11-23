from pathlib import Path

from zy_quant.config import AppSettings
from zy_quant.models import OrderSide, PositionPlan, SetupType
from zy_quant.risk.mandate_gate import MandateRiskGate
from zy_quant.storage.state_store import StateStore


def test_idempotent_orders_reconciliation_and_kill_switch(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "test_state.db")
    settings = AppSettings.load()
    gate = MandateRiskGate(store, settings.strategy_cfg.daemon)
    tier_10k = settings.tiers_cfg.get_active("tier_10k")

    # 1. Idempotent order journal prevents duplicate submission on retry
    assert store.record_order_intent("BUY-NVDA-202611231500", "NVDA", OrderSide.BUY, 10, 150.0, "init") is True
    assert store.record_order_intent("BUY-NVDA-202611231500", "NVDA", OrderSide.BUY, 10, 150.0, "dup") is False

    # 2. Startup broker reconciliation adopts live Futu holdings & cleans phantom positions
    rec = store.reconcile_broker_positions({"MSFT": {"shares": 12.0, "avg_price": 420.0}})
    assert rec["adopted"] == ["MSFT"]
    positions = store.get_positions()
    assert "MSFT" in positions
    assert positions["MSFT"].current_stop < 420.0

    rec2 = store.reconcile_broker_positions({})
    assert rec2["closed_remotely"] == ["MSFT"]
    assert store.get_positions() == {}

    # 3. Kill-Switch blocks MandateRiskGate immediately
    store.set_halted(True, "Unit test emergency stop")
    plan = PositionPlan(
        symbol="AAPL",
        sector="Technology",
        shares=15.0,
        entry_price=220.0,
        limit_price=220.1,
        initial_stop=216.0,
        risk_per_share=4.0,
        total_risk_usd=60.0,
        notional_usd=3300.0,
        estimated_round_trip_fee_usd=4.5,
        expected_reward_r=2.5,
        expectancy_r=0.6,
        setup=SetupType.VWAP_PULLBACK,
    )
    decision = gate.check_entry(plan, tier_10k, 10000.0, {})
    assert decision.allowed is False
    assert "Kill-Switch" in decision.reason
