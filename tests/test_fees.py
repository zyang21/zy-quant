from zy_quant.fees import FutuUSFeeModel
from zy_quant.models import OrderSide


def test_standard_small_lot_minimums() -> None:
    fm = FutuUSFeeModel(default_slippage_bps=0.0)
    buy_fee = fm.estimate_leg(shares=10, price=150.0, side=OrderSide.BUY)
    sell_fee = fm.estimate_leg(shares=10, price=150.0, side=OrderSide.SELL)

    # 10 shares: min $0.99 comm + min $1.00 platform + $0.03 settlement + tiny CAT
    assert round(buy_fee.commission_usd, 2) == 0.99
    assert round(buy_fee.platform_fee_usd, 2) == 1.00
    assert round(buy_fee.settlement_fee_usd, 2) == 0.03
    assert buy_fee.regulatory_sec_fee_usd == 0.0
    assert sell_fee.regulatory_sec_fee_usd >= 0.01

    rt = fm.estimate_round_trip(shares=10, entry_price=150.0, slippage_bps=0.0)
    assert 4.04 <= rt["hard_fee_usd"] <= 4.10


def test_penny_stock_high_share_count_penalty() -> None:
    fm = FutuUSFeeModel(default_slippage_bps=0.0)
    # $5,000 in a $5 stock = 1,000 shares -> per-share fees explode (~$25.8+ round trip!)
    penny_rt = fm.estimate_round_trip(shares=1000, entry_price=5.0, slippage_bps=0.0)
    # $5,000 in a $250 leader = 20 shares -> ~$4.12 round trip
    leader_rt = fm.estimate_round_trip(shares=20, entry_price=250.0, slippage_bps=0.0)
    assert penny_rt["hard_fee_usd"] > 25.0
    assert leader_rt["hard_fee_usd"] < 4.30


def test_fractional_share_special_rule() -> None:
    fm = FutuUSFeeModel(default_slippage_bps=0.0)
    # 0.90 shares @ $900 = $810 notional -> 0 comm, $0.99 capped platform fee, 0 settlement
    rt = fm.estimate_round_trip(shares=0.90, entry_price=900.0, slippage_bps=0.0)
    assert rt["hard_fee_usd"] == 1.98
