from __future__ import annotations

import math
from dataclasses import dataclass

from zy_quant.models import OrderSide


@dataclass(frozen=True)
class FeeBreakdown:
    commission_usd: float
    platform_fee_usd: float
    settlement_fee_usd: float
    regulatory_sec_fee_usd: float
    cat_fee_usd: float
    slippage_usd: float

    @property
    def hard_fee_usd(self) -> float:
        return round(
            self.commission_usd
            + self.platform_fee_usd
            + self.settlement_fee_usd
            + self.regulatory_sec_fee_usd
            + self.cat_fee_usd,
            4,
        )

    @property
    def total_friction_usd(self) -> float:
        return round(self.hard_fee_usd + self.slippage_usd, 4)


class FutuUSFeeModel:
    """
    Exact 2026 Futu HK US Equities & ETFs Fee Schedule:
    - Standard (>= 1 share, Fixed Package recommended below 274k shares/month):
        * Commission: $0.0049/share, min $0.99/order, max 0.5% of trade value (floor $0.99)
        * Platform Fee (Fixed): $0.0050/share, min $1.00/order, max 0.5% of trade value (floor $1.00)
        * Settlement Fee: $0.0030/share
        * Consolidated Audit Trail (CAT): $0.000003/share
        * SEC Regulatory Fee (SELL only): 0.0000206 * notional, min $0.01
    - Fractional (< 1 share, Futu HK rule 2.1):
        * Commission: $0.00
        * Platform Fee: 0.99% * notional, max $0.99/order (rounded up to nearest $0.01)
        * Third-party Settlement / SEC / CAT: $0.00
    """

    def __init__(self, default_slippage_bps: float = 1.5) -> None:
        self.default_slippage_bps = default_slippage_bps

    def estimate_leg(
        self,
        shares: float,
        price: float,
        side: OrderSide,
        slippage_bps: float | None = None,
    ) -> FeeBreakdown:
        if shares <= 0 or price <= 0:
            return FeeBreakdown(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

        notional = shares * price
        bps = self.default_slippage_bps if slippage_bps is None else slippage_bps
        slippage = notional * (bps / 10000.0)

        # Fractional share rule (< 1.0 share)
        if shares < 1.0:
            raw_plat = math.ceil((0.0099 * notional) * 100.0) / 100.0
            platform_fee = min(0.99, max(0.01, raw_plat))
            return FeeBreakdown(
                commission_usd=0.0,
                platform_fee_usd=platform_fee,
                settlement_fee_usd=0.0,
                regulatory_sec_fee_usd=0.0,
                cat_fee_usd=0.0,
                slippage_usd=slippage,
            )

        # Standard whole/mixed share order (>= 1.0 share)
        half_pct_cap = 0.005 * notional
        commission = max(0.99, min(0.0049 * shares, max(0.99, half_pct_cap)))
        platform_fee = max(1.00, min(0.0050 * shares, max(1.00, half_pct_cap)))
        settlement = 0.0030 * shares
        cat_fee = 0.000003 * shares
        sec_fee = max(0.01, 0.0000206 * notional) if side == OrderSide.SELL else 0.0

        return FeeBreakdown(
            commission_usd=commission,
            platform_fee_usd=platform_fee,
            settlement_fee_usd=settlement,
            regulatory_sec_fee_usd=sec_fee,
            cat_fee_usd=cat_fee,
            slippage_usd=slippage,
        )

    def estimate_round_trip(
        self,
        shares: float,
        entry_price: float,
        exit_price: float | None = None,
        slippage_bps: float | None = None,
        scale_out: bool = False,
    ) -> dict[str, float]:
        sell_px = exit_price if exit_price is not None else entry_price
        buy_leg = self.estimate_leg(shares, entry_price, OrderSide.BUY, slippage_bps)

        if scale_out and shares >= 2.0:
            half_1 = math.floor(shares / 2.0)
            half_2 = shares - half_1
            sell_1 = self.estimate_leg(half_1, sell_px, OrderSide.SELL, slippage_bps)
            sell_2 = self.estimate_leg(half_2, sell_px, OrderSide.SELL, slippage_bps)
            hard_total = buy_leg.hard_fee_usd + sell_1.hard_fee_usd + sell_2.hard_fee_usd
            friction_total = (
                buy_leg.total_friction_usd
                + sell_1.total_friction_usd
                + sell_2.total_friction_usd
            )
        else:
            sell_leg = self.estimate_leg(shares, sell_px, OrderSide.SELL, slippage_bps)
            hard_total = buy_leg.hard_fee_usd + sell_leg.hard_fee_usd
            friction_total = buy_leg.total_friction_usd + sell_leg.total_friction_usd

        notional = shares * entry_price
        return {
            "notional_usd": round(notional, 2),
            "hard_fee_usd": round(hard_total, 4),
            "total_friction_usd": round(friction_total, 4),
            "breakeven_pct": round((friction_total / notional) * 100.0, 4) if notional > 0 else 0.0,
        }
