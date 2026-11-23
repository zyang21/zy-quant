from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from zy_quant.config import UniverseSecurity, WatchlistScoringConfig
from zy_quant.models import (
    CapitalFlowSnapshot,
    InsiderActivity,
    QuoteSnapshot,
    RegimeLight,
    WatchlistCandidate,
)


class ConservativeWatchlistScorer:
    """
    Step 2: Conservative Institutional & ETF-Anchored Watchlist Scorer
    Driven entirely by `config/watchlist_scoring.yaml`:
    - Hard Veto Filters:
        1. Price >= $50 (avoids Futu per-share settlement fee explosion)
        2. Avg Daily Dollar Volume >= $500M (tight spreads)
        3. Institutional Ownership >= 55%
        4. Earnings Blackout (+-2 days)
        5. Live Bid-Ask Spread <= 8 bps
    - Weighted Composite Score (0 - 100):
        * ETF Core Weight (30%)
        * Institutional Ownership (20%)
        * Low-Drawdown Stability + Red-Day Relative Strength vs QQQ (15%)
        * Futu Real-Time Big-Order Capital Flow (20%)
        * SEC EDGAR Form 4 Recent Catalyst (15%)
    """

    def __init__(self, cfg: WatchlistScoringConfig) -> None:
        self.cfg = cfg

    def score_security(
        self,
        sec: UniverseSecurity,
        quote: QuoteSnapshot,
        stock_intraday_return: float,
        qqq_intraday_return: float,
        regime_light: RegimeLight,
        beta: float = 1.0,
        institutional_pct: Optional[float] = None,
        capital_flow: Optional[CapitalFlowSnapshot] = None,
        insider: Optional[InsiderActivity] = None,
        next_earnings_date: Optional[date] = None,
        today: Optional[date] = None,
    ) -> WatchlistCandidate:
        hf = self.cfg.hard_filters
        w = self.cfg.weights
        now = datetime.now(timezone.utc)
        ref_day = today or now.date()

        inst_pct = institutional_pct if institutional_pct is not None else sec.baseline_inst_pct
        reasons: List[str] = []

        if quote.last_price < hf.min_price_usd:
            reasons.append(f"price ${quote.last_price:.2f} < ${hf.min_price_usd:.0f}")
        if quote.avg_daily_dollar_vol < hf.min_avg_daily_dollar_vol:
            reasons.append(
                f"ADV ${quote.avg_daily_dollar_vol/1e6:.0f}M < ${hf.min_avg_daily_dollar_vol/1e6:.0f}M"
            )
        if inst_pct < hf.min_institutional_pct:
            reasons.append(f"inst {inst_pct*100:.0f}% < {hf.min_institutional_pct*100:.0f}%")
        if quote.spread_bps > hf.max_spread_bps:
            reasons.append(f"spread {quote.spread_bps:.1f}bps > {hf.max_spread_bps:.1f}bps")
        if next_earnings_date is not None:
            days_to_earn = abs((next_earnings_date - ref_day).days)
            if days_to_earn <= hf.earnings_blackout_days:
                reasons.append(f"earnings blackout ({days_to_earn}d)")

        passed = len(reasons) == 0
        filter_reason = "PASS" if passed else "; ".join(reasons)

        # 1. ETF Core Score (0 - 100 raw -> weighted)
        etf_raw = max(0.0, min(100.0, sec.etf_core_score * 100.0))

        # 2. Institutional Ownership Score (55% -> 60 pts, 85%+ -> 100 pts)
        inst_norm = min(1.0, max(0.0, (inst_pct - 0.40) / 0.45))
        inst_raw = 50.0 + 50.0 * inst_norm

        # 3. Low-Drawdown Stability + Red-Day Relative Strength (RS vs QQQ)
        # Lower beta is rewarded conservatively; outperforming QQQ (especially on RED/YELLOW days) gets boosted.
        rs_diff = stock_intraday_return - qqq_intraday_return  # e.g. +0.015 means +1.5% stronger than QQQ
        beta_stability = max(0.0, min(100.0, 100.0 - max(0.0, beta - 0.85) * 45.0))
        rs_multiplier = 900.0 if regime_light != RegimeLight.GREEN else 500.0
        rs_bonus = max(
            -25.0,
            min(
                self.cfg.scoring_thresholds.red_day_rs_bonus_cap * 2.0,
                rs_diff * rs_multiplier,
            ),
        )
        stability_raw = max(0.0, min(100.0, beta_stability * 0.75 + 20.0 + rs_bonus))

        # 4. Real-Time Big Order Capital Flow (Super-Large + Large net inflow ratio in [-1, +1] -> [0, 100])
        big_net_usd = capital_flow.big_order_net_inflow if capital_flow else 0.0
        if capital_flow is not None:
            flow_raw = max(0.0, min(100.0, 50.0 + 50.0 * capital_flow.big_order_inflow_ratio))
        else:
            flow_raw = 50.0  # Neutral baseline when market closed / offline

        # 5. SEC Form 4 Insider Catalyst (conservative bonus on recent cluster filings)
        if insider and insider.open_market_buy_count_14d > 0:
            insider_raw = min(100.0, 55.0 + 18.0 * insider.open_market_buy_count_14d)
        else:
            insider_raw = 45.0  # Neutral conservative baseline (most mega-caps have quiet windows)

        etf_comp = round(etf_raw * w.etf_core_weight, 2)
        inst_comp = round(inst_raw * w.institutional_ownership, 2)
        stab_comp = round(stability_raw * w.low_drawdown_stability, 2)
        flow_comp = round(flow_raw * w.realtime_big_order_flow, 2)
        ins_comp = round(insider_raw * w.insider_form4_catalyst, 2)
        total = round(etf_comp + inst_comp + stab_comp + flow_comp + ins_comp, 2)

        return WatchlistCandidate(
            symbol=sec.symbol,
            sector=sec.sector,
            price=round(quote.last_price, 2),
            total_score=total,
            passed_hard_filters=passed,
            filter_reason=filter_reason,
            etf_component_score=etf_comp,
            institutional_component_score=inst_comp,
            stability_rs_component_score=stab_comp,
            big_flow_component_score=flow_comp,
            insider_component_score=ins_comp,
            relative_strength_vs_qqq=round(rs_diff * 100.0, 3),
            big_order_net_usd=round(big_net_usd, 2),
            updated_at=now,
        )

    def rank_universe(
        self,
        quotes: Dict[str, QuoteSnapshot],
        intraday_returns: Dict[str, float],
        qqq_intraday_return: float,
        regime_light: RegimeLight,
        capital_flows: Optional[Dict[str, CapitalFlowSnapshot]] = None,
        insiders: Optional[Dict[str, InsiderActivity]] = None,
        earnings_dates: Optional[Dict[str, date]] = None,
        betas: Optional[Dict[str, float]] = None,
    ) -> List[WatchlistCandidate]:
        flows = capital_flows or {}
        ins_map = insiders or {}
        earn_map = earnings_dates or {}
        beta_map = betas or {}

        scored: List[WatchlistCandidate] = []
        for sec in self.cfg.universe:
            if sec.sector == "Broad_ETF":
                continue
            q = quotes.get(sec.symbol)
            if q is None:
                continue
            cand = self.score_security(
                sec=sec,
                quote=q,
                stock_intraday_return=intraday_returns.get(sec.symbol, 0.0),
                qqq_intraday_return=qqq_intraday_return,
                regime_light=regime_light,
                beta=beta_map.get(sec.symbol, 1.05),
                capital_flow=flows.get(sec.symbol),
                insider=ins_map.get(sec.symbol),
                next_earnings_date=earn_map.get(sec.symbol),
            )
            scored.append(cand)

        scored.sort(key=lambda c: (c.passed_hard_filters, c.total_score), reverse=True)
        return scored
