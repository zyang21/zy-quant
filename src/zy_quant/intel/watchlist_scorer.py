from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from zy_quant.config import SwingPipelineConfig, UniverseSecurity, WatchlistScoringConfig
from zy_quant.intel.market_hours import (
    ET_TZ,
    count_trading_days_between,
    daily_bar_session_date,
)
from zy_quant.models import (
    Bar,
    CapitalFlowSnapshot,
    EarningsEvent,
    InsiderActivity,
    QuoteSnapshot,
    RegimeLight,
    WatchlistCandidate,
)
from zy_quant.strategy.indicators import compute_sma


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
        ref_day = today or now.astimezone(ET_TZ).date()

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

    def select_active_top_n(
        self,
        scored_rows: Sequence[Dict[str, Any]],
        top_n: Optional[int] = None,
        min_score: Optional[float] = None,
        max_per_sector: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Selects up to `top_n_active` eligible candidates while enforcing the GICS Sector Cap
        (`max_per_sector_in_top_n`) so a single hot sector never crowds out cross-sector leaders.
        """
        th = self.cfg.scoring_thresholds
        limit_n = top_n if top_n is not None else th.top_n_active
        floor_score = min_score if min_score is not None else th.min_watch_score
        sec_cap = max_per_sector if max_per_sector is not None else th.max_per_sector_in_top_n

        selected: List[Dict[str, Any]] = []
        sector_counts: Dict[str, int] = {}
        for row in scored_rows:
            if not row.get("passed_hard_filters") or float(row.get("total_score", 0.0)) < floor_score:
                continue
            sec = str(row.get("sector") or "Unknown")
            if sector_counts.get(sec, 0) >= sec_cap:
                continue
            selected.append(row)
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
            if len(selected) >= limit_n:
                break
        return selected


class SwingWatchlistScorer(ConservativeWatchlistScorer):
    """
    Multi-Week Daily Swing Watchlist Scorer (`swing_1d`):
      - Scores 20d & 60d Relative Strength vs QQQ, Stage-2 daily trend alignment
        (`Close > SMA50 > SMA200`), volatility contraction (`ATR_5d / ATR_20d`),
        institutional/ETF core anchor, and EDGAR Form 4 catalyst
      - Enforces `>= 10` NYSE trading-day / `14` calendar-day pre-earnings entry buffer!
    """

    def __init__(
        self,
        cfg: WatchlistScoringConfig,
        swing_cfg: Optional[SwingPipelineConfig] = None,
    ) -> None:
        super().__init__(cfg)
        self.swing_cfg = swing_cfg or SwingPipelineConfig()

    def rank_swing_universe(
        self,
        quotes: Mapping[str, QuoteSnapshot],
        daily_bars_map: Mapping[str, Sequence[Bar]],
        qqq_daily_bars: Sequence[Bar],
        regime_light: RegimeLight,
        as_of_date: date,
        capital_flows: Optional[Mapping[str, CapitalFlowSnapshot]] = None,
        insiders: Optional[Mapping[str, InsiderActivity]] = None,
        earnings_events: Optional[Mapping[str, EarningsEvent]] = None,
        qqq_partial_bar: Optional[Bar] = None,
        partial_bars_map: Optional[Mapping[str, Optional[Bar]]] = None,
        include_completed_as_of: bool = False,
        universe_override: Optional[Sequence[UniverseSecurity]] = None,
    ) -> List[WatchlistCandidate]:
        flows = capital_flows or {}
        ins_map = insiders or {}
        earn_map = earnings_events or {}
        part_map = partial_bars_map or {}
        hf = self.cfg.hard_filters
        w = self.cfg.weights
        now = datetime.now(timezone.utc)

        if include_completed_as_of:
            q_series = [
                b for b in qqq_daily_bars if daily_bar_session_date(b.timestamp) <= as_of_date
            ]
            q_causal = q_series[:-1]
            q_curr_px = q_series[-1].close if q_series else 0.0
        else:
            q_causal = [
                b for b in qqq_daily_bars if daily_bar_session_date(b.timestamp) < as_of_date
            ]
            q_quote = quotes.get("QQQ")
            q_curr_px = (
                qqq_partial_bar.close
                if qqq_partial_bar is not None
                else (q_quote.last_price if q_quote is not None else (q_causal[-1].close if q_causal else 0.0))
            )

        q_ret_20 = 0.0
        q_ret_60 = 0.0
        if len(q_causal) >= 2 and q_curr_px > 0:
            lb20 = min(20, len(q_causal))
            lb60 = min(60, len(q_causal))
            q_ret_20 = (q_curr_px - q_causal[-lb20].close) / max(0.01, q_causal[-lb20].close)
            q_ret_60 = (q_curr_px - q_causal[-lb60].close) / max(0.01, q_causal[-lb60].close)

        scored: List[WatchlistCandidate] = []
        active_universe = universe_override if universe_override is not None else self.cfg.universe
        for sec in active_universe:
            if sec.sector == "Broad_ETF":
                continue
            q = quotes.get(sec.symbol)
            s_all = daily_bars_map.get(sec.symbol, [])
            if include_completed_as_of:
                s_upto = [
                    b for b in s_all if daily_bar_session_date(b.timestamp) <= as_of_date
                ]
                if not s_upto:
                    continue
                s_today = s_upto[-1]
                s_causal = s_upto[:-1]
                curr_px = s_today.close
                adv_bars = s_upto[-20:]
            else:
                s_causal = [
                    b for b in s_all if daily_bar_session_date(b.timestamp) < as_of_date
                ]
                p_bar = part_map.get(sec.symbol)
                if q is None and p_bar is None and not s_causal:
                    continue
                curr_px = (
                    p_bar.close
                    if p_bar is not None
                    else (q.last_price if q is not None else s_causal[-1].close)
                )
                adv_bars = s_causal[-20:]

            adv = q.avg_daily_dollar_vol if q is not None else (
                sum(b.close * b.volume for b in adv_bars) / max(1, len(adv_bars))
            )
            spread_bps = q.spread_bps if q is not None else 3.0

            reasons: List[str] = []
            if curr_px < hf.min_price_usd:
                reasons.append(f"price ${curr_px:.2f} < ${hf.min_price_usd:.0f}")
            if adv < hf.min_avg_daily_dollar_vol:
                reasons.append(f"ADV ${adv/1e6:.0f}M < ${hf.min_avg_daily_dollar_vol/1e6:.0f}M")
            if sec.baseline_inst_pct < hf.min_institutional_pct:
                reasons.append(f"inst {sec.baseline_inst_pct*100:.0f}% < {hf.min_institutional_pct*100:.0f}%")
            if spread_bps > hf.max_spread_bps:
                reasons.append(f"spread {spread_bps:.1f}bps > {hf.max_spread_bps:.1f}bps")

            ev = earn_map.get(sec.symbol)
            if ev is not None and ev.report_date >= as_of_date:
                cal_d = (ev.report_date - as_of_date).days
                trd_d = count_trading_days_between(as_of_date, ev.report_date)
                if (
                    cal_d < self.swing_cfg.pre_earnings_buffer_calendar_days
                    or trd_d < self.swing_cfg.pre_earnings_buffer_trading_days
                ):
                    reasons.append(f"earnings buffer ({trd_d}td/{cal_d}cd, {ev.timing})")

            # Stage-2 + 20d/60d RS + Volatility Contraction scoring (symmetric `curr_px` vs `N` completed days ago)
            rs_20 = 0.0
            rs_60 = 0.0
            stage2_bonus = 0.0
            vcp_bonus = 0.0
            if len(s_causal) >= 21:
                lb20 = min(20, len(s_causal))
                lb60 = min(60, len(s_causal))
                rs_20 = ((curr_px - s_causal[-lb20].close) / max(0.01, s_causal[-lb20].close)) - q_ret_20
                rs_60 = ((curr_px - s_causal[-lb60].close) / max(0.01, s_causal[-lb60].close)) - q_ret_60
                closes = [b.close for b in s_causal]
                sma50 = compute_sma(closes, min(50, len(closes)))
                sma200 = compute_sma(closes, min(200, len(closes)))
                if curr_px >= sma50 >= sma200 * 0.99:
                    stage2_bonus = 15.0
                elif curr_px < sma50:
                    stage2_bonus = -18.0
                atr5 = sum(b.high - b.low for b in s_causal[-5:]) / 5.0
                atr20 = sum(b.high - b.low for b in s_causal[-20:]) / 20.0
                if atr20 > 0 and (atr5 / atr20) <= 0.88:
                    vcp_bonus = 10.0

            etf_raw = max(0.0, min(100.0, sec.etf_core_score * 100.0))
            inst_norm = min(1.0, max(0.0, (sec.baseline_inst_pct - 0.40) / 0.45))
            inst_raw = 50.0 + 50.0 * inst_norm

            blended_rs = 0.60 * rs_20 + 0.40 * rs_60
            rs_scale = 320.0 if regime_light != RegimeLight.GREEN else 240.0
            stability_raw = max(0.0, min(100.0, 55.0 + stage2_bonus + vcp_bonus + blended_rs * rs_scale))

            flow = flows.get(sec.symbol)
            big_net_usd = flow.big_order_net_inflow if flow else 0.0
            flow_raw = max(0.0, min(100.0, 50.0 + 50.0 * flow.big_order_inflow_ratio)) if flow else 55.0

            ins = ins_map.get(sec.symbol)
            ins_raw = min(100.0, 55.0 + 18.0 * ins.open_market_buy_count_14d) if (ins and ins.open_market_buy_count_14d > 0) else 48.0

            etf_comp = round(etf_raw * w.etf_core_weight, 2)
            inst_comp = round(inst_raw * w.institutional_ownership, 2)
            stab_comp = round(stability_raw * w.low_drawdown_stability, 2)
            flow_comp = round(flow_raw * w.realtime_big_order_flow, 2)
            ins_comp = round(ins_raw * w.insider_form4_catalyst, 2)
            total = round(etf_comp + inst_comp + stab_comp + flow_comp + ins_comp, 2)

            passed = len(reasons) == 0
            scored.append(
                WatchlistCandidate(
                    symbol=sec.symbol,
                    sector=sec.sector,
                    price=round(curr_px, 2),
                    total_score=total,
                    passed_hard_filters=passed,
                    filter_reason="PASS" if passed else "; ".join(reasons),
                    etf_component_score=etf_comp,
                    institutional_component_score=inst_comp,
                    stability_rs_component_score=stab_comp,
                    big_flow_component_score=flow_comp,
                    insider_component_score=ins_comp,
                    relative_strength_vs_qqq=round(rs_20 * 100.0, 3),
                    big_order_net_usd=round(big_net_usd, 2),
                    updated_at=now,
                )
            )

        scored.sort(key=lambda c: (c.passed_hard_filters, c.total_score), reverse=True)
        return scored
