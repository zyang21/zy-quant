from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

import yaml

from zy_quant.config import UniverseSecurity


@dataclass
class UniverseRebalanceSummary:
    kept: List[str]
    added: List[str]
    removed: List[str]
    sector_distribution: Dict[str, int]
    total_count: int


class PointInTimeUniverseFilter:
    """
    Eliminates survivorship bias in multi-year backtests (`2020-2026`):
    Loads a Point-in-Time (PIT) membership CSV with columns:
      `symbol,start_date,end_date`  (ISO YYYY-MM-DD; empty `end_date` means still active)
    When provided, `is_member_on(symbol, as_of_date)` guarantees the backtester only ranks
    securities that were genuinely in the index constituent pool on `as_of_date`.
    """

    def __init__(self, intervals: Optional[Dict[str, List[tuple[date, Optional[date]]]]] = None) -> None:
        self._intervals: Dict[str, List[tuple[date, Optional[date]]]] = intervals or {}

    @classmethod
    def from_csv(cls, csv_path: Path) -> "PointInTimeUniverseFilter":
        intervals: Dict[str, List[tuple[date, Optional[date]]]] = {}
        if not csv_path.exists():
            return cls(intervals)
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = str(row.get("symbol", "")).strip().upper()
                start_raw = str(row.get("start_date", "")).strip()
                end_raw = str(row.get("end_date", "")).strip()
                if not sym or not start_raw:
                    continue
                start_d = date.fromisoformat(start_raw[:10])
                end_d = date.fromisoformat(end_raw[:10]) if end_raw else None
                intervals.setdefault(sym, []).append((start_d, end_d))
        return cls(intervals)

    @property
    def enabled(self) -> bool:
        return len(self._intervals) > 0

    def is_member_on(self, symbol: str, as_of: date) -> bool:
        if not self._intervals:
            return True
        sym = symbol.upper()
        spans = self._intervals.get(sym)
        if not spans:
            return False
        return any(start_d <= as_of and (end_d is None or as_of <= end_d) for start_d, end_d in spans)


def rebalance_universe_with_buffer(
    current_universe: Sequence[UniverseSecurity],
    candidates_with_metrics: Sequence[Dict[str, Any]],
    target_equities_count: int = 100,
    enter_rank_cutoff: int = 85,
    exit_rank_cutoff: int = 120,
    min_price_usd: float = 50.0,
    min_adv_usd: float = 500_000_000.0,
    max_per_sector: int = 24,
) -> tuple[List[UniverseSecurity], UniverseRebalanceSummary]:
    """
    Quarterly objective universe rebalance implementing MSCI/S&P-style buffer hysteresis:
      1. Filters candidates by hard liquidity floors (`price >= $50`, `adv_60d >= $500M`).
      2. Ranks by 60-day Average Daily Dollar Volume (`adv_60d_usd`).
      3. Retains existing members still ranked `<= exit_rank_cutoff` (120), promotes newcomers
         ranked `<= enter_rank_cutoff` (85), and fills remaining slots up to `target_equities_count`
         subject to `max_per_sector` (24) so no single GICS sector dominates the 100-stock base pool.
      4. Preserves all `Sector_ETF` / `Commodities_ETF` / `Broad_ETF` anchors intact.
    """
    etf_anchors = [
        u for u in current_universe if u.sector.endswith("_ETF")
    ]
    current_equities = {
        u.symbol.upper(): u for u in current_universe if not u.sector.endswith("_ETF")
    }

    eligible = [
        c
        for c in candidates_with_metrics
        if float(c.get("price", 0.0)) >= min_price_usd
        and float(c.get("adv_60d_usd", 0.0)) >= min_adv_usd
        and not str(c.get("sector", "")).endswith("_ETF")
    ]
    eligible.sort(key=lambda c: float(c.get("adv_60d_usd", 0.0)), reverse=True)

    selected: List[UniverseSecurity] = []
    selected_syms: Set[str] = set()
    sector_counts: Dict[str, int] = {}

    def _try_add(item: Dict[str, Any]) -> bool:
        sym = str(item["symbol"]).upper()
        if sym in selected_syms or len(selected) >= target_equities_count:
            return False
        existing = current_equities.get(sym)
        sector = str(item.get("sector") or (existing.sector if existing else "Technology"))
        if sector_counts.get(sector, 0) >= max_per_sector:
            return False
        sec_obj = existing or UniverseSecurity(
            symbol=sym,
            sector=sector,
            cik=str(item.get("cik") or "0000000000"),
            etf_core_score=float(item.get("etf_core_score") or 0.80),
            baseline_inst_pct=float(item.get("baseline_inst_pct") or 0.72),
        )
        selected.append(sec_obj)
        selected_syms.add(sym)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        return True

    # Pass 1: Promote top-conviction newcomers & incumbents inside `enter_rank_cutoff` (<= 85)
    for rank_idx, item in enumerate(eligible, start=1):
        if rank_idx <= enter_rank_cutoff:
            _try_add(item)

    # Pass 2: Buffer protection — retain incumbent members still inside `exit_rank_cutoff` (<= 120)
    for rank_idx, item in enumerate(eligible, start=1):
        sym = str(item["symbol"]).upper()
        if rank_idx <= exit_rank_cutoff and sym in current_equities:
            _try_add(item)

    # Pass 3: Fill any remaining slots up to `target_equities_count` by highest ADV within sector caps
    for item in eligible:
        if len(selected) >= target_equities_count:
            break
        _try_add(item)

    old_syms = set(current_equities.keys())
    kept = sorted(old_syms & selected_syms)
    added = sorted(selected_syms - old_syms)
    removed = sorted(old_syms - selected_syms)

    full_list = [*selected, *etf_anchors]
    summary = UniverseRebalanceSummary(
        kept=kept,
        added=added,
        removed=removed,
        sector_distribution=dict(sorted(sector_counts.items())),
        total_count=len(selected),
    )
    return full_list, summary


def save_universe_yaml(path: Path, universe: Sequence[UniverseSecurity]) -> None:
    payload = {
        "universe": [
            {
                "symbol": u.symbol,
                "sector": u.sector,
                "cik": u.cik,
                "etf_core_score": round(float(u.etf_core_score), 2),
                "baseline_inst_pct": round(float(u.baseline_inst_pct), 2),
            }
            for u in universe
        ]
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
