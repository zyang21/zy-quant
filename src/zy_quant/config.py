from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


def default_project_root() -> Path:
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env", override=False)
    return root


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


class FutuConnectionConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 11111
    trd_env: str = "SIMULATE"
    api_token: str = ""
    trade_pwd_md5: str = ""


class WebServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8088
    auto_start_daemon: bool = True


class RuntimeSettingsConfig(BaseModel):
    account_tier: Optional[str] = None
    allow_synthetic_fallback: bool = False
    futu: FutuConnectionConfig = Field(default_factory=FutuConnectionConfig)
    web: WebServerConfig = Field(default_factory=WebServerConfig)


class TierProfile(BaseModel):
    label: str
    reference_equity_usd: float
    max_concurrent_positions: int
    max_position_pct: float
    min_order_notional_usd: float
    allow_fractional_shares: bool = False
    risk_per_trade_pct: float
    max_daily_loss_pct: float
    max_trades_per_day: int
    max_trades_per_week: int
    min_expected_move_pct: float
    edge_to_fee_multiple: float = 8.0
    allow_overnight_hold: bool = True
    enable_scale_out: bool = False
    scale_out_fraction: float = 0.50
    scale_out_target_r: float = 2.0
    enable_index_hedge: bool = False
    max_sector_weight_pct: float = 1.0
    order_protection_bps: float = 5.0


class TiersConfig(BaseModel):
    active_tier: str = "tier_10k"
    tiers: Dict[str, TierProfile]

    def get_active(self, override: Optional[str] = None) -> TierProfile:
        key = override or os.getenv("ZY_ACCOUNT_TIER") or self.active_tier
        if key not in self.tiers:
            raise KeyError(f"Unknown account tier '{key}'. Available: {list(self.tiers)}")
        return self.tiers[key]


class HardFiltersConfig(BaseModel):
    min_price_usd: float = 50.0
    min_avg_daily_dollar_vol: float = 500_000_000.0
    min_institutional_pct: float = 0.55
    earnings_blackout_days: int = 2
    max_spread_bps: float = 8.0


class ScoringWeightsConfig(BaseModel):
    etf_core_weight: float = 0.30
    institutional_ownership: float = 0.20
    low_drawdown_stability: float = 0.15
    realtime_big_order_flow: float = 0.20
    insider_form4_catalyst: float = 0.15


class ScoringThresholdsConfig(BaseModel):
    min_watch_score: float = 62.0
    top_n_active: int = 8
    red_day_rs_bonus_cap: float = 15.0


class UniverseSecurity(BaseModel):
    symbol: str
    sector: str
    cik: str
    etf_core_score: float = Field(ge=0.0, le=1.0)
    baseline_inst_pct: float = Field(ge=0.0, le=1.0)


class WatchlistScoringConfig(BaseModel):
    hard_filters: HardFiltersConfig
    weights: ScoringWeightsConfig
    scoring_thresholds: ScoringThresholdsConfig
    universe: List[UniverseSecurity]


class RegimeConfig(BaseModel):
    primary_benchmark: str = "QQQ"
    secondary_benchmark: str = "SPY"
    volatility_symbol: str = "^VIX"
    daily_sma_period: int = 20
    vix_yellow_threshold: float = 28.0
    vix_red_threshold: float = 38.0
    sharp_intraday_drop_pct: float = -0.012
    yellow_position_scale: float = 0.65
    green_scan_interval_minutes: int = 15
    yellow_watchlist_interval_minutes: int = 45
    rest_watchlist_interval_minutes: int = 120


class VwapPullbackConfig(BaseModel):
    enabled: bool = True
    min_morning_rvol: float = 1.5
    max_pullback_rvol: float = 1.1
    vwap_touch_band_atr: float = 0.35


class OrbConfig(BaseModel):
    enabled: bool = True
    orb_minutes: int = 30
    min_breakout_rvol: float = 1.8
    require_positive_big_order_flow: bool = True


class EntryConfig(BaseModel):
    bar_timeframe: str = "15m"
    atr_period: int = 14
    ema_fast_period: int = 9
    ema_trend_period: int = 21
    entry_start_hour_et: int = 10
    entry_end_hour_et: int = 14
    entry_end_minute_et: int = 30
    require_stock_above_sma20: bool = True
    yellow_min_rs_outperformance: float = 0.015
    cooldown_hours_after_exit: int = 20
    vwap_pullback: VwapPullbackConfig
    opening_range_breakout: OrbConfig


class ExitModelsConfig(BaseModel):
    atr_stop_multiplier: float = 2.8
    min_stop_pct: float = 0.015
    max_hard_stop_pct: float = 0.040
    breakeven_trigger_r: float = 1.8
    chandelier_atr_multiplier: float = 4.2
    time_stop_bars_15m: int = 52
    time_stop_min_progress_r: float = 0.15
    assumed_base_win_rate: float = 0.48
    expected_reward_atr_multiple: float = 6.5
    min_expectancy_r: float = 0.25


class DaemonConfig(BaseModel):
    heartbeat_timeout_seconds: int = 30
    max_orders_per_minute: int = 3
    place_server_side_stop: bool = True


class StrategyConfig(BaseModel):
    regime: RegimeConfig
    entry: EntryConfig
    exit_models: ExitModelsConfig
    daemon: DaemonConfig


class AppSettings(BaseModel):
    root_dir: Path
    runtime_cfg: RuntimeSettingsConfig
    tiers_cfg: TiersConfig
    watchlist_cfg: WatchlistScoringConfig
    strategy_cfg: StrategyConfig

    @property
    def active_tier_key(self) -> str:
        return self.runtime_cfg.account_tier or os.getenv("ZY_ACCOUNT_TIER") or self.tiers_cfg.active_tier

    @property
    def active_tier_profile(self) -> TierProfile:
        return self.tiers_cfg.get_active(self.active_tier_key)

    @classmethod
    def load(cls, root_dir: Optional[Path] = None) -> "AppSettings":
        root = root_dir or default_project_root()
        cfg_dir = root / "config"

        # Load unified JSON runtime settings + optional gitignored settings.local.json + env fallbacks
        runtime_raw: Dict[str, Any] = {}
        settings_json = cfg_dir / "settings.json"
        if settings_json.exists():
            runtime_raw = json.loads(settings_json.read_text(encoding="utf-8"))
        local_json = cfg_dir / "settings.local.json"
        if local_json.exists():
            runtime_raw = _deep_merge(
                runtime_raw, json.loads(local_json.read_text(encoding="utf-8"))
            )

        futu_dict = dict(runtime_raw.get("futu", {}))
        if os.getenv("ZY_FUTU_HOST"):
            futu_dict["host"] = os.environ["ZY_FUTU_HOST"]
        if os.getenv("ZY_FUTU_PORT"):
            futu_dict["port"] = int(os.environ["ZY_FUTU_PORT"])
        if os.getenv("ZY_TRD_ENV"):
            futu_dict["trd_env"] = os.environ["ZY_TRD_ENV"]
        if os.getenv("ZY_FUTU_API_TOKEN") and not futu_dict.get("api_token"):
            futu_dict["api_token"] = os.environ["ZY_FUTU_API_TOKEN"]
        if os.getenv("ZY_FUTU_TRADE_PWD_MD5") and not futu_dict.get("trade_pwd_md5"):
            futu_dict["trade_pwd_md5"] = os.environ["ZY_FUTU_TRADE_PWD_MD5"]
        runtime_raw["futu"] = futu_dict

        if not runtime_raw.get("account_tier") and os.getenv("ZY_ACCOUNT_TIER"):
            runtime_raw["account_tier"] = os.environ["ZY_ACCOUNT_TIER"]

        runtime_cfg = RuntimeSettingsConfig.model_validate(runtime_raw)

        with open(cfg_dir / "tiers.yaml", "r", encoding="utf-8") as f:
            tiers_raw = yaml.safe_load(f)
        if runtime_cfg.account_tier:
            tiers_raw["active_tier"] = runtime_cfg.account_tier
        with open(cfg_dir / "watchlist_scoring.yaml", "r", encoding="utf-8") as f:
            watch_raw = yaml.safe_load(f)
        with open(cfg_dir / "strategy.yaml", "r", encoding="utf-8") as f:
            strat_raw = yaml.safe_load(f)

        return cls(
            root_dir=root,
            runtime_cfg=runtime_cfg,
            tiers_cfg=TiersConfig.model_validate(tiers_raw),
            watchlist_cfg=WatchlistScoringConfig.model_validate(watch_raw),
            strategy_cfg=StrategyConfig.model_validate(strat_raw),
        )
