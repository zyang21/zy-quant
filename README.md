# zy-quant

Resilient US Equities Intraday & Short-Swing Quantitative Trading System built for **Futu OpenAPI (`FutuOpenD`)** with automatic offline `yfinance` fallback, exact 2026 Futu HK fee accounting, and multi-tier capital scaling (`$1,000` -> `$10,000` -> `$100,000`).

## Core 4-Step Pipeline

1. **Market Regime Traffic Light (`src/zy_quant/intel/regime.py`)**
   - **`GREEN`:** `QQQ >= Session VWAP`, `QQQ >= 20d SMA`, `VIX < 24` -> Unlocks **15-minute** trade & watchlist scan.
   - **`YELLOW` / `RED`:** Locks all new buy orders (`0` entries, `$0` churn fees). When flat, throttles background scans to **every 2 hours (`120m`)** to rank **Relative Strength (`RS vs QQQ`)** leaders for the next market turn.
2. **Conservative Institutional & ETF Watchlist Scorer (`config/watchlist_scoring.yaml`)**
   - Hard Veto Filters: `Price >= $50` (avoids per-share settlement fee explosion), `ADV >= $500M`, `Institutional Ownership >= 55%`, `+-2d Earnings Blackout`, `Spread <= 8 bps`.
   - Weighted Ranking: ETF Core Weight (`30%`) + Institutional Ownership (`20%`) + Stability & Red-Day RS (`15%`) + Futu Real-Time Big-Order Inflow (`20%`) + SEC EDGAR Form 4 Catalyst (`15%`).
3. **15-Minute Trigger Setups (`src/zy_quant/strategy/signals.py`)**
   - **Setup A (`VWAP_PULLBACK`):** Morning impulse (`RVOL >= 1.5x`) -> contracted-volume test of session `VWAP` -> bullish reclaim above `VWAP` & `EMA21`.
   - **Setup B (`ORB_30M`):** 30-minute Opening Range Breakout on `RVOL >= 1.8x` with positive institutional block flow.
4. **Scientific Exit & Sizing Engine (`src/zy_quant/strategy/exit_models.py`)**
   - **Wilder ATR Initial Stop:** `S_init = P_entry - 1.8 * ATR_14`
   - **Breakeven + Chandelier Ratchet:** Lifts stop above round-trip fees at `+1.5R`, then trails `H_max - 2.8 * ATR_14` monotonically upward.
   - **Triple-Barrier Time Stop:** Exits dead-money trades after `16` bars (`4h`) if unrealized `< +0.30R`.
   - **Positive Expectancy & Edge-to-Fee Gate:** Rejects any order where expected swing `< 8x` Futu round-trip friction or `E < +0.25R`.

## Account Tiers (`config/tiers.yaml`)

| Tier | Capital | Max Positions | Style | Exit Mode |
| :--- | :--- | :--- | :--- | :--- |
| `tier_1k` | `$1,000` | `1` (`<= 3/wk`) | 1-5d Swing + `<1 sh` Fractional Optimization | Full Chandelier Trail |
| `tier_10k` | `$10,000` | `3` (`<= 3/day`) | 15m Intraday + Overnight Continuation | Full Chandelier Trail |
| `tier_100k` | `$100,000` | `6` (`<= 8/day`, `<= 40%/sector`) | Multi-Position Intraday + Swing | **50% Scale-Out @ `+2R`** + 50% Chandelier |

## Reliability & Fail-Safe Design

- **Server-Side Protective Stop Orders:** Submits native `STOP_LIMIT` protective orders directly to Futu servers upon entry and ratchets them upward.
- **SQLite WAL Idempotent Ledger & Startup Reconciliation:** Prevents duplicate orders across restarts and reconciles live `FutuOpenD` positions automatically on boot.
- **Hard Mandate Circuit Breakers:** Persistent global Kill-Switch (`HALT`), daily max loss lock, and 60-second runaway burst breaker.

## Quickstart

```bash
# Install in isolated virtual environment
uv venv --python 3.12
uv pip install -e ".[dev]"

# Run unit tests
.venv/bin/pytest

# Calculate exact Futu 2026 US round-trip friction
.venv/bin/zy-quant fee-calc --shares 25 --price 190

# Run 15m fee-accurate backtest
.venv/bin/zy-quant backtest --tier tier_10k --days 30

# Run a single daemon scan cycle
.venv/bin/zy-quant daemon --once --tier tier_1k

# Launch Web Quantitative Control Center (http://127.0.0.1:8088)
.venv/bin/zy-quant dashboard --port 8088
```
