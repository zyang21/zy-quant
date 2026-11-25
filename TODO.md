# zy-quant Roadmap & Engineering TODO

Prioritized engineering backlog for upgrading `zy-quant` from a single-gateway prototype into a decoupled, execution-verified, multi-regime quantitative trading system.

---

## Priority Legend

- **`[P0]` Critical Execution & Safety** — Eliminates live/paper ledger drift and tail-risk blind spots before risking real capital.
- **`[P1]` Data Decoupling & Universe Scaling** — Removes Futu API rate-limit bottlenecks (`60 req / 30s`), expands to a 100-stock multi-sector pool, and unlocks multi-year walk-forward backtesting.
- **`[P2]` Statistical Adaptivity** — Replaces static hardcoded strategy assumptions with self-calibrating mathematical estimators validated out-of-sample.
- **`[P3]` Cross-Project SQLite Intel Contract** — Read-only schema consumption for the standalone Pre/Post-Market LLM Intel Agent project.
- **`[P4]` Futu HK `T+0` Rules, Progressive Chandelier & `tier_10k` Scale-Out** — Fixes `+1.8R` profit giveback, removes false US `$25k` PDT constraint for Futu HK accounts, adds conditional EOD/Friday `T+0` exits, and unlocks 50% partial profit lock on `tier_10k`.

---

## `[P0]` Real Order Execution Lifecycle & Callback State Machine
**Target Branch:** `feat/futu-order-callbacks`

Fix the optimistic fill assumption in `TradingDaemon.run_cycle()` (`src/zy_quant/runtime/daemon.py:383-399`) where orders are marked `FILLED` at signal price immediately upon receiving a `broker_order_id`.

### Functions & Tasks
- [x] **Extend `OrderStatus` & `orders` Schema (`models.py`, `storage/state_store.py`)**
  - [x] Add `SUBMITTED`, `PARTIAL_FILLED`, `CANCELLED`, `EXPIRED`, `FAILED` states.
  - [x] Add `filled_shares`, `reconciled_shares`, `avg_fill_price`, and `plan_json` columns (idempotent `ALTER TABLE` migration in `StateStore._init_db()`).
- [x] **Register Futu Push Handlers (`gateway/futu_adapter.py`)**
  - [x] Implement `ZyTradeOrderHandler(ft.TradeOrderHandlerBase)` to process `on_recv_rsp()` push updates (`order_status`, `dealt_qty`, `dealt_avg_price`, `remark` / `order_id`).
  - [x] *Simulation Market Note:* Rely on `TradeOrderHandlerBase` (`dealt_qty`, `dealt_avg_price`) as primary source of truth because `TradeDealHandlerBase` (`deal_list`) is often silent in Futu `SIMULATE`.
- [x] **Polling Fallback & Fee Reconciliation (`gateway/futu_adapter.py`, `runtime/daemon.py`)**
  - [x] Add `FutuGatewayAdapter.sync_open_orders()` calling `order_list_query()` each cycle so dropped TCP packets never leave stale `SUBMITTED` orders.
  - [x] Query `order_fee_query()` on terminal fills in `REAL` mode with automatic fallback to `FutuUSFeeModel` on `'Unknown protocol ID'` or unsettled `$0` fees.
- [x] **Unfilled Order Timeout, In-Flight Slot Locks & Partial-Fill Handling (`risk/mandate_gate.py`, `runtime/daemon.py`)**
  - [x] Transition newly placed entry orders to `SUBMITTED` (do **not** create `OpenPosition` or server-side stop until `dealt_qty > 0`, reconstructing from `orders.plan_json`).
  - [x] Lock position slots, duplicate-symbol guard, sector concentration cap, and reserved cash on in-flight `SUBMITTED` / `PARTIAL_FILLED` buy orders; preserve `PAPER-LOCAL` virtual fills when `acc_id == 0`.
  - [x] Cancel stale `SUBMITTED` entry orders after configurable `entry_order_ttl_seconds` (`90s`) without burning daily/weekly fee-turnover caps on 0-share expiries.
  - [x] If partially filled upon timeout, cancel remaining open quantity, open `OpenPosition` for exact `dealt_qty`, and place matching server-side protective stop.
  - [x] Detect server-side protective stop fills (`broker_stop_order_id`) automatically during push/polling and record closed trades into `closed_trades`.
- [x] **Simulation Market Verification (`tests/test_order_lifecycle.py`)**
  - [x] Unit tests for `SUBMITTED -> PARTIAL_FILLED -> FILLED`, `PARTIAL_FILLED -> CANCELLED` TTL lock, fast-push race handling, `order_fee_query` fallback, server-side stop fill, and `PAPER-LOCAL` mode.
  - [ ] Live smoke test against Futu `SIMULATE` environment during US market hours.

---

## `[P1]` Alpaca Market Data Integration & 100-Stock Dynamic Universe
**Target Branch:** `feat/alpaca-universe`

Decouple **wide market scanning & historical research** (Alpaca Market Data API + local DuckDB/Parquet cache) from **live order execution & L1 spread/stop management** (Futu OpenAPI).

### Functions & Tasks
- [ ] **Alpaca Market Data Adapter (`src/zy_quant/gateway/alpaca_adapter.py`)**
  - [ ] Implement `AlpacaMarketDataAdapter(MarketDataAdapter)` using free IEX real-time snapshots & 2016–present historical `15m` / `1d` bars (`alpaca-py` or lightweight REST client).
  - [ ] Support batch multi-symbol snapshot & bar requests (`100` symbols per HTTP call) to scan the full 100-stock pool in `< 1.5s` with zero Futu quota usage.
  - [ ] Add credentials (`alpaca_api_key`, `alpaca_secret_key`) strictly to gitignored `config/settings.local.json`.
- [ ] **Local Historical Parquet / SQLite Bar Cache (`src/zy_quant/storage/bar_cache.py`)**
  - [ ] Cache downloaded historical `15m` & `1d` bars locally so 3–6 year walk-forward backtests (`2020–2026`) run offline in seconds.
  - [ ] Remove the 58-day Yahoo truncation ceiling in `BacktestEngine.run()` (`src/zy_quant/backtest/engine.py:81`).
- [ ] **100-Stock Sector-Balanced Core Pool (`config/universe_100.yaml` + `scripts/rebalance_universe.py`)**
  - [ ] Expand static 14-stock tech-heavy list to **100 liquid institutional leaders + 10 core sector ETFs** (`XLK, SMH, XLF, XLV, XLE, XLI, XLY, XLP, XLU, GLD`).
  - [ ] Enforce **GICS Sector Cap** during active Top-8 selection (e.g., max `2–3` symbols per sector in `top_n_active`) to prevent 100% semiconductor/AI crowding.
  - [ ] Build quarterly/monthly universe maintenance CLI (`zy-quant refresh-universe`) using objective filters (`ADV_60d >= $500M`, `Price >= $50`, `MarketCap >= $20B`) + Point-in-Time historical constituent CSV support for survivorship-bias-free backtests.
- [ ] **Earnings Blackout Calendar Feed (`src/zy_quant/intel/earnings_calendar.py`)**
  - [ ] Wire `next_earnings_date` into `TradingDaemon.refresh_watchlist()` (`src/zy_quant/runtime/daemon.py:149`) and force pre-earnings exit/veto within `+-2` trading days.

---

## `[P2]` Self-Calibrating Statistical Parameters (Anti-Decay)
**Target Branch:** `feat/adaptive-params`

Replace fragile static priors in `config/strategy.yaml` with bounded mathematical estimators, validated across `2020–2026` out-of-sample regimes (2020 crash, 2021 bull, 2022 bear, 2023–2026 AI cycle).

### Functions & Tasks
- [ ] **Create `src/zy_quant/strategy/adaptive.py`**
  - [ ] `compute_empirical_bayes_expectancy()`: Beta-Binomial conjugate update for `win_rate` and shrinkage estimator for `avg_win_r` over trailing `N=60` trades per setup (`VWAP_PULLBACK`, `ORB_30M`), replacing hardcoded `assumed_base_win_rate: 0.48` and `expected_reward_atr_multiple: 6.5`.
  - [ ] `compute_adaptive_vol_thresholds()`: 252-day rolling 80th / 95th volatility quantile thresholds with safety clamps (`[18, 30]` Yellow, `[25, 42]` Red), replacing static `vix_yellow_threshold: 28.0` / `38.0`.
  - [ ] `compute_efficiency_adjusted_atr_mult()`: Kaufman Efficiency Ratio (`ER`) scaling around `base_mult=2.8` (`[2.3x, 3.4x] ATR`) to widen stops in choppy tape while shrinking share count to keep dollar risk constant.
- [ ] **Walk-Forward Validation Harness (`src/zy_quant/backtest/walk_forward.py`)**
  - [ ] Compare static vs. adaptive parameter sets across rolling 6-month train / 3-month out-of-sample windows before enabling by default in live config.

---

## `[P3]` SQLite Data Contract for Standalone External Intel Agent Project
**Target Branch (in `zy-quant`):** `feat/intel-db-contract`  
**External Repo:** Standalone Pre/Post-Market Agent Pipeline (runs outside RTH, e.g., `08:30 ET` & `17:30 ET`)

`zy-quant` remains a deterministic, zero-LLM-latency trading engine. The external Agent project writes structured intelligence rows into the shared SQLite WAL database (`data/zy_quant_sim.db` / `zy_quant_live.db`), and `zy-quant` reads them in `O(1)` during market hours.

### Shared SQLite Schema Contract (`StateStore._init_db()`)
- [ ] **Table 1: `symbol_intel_overlay` (Written by External Agent, Read by `zy-quant`)**
  - `symbol TEXT PRIMARY KEY`
  - `veto_entry INTEGER NOT NULL DEFAULT 0` — Hard block on SEC 8-K red flags, DOJ/SEC probes, guidance cuts, secondary offerings, or earnings risk.
  - `veto_reason TEXT NOT NULL DEFAULT ''`
  - `catalyst_score_adj REAL NOT NULL DEFAULT 0.0` — Bounded `[-15.0, +15.0]` adjustment to `watchlist_scores.total_score`.
  - `next_earnings_date TEXT` — ISO `YYYY-MM-DD`.
  - `expires_at TEXT NOT NULL` — TTL guard so stale agent outputs automatically expire to neutral (`0.0`) if the external agent job fails to run.
  - `updated_at TEXT NOT NULL`
- [ ] **Table 2: `macro_blackout_windows` (Written by External Agent, Read by `MandateRiskGate`)**
  - `window_id TEXT PRIMARY KEY`
  - `event_name TEXT NOT NULL` (e.g., `FOMC Rate Decision`, `CPI Release`, `Fed Chair Press Conference`)
  - `start_utc TEXT NOT NULL`
  - `end_utc TEXT NOT NULL`
  - `action TEXT NOT NULL` (`BLOCK_NEW_ENTRIES` or `TIGHTEN_STOPS`)
  - `updated_at TEXT NOT NULL`
- [ ] **Read-Side Hooks in `zy-quant`**
  - [ ] `ConservativeWatchlistScorer`: Apply `veto_entry` and clamped `catalyst_score_adj` when `expires_at > now`.
  - [ ] `MandateRiskGate.check_entry()`: Reject new entries during active `macro_blackout_windows`.
  - [ ] Web Dashboard (`overview.js`): Display active Macro Blackout badges and Agent Veto reasons next to watchlist tickers.

---

## `[P4]` Futu HK `T+0` Exit Governance, Progressive Chandelier & `tier_10k` Scale-Out
**Target Branch:** `feat/futu-hk-t0-and-exit-tuning`

Eliminates the inverted payoff ratio observed in 30d/55d backtests (where `4.2x ATR` Chandelier gave back ~65% of `+1.8R` gains) and replaces the non-applicable US FINRA `$25k` PDT restriction with **Futu HK SFC-regulated infinite `T+0` rules** calibrated strictly by fee-to-notional economics.

### Functions & Tasks
- [x] **Progressive Profit-Lock & Tightened Chandelier (`config/strategy.yaml`, `strategy/exit_models.py`)**
  - [x] Arm breakeven-plus-fees stop at `+1.5R` (`breakeven_trigger_r: 1.5` vs `1.8`).
  - [x] Tighten trailing `chandelier_atr_multiplier` from `4.2` to `2.8` and add progressive profit-floor ratchet (`profit_lock_trigger_r: 1.8`, `profit_lock_floor_ratio: 0.55`) so winners reaching `+1.8R+` lock at least `55%` of peak open gain.
- [x] **Futu HK Defensive `T+0` EOD & Weekend Shield (`config/tiers.yaml`, `strategy/exit_models.py`, `backtest/engine.py`, `runtime/daemon.py`)**
  - [x] Add `ExitReason.EOD_T0_FLATTEN` evaluated during the closing 30m window (`session_bar_num >= 25`, `15:30–16:00 ET`).
  - [x] Cut genuinely failing day-0 setups (`eod_weak_exit_below_r: -0.65`) and losing Friday positions (`friday_min_cushion_r: -0.35`) `T+0` before overnight/weekend gaps, while sparing healthy consolidations (`-0.2R..+0.25R`) from double-fee churn.
- [x] **Unlock Partial Scale-Out in `tier_10k` (`config/tiers.yaml`, `strategy/exit_models.py`)**
  - [x] Enable 50% scale-out at `+1.8R` in `tier_10k` (guarded by `min_scale_out_notional_usd: 1500.0` so small lots never pay double minimum fees).
- [x] **30D & 55D Real Futu Historical Backtest Verification**
  - [x] Verified on live Futu historical `15m` bars (`2026-08-25 -> 2026-11-24`, QQQ `-11.1%`): `tier_10k` flipped `-$71.21 -> +$178.20` (`PF 1.19`), `tier_100k` flipped `-$1,429.96 -> +$661.00` (`PF 1.08`), and `tier_1k` cut drawdown `-4.45% -> -0.79%` (gross `+$35.00`).
