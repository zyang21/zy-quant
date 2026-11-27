# zy-quant Active Roadmap & Engineering TODO

Active engineering backlog for `zy-quant`. Completed milestones (`[P0]`, `[P1]`, `[P4]`) are archived in [`HISTORY.md`](HISTORY.md).

---

## Priority Legend

- **`[P1]` Swing Software Architecture & Phase-Driven Pipeline Redesign (Next Up)** — Replaces the continuous 3-minute `15m`-bar intraday loop with a deterministic 4-phase daily swing pipeline (`PRE_OPEN -> OPEN_GUARD -> EOD_DECISION -> POST_CLOSE`), causal `1d` / `15:45 ET` dual-clock backtester, `GTC` stop-healer, and `Portfolio Heat` risk governance.
- **`[P2]` Low-Frequency Swing Engine, Pre-Earnings Cushion & 2021–2026 Cross-Regime Validation** — Transitions core strategy math, tier sizing, 3-tier "House Money" earnings governance, and macro volatility quantiles to multi-week institutional swing on Futu HK, validated across 2021–2026.
- **`[P3]` Cross-Project SQLite Intel Contract** — Read-only schema consumption for the standalone Pre/Post-Market LLM Intel Agent project.
- **`[P5]` Dev / Prod Split Architecture, Self-Hosted CI/CD & Mesh Networking** — Separates local development/backtesting from native 24/5 production execution on `zyang@MacBook-Air.local` with automated GitHub Actions deployment and private remote access.

---

## `[P1]` Swing Software Architecture & Phase-Driven Pipeline Redesign
**Target Branch:** `feat/swing-pipeline-arch-p1`

Re-architects `zy-quant`'s runtime scheduler, strategy interfaces, data/backtest clock, broker stop lifecycle, and risk gate from a continuous 15-minute intraday loop (`~130` ticks/day, millions of `15m` bars) to an event/phase-driven **Daily Swing Execution Pipeline** built for `~25–45 trades/yr`.

### Architectural Shifts & Tasks
- [ ] **0. Pre-Flight Codebase Fixes from Previous `[P1]` Merge (`cli.py`, `bar_cache.py`, `state_store.py`, `market_hours.py`)**
  - [ ] Preserve `alpaca` & `web` keys when running `zy-quant config --consolidate-local` (`src/zy_quant/cli.py:271-282` currently overwrites `settings.local.json` with only `futu`).
  - [ ] Add overlap-bar split/dividend cliff detection (`|new_close - cached_close| / cached_close > 0.2%` -> auto re-sync symbol) in `AlpacaMarketDataAdapter` + `HistoricalBarCache`, reuse persistent `httpx.Client` keep-alive pooling, prune stale symbols in `StateStore.save_watchlist_scores()`, and extend `NYSE_HOLIDAYS` / `NYSE_EARLY_CLOSE_DATES` across `2021–2027`.
- [ ] **1. 4-Phase Deterministic Daily Pipeline State Machine (`src/zy_quant/runtime/pipeline.py`, `src/zy_quant/runtime/daemon.py`)**
  - [ ] Replace the undifferentiated `3m` `while True` intraday scan loop with an idempotent, SQLite-checkpointed **4-Phase Session DAG** (`daily_pipeline_runs` table keyed by `(trading_date, phase)` so restarts/wakeups never double-fire):
    1. **`PRE_OPEN_SYNC` (`09:20–09:30 ET`):** Reconcile Futu holdings & open orders; backfill completed `T-1` `1d` bars into `HistoricalBarCache`; compute per-symbol 20d `SIP / IEX` volume calibration factors; refresh `EarningsCalendarFeed` (`+14d` window with `BMO`/`AMC` timing) & EDGAR Form 4; run **GTC Stop-Audit Healer** (distinguishing `FILLED` stops from exchange ex-dividend / 90-day `CANCELLED/EXPIRED` `0`-fill GTC stops and immediately re-arming missing `GTC STOP` orders before `09:30 ET`).
    2. **`OPEN_GAP_GUARD` (`09:35–09:45 ET`):** Reconcile opening-auction `STOP` fills & `query_order_fee()`; execute client-side RTH stop fallback for `<1.0 sh` fractional positions (unsupported by Futu server-side `STOP`); transition daemon into low-power `IDLE_GTC_ARMED` watch (or midday exit only when `100%` of open positions are whole-share `GTC`-protected and today is not an early-close half-day).
    3. **`EOD_DECISION_WINDOW` (`15:35–15:55 ET` normal / `12:35–12:55 ET` early-close):** Build zero-lookahead **Causal `partial_1d_bar`** (`09:30 -> 15:40 ET` session OHLC + SIP-calibrated/time-of-day normalized volume appended to `<= T-1` completed `1d` bars) -> run DAG in strict order: **(a)** Daily Macro & Breadth Regime -> **(b)** Pre-Earnings Cushion (`BMO` `T-1` / `AMC` `T-0`), Daily Time-Stop & Chandelier `1d ATR` stop upward ratchets -> **(c)** 100-stock Daily RS Ranking & Swing Entry Signals -> marketable limit orders (`60s`/`90s` TTL) + immediate `GTC` server-side stop arming on fill.
    4. **`POST_CLOSE_SETTLEMENT` (`16:02–16:10 ET` / `13:02–13:10 ET` early-close):** Persist finalized `T` `1d` bars, record `daily_nav_snapshots` (equity, cash, accrued idle-cash sweep yield, open portfolio heat `%`), verify `0` dangling `DAY` orders and `100%` overnight `GTC` stop coverage, emit daily digest, and exit cleanly to release `caffeinate -is`.
- [ ] **2. Pluggable Timeframe-Agnostic Strategy & Signal Protocol (`src/zy_quant/strategy/base.py`, `signals_swing.py`, `regime.py`, `watchlist_scorer.py`)**
  - [ ] Decouple `TradingDaemon` and `BacktestEngine` from hardcoded `IntradaySignalEngine.evaluate_15m()` behind a clean `StrategyPipelineProtocol` (`strategy.mode: "swing_1d" | "intraday_15m"` for reproducible A/B benchmarks).
  - [ ] Add `DailySwingSignalEngine` (`DAILY_PULLBACK_EMA21`, `VCP_PIVOT_BREAKOUT`, `DONCHIAN_20D_RS`, `PEAD_CONTINUATION`), `DailyRegimeDetector` (`QQQ/SPY` vs `SMA50/SMA200` + 100-stock breadth `% > SMA50`), and multi-week `SwingWatchlistScorer` (`20d/60d` RS vs `QQQ`, Stage-2 alignment, volatility contraction, `>= 10` trading-day / `14` calendar-day pre-earnings entry buffer).
- [ ] **3. Dual-Clock Daily / Hybrid Backtest Engine (`src/zy_quant/backtest/engine.py`, `storage/bar_cache.py`)**
  - [ ] Add **`1d` Primary Clock Mode** (`~169k` daily bars across 112 symbols for `2021–2026`, `< 100ms` per 6-year run for instant walk-forward sweeps) with conservative gap-aware stop execution (`fill_px = min(next_open, current_stop)`).
  - [ ] Add **`1d + 15:45 ET` Hybrid Execution Clock Mode** over cached `15m` windows (`2025–2026`) constructing the exact same causal `partial_1d_bar` used in live `Phase 3 (EOD_DECISION_WINDOW)` as a Clock-Parity Verification Gate (`< 0.3%` drift check).
- [ ] **4. Multi-Week Broker Stop Persistence & Position Schema Upgrade (`gateway/futu_adapter.py`, `models.py`, `storage/state_store.py`)**
  - [ ] Upgrade `FutuGatewayAdapter.place_or_update_protective_stop()` from expiring default `TimeInForce.DAY` + `STOP_LIMIT` to persistent **`TimeInForce.GTC` + `OrderType.STOP` (`fill_outside_rth=False`)** for whole-share lots (`>= 1 sh`), plus explicit client-side RTH stop fallback (`SIM-CLIENT-FRAC-STOP`) for `< 1 sh` fractional lots.
  - [ ] Migrate `positions` & `orders` schema (`days_held`, `entry_atr_1d`, `stop_order_type`, `earnings_cushion_applied_date`) + add `daily_pipeline_runs` and `daily_nav_snapshots` SQLite tables.
- [ ] **5. Portfolio-Heat Risk Gate & Two-Window Power Scheduling (`risk/mandate_gate.py`, `deploy/`, `web/`, `cli.py`)**
  - [ ] Upgrade `MandateRiskGate` to track both **Principal Heat** (`sum(max(0, entry_price - current_stop) * shares)` + in-flight `SUBMITTED` buy risk `<= max_portfolio_heat_pct`) and **Open-Equity Drawdown Heat** (`sum(max(0, mark_price - current_stop) * shares) / equity`), alongside hard single-equity (`<= 25%`) / `Sector_ETF` (`<= 35%`) notional caps.
  - [ ] Update `deploy/com.zyquant.daemon.plist` & `deploy/run_daemon.sh` to support **Two-Window Scheduled Wake** (`09:20–09:50 ET` Open Guard + `15:25–16:10 ET` Close Decision Window; staying continuously awake `09:20–13:10 ET` on `13:00 ET` early-close half-days or whenever an unprotected `< 1 sh` fractional position is open).
  - [ ] Update Web Terminal (`overview.js`, `backtest.js`) & `zy-quant status` to surface **Pipeline Phase**, **Open Portfolio Heat (`$ / %`)**, **Days Held**, **GTC Stop Health**, and **Earnings Countdown / Cushion Tier**.

---

## `[P2]` Low-Frequency Multi-Week Swing Math, BMO/AMC Earnings Cushion & 2021–2026 Validation
**Target Branch:** `feat/low-freq-swing-p2`

Calibrates strategy sizing math, `1d` initial vs. trailing ATR stops, `BMO/AMC` pre-earnings "House Money" governance, macro volatility quantiles, and survivorship-free `2021–2026` validation on top of `[P1]`'s daily swing pipeline.  
*(Design note: Dropped the 60-trade Empirical Bayes win-rate estimator because ~30 trades/yr makes a 60-trade trailing window lag by 1.5–2 years across regime turns.)*

### Functions & Tasks
- [ ] **1. Decoupled `1d` Initial Stop vs. Chandelier Trail & Tier Sizing (`config/strategy.yaml`, `config/tiers.yaml`, `strategy/exit_models.py`)**
  - [ ] **Separate Initial Daily Stop from Chandelier Trail (Eliminating `max_hard_stop_pct: 4%` Clamping Collision):**
    - Initial Daily Stop: **`initial_atr_1d_multiplier: 2.2`** (`[2.0x, 2.5x] 1d ATR`), **`min_stop_pct: 0.030`**, **`max_hard_stop_pct: 0.090`** (`8.5%–10%` ceiling so a `~6%` `1d` stop at `1.5%` dollar risk sizes cleanly to `1.5% / 6% = 25%` notional—aligning with the `25%` single-stock cap without silent clamping or `$2,500` min-notional starvation).
    - Trailing Stop: **`breakeven_trigger_r: 1.5`**, **`chandelier_atr_1d_multiplier: 4.0`** (scaled within `[3.6x, 4.8x] 1d ATR` via `compute_daily_efficiency_atr_mult()`).
    - Swing Horizon Gates: Replace `time_stop_bars_15m: 52` with **`time_stop_trading_days: 14`** (`12–15` trading days if `< +0.25R`) and disable `friday_min_cushion_r: null`.
  - [ ] **Tier Profiles (`config/tiers.yaml`):**
    - Re-scope `tier_1k` explicitly as **`Smoke-Test / Plumbing Tier`** (live execution/callback verification only).
    - Configure `tier_5k`, `tier_10k` (3 slots, `<1 sh` fractional enabled on `$900+` leaders with client-side RTH stop fallback), and `tier_100k` (4–5 slots, `max_trades_per_week: 5`, `risk_per_trade_pct: 0.015` to eliminate bull-turn cash drag); replace post-split `< $50` tickers (`CMG`, `BSX`) in `config/universe_100.yaml`.
- [ ] **2. `BMO` vs. `AMC` Pre-Earnings "House Money" Governance & GTC Qty Sync (`intel/earnings_calendar.py`, `strategy/exit_models.py`, `runtime/daemon.py`)**
  - [ ] **Parse Nasdaq `time` Field (`BMO` vs. `AMC`) & Widen Entry Blackout to `+14d` (`10` trading days):**
    - `time-pre-market` (**BMO**) or unknown/unspecified -> evaluate on **`T-1` close (`15:35–15:50 ET`)**.
    - `time-after-hours` (**AMC**) -> evaluate on **`T-0` close (`15:35–15:50 ET`)** so Wednesday AMC reporters keep Wednesday's pre-earnings session.
  - [ ] **3-Tier `ExitReason.PRE_EARNINGS_CUSHION` + Server-Side GTC Quantity Sync:**
    - **`curr_r < +1.0R` (No Cushion):** Exit **100%** at pre-earnings close (cancel active `GTC STOP`).
    - **`+1.0R <= curr_r < +2.5R` (Moderate Cushion):** Scale out **50%** (enforcing whole-share `sell_qty >= 1` & min-notional guard) **AND immediately `modify_order` the Futu server-side `GTC STOP` quantity down to the remaining `50%` shares** on fill so an overnight gap never over-sells short!
    - **`curr_r >= +2.5R` (Deep Cushion):** Hold **100%** on house money; preserve **PEAD** (`T+1..T+5`) continuation re-entry.
  - [ ] **Historical Earnings Table (`data/cache/historical_earnings_2021_2026.json`):** Seed 2021–2026 quarterly earnings dates + `BMO`/`AMC` timing for the universe so walk-forward backtests exercise `PRE_EARNINGS_CUSHION` and `+14d` entry blackouts across all 6 years.
- [ ] **3. Macro Volatility Quantiles & Daily Efficiency Scaling (`src/zy_quant/strategy/adaptive.py`)**
  - [ ] `compute_adaptive_vol_thresholds()`: 252-day rolling 80th / 95th benchmark volatility quantile thresholds with safety clamps (`[18, 30]` Yellow, `[25, 42]` Red), integrated with `DailyRegimeDetector` (`SMA50 / SMA200` + 100-stock `% > SMA50` breadth).
  - [ ] `compute_daily_efficiency_atr_mult()`: 20-day **daily (`1d`)** Kaufman Efficiency Ratio (`ER`) scaling around the trailing Chandelier ATR multiplier (`[3.6x, 4.8x]`).
- [ ] **4. Survivorship-Free 2021–2026 PIT Universe & Bear-Market Walk-Forward (`config/pit_universe_2021_2026.csv`, `backtest/walk_forward.py`)**
  - [ ] Populate `config/pit_universe_2021_2026.csv` for `PointInTimeUniverseFilter` (gating post-2021 IPOs/index additions such as `CEG, GEV, ARM, PLTR` by true inclusion date while including 2021 incumbents that drew down in 2022 such as `PYPL, ZM, DOCU, NFLX`) to eliminate hindsight bias.
  - [ ] Cache `2021-01-01 -> 2026-11-25` Alpaca `1d` bars and validate across **2021 Bull**, **2022 Nasdaq `-33%` Bear Market**, and **2023–2026 AI Cycle**, reporting both pure equity return and total return inclusive of `~4.0%` Futu HK USD Cash Plus (`idle_cash_apy`) sweep.

---

## `[P3]` SQLite Data Contract for Standalone External Intel Agent Project
**Target Branch (in `zy-quant`):** `feat/intel-db-contract`  
**External Repo:** Standalone Pre/Post-Market Agent Pipeline (runs outside RTH, e.g., `08:30 ET` & `17:30 ET`)

`zy-quant` remains a deterministic, zero-LLM-latency trading engine. The external Agent project writes structured intelligence rows into the shared SQLite WAL database (`data/zy_quant_sim.db` / `zy_quant_real.db`), and `zy-quant` reads them in `O(1)` during market hours.

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

## `[P5]` Dev / Prod Split Architecture (`MacBook-Air.local`), Self-Hosted Runner & Mesh Networking
**Target Branch:** `feat/prod-deploy-pipeline`

Decouple portable strategy development & backtesting (Dev Mac, on or off home Wi-Fi) from dedicated live/paper execution, SQLite state persistence, and Web Dashboard hosting on the personal MacBook (`zyang@MacBook-Air.local`).

### Functions & Tasks
- [ ] **Dev Machine Responsibilities (Code, Unit Tests & Anywhere Backtesting)**
  - [ ] Keep background `launchd` service stopped on Dev Mac (`./deploy/stop.sh`) so it never competes with Prod for `FutuOpenD` connections or submits duplicate orders.
  - [ ] Run `.venv/bin/pytest` and `.venv/bin/zy-quant backtest` locally anywhere via primary `AlpacaMarketDataAdapter` + local `bar_cache` (zero `FutuOpenD` dependency).
  - [ ] Configure SSH public-key auth (`ssh-copy-id zyang@MacBook-Air.local`) for frictionless LAN bootstrap and diagnostics.
- [ ] **Prod Machine Responsibilities (`zyang@MacBook-Air.local` Native macOS Runtime)**
  - [ ] Add idempotent `deploy/bootstrap_prod.sh` to provision Python 3.12 `.venv`, directory layout, `config/settings.local.json` (`chmod 600`), and `launchd` pre-market `StartCalendarInterval` on `MacBook-Air.local`.
  - [ ] Host `FutuOpenD`, `zy-quant dashboard` + `TradingDaemon`, and authoritative SQLite WAL ledgers (`data/zy_quant_sim.db`, `data/zy_quant_real.db`), plus live US market-hours smoke test of `[P0]` order callbacks on Futu `SIMULATE` sub-account.
  - [ ] Configure `pmset repeat wakeorpoweron` and `deploy/start.sh` / `deploy/stop.sh` so Prod sleeps cleanly off-hours and wakes reliably before `09:20 ET`.
- [ ] **Automated Deployment via GitHub Actions Self-Hosted Runner (`.github/workflows/deploy-prod.yml`)**
  - [ ] Install and register the official GitHub Actions Self-Hosted Runner as a background `launchd` service on `zyang@MacBook-Air.local`.
  - [ ] Trigger deployment workflow on push/merge to `main` (or `prod`): run `pytest` gate -> `git fetch && git reset --hard` / `pip install -e .` (preserving gitignored `data/*.db` and `config/settings.local.json`) -> sync `deploy/com.zyquant.daemon.plist` -> hot-restart `com.zyquant.daemon` only if currently running.
- [ ] **Overlay Mesh Networking, Clash Verge Split-Routing & Secure Remote Access**
  - [ ] Install mesh networking software (**Tailscale** recommended across both Macs & mobile devices; or **Cloudflare Tunnel `--protocol http2` + Zero Trust Email OTP**).
  - [ ] Bind Prod `web.host` in `config/settings.local.json` to the private Tailscale interface IP (or `127.0.0.1` behind Tunnel).
  - [ ] **Clash Verge Split-Tunneling Rules:**
    - Route **富途牛牛 / `FutuOpenD`** strictly via **`DIRECT`** on `MacBook-Air.local`.
    - Route **`data.alpaca.markets` / `api.alpaca.markets`**, **`query1.finance.yahoo.com` / `fc.yahoo.com`**, and **`github.com` / `actions.githubusercontent.com`** via **`PROXY`**.
    - Exclude Tailscale CGNAT subnet **`100.64.0.0/10`** from Clash Verge TUN with **Bypass LAN** enabled on both Macs.
