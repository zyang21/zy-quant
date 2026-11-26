# zy-quant Active Roadmap & Engineering TODO

Active engineering backlog for `zy-quant`. Completed milestones (`[P0]`, `[P4]`) are archived in [`HISTORY.md`](HISTORY.md).

---

## Priority Legend

- **`[P1]` Data Decoupling & Universe Scaling (In Progress)** — Removes Futu API rate-limit bottlenecks (`60 req / 30s`), expands to a 100-stock multi-sector pool, and unlocks multi-year walk-forward backtesting.
- **`[P2]` Statistical Adaptivity** — Replaces static hardcoded strategy assumptions with self-calibrating mathematical estimators validated out-of-sample.
- **`[P3]` Cross-Project SQLite Intel Contract** — Read-only schema consumption for the standalone Pre/Post-Market LLM Intel Agent project.
- **`[P5]` Dev / Prod Split Architecture, Self-Hosted CI/CD & Mesh Networking** — Separates local development/backtesting from native 24/5 production execution on `zyang@MacBook-Air.local` with automated GitHub Actions deployment and private remote access.

---

## `[P1]` Alpaca Market Data Integration & 100-Stock Dynamic Universe
**Target Branch:** `feat/alpaca-universe`

Decouple **wide market scanning & historical research** (Alpaca Market Data API + local SQLite WAL bar cache) from **live order execution & L1 spread/stop management** (Futu OpenAPI).

### Functions & Tasks
- [x] **Alpaca Market Data Adapter (`src/zy_quant/gateway/alpaca_adapter.py`)**
  - [x] Implement `AlpacaMarketDataAdapter(MarketDataAdapter)` using free IEX real-time snapshots & 2016–present split-adjusted historical `15m` (strictly RTH `09:30–16:00 ET`) / `1d` bars via lightweight `httpx` REST client.
  - [x] Support batch multi-symbol snapshot & bar requests (`100` symbols per HTTP call) to scan the full 100-stock pool in `< 1.5s` with zero Futu quota usage.
  - [x] Add credentials (`alpaca.api_key`, `alpaca.secret_key`) strictly to gitignored `config/settings.local.json` with hard leak-prevention guard in `AppSettings.load()`.
- [x] **Local Historical SQLite WAL Bar Cache (`src/zy_quant/storage/bar_cache.py`)**
  - [x] Cache downloaded historical `15m` & `1d` bars locally in `data/cache/market_bars.db` so multi-year backtests (`2020–2026`) run offline in seconds.
  - [x] Remove the 58-day Yahoo truncation ceiling in `BacktestEngine.run()` (`src/zy_quant/backtest/engine.py`).
- [x] **100-Stock Sector-Balanced Core Pool (`config/universe_100.yaml` + `src/zy_quant/intel/universe_manager.py`)**
  - [x] Expand static 14-stock tech-heavy list to **100 liquid institutional leaders + 12 core sector/index ETFs** (`SPY, QQQ, XLK, SMH, XLF, XLV, XLE, XLI, XLY, XLP, XLU, GLD`).
  - [x] Enforce **GICS Sector Cap** during active Top-8 selection (`max_per_sector_in_top_n: 3`) to prevent 100% semiconductor/AI crowding.
  - [x] Build quarterly/monthly universe maintenance CLI (`zy-quant refresh-universe` & `zy-quant test-alpaca`) using objective filters (`ADV_60d >= $500M`, `Price >= $50`) + Point-in-Time historical constituent CSV support for survivorship-bias-free backtests.
- [x] **Earnings Blackout Calendar Feed (`src/zy_quant/intel/earnings_calendar.py`)**
  - [x] Wire `next_earnings_date` into `TradingDaemon.refresh_watchlist()` (`src/zy_quant/runtime/daemon.py`) to veto new entries within `+-2` trading days of earnings.
- [x] **Live API Key Activation (`config/settings.local.json`) & Verification**
  - [x] Paste Alpaca free Market Data API keys into `config/settings.local.json` and verified `.venv/bin/zy-quant test-alpaca --full` (`ok: true`, `110` snapshots in `2.78s`, live earnings blackout active).
- [ ] **Carryover Live Market Smoke Check**
  - [ ] Live smoke test of `[P0]` order callbacks against Futu `SIMULATE` sub-account during US market hours.

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
  - [ ] Run `.venv/bin/pytest` and `.venv/bin/zy-quant backtest` locally anywhere (uses `yfinance` fallback prior to `[P1]`, and switches to primary `AlpacaMarketDataAdapter` + local `bar_cache` once `[P1]` lands—zero `FutuOpenD` dependency).
  - [ ] Configure SSH public-key auth (`ssh-copy-id zyang@MacBook-Air.local`) for frictionless LAN bootstrap and diagnostics.
- [ ] **Prod Machine Responsibilities (`zyang@MacBook-Air.local` Native macOS Runtime)**
  - [ ] Add idempotent `deploy/bootstrap_prod.sh` to provision Python 3.12 `.venv`, directory layout, `config/settings.local.json` (`chmod 600`), and `launchd` pre-market `StartCalendarInterval` on `MacBook-Air.local` (native macOS runtime rather than Docker so `FutuOpenD`, `launchd`, `caffeinate -is`, and `pmset` work natively).
  - [ ] Host `FutuOpenD`, `zy-quant dashboard` + `TradingDaemon`, and authoritative SQLite WAL ledgers (`data/zy_quant_sim.db`, `data/zy_quant_real.db`), plus on-host backtesting.
  - [ ] Configure `pmset repeat wakeorpoweron` and `deploy/start.sh` / `deploy/stop.sh` so Prod sleeps cleanly off-hours and wakes reliably before `09:20 ET`.
- [ ] **Automated Deployment via GitHub Actions Self-Hosted Runner (`.github/workflows/deploy-prod.yml`)**
  - [ ] Install and register the official GitHub Actions Self-Hosted Runner as a background `launchd` service on `zyang@MacBook-Air.local` (pure outbound HTTPS—works regardless of whether Dev Mac is on corporate VPN or outside the home LAN).
  - [ ] Trigger deployment workflow on push/merge to `main` (or `prod`): run `pytest` gate -> `git fetch && git reset --hard` / `pip install -e .` (preserving gitignored `data/*.db` and `config/settings.local.json`) -> sync `deploy/com.zyquant.daemon.plist` -> hot-restart `com.zyquant.daemon` only if currently running (leaving it idle with `StartCalendarInterval` armed when stopped off-hours).
- [ ] **Overlay Mesh Networking, Clash Verge Split-Routing & Secure Remote Access**
  - [ ] Install mesh networking software (**Tailscale** recommended across both Macs & mobile devices; or **Cloudflare Tunnel `--protocol http2` + Zero Trust Email OTP**).
  - [ ] Bind Prod `web.host` in `config/settings.local.json` to the private Tailscale interface IP (or `127.0.0.1` behind Tunnel) so port `8088` (which exposes `Halt` & manual scan controls) is reachable securely from outside without ever exposing unauthenticated ports to the public internet.
  - [ ] **Clash Verge Split-Tunneling Rules (Prevent Proxy Node Failover Disconnects on Futu):**
    - Route **富途牛牛 / `FutuOpenD`** strictly via **`DIRECT`** on `MacBook-Air.local` (Futu connects natively to HK servers with low latency; keeping it off Clash prevents proxy speed-test/node switches from dropping live order push callbacks or server-side stop management).
    - Route **`data.alpaca.markets` / `api.alpaca.markets`**, **`query1.finance.yahoo.com` / `fc.yahoo.com`**, and **`github.com` / `actions.githubusercontent.com`** (Self-Hosted Runner) via **`PROXY`**.
    - Exclude Tailscale CGNAT subnet **`100.64.0.0/10`** from Clash Verge TUN (`inet4-route-exclude-address` + `IP-CIDR,100.64.0.0/10,DIRECT,no-resolve` and `PROCESS-NAME,Tailscale/tailscaled,DIRECT`) with **Bypass LAN** enabled on both Macs.
