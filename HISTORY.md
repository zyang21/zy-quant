# zy-quant Engineering Delivery History

Archive of completed roadmap milestones, architecture upgrades, and backtest verification benchmarks moved out of `TODO.md`.

---

## 2026-11-25 — `[P0]` Real Order Execution Lifecycle & Callback State Machine
- **Branch / Commits:** `feat/futu-order-callbacks` (`2098567`, `cecc124`, merged in `412d816`)
- **Delivered Scope:**
  - Extended `OrderStatus` (`SUBMITTED`, `PARTIAL_FILLED`, `FILLED`, `CANCELLED`, `EXPIRED`, `FAILED`) and added `BrokerOrderUpdate` model (`src/zy_quant/models.py`).
  - Added idempotent SQLite schema migration on `orders` (`filled_shares`, `reconciled_shares`, `avg_fill_price`, `plan_json`) in `StateStore._init_db()` (`src/zy_quant/storage/state_store.py`).
  - Registered `ZyTradeOrderHandler(ft.TradeOrderHandlerBase)` real-time push callbacks on `OpenSecTradeContext` (`src/zy_quant/gateway/futu_adapter.py`), paired with `sync_open_orders()` polling fallback and `query_order_fee()` exact `REAL` fee lookup (auto-downgrading to `FutuUSFeeModel` on `'Unknown protocol ID'` or unsettled `$0` fees).
  - Updated `MandateRiskGate` and `TradingDaemon` for thread-safe incremental delta settlement (`delta_shares = filled_shares - reconciled_shares`), reconstructing `OpenPosition` from `orders.plan_json` only upon `dealt_qty > 0`, locking slots/cash/sector weights on in-flight `SUBMITTED / PARTIAL_FILLED` buy orders, enforcing `entry_order_ttl_seconds` (`90s`) / `exit_order_ttl_seconds` (`60s`) auto-cancellation, detecting server-side stop fills (`broker_stop_order_id`), and preserving `PAPER-LOCAL` immediate virtual execution when `acc_id == 0`.
  - Fixed `StateStore.upsert_position()` `ON CONFLICT` update clause to sync `initial_shares`, `entry_price`, and `risk_per_share` across partial-fill merges.
  - Added unit test suite `tests/test_order_lifecycle.py` (8 lifecycle scenarios passing).

---

## 2026-11-25 — `[P4]` Futu HK `T+0` Exit Governance, Progressive Chandelier & `tier_10k` Scale-Out
- **Branch / Commits:** `feat/futu-hk-t0-and-exit-tuning` (`d5a3c47`, merged in `9dc462b`)
- **Delivered Scope:**
  - Replaced non-applicable US FINRA `$25k` PDT assumption with **Futu HK SFC-regulated infinite `T+0` rules** governed by fee-to-notional economics.
  - Tightened `chandelier_atr_multiplier` (`4.2 -> 2.8`), armed breakeven+fees stop at `+1.5R`, and added progressive profit-floor lock (`profit_lock_trigger_r: 1.8`, `profit_lock_floor_ratio: 0.55`) to retain `>= 55%` of peak open excursion (`config/strategy.yaml`, `src/zy_quant/strategy/exit_models.py`).
  - Unlocked 50% partial profit scale-out at `+1.8R` in `tier_10k` (`min_scale_out_notional_usd: 1500.0` fee guard).
  - Added `ExitReason.EOD_T0_FLATTEN` defensive closing-window shield (`session_bar_num >= 25`, `15:30–16:00 ET`): cuts genuinely failing day-0 setups (`< -0.65R`) and losing Friday positions (`< -0.35R`) `T+0` before overnight/weekend gaps without churning flat consolidations.
- **Controlled 55-Day Futu Real-Data A/B Benchmark (`2026-08-25 -> 2026-11-24`, QQQ `-11.1%`):**
  - `tier_1k`: Net `-4.45%` (`-$44.48`) -> **`-0.79%` (`-$7.88`)**, Gross `-$1.78` -> **`+$35.00`**, PF `0.60 -> 0.93`, Max DD `6.73% -> 5.58%`.
  - `tier_10k`: Net `-0.71%` (`-$71.21`) -> **`+1.78%` (`+$178.20`)**, Gross `+$63.22` -> **`+$342.74`**, Win Rate `34.6% -> 52.8%`, PF `0.91 -> 1.19`, Max DD `6.30% -> 4.65%`.
  - `tier_100k`: Net `-1.43%` (`-$1,429.96`) -> **`+0.66%` (`+$661.00`)**, Gross `-$875.06` -> **`+$1,294.77`**, Win Rate `47.0% -> 51.9%`, PF `0.83 -> 1.08`, Max DD `4.33% -> 3.76%`.

---

## 2026-11-26 — `[P1]` Alpaca Market Data v2, 100-Stock Sector Universe, SQLite BarCache & Zero-Lookahead Fixes
- **Branch / Commits:** `feat/alpaca-universe` (`84d3a39`, merged in `bfd8a2e`)
- **Delivered Scope:**
  - Implemented `AlpacaMarketDataAdapter` (`src/zy_quant/gateway/alpaca_adapter.py`) with 100-symbol batch `/v2/stocks/snapshots` (`2.78s` full-pool scan, zero Futu quota), paginated split-adjusted `15m` (strictly US RTH + early-close filtered) / `1d` bars, historical `sip -> iex` fallback, and surgical head/tail delta fetching.
  - Added `HistoricalBarCache` (`src/zy_quant/storage/bar_cache.py`, `data/cache/market_bars.db`), caching `1,170,630` real bars across `112` symbols (`55D` 100-stock backtest in `0.7s`, `1.5Y` backtest in `5.3s`) and removing the 58-day ceiling in `BacktestEngine` & Web UI.
  - Expanded universe to **100 cross-sector institutional equities + 12 sector/broad ETFs** (`config/universe_100.yaml`), enforced GICS Sector Cap (`max_per_sector_in_top_n: 3`), added `PointInTimeUniverseFilter` + 85/120 buffer rebalancer (`universe_manager.py`), and wired Nasdaq `EarningsCalendarFeed` (`America/New_York` date-anchored).
  - Hardened `deploy/run_daemon.sh`, `deploy/start.sh`, `deploy/stop.sh`, and `zy-quant service` CLI (`.manual_start` flag sync, scheduled post-market auto-exit releasing `caffeinate -is`, non-blocking `sudo -n`).
  - Eliminated `[CRITICAL]` same-day `1d` close look-ahead leak via `bisect_left` across `BacktestEngine`, `IntradaySignalEngine`, and `MarketRegimeDetector` (locked by poison-bar regression test; 33/33 tests passing).
- **Key Quantitative Finding (1.17M Real Bars, Zero Look-Ahead, `2025-05 -> 2026-11`):**
  - High-frequency 15m trading (`~300 trades/yr`) suffered heavy noise & friction drag on `$5k / $10k` even under simulated IBKR `$0.35` fees (`-7.4%` / `-13.8%`), whereas **Low-Frequency Multi-Week Swing on Futu HK** (`~38 trades / 1.5Y`) delivered **`+9.87%` net on `$5k` (`PF 1.23`, DD `12.9%`), `+10.29%` net on `$10k` (`PF 1.20`, DD `11.5%`), and `+24.21%` net on unlocked 4-slot `$100k` (`PF 1.17`)**.
