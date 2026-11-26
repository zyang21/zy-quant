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
