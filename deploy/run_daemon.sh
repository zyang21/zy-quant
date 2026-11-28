#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANUAL_FLAG="${ROOT_DIR}/data/.manual_start"

mkdir -p "${ROOT_DIR}/data"

LAUNCH_MODE="SCHEDULED"
if [[ -f "${MANUAL_FLAG}" ]]; then
    LAUNCH_MODE="MANUAL"
    rm -f "${MANUAL_FLAG}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Manual start triggered for zy-quant service (persistent Web UI mode)..."
else
    # Triggered by launchd StartCalendarInterval (Two-Window Wake: 09:20 ET Open Guard & 15:25 ET EOD Window):
    SHOULD_START="$(
        "${ROOT_DIR}/.venv/bin/python" - "${ROOT_DIR}" <<'PY'
import sys
from datetime import datetime, time, timezone
from pathlib import Path

from zy_quant.config import AppSettings
from zy_quant.intel.market_hours import ET_TZ, get_us_market_session, is_trading_day, session_close_time
from zy_quant.storage.state_store import StateStore

root = Path(sys.argv[1])
now_utc = datetime.now(timezone.utc)
now_et = now_utc.astimezone(ET_TZ)
s = get_us_market_session(now_utc)
d = now_et.date()

if not is_trading_day(d):
    print(f"NO|{s.phase}|{s.et_now_str}|{s.minutes_until_open}")
    sys.exit(0)

settings = AppSettings.load(root)
hm = now_et.hour * 60 + now_et.minute
close_t = session_close_time(d)
close_m = close_t.hour * 60 + close_t.minute

if settings.strategy_cfg.mode != "swing_1d":
    if s.is_open or (s.phase == "PRE_MARKET" and s.minutes_until_open <= 45) or (close_m <= hm <= close_m + 15):
        print(f"YES|{s.phase}|{s.et_now_str}|{s.minutes_until_open}")
    else:
        print(f"NO|{s.phase}|{s.et_now_str}|{s.minutes_until_open}")
    sys.exit(0)

# swing_1d Two-Window Filter (prevents false wakes at 10:20 EDT summer / 14:25 EST winter & 08:20 EST / 16:25 EDT):
store = StateStore(settings.resolve_db_path())
today_iso = d.isoformat()
is_early_close_day = close_t != time(16, 0)
in_open_window = (9 * 60 + 10) <= hm <= (9 * 60 + 50)
in_eod_window = (close_m - 40) <= hm <= (close_m + 20)

# Catch-up safety: if Mac lid opened late and missed OPEN_GAP_GUARD or has <1sh fractional positions, allow start
open_guard_done = store.has_completed_pipeline_phase(today_iso, "OPEN_GAP_GUARD")
settlement_done = store.has_completed_pipeline_phase(today_iso, "POST_CLOSE_SETTLEMENT")
positions = store.get_positions()
needs_rth_watch = is_early_close_day or any(
    p.shares < 1.0
    or not p.broker_stop_order_id
    or str(p.broker_stop_order_id).startswith(("SIM-", "PAPER-"))
    for p in positions.values()
)
missed_open_guard = s.is_open and not open_guard_done
missed_settlement = (close_m < hm <= close_m + 35) and not settlement_done

if in_open_window or in_eod_window or (s.is_open and needs_rth_watch) or missed_open_guard or missed_settlement:
    print(f"YES|{s.phase}|{s.et_now_str}|{s.minutes_until_open}")
else:
    print(f"NO|{s.phase}|{s.et_now_str}|{s.minutes_until_open}")
PY
    )"
    IFS='|' read -r DECISION PHASE ET_NOW MINS_UNTIL <<< "${SHOULD_START}"
    if [[ "${DECISION}" != "YES" ]]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scheduled check skipped: phase=${PHASE}, et=${ET_NOW}, mins_until_open=${MINS_UNTIL}"
        exit 0
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scheduled window active (${ET_NOW}, phase=${PHASE}); starting session..."
fi

STOP_REQUESTED=0
CHILD_PID=""

cleanup() {
    STOP_REQUESTED=1
    if [[ -n "${CHILD_PID}" ]] && kill -0 "${CHILD_PID}" 2>/dev/null; then
        kill -TERM "${CHILD_PID}" 2>/dev/null || true
        wait "${CHILD_PID}" 2>/dev/null || true
    fi
    exit 0
}

trap cleanup TERM INT HUP

should_exit_scheduled_session() {
    "${ROOT_DIR}/.venv/bin/python" - "${ROOT_DIR}" <<'PY'
import sys
from datetime import datetime, time, timezone
from pathlib import Path

from zy_quant.config import AppSettings
from zy_quant.intel.market_hours import ET_TZ, get_us_market_session, is_trading_day, session_close_time
from zy_quant.storage.state_store import StateStore

root = Path(sys.argv[1])
now_utc = datetime.now(timezone.utc)
now_et = now_utc.astimezone(ET_TZ)
today_d = now_et.date()
today_iso = today_d.isoformat()
sess = get_us_market_session(now_utc)
settings = AppSettings.load(root)
store = StateStore(settings.resolve_db_path())

active_orders = store.get_active_orders()
if active_orders:
    print(f"WAIT_ORDERS|{len(active_orders)}")
    sys.exit(0)

if not is_trading_day(today_d):
    print(f"EXIT_IDLE_NON_TRADING_DAY|{sess.et_now_str}")
    sys.exit(0)

close_t = session_close_time(today_d)
close_m = close_t.hour * 60 + close_t.minute
now_m = now_et.hour * 60 + now_et.minute
is_early_close_day = close_t != time(16, 0)

# 0. Overnight / early pre-market (< 09:10 ET): never hold caffeinate overnight across midnight rollover!
if now_m < (9 * 60 + 10):
    print(f"EXIT_IDLE_OVERNIGHT|{sess.et_now_str}")
    sys.exit(0)

# 1. Post-close exit: once >= 5m past close (and POST_CLOSE_SETTLEMENT done, or hard +35m timeout reached), sleep!
if not sess.is_open and sess.phase != "PRE_MARKET":
    if now_m >= close_m + 5:
        if (
            settings.strategy_cfg.mode != "swing_1d"
            or store.has_completed_pipeline_phase(today_iso, "POST_CLOSE_SETTLEMENT")
            or now_m >= close_m + 35
        ):
            print(f"EXIT_IDLE_POST_CLOSE|{sess.et_now_str}")
            sys.exit(0)
    print("KEEP_RUNNING")
    sys.exit(0)

# 2. Midday sleep optimization (09:50 -> 15:20 ET) in `swing_1d` mode:
#    Only sleep midday when NOT an early-close half-day, OPEN_GAP_GUARD is completed,
#    and 100% of open positions are whole-share (>= 1.0 sh) protected by server-side GTC stops!
if (
    settings.strategy_cfg.mode == "swing_1d"
    and not is_early_close_day
    and (9 * 60 + 50) <= now_m < (close_m - 40)
    and store.has_completed_pipeline_phase(today_iso, "OPEN_GAP_GUARD")
):
    positions = store.get_positions()
    all_gtc_protected = all(
        p.shares >= 1.0
        and bool(p.broker_stop_order_id)
        and not str(p.broker_stop_order_id).startswith(("SIM-", "PAPER-"))
        for p in positions.values()
    )
    if all_gtc_protected:
        print(f"EXIT_IDLE_MIDDAY_GTC_ARMED|{sess.et_now_str}|positions={len(positions)}")
        sys.exit(0)

print("KEEP_RUNNING")
PY
}

# Keep system and network awake with caffeinate -is (allows display sleep) and auto-restart on unexpected crash.
while [[ "${STOP_REQUESTED}" -eq 0 ]]; do
    if [[ "${LAUNCH_MODE}" == "MANUAL" ]]; then
        /usr/bin/caffeinate -is "${ROOT_DIR}/.venv/bin/zy-quant" dashboard &
    else
        /usr/bin/caffeinate -is "${ROOT_DIR}/.venv/bin/zy-quant" dashboard --auto-daemon &
    fi
    CHILD_PID=$!

    if [[ "${LAUNCH_MODE}" == "MANUAL" ]]; then
        set +e
        wait "${CHILD_PID}"
        EXIT_CODE=$?
        set -e
    else
        EXIT_CODE=0
        while kill -0 "${CHILD_PID}" 2>/dev/null; do
            if [[ "${STOP_REQUESTED}" -eq 1 ]]; then
                break
            fi
            sleep 30
            if ! kill -0 "${CHILD_PID}" 2>/dev/null; then
                set +e
                wait "${CHILD_PID}"
                EXIT_CODE=$?
                set -e
                break
            fi
            CHECK_OUT="$(should_exit_scheduled_session || echo "KEEP_RUNNING")"
            if [[ "${CHECK_OUT}" == EXIT_IDLE* ]]; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scheduled window complete (${CHECK_OUT}); stopping daemon to release caffeinate sleep lock."
                STOP_REQUESTED=1
                kill -TERM "${CHILD_PID}" 2>/dev/null || true
                wait "${CHILD_PID}" 2>/dev/null || true
                exit 0
            fi
        done
    fi

    if [[ "${STOP_REQUESTED}" -eq 1 ]]; then
        break
    fi

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] zy-quant exited unexpectedly (code=${EXIT_CODE}); restarting in 5s..." >&2
    sleep 5
done
