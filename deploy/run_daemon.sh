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
        "${ROOT_DIR}/.venv/bin/python" - <<'PY'
from datetime import datetime, timezone
from zy_quant.intel.market_hours import ET_TZ, get_us_market_session, is_trading_day, session_close_time

now_utc = datetime.now(timezone.utc)
now_et = now_utc.astimezone(ET_TZ)
s = get_us_market_session(now_utc)
d = now_et.date()

if not is_trading_day(d):
    print(f"NO|{s.phase}|{s.et_now_str}|{s.minutes_until_open}")
else:
    hm = now_et.hour * 60 + now_et.minute
    close_t = session_close_time(d)
    close_m = close_t.hour * 60 + close_t.minute
    if s.is_open or (s.phase == "PRE_MARKET" and s.minutes_until_open <= 45) or (close_m <= hm <= close_m + 15):
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
from zy_quant.intel.market_hours import ET_TZ, get_us_market_session, session_close_time
from zy_quant.storage.state_store import StateStore

root = Path(sys.argv[1])
now_utc = datetime.now(timezone.utc)
now_et = now_utc.astimezone(ET_TZ)
today_iso = now_et.date().isoformat()
sess = get_us_market_session(now_utc)
settings = AppSettings.load(root)
store = StateStore(settings.resolve_db_path())

active_orders = store.get_active_orders()
if active_orders:
    print(f"WAIT_ORDERS|{len(active_orders)}")
    sys.exit(0)

close_t = session_close_time(now_et.date())
close_m = close_t.hour * 60 + close_t.minute
now_m = now_et.hour * 60 + now_et.minute
is_early_close_day = close_t != time(16, 0)

# 1. Post-close exit: once >= 5m past close (and POST_CLOSE_SETTLEMENT done in swing_1d mode), sleep overnight!
if not sess.is_open and sess.phase != "PRE_MARKET":
    if now_m >= close_m + 5:
        if settings.strategy_cfg.mode != "swing_1d" or store.has_completed_pipeline_phase(today_iso, "POST_CLOSE_SETTLEMENT"):
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
        p.shares >= 1.0 and bool(p.broker_stop_order_id) for p in positions.values()
    )
    if all_gtc_protected:
        print(f"EXIT_IDLE_MIDDAY_GTC_ARMED|{sess.et_now_str}|positions={len(positions)}")
        sys.exit(0)

print("KEEP_RUNNING")
PY
}

# Keep system and network awake with caffeinate -is (allows display sleep) and auto-restart on unexpected crash.
while [[ "${STOP_REQUESTED}" -eq 0 ]]; do
    /usr/bin/caffeinate -is "${ROOT_DIR}/.venv/bin/zy-quant" dashboard &
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
