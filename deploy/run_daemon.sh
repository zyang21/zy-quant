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
    # Triggered by launchd StartCalendarInterval (or wake coalescing):
    # Only launch during US regular open or within 45m before open (handles DST and skips NYSE holidays/weekends).
    SHOULD_START="$(
        "${ROOT_DIR}/.venv/bin/python" - <<'PY'
from zy_quant.intel.market_hours import get_us_market_session

s = get_us_market_session()
if s.is_open or (s.phase == "PRE_MARKET" and s.minutes_until_open <= 45):
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pre-market/open check passed (${ET_NOW}, phase=${PHASE}); starting scheduled session..."
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
sess = get_us_market_session(now_utc)
if sess.is_open or sess.phase == "PRE_MARKET":
    print("KEEP_RUNNING")
    sys.exit(0)

# Require at least 5 minutes past regular market close so closing auction fills settle cleanly
now_et = now_utc.astimezone(ET_TZ)
close_t = session_close_time(now_et.date())
grace_close_mins = close_t.hour * 60 + close_t.minute + 5
now_mins = now_et.hour * 60 + now_et.minute
if sess.phase == "AFTER_HOURS" and now_mins < grace_close_mins:
    print("KEEP_RUNNING")
    sys.exit(0)

settings = AppSettings.load(root)
store = StateStore(settings.resolve_db_path())
active_orders = store.get_active_orders()
if active_orders:
    print(f"WAIT_ORDERS|{len(active_orders)}")
else:
    print(f"EXIT_IDLE|{sess.phase}|{sess.et_now_str}")
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
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scheduled session complete (${CHECK_OUT}); stopping daemon to release caffeinate sleep lock."
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
