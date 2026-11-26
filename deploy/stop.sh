#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

LABEL="com.zyquant.daemon"
UID_NUM="$(id -u)"
DOMAIN="gui/${UID_NUM}"
PLIST_SRC="${SCRIPT_DIR}/com.zyquant.daemon.plist"
PLIST_DST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
MANUAL_FLAG="${ROOT_DIR}/data/.manual_start"

rm -f "${MANUAL_FLAG}"

wait_until_unloaded() {
    for _ in {1..25}; do
        if ! launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
}

bootstrap_plist() {
    launchctl enable "${DOMAIN}/${LABEL}" 2>/dev/null || true
    for attempt in 1 2 3 4 5; do
        if launchctl bootstrap "${DOMAIN}" "${PLIST_DST}" 2>/dev/null; then
            return 0
        fi
        if launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.4
    done
    launchctl bootstrap "${DOMAIN}" "${PLIST_DST}"
}

echo "==> [1/3] Stopping running instance while keeping launchd schedule armed (${LABEL})..."
mkdir -p "${HOME}/Library/LaunchAgents"
if [[ ! -f "${PLIST_DST}" ]] || ! cmp -s "${PLIST_SRC}" "${PLIST_DST}"; then
    echo "    Reloading updated launchd plist (RunAtLoad=false)..."
    launchctl bootout "${DOMAIN}/${LABEL}" 2>/dev/null || true
    launchctl bootout "${DOMAIN}" "${PLIST_DST}" 2>/dev/null || true
    wait_until_unloaded
    cp "${PLIST_SRC}" "${PLIST_DST}"
    bootstrap_plist
else
    if launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
        launchctl kill SIGTERM "${DOMAIN}/${LABEL}" 2>/dev/null || true
    else
        bootstrap_plist
    fi
fi

echo "==> [2/3] Cleaning up residual zy-quant and caffeinate processes..."
pkill -TERM -f "deploy/run_daemon.sh" 2>/dev/null || true
pkill -TERM -f "caffeinate.*zy-quant" 2>/dev/null || true
pkill -TERM -f "zy-quant (dashboard|daemon)" 2>/dev/null || true

for _ in {1..10}; do
    if ! pgrep -f "zy-quant (dashboard|daemon)" >/dev/null 2>&1 && \
       ! pgrep -f "caffeinate.*zy-quant" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

if pgrep -f "zy-quant (dashboard|daemon)" >/dev/null 2>&1 || \
   pgrep -f "caffeinate.*zy-quant" >/dev/null 2>&1; then
    echo "    Timeout reached; force-killing residual processes..."
    pkill -KILL -f "deploy/run_daemon.sh" 2>/dev/null || true
    pkill -KILL -f "caffeinate.*zy-quant" 2>/dev/null || true
    pkill -KILL -f "zy-quant (dashboard|daemon)" 2>/dev/null || true
fi

echo "==> [3/3] Checking macOS SleepDisabled setting..."
if pmset -g | grep -qE 'SleepDisabled[[:space:]]+1'; then
    echo "    SleepDisabled=1 detected; attempting non-interactive 'sudo -n pmset -a disablesleep 0'..."
    if sudo -n pmset -a disablesleep 0 2>/dev/null; then
        echo "    SleepDisabled restored to 0."
    else
        echo "    WARN: Password required to reset SleepDisabled. Run manually: sudo pmset -a disablesleep 0"
    fi
else
    echo "    SleepDisabled is already 0."
fi

SLEEP_DISABLED_NOW="$(pmset -g | awk '/SleepDisabled/{print $2}')"
SCHEDULE_STATE="NOT_LOADED"
if launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    SCHEDULE_STATE="ARMED (Mon-Fri 09:20 ET pre-market, skips NYSE holidays)"
fi

echo ""
echo "================ Service Status ================"
echo "  Daemon State      : STOPPED"
echo "  Launchd Schedule  : ${SCHEDULE_STATE}"
echo "  zy-quant Procs    : $(pgrep -fc 'zy-quant (dashboard|daemon)' || true)"
echo "  caffeinate Procs  : $(pgrep -fc 'caffeinate.*zy-quant' || true)"
echo "  SleepDisabled     : ${SLEEP_DISABLED_NOW:-0}"
echo "================================================"
