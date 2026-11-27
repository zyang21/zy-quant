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

SUPERKILL=0
case "${1:-}" in
    --superkill|—superkill)
        SUPERKILL=1
        ;;
    "")
        ;;
    *)
        echo "Usage: $0 [--superkill]" >&2
        exit 1
        ;;
esac

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
    for _ in 1 2 3 4 5; do
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

run_sudo_cmd() {
    local desc="$1"
    shift
    if sudo -n "$@" 2>/dev/null; then
        echo "    ${desc}: OK."
    else
        echo "    WARN: Password required for '${desc}'. Run manually: sudo $*"
    fi
}

if [[ "${SUPERKILL}" -eq 1 ]]; then
    echo "==> [1/4] SUPERKILL: Disarming and removing launchd agent (${LABEL})..."
    launchctl bootout "${DOMAIN}/${LABEL}" 2>/dev/null || true
    launchctl bootout "${DOMAIN}" "${PLIST_DST}" 2>/dev/null || true
    wait_until_unloaded
    launchctl disable "${DOMAIN}/${LABEL}" 2>/dev/null || true
    rm -f "${PLIST_DST}"
    echo "    Removed ${PLIST_DST} and disabled ${DOMAIN}/${LABEL}."
else
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
fi

STEP_TOTAL=$([[ "${SUPERKILL}" -eq 1 ]] && echo "4" || echo "3")
echo "==> [2/${STEP_TOTAL}] Cleaning up residual zy-quant and caffeinate processes..."
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

echo "==> [3/${STEP_TOTAL}] Checking macOS SleepDisabled setting..."
if pmset -g | grep -qE 'SleepDisabled[[:space:]]+1'; then
    run_sudo_cmd "Restore macOS sleep (pmset -a disablesleep 0)" pmset -a disablesleep 0
else
    echo "    SleepDisabled is already 0."
fi

if [[ "${SUPERKILL}" -eq 1 ]]; then
    echo "==> [4/4] Checking hardware repeating wake schedule (pmset)..."
    if pmset -g sched 2>/dev/null | grep -qiE 'wakeorpoweron|wake'; then
        run_sudo_cmd "Clear hardware wake schedule (pmset repeat cancel)" pmset repeat cancel
    else
        echo "    No repeating hardware wake schedule active."
    fi
fi

SLEEP_DISABLED_NOW="$(pmset -g | awk '/SleepDisabled/{print $2}')"
SCHEDULE_STATE="UNINSTALLED (disarmed)"
if launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    SCHEDULE_STATE="ARMED (Mon-Fri 09:20 ET pre-market, skips NYSE holidays)"
elif [[ -f "${PLIST_DST}" ]]; then
    SCHEDULE_STATE="NOT_LOADED"
fi

echo ""
echo "================ Service Status ================"
echo "  Daemon State      : $([[ "${SUPERKILL}" -eq 1 ]] && echo "SUPERKILLED (completely removed)" || echo "STOPPED")"
echo "  Launchd Schedule  : ${SCHEDULE_STATE}"
echo "  zy-quant Procs    : $(pgrep -fc 'zy-quant (dashboard|daemon)' || true)"
echo "  caffeinate Procs  : $(pgrep -fc 'caffeinate.*zy-quant' || true)"
echo "  SleepDisabled     : ${SLEEP_DISABLED_NOW:-0}"
echo "================================================"
