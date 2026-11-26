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

ENABLE_CLAMSHELL=0
if [[ "${1:-}" == "--clamshell" ]]; then
    ENABLE_CLAMSHELL=1
fi

if [[ ! -f "${PLIST_SRC}" ]]; then
    echo "ERROR: Missing plist at ${PLIST_SRC}" >&2
    exit 1
fi

if [[ ! -x "${ROOT_DIR}/.venv/bin/zy-quant" ]]; then
    echo "ERROR: Missing executable at ${ROOT_DIR}/.venv/bin/zy-quant" >&2
    exit 1
fi

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

read -r FUTU_HOST FUTU_PORT WEB_HOST WEB_PORT < <(
    "${ROOT_DIR}/.venv/bin/python" - "${ROOT_DIR}/config/settings.json" <<'PY'
import json, sys
from pathlib import Path

cfg_path = Path(sys.argv[1])
cfg = json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}
futu = cfg.get("futu", {})
web = cfg.get("web", {})
print(
    futu.get("host", "127.0.0.1"),
    futu.get("port", 11111),
    web.get("host", "127.0.0.1"),
    web.get("port", 8088),
)
PY
)

echo "==> [1/4] Checking FutuOpenD gateway (${FUTU_HOST}:${FUTU_PORT})..."
if nc -z -G 1 "${FUTU_HOST}" "${FUTU_PORT}" >/dev/null 2>&1; then
    echo "    FutuOpenD port is reachable (${FUTU_HOST}:${FUTU_PORT})."
else
    echo "    NOTE: FutuOpenD not reachable at ${FUTU_HOST}:${FUTU_PORT}; service will auto-reconnect or use fallback."
fi

echo "==> [2/4] Syncing launchd plist and starting service (${LABEL})..."
mkdir -p "${HOME}/Library/LaunchAgents" "${ROOT_DIR}/data"
touch "${MANUAL_FLAG}"

if [[ ! -f "${PLIST_DST}" ]] || ! cmp -s "${PLIST_SRC}" "${PLIST_DST}"; then
    launchctl bootout "${DOMAIN}/${LABEL}" 2>/dev/null || true
    launchctl bootout "${DOMAIN}" "${PLIST_DST}" 2>/dev/null || true
    wait_until_unloaded
    cp "${PLIST_SRC}" "${PLIST_DST}"
    bootstrap_plist
elif ! launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    bootstrap_plist
fi

launchctl kickstart -k "${DOMAIN}/${LABEL}"

echo "==> [3/4] Checking sleep prevention settings..."
if [[ "${ENABLE_CLAMSHELL}" -eq 1 ]]; then
    echo "    --clamshell enabled; running 'sudo pmset -a disablesleep 1'..."
    sudo pmset -a disablesleep 1
else
    echo "    Active session protected via 'caffeinate -is' (display sleep allowed; pass --clamshell for closed-lid mode)."
fi

echo "==> [4/4] Waiting for Web Terminal readiness (http://${WEB_HOST}:${WEB_PORT})..."
READY=0
for _ in {1..20}; do
    if curl -fsS -o /dev/null "http://${WEB_HOST}:${WEB_PORT}/" 2>/dev/null; then
        READY=1
        break
    fi
    sleep 0.5
done

ZY_PID="$(pgrep -f 'zy-quant dashboard' | head -n 1 || true)"
CAF_PID="$(pgrep -f 'caffeinate.*zy-quant' | head -n 1 || true)"
SLEEP_DISABLED_NOW="$(pmset -g | awk '/SleepDisabled/{print $2}')"

echo ""
echo "================ Service Status ================"
if [[ "${READY}" -eq 1 ]]; then
    echo "  Daemon State      : RUNNING (HTTP ready)"
else
    echo "  Daemon State      : STARTING (check /tmp/zy-quant-daemon.err.log)"
fi
echo "  Web Terminal URL  : http://${WEB_HOST}:${WEB_PORT}"
echo "  Launchd Schedule  : ARMED (Mon-Fri 09:20 ET pre-market)"
echo "  zy-quant PID      : ${ZY_PID:-N/A}"
echo "  caffeinate PID    : ${CAF_PID:-N/A}"
echo "  SleepDisabled     : ${SLEEP_DISABLED_NOW:-0}"
echo "  Logs              : /tmp/zy-quant-daemon.out.log | /tmp/zy-quant-daemon.err.log"
echo "================================================"
