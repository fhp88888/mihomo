#!/bin/zsh

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-genver}"
LOCAL_CORE="bin/mihomo-linux-amd64-v2"
REMOTE_CORE="/etc/openclash/core/clash_meta"
STOP_TIMEOUT_SECONDS="${STOP_TIMEOUT_SECONDS:-30}"
START_TIMEOUT_SECONDS="${START_TIMEOUT_SECONDS:-60}"

service_is_running() {
  local result

  if ssh "$REMOTE_HOST" 'service openclash running' >/dev/null 2>&1; then
    return 0
  else
    result=$?
  fi

  if (( result == 1 )); then
    return 1
  fi

  echo "ERROR: Failed to query OpenClash on ${REMOTE_HOST} (exit code ${result})."
  return 2
}

wait_for_state() {
  local desired_state="$1"
  local timeout_seconds="$2"
  local elapsed=0
  local check_result

  while (( elapsed < timeout_seconds )); do
    if service_is_running; then
      check_result=0
    else
      check_result=$?
    fi

    if (( check_result == 2 )); then
      return 1
    fi

    if [[ "$desired_state" == "stopped" && $check_result -ne 0 ]]; then
      return 0
    fi

    if [[ "$desired_state" == "running" && $check_result -eq 0 ]]; then
      return 0
    fi

    sleep 1
    (( elapsed += 1 ))
  done

  return 1
}

echo "[1/6] Building Linux amd64 v2 core..."
make linux-amd64-v2
[[ -x "$LOCAL_CORE" ]] || {
  echo "ERROR: Build output is missing or not executable: ${LOCAL_CORE}"
  exit 1
}
echo "[1/6] Build completed: ${LOCAL_CORE}"

echo "[2/6] Stopping OpenClash on ${REMOTE_HOST}..."
# OpenClash may return a non-zero status when it is already stopped, so the
# following wait verifies the actual service state.
ssh "$REMOTE_HOST" 'service openclash stop' || \
  echo "[2/6] Stop command returned non-zero; checking the actual service state..."

echo "[3/6] Waiting for OpenClash to stop (timeout: ${STOP_TIMEOUT_SECONDS}s)..."
if ! wait_for_state stopped "$STOP_TIMEOUT_SECONDS"; then
  echo "ERROR: OpenClash did not stop cleanly within ${STOP_TIMEOUT_SECONDS}s."
  exit 1
fi
echo "[3/6] OpenClash is stopped."

echo "[4/6] Uploading ${LOCAL_CORE} to ${REMOTE_HOST}:${REMOTE_CORE}..."
scp "$LOCAL_CORE" "${REMOTE_HOST}:${REMOTE_CORE}"
echo "[4/6] New core uploaded and old core replaced."

echo "[5/6] Starting OpenClash on ${REMOTE_HOST}..."
ssh "$REMOTE_HOST" 'service openclash start'
echo "[5/6] Start command completed."

echo "[6/6] Waiting for OpenClash to report running (timeout: ${START_TIMEOUT_SECONDS}s)..."
if ! wait_for_state running "$START_TIMEOUT_SECONDS"; then
  echo "ERROR: OpenClash did not start within ${START_TIMEOUT_SECONDS}s."
  echo "[6/6] Current OpenClash service info:"
  ssh "$REMOTE_HOST" 'service openclash info' || true
  exit 1
fi

echo "[6/6] OpenClash is running. Current service info:"
ssh "$REMOTE_HOST" 'service openclash info'
echo "Deployment completed successfully."
