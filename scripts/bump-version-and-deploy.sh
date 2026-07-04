#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="${ROOT_DIR}/VERSION.txt"
MASTER_URL="${MASTER_URL:-http://127.0.0.1:8000}"
ADMIN_TOKEN="${ADMIN_TOKEN:-velox-dev-token}"
APPLY_ANSIBLE="${APPLY_ANSIBLE:-1}"
TARGETS="${TARGETS:-}"
WAIT_FOR_ANSIBLE="${WAIT_FOR_ANSIBLE:-1}"
ANSIBLE_WAIT_TIMEOUT_SECONDS="${ANSIBLE_WAIT_TIMEOUT_SECONDS:-1800}"
ANSIBLE_WAIT_POLL_SECONDS="${ANSIBLE_WAIT_POLL_SECONDS:-10}"

log() {
  printf '[bump-deploy] %s\n' "$*"
}

fail() {
  printf '[bump-deploy] ERROR: %s\n' "$*" >&2
  exit 1
}

json_field() {
  python3 - "$1" "$2" <<'PY'
import json
import sys

path = sys.argv[1]
field = sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
value = data.get(field, "")
if isinstance(value, (dict, list)):
    print(json.dumps(value))
elif value is None:
    print("")
else:
    print(value)
PY
}

wait_for_ansible_run() {
  local run_id="$1"
  local started_at
  started_at="$(date +%s)"

  log "Waiting for Ansible run ${run_id}..."
  while true; do
    local status_file status
    status_file="$(mktemp)"

    if ! curl -sf \
      -H 'Content-Type: application/json' \
      -H "X-Velox-Admin-Token: ${ADMIN_TOKEN}" \
      "${MASTER_URL}/api/v1/admin/ansible/runs/${run_id}" \
      -o "$status_file"; then
      rm -f "$status_file"
      fail "Unable to fetch Ansible run ${run_id}"
    fi

    status="$(json_field "$status_file" status)"
    if [[ "$status" == "completed" || "$status" == "failed" ]]; then
      if [[ "$status" == "failed" ]]; then
        log "Run ${run_id} failed"
        json_field "$status_file" output >&2 || true
        rm -f "$status_file"
        fail "Ansible deployment failed"
      fi
      log "Run ${run_id} completed"
      rm -f "$status_file"
      return 0
    fi

    rm -f "$status_file"

    if (( $(date +%s) - started_at >= ANSIBLE_WAIT_TIMEOUT_SECONDS )); then
      fail "Timed out waiting for Ansible run ${run_id}"
    fi

    sleep "$ANSIBLE_WAIT_POLL_SECONDS"
  done
}

current_version="$(tr -d '[:space:]' < "$VERSION_FILE" || true)"
if [[ -z "$current_version" ]]; then
  fail "VERSION.txt is empty"
fi

if [[ "$current_version" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  next_version="v${major}.${minor}.$((patch + 1))"
else
  fail "Unsupported version format: ${current_version} (expected vMAJOR.MINOR.PATCH)"
fi

log "Current version: ${current_version}"
log "Next version:    ${next_version}"

printf '%s\n' "$next_version" > "$VERSION_FILE"

log "Building worker agent..."
make -C "$ROOT_DIR/RemoteCodex/native/worker-agent-go" agent

log "Building master/server..."
(cd "$ROOT_DIR/DataServer" && go build -o bin/velox-server ./cmd/server)

log "Rebuilding worker bundle..."
"$ROOT_DIR/DataServer/bin/velox-bundler" --source "$ROOT_DIR" --output "$ROOT_DIR/DataServer/data/worker_downloads"

if [[ "$APPLY_ANSIBLE" != "0" ]]; then
  if [[ -z "$TARGETS" ]]; then
    if command -v jq >/dev/null 2>&1 && [[ -f "$ROOT_DIR/DataServer/data/ansible/ansible_computers.json" ]]; then
      TARGETS="$(jq -r 'keys[]' "$ROOT_DIR/DataServer/data/ansible/ansible_computers.json" | paste -sd, -)"
    fi
  fi

  if [[ -n "$TARGETS" ]]; then
    log "Launching Ansible deploy_workers for targets: ${TARGETS}"
    run_resp_file="$(mktemp)"
    if ! curl -sf \
      -X POST \
      -H 'Content-Type: application/json' \
      -H "X-Velox-Admin-Token: ${ADMIN_TOKEN}" \
      "${MASTER_URL}/api/v1/admin/ansible/computers/run_action" \
      -d "{\"action\":\"deploy_workers\",\"batch_size\":5,\"canary_percent\":10,\"computer_ids\":[\"$(printf '%s' "$TARGETS" | sed 's/,/\",\"/g')\"]}" \
      -o "$run_resp_file"; then
      rm -f "$run_resp_file"
      fail "Failed to queue Ansible update"
    fi

    run_id="$(json_field "$run_resp_file" run_id)"
    run_status="$(json_field "$run_resp_file" status)"
    rm -f "$run_resp_file"

    if [[ -z "$run_id" ]]; then
      fail "Ansible response did not include run_id"
    fi

    log "Ansible queued: run_id=${run_id} status=${run_status}"
    if [[ "$WAIT_FOR_ANSIBLE" != "0" ]]; then
      wait_for_ansible_run "$run_id"
    fi
  else
    log "No Ansible targets found, skipping remote deploy"
  fi
fi

log "Version bump and deploy complete: ${next_version}"
