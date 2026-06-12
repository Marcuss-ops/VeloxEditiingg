#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="${ROOT_DIR}/VERSION.txt"
MASTER_URL="${MASTER_URL:-http://127.0.0.1:8000}"
ADMIN_TOKEN="${ADMIN_TOKEN:-velox-dev-token}"
APPLY_ANSIBLE="${APPLY_ANSIBLE:-1}"
TARGETS="${TARGETS:-}"

log() {
  printf '[bump-deploy] %s\n' "$*"
}

fail() {
  printf '[bump-deploy] ERROR: %s\n' "$*" >&2
  exit 1
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
    log "Launching Ansible update_workers for targets: ${TARGETS}"
    run_resp="$(
      curl -sf \
        -X POST \
        -H 'Content-Type: application/json' \
        -H "X-Velox-Admin-Token: ${ADMIN_TOKEN}" \
        "${MASTER_URL}/api/v1/ansible/computers/run_action" \
        -d "{\"action\":\"update_workers\",\"computer_ids\":[\"$(printf '%s' "$TARGETS" | sed 's/,/","/g')\"]}"
    )"
    log "Ansible queued: ${run_resp}"
  else
    log "No Ansible targets found, skipping remote deploy"
  fi
fi

log "Version bump and deploy complete: ${next_version}"
