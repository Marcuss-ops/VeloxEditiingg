#!/usr/bin/env bash
# Fixture-based self-test for check-no-legacy.sh.
# It proves removed server mounts and generic completed writers fail, while
# remote client calls and typed input-assembly status writes remain allowed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CHECK_REAL="$REPO_ROOT/scripts/ci/check-no-legacy.sh"
LIB_REAL="$REPO_ROOT/scripts/ci/lib/diff-scope.sh"
[[ -x "$CHECK_REAL" ]] || { echo "missing executable guard: $CHECK_REAL" >&2; exit 2; }
[[ -r "$LIB_REAL" ]] || { echo "missing diff-scope helper: $LIB_REAL" >&2; exit 2; }

WORK="$(mktemp -d /tmp/velox-test-check-no-legacy.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

provision_repo() {
  local repo="$1"
  mkdir -p "$repo/scripts/ci/lib" "$repo/DataServer/cmd/server" \
    "$repo/DataServer/internal/app" "$repo/DataServer/internal/handlers" \
    "$repo/DataServer/internal/jobs/enqueue" "$repo/DataServer/internal/handlers/server/pipeline"
  cp "$CHECK_REAL" "$repo/scripts/ci/check-no-legacy.sh"
  cp "$LIB_REAL" "$repo/scripts/ci/lib/diff-scope.sh"
  (cd "$repo" && git init -q && git config user.email ci@test.local && git config user.name ci)
  (cd "$repo" && git add . && git commit -q -m baseline)
}

run_case() {
  local label="$1" expected="$2" repo="$3"
  local actual=0
  set +e
  (cd "$repo" && BASE_REF=HEAD^ ./scripts/ci/check-no-legacy.sh >/dev/null 2>&1)
  actual=$?
  set -e
  if [[ "$actual" -ne "$expected" ]]; then
    echo "FAIL: $label (wanted rc=$expected, got rc=$actual)" >&2
    return 1
  fi
  printf '[OK] %s (rc=%d)\n' "$label" "$actual"
}

# A removed route mount must fail.
ROUTE="$WORK/route"
provision_repo "$ROUTE"
printf '%s\n' 'package server' 'func mount(r *Router) {' '  r.POST("/api/remote/pipeline/generate", handler)' '}' \
  > "$ROUTE/DataServer/cmd/server/routes.go"
(cd "$ROUTE" && git add . && git commit -q -m route-regression)
run_case 'retired server route registration is rejected' 1 "$ROUTE"

# A remote-engine client call is not a server mount and must pass.
CLIENT="$WORK/client"
provision_repo "$CLIENT"
printf '%s\n' 'package remoteengine' 'func call(c *Client) {' '  c.POST("/api/script-simple", body)' '}' \
  > "$CLIENT/DataServer/internal/handlers/client.go"
(cd "$CLIENT" && git add . && git commit -q -m client-call)
run_case 'remote client call is allowed' 0 "$CLIENT"

# Generic completed assignment in a canonical writer must fail.
STATUS="$WORK/status"
provision_repo "$STATUS"
printf '%s\n' 'package pipeline' 'func write(payload map[string]any) {' '  payload["status"] = "completed"' '}' \
  > "$STATUS/DataServer/internal/handlers/server/pipeline/status.go"
(cd "$STATUS" && git add . && git commit -q -m status-regression)
run_case 'generic completed writer is rejected' 1 "$STATUS"

# The typed contract is the allowed input-assembly writer.
TYPED="$WORK/typed"
provision_repo "$TYPED"
printf '%s\n' 'package pipeline' 'func write(payload map[string]any) {' '  payload["status"] = string(contract.InputAssemblyCompleted)' '}' \
  > "$TYPED/DataServer/internal/handlers/server/pipeline/status.go"
(cd "$TYPED" && git add . && git commit -q -m typed-status)
run_case 'typed input-assembly status is allowed' 0 "$TYPED"

# JSON-style generic status literals are rejected as well.
JSON_STATUS="$WORK/json-status"
provision_repo "$JSON_STATUS"
printf '%s\n' 'package pipeline' 'var payload = map[string]any{' '  "status": "completed",' '}' \
  > "$JSON_STATUS/DataServer/internal/handlers/server/pipeline/status.go"
(cd "$JSON_STATUS" && git add . && git commit -q -m json-status-regression)
run_case 'generic completed JSON status is rejected' 1 "$JSON_STATUS"

echo 'test-check-no-legacy: OK'
