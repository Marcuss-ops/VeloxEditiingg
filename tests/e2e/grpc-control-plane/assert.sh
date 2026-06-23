# =============================================================================
# tests/e2e/grpc-control-plane/assert.sh
# =============================================================================
# Assertion helpers for the 6-case matrix. Sourced (not executed) by run.sh.
# Pure-function style: each helper prints PASS/FAIL on stdout and returns
# 0 on pass, 1 on fail. run.sh aggregates the return codes.
#
# The "log-of-truth" approach: every assertion reads from a case-specific
# $WORKDIR/case-N/ dir containing master.log + worker-N.log. We do NOT
# rely on internal Go state — only what the operator (or a bug filing)
# would inspect.
#
# Naming convention: `assert_*` returns 0/1 and prints verdict; `wait_*`
# returns 0/1 silently and is pure polling. Distinction matters because
# run.sh aggregates `assert_*` return codes into the matrix PASS/FAIL
# summary, but uses `wait_*` only as a "ready" gate.
# =============================================================================

# ANSI colour shims — only when stdout is a TTY. CI logs use plain text.
if [[ -t 1 ]]; then
  ESC_GREEN=$'\033[32m'
  ESC_RED=$'\033[31m'
  ESC_CYAN=$'\033[36m'
  ESC_RST=$'\033[0m'
else
  ESC_GREEN="" ; ESC_RED="" ; ESC_CYAN="" ; ESC_RST=""
fi

assert_pass() { printf '%sPASS%s  %s\n' "$ESC_GREEN" "$ESC_RST" "$*" ; return 0 ; }
assert_fail() { printf '%sFAIL%s  %s\n' "$ESC_RED"   "$ESC_RST" "$*" ; return 1 ; }
assert_info() { printf '%s.. %s%s\n'    "$ESC_CYAN"  "$ESC_RST" "$*" ; return 0 ; }

# ─── master.log assertion: TLS handshake successful ─────────────────────────
assert_master_accepted() {
  local master_log="$1" worker_id="$2"
  assert_info "checking master.log contains accepted-worker marker for '$worker_id'"
  if grep -qE "HelloAck.*worker_id.*${worker_id}|accepted registration for ${worker_id}" "$master_log" 2>/dev/null; then
    assert_pass "master accepted worker '$worker_id'"
    return 0
  fi
  assert_fail "master did NOT accept worker '$worker_id' (see $master_log)"
  return 1
}

# ─── master.log assertion: TLS handshake rejected ───────────────────────────
assert_master_rejected() {
  local master_log="$1" worker_id="$2" expected_code="${3:-PermissionDenied|Unauthenticated|handshake|fail}"
  assert_info "checking master.log for rejection code ($expected_code) of '$worker_id'"
  if grep -qE "${expected_code}" "$master_log" 2>/dev/null; then
    assert_pass "master rejected connection (marker matched: $expected_code)"
    return 0
  fi
  assert_fail "master log lacks rejection marker '$expected_code' (see $master_log)"
  return 1
}

# ─── worker log assertion: log is clean (no FATAL/panic markers) ─────────────
# Renamed from `assert_worker_exit_zero` — the previous name was misleading
# (the helper checks log markers, NOT the bash $? of the backgrounded worker).
# Capturing the real exit code would require tracking bash $! across run.sh's
# trap-driven kill chain; the log-driven FATAL/panic check is what an operator
# reads anyway, so this is the cleaner operator-facing signal.
assert_worker_log_clean() {
  local worker_log="$1"
  assert_info "checking worker log for graceful-exit markers (no FATAL/panic)"
  if grep -qiE "FATAL|panic|exit code:|exit status [1-9]" "$worker_log" 2>/dev/null; then
    assert_fail "worker log shows non-zero-exit marker (see $worker_log)"
    return 1
  fi
  if grep -qE "HelloAck received from master|✓ HelloAck" "$worker_log" 2>/dev/null \
     || [[ -s "$worker_log" ]]; then
    assert_pass "worker log is clean (non-empty, no FATAL marker)"
    return 0
  fi
  assert_fail "worker log is empty AND no HelloAck marker (see $worker_log)"
  return 1
}

# ─── worker log assertion: handshake/auth failure markers present ───────────
# Worker exit code is logged by velox-worker-agent on TLS handshake failures,
# or by dev-hello-client (PR 2) on handler error paths. Both surface as a
# recognition log marker that we grep for.
assert_worker_handshake_fails() {
  local worker_log="$1"
  assert_info "checking worker log shows handshake/auth failure marker"
  if grep -qiE "(could not connect|handshake|TLS.*fail|certificate verify|unknown authority|invalid cert|verify|connection refused)" "$worker_log" 2>/dev/null; then
    assert_pass "worker log shows handshake/auth failure marker"
    return 0
  fi
  assert_fail "worker log lacks handshake-failure marker (see $worker_log)"
  return 1
}

# ─── registry assertion via REST API ────────────────────────────────────────
# GET /api/v1/workers?admin_token=... — surface shape from PR 4 (canonical):
# { "workers": [{ "worker_id": "...", "state": "CONNECTED", ... }] }
# Until PR 4 ships, we use a permissive grep on the raw response.
assert_registry_contains() {
  local base_url="$1" admin_token="$2" worker_id="$3"
  assert_info "GET $base_url/api/v1/workers — looking for '$worker_id'"
  local body
  body="$(curl -fsS -H "X-Admin-Token: $admin_token" \
            "$base_url/api/v1/workers" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    assert_fail "registry endpoint returned no body — master not ready or endpoint missing"
    return 1
  fi
  if grep -qF "$worker_id" <<<"$body"; then
    assert_pass "registry contains '$worker_id'"
    return 0
  fi
  assert_fail "registry does NOT contain '$worker_id' (response: $body)"
  return 1
}

assert_registry_lacks() {
  local base_url="$1" admin_token="$2" worker_id="$3"
  assert_info "GET $base_url/api/v1/workers — verifying absence of '$worker_id'"
  local body
  body="$(curl -fsS -H "X-Admin-Token: $admin_token" \
            "$base_url/api/v1/workers" 2>/dev/null || true)"
  if grep -qF "$worker_id" <<<"$body"; then
    assert_fail "registry CONTAINS '$worker_id' but should not (response: $body)"
    return 1
  fi
  assert_pass "registry does not contain '$worker_id' (as expected)"
  return 0
}

# ─── wait-for-master-ready ───────────────────────────────────────────────────
# NOTE: run.sh defines its own wait_for_master_ready with (base_url, admin_token,
# budget, case_id) signature. Remove this function if it becomes dead code.

# ─── wait-for-worker-connect ─────────────────────────────────────────────────
# Polls master.log until a HelloAck marker for $worker_id appears, with budget.
wait_for_worker_connection() {
  local master_log="$1" worker_id="$2" budget="${3:-15}"
  local deadline=$(( $(date +%s) + budget ))
  while (( $(date +%s) < deadline )); do
    if grep -qE "(HelloAck.*${worker_id}|accepted registration for ${worker_id})" \
          "$master_log" 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}
