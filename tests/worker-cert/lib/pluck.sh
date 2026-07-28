#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/lib/pluck.sh — helpers locali al worker-cert harness.
#
# Cross-test helpers (logging/pid-trap/aggregate/retry/check/ensure) live in
# tests/_lib/sh/ and are sourced via _lib.sh. The helpers below are *only*
# relevant to the per-worker smoke flow and do not leak into other suites.
#
# Sourced by tests/worker-cert/smoke_one.sh. Idempotent: re-sourcing refreshes
# globals without re-declaring.
#
# SMOKE_PLUCKER_VARS_REV is bumped when a helper's exported signature or
# observed output schema changes (e.g., smoke_scrape_lease grew worker_id
# between rev 2 and rev 3). Each smoke.json captures the rev it was written
# with so older smokes missing fields can be triaged.
# =============================================================================

SMOKE_PLUCKER_VARS_REV=3
export SMOKE_PLUCKER_VARS_REV

# ─── M2M provisioning & cache ──────────────────────────────────────────────
# smoke_mint_m2m <admin_token> <master_url>
#   POST /api/v1/admin/m2m/keys, sets globals M2M_BEARER / PROVISIONED_CLIENT_ID.
#   On 409/400 (concurrent smoke within the same epoch second) retried once
#   with +1 second in client_id, matching scripts/api/jobs_smoke.sh's
#   canonical pattern. Returns:
#     3 — network curl failure
#     4 — non-{201,400,409} on POST after retries
#     5 — 201 returned but plaintext_secret empty/unparseable
smoke_mint_m2m() {
  local admin_token="$1" master_url="$2"
  local epoch client_id offset issue_req issue_body issue_status body_file hdrs_file
  body_file=$(mktemp); hdrs_file=$(mktemp)
  for offset in 0 1; do
    epoch=$(date +%s)
    client_id="smoke-cert-${TARGET_WORKER_ID:-nopin}-$((${epoch}+offset))-$$"
    issue_req=$(cat <<JSON
{
  "client_id": "${client_id}",
  "description": "tests/worker-cert/smoke_one.sh ephemeral client",
  "scopes": ["jobs.submit"],
  "rate_limit_rps": 5,
  "rate_limit_burst": 10,
  "quota_max_scenes": 100,
  "quota_max_total_secs": 600
}
JSON
)
    issue_status=""
    if ! curl -sS -m 15 -X POST \
          -H "Authorization: Bearer $admin_token" \
          -H "Content-Type: application/json" \
          --data-raw "$issue_req" \
          -D "$hdrs_file" -o "$body_file" \
          "${master_url}/api/v1/admin/m2m/keys" >/dev/null 2>&1; then
      rm -f "$body_file" "$hdrs_file"
      return 3
    fi
    issue_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$hdrs_file")
    issue_body=$(cat "$body_file")
    # Status-driven retry: 201 ⇒ success (break); 409|400 ⇒ retry the next
    # offset; everything else ⇒ bail (operator should investigate).
    case "$issue_status" in
      201) break ;;
      409|400) continue ;;
      *) rm -f "$body_file" "$hdrs_file"; return 4 ;;
    esac
  done
  if [[ "$issue_status" != "201" ]]; then
    log_error "M2M provisioning failed (after retry): HTTP $issue_status"
    log_error "  body: $(printf '%s' "$issue_body" | head -c 400)"
    rm -f "$body_file" "$hdrs_file"
    return 4
  fi
  M2M_BEARER=$(printf '%s' "$issue_body" | jq -er '.plaintext_secret // empty') \
    || { rm -f "$body_file" "$hdrs_file"; return 5; }
  [[ -n "$M2M_BEARER" ]] || { rm -f "$body_file" "$hdrs_file"; return 5; }
  PROVISIONED_CLIENT_ID="$client_id"
  export M2M_BEARER PROVISIONED_CLIENT_ID
  rm -f "$body_file" "$hdrs_file"
  return 0
}

# ─── Worker-list inspection ────────────────────────────────────────────────
# smoke_workers_list <bearer> <master_url> — sets WORKERS_JSON to the raw
# response (single GET /api/v1/workers). Returns curl rc.
smoke_workers_list() {
  local bearer="$1" master_url="$2"
  WORKERS_JSON=$(curl -sS -m 30 \
    -H "Authorization: Bearer $bearer" \
    "${master_url}/api/v1/workers") || return 3
  echo "$WORKERS_JSON" | jq -e '.workers | type == "array"' >/dev/null 2>&1 \
    || { log_error "workers list payload malformed"; return 4; }
  export WORKERS_JSON
}

# smoke_worker_by_id <worker_id> — prints the JSON record for <worker_id>
# from $WORKERS_JSON, or empty if absent.
smoke_worker_by_id() {
  local worker_id="$1"
  printf '%s' "$WORKERS_JSON" | jq -er --arg w "$worker_id" \
    '.workers[] | select(.worker_id == $w)' 2>/dev/null || true
}

# smoke_assert_pin_clarity <target_worker_id> — count CONNECTED non-target
# workers; emit WARN if any. With SMOKE_STRICT_PIN=1 the WARN escalates to
# return 4 (deterministic enforcement for unattended CI where stderr may
# not be tailed). The actual pin evidence is at the post-SUCCEEDED step
# where the script verifies the lease was granted to <worker_id> via the
# master log scrape (see smoke_scrape_lease).
smoke_assert_pin_clarity() {
  local target="$1"
  local count
  count=$(printf '%s' "$WORKERS_JSON" \
    | jq --arg w "$target" \
       '[.workers[] | select(.worker_id != $w and .status == "CONNECTED" and .session_active == true)] | length')
  if (( count > 0 )); then
    log_warn "placement-pin clarity: $count CONNECTED non-target worker(s) present; smoke depends on operator pause/drain for determinism"
    if [[ "${SMOKE_STRICT_PIN:-0}" == "1" ]]; then
      log_error "SMOKE_STRICT_PIN=1: refusing smoke before submit; pause/drain non-target workers first"
      return 4
    fi
  else
    log_info "placement-pin clarity: only target worker <$target> in CONNECTED pool"
  fi
  return 0
}

# ─── Job status helpers ────────────────────────────────────────────────────
# smoke_parse_iso8601 <str> — print epoch seconds (float, possibly .N) or
# empty on failure. Supports plain "Z" form and "+0000" offsets.
smoke_parse_iso8601() {
  local s="$1"
  [[ -z "$s" ]] && { echo ""; return 0; }
  date -u -d "$s" +%s.%N 2>/dev/null || \
  date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$s" +%s 2>/dev/null || \
  date -u -j -f '%Y-%m-%dT%H:%M:%S%z' "$s" +%s 2>/dev/null || \
  echo ""
}

# smoke_scrape_lease <job_id> <log_path> — find the most recent
# "TaskLeaseGranted sent to worker <ID> (session <sid>): task=<id> job=<id>
# attempt=<id> lease=<id>" line in <log_path> (falls back to journalctl).
# Emits a JSON object on stdout:
#   {"worker_id":"...","task_id":"...","attempt_id":"...","lease_id":"..."}
# Authoritative on placement-pin enforcement: the worker_id is the master-
# stated recipient of the TaskLeaseGranted envelope, not the race-prone
# per-worker current_task_id (already cleared post-SUCCEEDED).
#
# POSIX awk shape (no gawk function {} blocks) so it works on mawk/busybox.
smoke_scrape_lease() {
  local job_id="$1" log_path="$2"
  local line src=""
  if [[ -n "$log_path" && -r "$log_path" ]]; then
    src="path:$log_path"
    line=$(grep -F "$job_id" "$log_path" 2>/dev/null \
      | grep -F 'TaskLeaseGranted' | tail -1 || true)
  elif command -v journalctl >/dev/null 2>&1; then
    src="journalctl:-u velox-server -n 5000"
    line=$(journalctl -u velox-server -n 5000 --no-pager 2>/dev/null \
      | grep -F "$job_id" | grep -F 'TaskLeaseGranted' | tail -1 || true)
  else
    log_warn "smoke_scrape_lease: no log source available (set VELOX_MASTER_LOG_PATH or install journalctl); task_id/attempt_id/worker_id will be empty"
  fi
  log_debug "smoke_scrape_lease src=${src:-<none>} job_id=$job_id line_present=${line:+yes}"
  [[ -z "$line" ]] && echo '{"worker_id":"","task_id":"","attempt_id":"","lease_id":""}' && return 0
  printf '%s' "$line" | awk -v jid="$job_id" '
    BEGIN {
      q["worker_id"]=""; q["task_id"]=""; q["attempt_id"]=""; q["lease_id"]=""
    }
    # Per-row extraction. TaskLeaseGranted log shape (handler_stream.go:455):
    #   "[GRPC] TaskLeaseGranted sent to worker <ID> (session <sid>):
    #    task=<TID> job=<JID> attempt=<AID> lease=<LID>"
    # Each match() resets RSTART/RLENGTH; substr() at that point captures
    # the match; sub() strips the key prefix. No outer function {} block.
    {
      if (index($0, jid) == 0) next
      if (match($0, /sent to worker [^ ]+/)) {
        q["worker_id"] = substr($0, RSTART, RLENGTH)
        sub(/^sent to worker /, "", q["worker_id"])
      }
      if (match($0, /task=[^ ]+/))     { q["task_id"]    = substr($0, RSTART, RLENGTH); sub(/^task=/,    "", q["task_id"]) }
      if (match($0, /attempt=[^ ]+/)) { q["attempt_id"] = substr($0, RSTART, RLENGTH); sub(/^attempt=/, "", q["attempt_id"]) }
      if (match($0, /lease=[^ ]+/))   { q["lease_id"]   = substr($0, RSTART, RLENGTH); sub(/^lease=/,   "", q["lease_id"]) }
    }
    # JSON shape: worker_id/task_id/attempt_id/lease_id, verbatim. Values
    # are alnum + [._:/-], no escape needed.
    END {
      printf("{")
      printf("\"worker_id\":\"%s\",",   q["worker_id"])
      printf("\"task_id\":\"%s\",",     q["task_id"])
      printf("\"attempt_id\":\"%s\",",  q["attempt_id"])
      printf("\"lease_id\":\"%s\"",     q["lease_id"])
      printf("}\n")
    }'
}

# smoke_artifact_size <url> [bearer] — HEAD-like size probe; prints integer
# bytes (or 0 if unreachable). Bearer is optional; artifact URLs in the
# current API are expected to be path-protected or externally signed.
smoke_artifact_size() {
  local url="$1" bearer="${2:-}"
  local hdrs body status cl
  [[ -z "$url" ]] && { echo 0; return 0; }
  hdrs=$(mktemp); body=$(mktemp)
  if [[ -n "$bearer" ]]; then
    curl -sS -m 20 -I -H "Authorization: Bearer $bearer" "$url" \
      -D "$hdrs" -o "$body" >/dev/null 2>&1 || true
  else
    curl -sS -m 20 -I "$url" -D "$hdrs" -o "$body" >/dev/null 2>&1 || true
  fi
  status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$hdrs")
  if [[ "$status" != "200" && "$status" != "206" ]]; then
    rm -f "$hdrs" "$body"; echo 0; return 0
  fi
  cl=$(awk 'tolower($1) == "content-length:" {gsub(/[\r\n]/,""); print $2; exit}' "$hdrs")
  rm -f "$hdrs" "$body"
  [[ "$cl" =~ ^[0-9]+$ ]] && echo "$cl" || echo 0
}
