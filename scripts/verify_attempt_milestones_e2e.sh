#!/usr/bin/env bash
#
# scripts/verify_attempt_milestones_e2e.sh
#
# Live E2E check that the heartbeat/live endpoint surfaces the complete
# attempt milestone timeline with monotonic elapsed_ms, from
# attempt.accepted through attempt.completed, and that the durable job
# inspect view still shows the same timeline after completion.
#
# Prerequisites:
#   - fleetctl built or buildable: REPO_ROOT/build/fleetctl[.exe] is used
#     when present, otherwise falls back to `go run ./cmd/fleetctl`.
#   - python3 on PATH (JSON validation; no jq dependency).
#   - Same token resolution as fleetctl: $VELOX_ADMIN_TOKEN (or
#     $TOKEN_FILE) + $VELOX_MASTER_URL (--master also works).
#
# Usage:
#   # verify an already-running/completed job:
#   VELOX_MASTER_URL=https://master:8000 TOKEN_FILE=/path/tok \
#     scripts/verify_attempt_milestones_e2e.sh --job <JOB_ID> [--wait]
#
#   # submit a payload first (pinned to one worker), then verify live:
#   VELOX_MASTER_URL=https://master:8000 \
#     scripts/verify_attempt_milestones_e2e.sh \
#     --payload ops/jobs/<job>.generate.json --workers velox-worker-01
#
# Flags:
#   --job ID        verify an existing job instead of submitting
#   --payload FILE  job submit payload (requires --workers)
#   --workers LIST  comma-separated worker IDs (default: all)
#   --wait          block until terminal before final validation (default
#                   on when submitting without --no-wait)
#   --interval S    poll interval seconds (default 5)
#   --timeout S     overall wait budget seconds (default 3600)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log()  { printf '%s\n' "[milestones-e2e] $*"; }
fail() { printf '%s\n' "[milestones-e2e] FAIL: $*" >&2; exit 1; }

JOB_ID="" PAYLOAD="" WORKERS="" WAIT=""
POLL_INTERVAL="${POLL_INTERVAL:-5}" POLL_TIMEOUT="${POLL_TIMEOUT:-3600}"
SNAP_DIR="$(mktemp -d /tmp/velox-milestones-e2e.XXXXXX)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job)      JOB_ID="$2"; shift 2 ;;
    --payload)  PAYLOAD="$2"; shift 2 ;;
    --workers)  WORKERS="$2"; shift 2 ;;
    --wait)     WAIT=1; shift ;;
    --no-wait)  WAIT=0; shift ;;
    --interval) POLL_INTERVAL="$2"; shift 2 ;;
    --timeout)  POLL_TIMEOUT="$2"; shift 2 ;;
    *) fail "unknown flag: $1 (see header for usage)" ;;
  esac
done

command -v python3 >/dev/null 2>&1 || fail "python3 required on PATH"
[[ -n "${VELOX_MASTER_URL:-}" || -n "${FLEETCTL_MASTER:-}" ]] \
  || fail "VELOX_MASTER_URL not set (or pass FLEETCTL_MASTER=--master=https://HOST:8000)"

if [[ -x "${REPO_ROOT}/build/fleetctl" ]]; then
  FLEETCTL="${REPO_ROOT}/build/fleetctl"
elif [[ -x "${REPO_ROOT}/build/fleetctl.exe" ]]; then
  FLEETCTL="${REPO_ROOT}/build/fleetctl.exe"
else
  FLEETCTL="go run ./cmd/fleetctl"
  log "no prebuilt fleetctl under build/; falling back to: ${FLEETCTL} (cwd DataServer)"
fi

fleet() {
  local flagc=()
  [[ -n "${FLEETCTL_MASTER:-}" ]] && flagc+=("$FLEETCTL_MASTER")
  if [[ "${FLEETCTL}" == go\ run* ]]; then
    ( cd "${REPO_ROOT}/DataServer" && ${FLEETCTL} "${flagc[@]}" "$@" )
  else
    "${FLEETCTL}" "${flagc[@]}" "$@"
  fi
}

# ── resolve job id ──────────────────────────────────────────────────────────
if [[ -z "$JOB_ID" ]]; then
  [[ -n "$PAYLOAD" ]] || fail "nothing to verify: pass --job ID or --payload FILE"
  [[ -f "$PAYLOAD" ]] || fail "payload file not found: $PAYLOAD"
  submit_args=(job submit "--payload=${PAYLOAD}")
  [[ -n "$WORKERS" ]] && submit_args+=("--workers=${WORKERS}")
  log "submitting job..."
  submitted="$(fleet "${submit_args[@]}")" || fail "job submit failed (output above)"
  JOB_ID="$(printf '%s\n' "$submitted" | grep -oE 'submitted job=[^ ]+' | head -1 | cut -d= -f2)"
  [[ -n "$JOB_ID" ]] || { printf '%s\n' "$submitted" >&2; fail "could not parse submitted job id"; }
  : # default to waiting for freshly submitted jobs unless --no-wait
  WAIT="${WAIT:-1}"
fi
log "verifying job_id=${JOB_ID}"

MASTER_BASE="${VELOX_MASTER_URL%/}"
auth_header=""
if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
  auth_header="Authorization: Bearer ${VELOX_ADMIN_TOKEN}"
elif [[ -n "${TOKEN_FILE:-}" && -r "$TOKEN_FILE" ]]; then
  auth_header="Authorization: Bearer $(tr -d '[:space:]' < "$TOKEN_FILE" | sed 's/^VELOX_ADMIN_TOKEN=//')"
else
  fail "token required: set VELOX_ADMIN_TOKEN or TOKEN_FILE"
fi

fetch_json() { # fetch_json <path> <outfile>
  curl -fsS -H "$auth_header" "${MASTER_BASE}$1" > "$2" 2>/dev/null
}

# ── poll live until terminal (collect snapshots) ────────────────────────────
deadline=$(( $(date +%s) + POLL_TIMEOUT ))
terminal=""
poll_n=0
while :; do
  poll_n=$((poll_n + 1))
  snap="${SNAP_DIR}/live_${poll_n}.json"
  if fetch_json "/api/v1/admin/jobs/${JOB_ID}/live" "$snap"; then
    status="$(FLEETCTL_SNAPSHOT="$snap" python3 -c '
import json, os, sys
try:
    doc = json.load(open(os.environ["FLEETCTL_SNAPSHOT"]))
except Exception:
    sys.exit(0)
print(doc.get("status") or doc.get("job", {}).get("status") or "")')"
    log "poll #${poll_n}: status=${status:-<unavailable>} snapshot=${snap}"
    [[ "$status" =~ ^(SUCCEEDED|FAILED|CANCELLED|COMPLETED)$ ]] && { terminal="$status"; break; }
  else
    log "poll #${poll_n}: /live unavailable (404 = pre-milestone server or transient)"
  fi
  [[ $WAIT == 1 ]] || break
  (( $(date +%s) < deadline )) || fail "timed out after ${POLL_TIMEOUT}s waiting for terminal status (last=$status)"
  sleep "$POLL_INTERVAL"
done

# ── durable snapshot post-terminal (attempt timeline must persist) ──────────
durable="${SNAP_DIR}/durable.json"
fetch_json "/api/v1/admin/jobs/${JOB_ID}?include=execution,waterfall" "$durable" \
  || fetch_json "/api/v1/admin/jobs/${JOB_ID}" "$durable" \
  || log "WARN: durable inspect fetch failed"

# ── validate ────────────────────────────────────────────────────────────────
export SNAP_DIR JOB_ID TERMINAL_STATUS="$terminal"
python3 "$SCRIPT_DIR/verify_attempt_milestones_check.py"
rc=$?
rm -rf "$SNAP_DIR"
exit "$rc"
