#!/usr/bin/env bash
# scripts/ops/verify-fmp4-rollout.sh — per-worker verification of the fMP4
# streaming-profile admission gate (VELOX_FMP4_STREAM_PROFILE).
# ─────────────────────────────────────────────────────────────────────────────
# WHY THIS EXISTS
#
# deploy/runtime/worker.env.example only seeds FRESH hosts: copying the template
# is a one-time step, so flipping the flag there does NOT update workers that
# are already installed. Meanwhile docs/operations/worker-rollout-paths.md §5
# forbids hand-editing /etc/velox-worker/worker.env. This script is the
# read-only verification half of the canonical rollout:
#
#   write path:  scripts/fleetctl worker-config set <worker_id> --fmp4-stream-profile 1
#                -> Master API -> velox-worker-set-config (atomic + restart + rollback)
#   verify path: this script — is the gate LIVE on the worker right now?
#
# It reads BOTH sides and reports drift, because the container only sees the
# env file contents captured at container creation:
#
#   env file (on disk)      vs   effective env inside the running container
#
# A file that says 1 while the container still says 0/absent means the flag has
# NOT been activated yet (a restart/convergence is missing) — the exact P0
# failure mode reported for the fMP4 migration.
#
# STATES (mirrors the capability state machine: DISABLED / READY / MISCONFIGURED)
#   READY         file and container both enable the gate
#   DISABLED      file and container both leave it disabled (intentional)
#   MISCONFIGURED drift between the two, or a value the gate does not recognise
#                 (the Go gate silently treats unknown values as disabled, so
#                 only this check surfaces the typo)
#
# Usage:
#   scripts/ops/verify-fmp4-rollout.sh --fleet
#   scripts/ops/verify-fmp4-rollout.sh --worker <worker_id> <host> <ssh_user>
#   VELOX_FMP4_WORKERS="id:host:user id2:host2:user2" scripts/ops/verify-fmp4-rollout.sh
#   scripts/ops/verify-fmp4-rollout.sh --fleet --json
#   scripts/ops/verify-fmp4-rollout.sh --help
#
# Env overrides:
#   VELOX_FMP4_ENV_FILE   canonical worker.env path on the host
#                         (default /etc/velox-worker/worker.env)
#   SSH_KEY               SSH identity (default /etc/velox/ssh/id_ed25519_velox,
#                         the root-managed key shared with WorkerNodeRegistry)
#
# Exit codes:
#   0   every worker is READY
#   1   at least one worker is DISABLED or MISCONFIGURED (rollout not complete)
#   2   usage error, no workers selected, or an SSH transport failure
#
# This script is READ-ONLY: it never writes worker.env, never restarts a unit,
# and never prints secret material.
set -uo pipefail

ENV_PATH="${VELOX_FMP4_ENV_FILE:-/etc/velox-worker/worker.env}"
SSH_KEY="${SSH_KEY:-/etc/velox/ssh/id_ed25519_velox}"
JSON=0

usage() {
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
  exit 2
}

# ── Argument parsing ────────────────────────────────────────────────────────
declare -a WORKERS=()
FLEET=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fleet) FLEET=1; shift ;;
    --json) JSON=1; shift ;;
    --worker)
      [[ $# -ge 4 ]] || { echo "usage: --worker <worker_id> <host> <ssh_user>" >&2; exit 2; }
      WORKERS+=("$2:$3:$4"); shift 4 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

if [[ -n "${VELOX_FMP4_WORKERS:-}" ]]; then
  for entry in $VELOX_FMP4_WORKERS; do WORKERS+=("$entry"); done
fi
if (( FLEET == 1 )); then
  # Canonical fleet inventory (same hosts as scripts/ops/runtime-cert.sh).
  # The authoritative inventory is the Master WorkerNodeRegistry; this static
  # list exists only so an operator can run the check without extra tooling.
  WORKERS+=(
    "host_57_129_132_133:57.129.132.133:pierone"
    "host_57_131_20_173:57.131.20.173:pierone"
    "velox-worker-13197:149.56.131.97:pierone"
    "velox-worker-523925eb:51.222.204.158:ubuntu"
  )
fi

if (( ${#WORKERS[@]} == 0 )); then
  echo "no workers selected: pass --fleet, --worker, or VELOX_FMP4_WORKERS" >&2
  exit 2
fi

SSH_COMMON=(-i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o BatchMode=yes \
  -o ConnectTimeout=8 -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

# ── Gate semantics (must mirror shared/contract/canonical_video_profile.go) ──
# normalize <raw> -> enabled | disabled | invalid
normalize() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')" in
    ""|0|false|no|off|disabled) echo "disabled" ;;
    1|true|yes|on|enabled) echo "enabled" ;;
    *) echo "invalid" ;;
  esac
}

json_escape() {
  printf '%s' "${1:-}" | tr -d '\000-\037' | sed 's/\\/\\\\/g; s/"/\\"/g'
}

# ── Remote probe (one SSH round trip per worker) ────────────────────────────
probe() {
  local user="$1" host="$2" worker_id="$3"
  ssh "${SSH_COMMON[@]}" "$user@$host" bash -s -- "$worker_id" "$ENV_PATH" <<'REMOTE'
set -u
WID="$1"
ENV_FILE="$2"

kv() { printf '%s=%s\n' "$1" "$2"; }

kv worker_id "$WID"
kv env_file "$ENV_FILE"

if sudo -n test -r "$ENV_FILE" 2>/dev/null; then
  kv env_file_readable 1
  file_value="$(sudo -n grep -E '^VELOX_FMP4_STREAM_PROFILE=' "$ENV_FILE" 2>/dev/null \
    | tail -n 1 | cut -d= -f2-)"
  kv env_file_value "$file_value"
  kv env_file_mtime "$(sudo -n stat -c %y "$ENV_FILE" 2>/dev/null | head -n 1)"
else
  kv env_file_readable 0
fi

cid="$(sudo -n docker ps --format '{{.ID}} {{.Names}}' 2>/dev/null \
  | grep 'velox-worker' | awk '{print $1}' | head -n 1)"
kv container_id "$cid"
if [ -n "$cid" ]; then
  kv container_started "$(sudo -n docker inspect --format '{{.State.StartedAt}}' "$cid" 2>/dev/null)"
  container_value="$(sudo -n docker exec "$cid" sh -c 'printf %s "${VELOX_FMP4_STREAM_PROFILE-}"' 2>/dev/null || true)"
  kv container_value "$container_value"
fi
REMOTE
}

# ── Per-worker evaluation ───────────────────────────────────────────────────
get() { printf '%s\n' "$1" | sed -n "s/^$2=//p" | head -n 1; }

declare -a RESULTS=()
ready=0; disabled=0; misconfigured=0; transport=0

for entry in "${WORKERS[@]}"; do
  if [[ ! "$entry" =~ ^[^:]+:[^:]+:[^:]+$ ]]; then
    echo "invalid worker entry (want worker_id:host:ssh_user): $entry" >&2
    exit 2
  fi
  worker_id="${entry%%:*}"
  rest="${entry#*:}"
  host="${rest%%:*}"
  user="${rest#*:}"

  raw="$(probe "$user" "$host" "$worker_id")"
  if [[ -z "$raw" ]]; then
    RESULTS+=("$worker_id|$host|ssh_failed|||INFO|SSH probe returned nothing")
    transport=$((transport + 1))
    continue
  fi

  file_readable="$(get "$raw" env_file_readable)"
  file_value="$(get "$raw" env_file_value)"
  container_id="$(get "$raw" container_id)"
  container_value="$(get "$raw" container_value)"
  container_started="$(get "$raw" container_started)"

  file_state="$(normalize "$file_value")"
  if [[ -z "$container_id" ]]; then
    container_state="missing"
  else
    container_state="$(normalize "$container_value")"
  fi

  state=""
  detail=""
  if [[ "$file_readable" != "1" ]]; then
    state="MISCONFIGURED"
    detail="worker.env unreadable ($ENV_PATH)"
  elif [[ "$file_state" == "invalid" || "$container_state" == "invalid" ]]; then
    state="MISCONFIGURED"
    detail="unrecognised value (file='$file_value' container='$container_value'); the gate treats unknown values as disabled"
  elif [[ "$container_state" == "missing" ]]; then
    state="MISCONFIGURED"
    detail="no running velox-worker container"
  elif [[ "$file_state" == "enabled" && "$container_state" == "enabled" ]]; then
    state="READY"
    detail="gate live (container started $container_started)"
  elif [[ "$file_state" == "disabled" && "$container_state" == "disabled" ]]; then
    state="DISABLED"
    detail="gate intentionally closed (file='${file_value:-empty}' container='${container_value:-empty}')"
  else
    state="MISCONFIGURED"
    detail="drift: worker.env='${file_value:-empty}' but container='${container_value:-empty}'; restart velox-worker.service to activate"
  fi

  case "$state" in
    READY) ready=$((ready + 1)) ;;
    DISABLED) disabled=$((disabled + 1)) ;;
    MISCONFIGURED) misconfigured=$((misconfigured + 1)) ;;
  esac
  RESULTS+=("$worker_id|$host|${file_value:-}|${container_value:-}|$container_id|$state|$detail")
done

# ── Report ──────────────────────────────────────────────────────────────────
if (( JSON == 1 )); then
  printf '{"env_file":"%s","ready":%d,"disabled":%d,"misconfigured":%d,"transport_failures":%d,"workers":[' \
    "$(json_escape "$ENV_PATH")" "$ready" "$disabled" "$misconfigured" "$transport"
  first=1
  for row in "${RESULTS[@]}"; do
    IFS='|' read -r wid host file_value container_value container_id state detail <<<"$row"
    (( first == 1 )) || printf ','
    first=0
    printf '{"worker_id":"%s","host":"%s","env_file_value":"%s","container_value":"%s","container_id":"%s","state":"%s","detail":"%s"}' \
      "$(json_escape "$wid")" "$(json_escape "$host")" "$(json_escape "$file_value")" \
      "$(json_escape "$container_value")" "$(json_escape "$container_id")" \
      "$(json_escape "$state")" "$(json_escape "$detail")"
  done
  printf ']}\n'
else
  printf 'FMP4 STREAM PROFILE GATE — %s\n' "$ENV_PATH"
  printf '%-26s %-18s %-10s %-10s %s\n' WORKER HOST ENV_FILE CONTAINER STATE
  for row in "${RESULTS[@]}"; do
    IFS='|' read -r wid host file_value container_value container_id state detail <<<"$row"
    printf '%-26s %-18s %-10s %-10s %s\n' "$wid" "$host" "${file_value:-<unset>}" "${container_value:-<unset>}" "$state"
    printf '  └─ %s\n' "$detail"
  done
  printf '\nREADY=%d DISABLED=%d MISCONFIGURED=%d SSH_FAILURES=%d\n' "$ready" "$disabled" "$misconfigured" "$transport"
  if (( disabled + misconfigured + transport > 0 )); then
    cat >&2 <<'HINT'
The fMP4 gate is NOT live on every worker. Apply it through the audited path:
  scripts/fleetctl worker-config set <worker_id> --fmp4-stream-profile 1 "<reason>"
Then re-run this script; a MISCONFIGURED "drift" line means the helper already
wrote worker.env and the service restart did not complete.
HINT
  fi
fi

(( transport > 0 )) && exit 2
(( disabled + misconfigured > 0 )) && exit 1
exit 0
