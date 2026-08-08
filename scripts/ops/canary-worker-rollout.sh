#!/usr/bin/env bash
# =============================================================================
# canary-worker-rollout.sh — one-worker serial canary for the canonical worker.
#
# This is deliberately a single-worker operator surface. It never calls
# `fleetctl status` or iterates over an implicit fleet list.
#
# Usage:
#   ./scripts/ops/canary-worker-rollout.sh \
#     --worker-id velox-worker-13197 --dry-run
#   ./scripts/ops/canary-worker-rollout.sh \
#     --worker-id velox-worker-13197 --apply
#   ./scripts/ops/canary-worker-rollout.sh \
#     --worker-id velox-worker-13197 --rollback
#
# Mutations are opt-in. `--dry-run` is read-only; `--apply` updates exactly
# the requested worker; `--rollback` explicitly invokes fleetctl rollback for
# exactly that worker. A failed apply never performs an implicit rollback.
#
# Required for --apply/--rollback:
#   VELOX_ADMIN_TOKEN or the canonical fleetctl token file
#   VELOX_MASTER_URL (or fleetctl's configured master URL)
#   fleetctl in PATH (or FLEETCTL_BIN)
#
# Exit codes:
#   0 success
#   2 usage / missing prerequisite / contract mismatch
#   4 update or rollback operation failed
# =============================================================================

set -euo pipefail

readonly TARGET_IMAGE='ghcr.io/marcuss-ops/velox-worker@sha256:beb1cfc48d4ffb591e954cff0572ede8b9bf36fdd215239f05c5a403b8278415'
readonly TARGET_DIGEST='sha256:beb1cfc48d4ffb591e954cff0572ede8b9bf36fdd215239f05c5a403b8278415'
readonly TARGET_VERSION='v1.2.28-canonical'
readonly SCRIPT_NAME='canary-worker-rollout'

WORKER_ID=''
MASTER_URL="${VELOX_MASTER_URL:-}"
FLEETCTL_BIN="${FLEETCTL_BIN:-fleetctl}"
MODE=''
REASON="canary ${TARGET_VERSION}"

usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# //' >&2
  exit "${1:-0}"
}

fail() {
  printf '%s: ERROR: %s\n' "$SCRIPT_NAME" "$*" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id)
      [[ $# -ge 2 ]] || fail '--worker-id requires a value'
      WORKER_ID="$2"
      shift 2
      ;;
    --master)
      [[ $# -ge 2 ]] || fail '--master requires a value'
      MASTER_URL="$2"
      shift 2
      ;;
    --fleetctl)
      [[ $# -ge 2 ]] || fail '--fleetctl requires a value'
      FLEETCTL_BIN="$2"
      shift 2
      ;;
    --reason)
      [[ $# -ge 2 ]] || fail '--reason requires a value'
      REASON="$2"
      shift 2
      ;;
    --dry-run|--apply|--rollback)
      [[ -z "$MODE" ]] || fail 'choose exactly one of --dry-run, --apply, or --rollback'
      MODE="${1#--}"
      shift
      ;;
    -h|--help)
      usage 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

[[ -n "$WORKER_ID" ]] || fail '--worker-id is required; no worker is selected implicitly'
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] || fail "invalid worker ID: $WORKER_ID"
[[ -n "$MODE" ]] || fail 'one explicit mode is required: --dry-run, --apply, or --rollback'
[[ "$REASON" != *$'\n'* && "$REASON" != *$'\r'* ]] || fail '--reason must not contain newlines'

if [[ -n "$MASTER_URL" ]]; then
  MASTER_URL="${MASTER_URL%/}"
fi

print_contract() {
  printf '=== %s ===\n' "$SCRIPT_NAME"
  printf '  worker_id:     %s\n' "$WORKER_ID"
  printf '  target_image:  %s\n' "$TARGET_IMAGE"
  printf '  target_digest: %s\n' "$TARGET_DIGEST"
  printf '  target_version: %s\n' "$TARGET_VERSION"
  printf '  mode:          %s\n' "$MODE"
  if [[ -n "$MASTER_URL" ]]; then
    printf '  master_url:    %s\n' "$MASTER_URL"
  fi
}

fleetctl() {
  if [[ -n "$MASTER_URL" ]]; then
    VELOX_MASTER_URL="$MASTER_URL" "$FLEETCTL_BIN" "$@"
  else
    "$FLEETCTL_BIN" "$@"
  fi
}

require_runtime() {
  command -v "$FLEETCTL_BIN" >/dev/null 2>&1 || fail "fleetctl not found: $FLEETCTL_BIN"
  command -v jq >/dev/null 2>&1 || fail 'jq is required for WorkerCard verification'
}

card_value() {
  local body="$1" expression="$2"
  jq -r "$expression // empty" <<<"$body" 2>/dev/null || true
}

card_digest() {
  local body="$1" value
  value="$(card_value "$body" '.image_digest // .release_identity.image_digest')"
  if [[ "$value" == *@* ]]; then
    value="${value##*@}"
  fi
  printf '%s' "$value"
}

card_version() {
  local body="$1"
  card_value "$body" '.software_version // .release_identity.software_version'
}

verify_healthy_card() {
  local body="$1" phase="$2" digest version status health active
  [[ -n "$body" ]] || fail "$phase WorkerCard is empty"
  jq -e . <<<"$body" >/dev/null 2>&1 || fail "$phase WorkerCard is not valid JSON"
  digest="$(card_digest "$body")"
  version="$(card_version "$body")"
  status="$(card_value "$body" '.status')"
  health="$(card_value "$body" '.health // .health_state')"
  active="$(card_value "$body" '.active_jobs // .active_tasks // .active_slots')"

  [[ "$status" == 'CONNECTED' ]] || fail "$phase worker status is ${status:-<missing>} (expected CONNECTED)"
  [[ "$health" == 'HEALTHY' ]] || fail "$phase worker health is ${health:-<missing>} (expected HEALTHY)"
  [[ "$active" =~ ^0$ ]] || fail "$phase active work is ${active:-<missing>} (expected 0)"
  printf '%s' "$body"
}

inspect_worker() {
  fleetctl inspect "$WORKER_ID"
}

print_explicit_rollback() {
  printf '%s\n' "Explicit rollback command (not run automatically):"
  if [[ -n "$MASTER_URL" ]]; then
    printf '  VELOX_MASTER_URL=%q %q rollback %q --reason %q\n' \
      "$MASTER_URL" "$FLEETCTL_BIN" "$WORKER_ID" "rollback ${TARGET_VERSION} canary"
  else
    printf '  %q rollback %q --reason %q\n' \
      "$FLEETCTL_BIN" "$WORKER_ID" "rollback ${TARGET_VERSION} canary"
  fi
}

print_contract

if [[ "$MODE" == 'dry-run' ]]; then
  printf '[DRY-RUN] no fleetctl command will be executed\n'
  printf '[DRY-RUN] would inspect %s before the canary\n' "$WORKER_ID"
  printf '[DRY-RUN] would update only %s with --digest %s\n' "$WORKER_ID" "$TARGET_DIGEST"
  printf '[DRY-RUN] would inspect digest/version, require CONNECTED/HEALTHY/active=0\n'
  printf '[DRY-RUN] would run Level-D smoke for %s\n' "$WORKER_ID"
  printf '[DRY-RUN] would inspect final reconnect/health state\n'
  print_explicit_rollback
  exit 0
fi

require_runtime

case "$MODE" in
  apply)
    before="$(inspect_worker)" || {
      printf '%s\n' 'Pre-canary inspect failed; no update was requested.' >&2
      exit 4
    }
    verify_healthy_card "$before" 'pre-canary' >/dev/null
    printf 'pre-canary: worker %s is CONNECTED/HEALTHY with active=0\n' "$WORKER_ID"

    if ! fleetctl update "$WORKER_ID" --digest "$TARGET_DIGEST" --reason "$REASON"; then
      printf '%s\n' 'Update operation failed.' >&2
      print_explicit_rollback >&2
      exit 4
    fi

    after_update="$(inspect_worker)" || {
      printf '%s\n' 'Post-update inspect failed.' >&2
      print_explicit_rollback >&2
      exit 4
    }
    verify_healthy_card "$after_update" 'post-update' >/dev/null
    [[ "$(card_digest "$after_update")" == "$TARGET_DIGEST" ]] || {
      printf '%s\n' "post-update digest mismatch (expected ${TARGET_DIGEST})" >&2
      print_explicit_rollback >&2
      exit 4
    }
    [[ "$(card_version "$after_update")" == "$TARGET_VERSION" ]] || {
      printf '%s\n' "post-update version mismatch (expected ${TARGET_VERSION})" >&2
      print_explicit_rollback >&2
      exit 4
    }
    printf 'post-update: digest and version match %s / %s\n' "$TARGET_DIGEST" "$TARGET_VERSION"

    if ! fleetctl smoke "$WORKER_ID"; then
      printf '%s\n' 'Level-D smoke failed.' >&2
      print_explicit_rollback >&2
      exit 4
    fi

    final="$(inspect_worker)" || {
      printf '%s\n' 'Final reconnect inspect failed.' >&2
      print_explicit_rollback >&2
      exit 4
    }
    verify_healthy_card "$final" 'final' >/dev/null
    [[ "$(card_digest "$final")" == "$TARGET_DIGEST" ]] || {
      printf '%s\n' "final digest mismatch (expected ${TARGET_DIGEST})" >&2
      print_explicit_rollback >&2
      exit 4
    }
    [[ "$(card_version "$final")" == "$TARGET_VERSION" ]] || {
      printf '%s\n' "final version mismatch (expected ${TARGET_VERSION})" >&2
      print_explicit_rollback >&2
      exit 4
    }
    printf 'CANARY SUCCEEDED: one worker %s is CONNECTED/HEALTHY on %s (%s)\n' \
      "$WORKER_ID" "$TARGET_VERSION" "$TARGET_DIGEST"
    ;;

  rollback)
    before="$(inspect_worker)" || {
      printf '%s\n' 'Pre-rollback inspect failed; no rollback was requested.' >&2
      exit 4
    }
    pre_rollback_digest="$(card_digest "$before")"
    pre_rollback_version="$(card_version "$before")"
    [[ "$pre_rollback_digest" == "$TARGET_DIGEST" ]] || fail "pre-rollback digest is ${pre_rollback_digest:-<missing>}; expected active canary digest"
    [[ "$pre_rollback_version" == "$TARGET_VERSION" ]] || fail "pre-rollback version is ${pre_rollback_version:-<missing>}; expected ${TARGET_VERSION}"
    printf 'pre-rollback: worker %s is on the canary digest/version\n' "$WORKER_ID"
    if ! fleetctl rollback "$WORKER_ID" --reason "rollback ${TARGET_VERSION} canary"; then
      printf '%s\n' 'Explicit rollback operation failed.' >&2
      exit 4
    fi
    after_rollback="$(inspect_worker)" || {
      printf '%s\n' 'Post-rollback inspect failed.' >&2
      exit 4
    }
    verify_healthy_card "$after_rollback" 'post-rollback' >/dev/null
    rollback_digest="$(card_digest "$after_rollback")"
    rollback_version="$(card_version "$after_rollback")"
    [[ -n "$rollback_digest" && "$rollback_digest" != "$TARGET_DIGEST" ]] || fail 'rollback completed but target canary digest is still active or missing'
    [[ "$rollback_digest" != "$pre_rollback_digest" ]] || fail 'rollback did not change the active digest'
    [[ "$rollback_version" != "$TARGET_VERSION" ]] || fail 'rollback completed but target canary version is still active'
    printf 'ROLLBACK SUCCEEDED: one worker %s returned from %s to previous digest %s (version %s)\n' \
      "$WORKER_ID" "$TARGET_DIGEST" "$rollback_digest" "$rollback_version"
    ;;

  *)
    fail "unsupported mode: $MODE"
    ;;
esac
