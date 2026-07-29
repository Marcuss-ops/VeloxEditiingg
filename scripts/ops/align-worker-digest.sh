#!/usr/bin/env bash
# =============================================================================
# scripts/ops/align-worker-digest.sh — Align a worker's Docker image digest
# to the fleet baseline.
# =============================================================================
# Usage:
#   ./scripts/ops/align-worker-digest.sh \
#       --worker-id velox-worker-13197 \
#       --digest sha256:a1774003... \
#       [--registry ghcr.io] \
#       [--owner <ghcr-org>] \
#       [--image-name velox-worker] \
#       [--skip-pin] \
#       [--dry-run]
#
# What it does:
#   1. Pre-flight: validates digest format, checks fleetctl/gh/cosign deps.
#   2. Builds fleetctl if not already present.
#   3. Runs `make pin-worker-digest` to cosign-verify + record the target
#      digest as a trusted baseline (unless --skip-pin).
#   4. Runs `fleetctl update <worker_id> --digest sha256:...` to trigger
#      the image update cascade on the target worker.
#   5. Runs `fleetctl status` to verify all workers have the same digest.
#   6. Reports diff if any worker still has a different digest.
#
# Env vars (alternative to flags):
#   VELOX_ADMIN_TOKEN   — operator admin token for fleetctl
#   VELOX_MASTER_URL    — Master API URL (default http://127.0.0.1:8080)
#   DIGEST              — target digest for make pin-worker-digest
#   GHCR_OWNER          — GHCR org/user for full image ref
#
# Exit codes:
#   0 — all workers aligned to the same digest
#   2 — usage / missing deps
#   3 — pin-worker-digest failed
#   4 — fleetctl update failed
#   5 — post-update digest mismatch (some workers still on old digest)
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# //'
  exit "${1:-0}"
}

# ─── Args ──────────────────────────────────────────────────────────────────
WORKER_ID=""
TARGET_DIGEST=""
REGISTRY="${REGISTRY:-ghcr.io}"
GHCR_OWNER="${GHCR_OWNER:-}"
IMAGE_NAME="${IMAGE_NAME:-velox-worker}"
SKIP_PIN=0
DRY_RUN=0
ALL_WORKERS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id)    WORKER_ID="$2"; shift 2 ;;
    --digest)       TARGET_DIGEST="$2"; shift 2 ;;
    --registry)     REGISTRY="$2"; shift 2 ;;
    --owner)        GHCR_OWNER="$2"; shift 2 ;;
    --image-name)   IMAGE_NAME="$2"; shift 2 ;;
    --workers)      ALL_WORKERS+=("$2"); shift 2 ;;
    --skip-pin)     SKIP_PIN=1; shift ;;
    --dry-run)      DRY_RUN=1; shift ;;
    -h|--help)      usage 0 ;;
    *)              echo "ERROR: unknown flag: $1" >&2; usage 2 ;;
  esac
done

# Default worker fleet if none specified.
if (( ${#ALL_WORKERS[@]} == 0 )); then
  ALL_WORKERS=(
    "host_57_129_132_133"
    "host_57_131_20_173"
    "velox-worker-13197"
    "velox-worker-523925eb"
  )
fi

# ─── Validate ───────────────────────────────────────────────────────────────
[[ -n "${WORKER_ID:-}" ]] || { echo "ERROR: --worker-id is required" >&2; usage 2; }
[[ -n "${TARGET_DIGEST:-}" ]] || TARGET_DIGEST="${DIGEST:-}"
[[ -n "${TARGET_DIGEST:-}" ]] || { echo "ERROR: --digest is required (or set DIGEST env)" >&2; usage 2; }

# Normalise: accept both sha256:<hex> and full ghcr.io/...@sha256:<hex>
if [[ "$TARGET_DIGEST" =~ @(sha256:[a-f0-9]{64})$ ]]; then
  FULL_DIGEST="$TARGET_DIGEST"
  SHA_ONLY="${BASH_REMATCH[1]}"
elif [[ "$TARGET_DIGEST" =~ ^(sha256:[a-f0-9]{64})$ ]]; then
  SHA_ONLY="$TARGET_DIGEST"
  # Need --owner to build full ref.
  if [[ -z "${GHCR_OWNER:-}" ]]; then
    echo "ERROR: --owner is required when --digest is sha256:<hex> only (need full ghcr.io/<owner>/<image>@sha256:...)" >&2
    exit 2
  fi
  FULL_DIGEST="${REGISTRY}/${GHCR_OWNER}/${IMAGE_NAME}@${SHA_ONLY}"
else
  echo "ERROR: --digest must be sha256:<64hex> or <registry>/<owner>/<name>@sha256:<64hex>" >&2
  echo "       got: $TARGET_DIGEST" >&2
  exit 2
fi

VELOX_MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8080}"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"

echo "=== align-worker-digest ==="
echo "  worker_id:     $WORKER_ID"
echo "  target_digest: $FULL_DIGEST"
echo "  sha_only:      $SHA_ONLY"
echo "  master_url:    $VELOX_MASTER_URL"
echo "  skip_pin:      $SKIP_PIN"
echo "  dry_run:       $DRY_RUN"
echo ""

# ─── Pre-flight: prerequisites ──────────────────────────────────────────────
need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: missing required binary: $1" >&2
    exit 2
  fi
}

if (( DRY_RUN )); then
  echo "[DRY-RUN] would align $WORKER_ID to $SHA_ONLY"
  echo "[DRY-RUN] 1. make pin-worker-digest DIGEST=$FULL_DIGEST"
  echo "[DRY-RUN] 2. fleetctl update $WORKER_ID --digest $SHA_ONLY"
  echo "[DRY-RUN] 3. fleetctl status (verify all workers on $SHA_ONLY)"
  exit 0
fi

# Auth token required for fleetctl.
if [[ -z "${VELOX_ADMIN_TOKEN:-}" ]]; then
  if [[ -r "/opt/velox/secrets/admin-token" ]]; then
    VELOX_ADMIN_TOKEN="$(cat /opt/velox/secrets/admin-token)"
  else
    echo "ERROR: VELOX_ADMIN_TOKEN not set and /opt/velox/secrets/admin-token not readable" >&2
    exit 2
  fi
fi
export VELOX_ADMIN_TOKEN

# ─── Build fleetctl if missing ──────────────────────────────────────────────
FLEETCTL_BIN="${REPO_ROOT}/DataServer/bin/fleetctl"
if ! command -v fleetctl >/dev/null 2>&1 && [[ ! -x "$FLEETCTL_BIN" ]]; then
  echo "→ building fleetctl ..."
  (cd "$REPO_ROOT/DataServer" && go build -o bin/fleetctl ./cmd/fleetctl/) || {
    echo "ERROR: failed to build fleetctl" >&2
    exit 2
  }
  FLEETCTL="$FLEETCTL_BIN"
elif command -v fleetctl >/dev/null 2>&1; then
  FLEETCTL="fleetctl"
else
  FLEETCTL="$FLEETCTL_BIN"
fi
echo "  fleetctl: $FLEETCTL"

need jq

# ─── Step 1: Pin the target digest ─────────────────────────────────────────
if (( SKIP_PIN == 0 )); then
  echo ""
  echo "--- Step 1: pin-worker-digest ---"
  need gh
  need cosign
  need python3

  if ! gh auth status >/dev/null 2>&1; then
    echo "ERROR: gh is not authenticated; run 'gh auth login' first" >&2
    exit 2
  fi

  echo "→ pinning $FULL_DIGEST ..."
  DIGEST="$FULL_DIGEST" make -C "$REPO_ROOT" pin-worker-digest || {
    echo "ERROR: pin-worker-digest failed for $FULL_DIGEST" >&2
    exit 3
  }
  echo "✓ digest pinned successfully"
else
  echo ""
  echo "--- Step 1: pin-worker-digest (SKIPPED) ---"
fi

# ─── Step 2: Update the worker ─────────────────────────────────────────────
echo ""
echo "--- Step 2: fleetctl update $WORKER_ID ---"

echo "→ triggering update to $SHA_ONLY ..."
"$FLEETCTL" --master="$VELOX_MASTER_URL" update "$WORKER_ID" --digest "$SHA_ONLY" \
  --reason="align-worker-digest: unify fleet to $SHA_ONLY" || {
  echo "ERROR: fleetctl update failed" >&2
  exit 4
}
echo "✓ update cascade completed"

# ─── Step 3: Verify fleet digest uniformity ────────────────────────────────
echo ""
echo "--- Step 3: fleetctl inspect (verify digest uniformity) ---"
echo "→ fetching per-worker image_digest ..."
declare -A WORKER_DIGESTS
MISMATCH=0
INSPECT_FAILS=0

for w in "${ALL_WORKERS[@]}"; do
  INSPECT="$("$FLEETCTL" --master="$VELOX_MASTER_URL" inspect "$w" 2>/dev/null)" || {
    echo "  ⚠ $w → INSPECT_FAILED (fleetctl returned non-zero)"
    WORKER_DIGESTS["$w"]="INSPECT_FAILED"
    INSPECT_FAILS=$((INSPECT_FAILS + 1))
    continue
  }
  # Distinguish: inspect returned valid JSON but no image_digest field
  # vs. inspect returned something not even parseable.
  if ! echo "$INSPECT" | jq -e '.worker_id' >/dev/null 2>&1; then
    echo "  ⚠ $w → INSPECT_MALFORMED (response not a valid WorkerCard)"
    WORKER_DIGESTS["$w"]="INSPECT_MALFORMED"
    INSPECT_FAILS=$((INSPECT_FAILS + 1))
    continue
  fi
  WDIGEST="$(echo "$INSPECT" | jq -r '.image_digest // ""' 2>/dev/null || echo "")"
  if [[ -z "$WDIGEST" ]]; then
    WDIGEST="NO_DIGEST"
  fi
  WORKER_DIGESTS["$w"]="$WDIGEST"

  if [[ "$WDIGEST" != "$SHA_ONLY" ]]; then
    MISMATCH=1
    echo "  ✗ $w → $WDIGEST  (expected $SHA_ONLY)"
  else
    echo "  ✓ $w → $WDIGEST"
  fi
done

echo ""

if (( MISMATCH == 1 )); then
  echo "=== RESULT: DIGEST MISMATCH ==="
  echo "Some workers are still on a different digest."
  echo "This may be transient — the update cascade might still be converging."
  echo "Re-run 'fleetctl status' in 30-60s to confirm."
if (( INSPECT_FAILS > 0 )); then
    echo "WARNING: $INSPECT_FAILS worker(s) could not be inspected (network/auth errors)"
  fi
  echo ""
  echo "Summary:"
  for w in "${ALL_WORKERS[@]}"; do
    printf "  %-30s %s\n" "$w" "${WORKER_DIGESTS[$w]}"
  done
  exit 5
fi

if (( INSPECT_FAILS > 0 )); then
  echo "WARNING: $INSPECT_FAILS worker(s) could not be inspected — results may be incomplete"
fi
echo "=== RESULT: ALL WORKERS ALIGNED ==="
echo "All ${#ALL_WORKERS[@]} workers on digest $SHA_ONLY"
echo ""
echo "Summary:"
for w in "${ALL_WORKERS[@]}"; do
  printf "  %-30s %s\n" "$w" "${WORKER_DIGESTS[$w]}"
done
exit 0
