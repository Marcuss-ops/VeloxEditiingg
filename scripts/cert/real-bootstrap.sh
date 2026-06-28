#!/usr/bin/env bash
# =============================================================================
# scripts/cert/real-bootstrap.sh
# =============================================================================
# Worker image certification — Phase 1 / cap. 2 of 100% Velox plan.
#
# Runs the actual published worker image (no fake FFmpeg/ffprobe, no
# stub render client) under production deps baked into the image and
# verifies that the [BOOTSTRAP_REPORT] produced by the binary's
# --bootstrap-report mode contains verdict=OK plus all 4 canonical
# step PASS:
#
#   1. bundle_hash        (RW-PROD-003 A8)
#   2. ffmpeg             (RW-PROD-003 A3 — ffmpeg/ffprobe + libx264)
#   3. output_dir         (RW-PROD-003 A4 — OutputDir writable)
#   4. engine_self_render (RW-PROD-003 A1+A2 — C++ render SHA baseline)
#
# Required env (or matching CLI flags):
#   WORKER_IMAGE              full @sha256:... digest ref (NEVER :latest)
#   EXPECTED_BUNDLE_HASH      64-hex; asserted vs container's BUNDLE_HASH.txt
#   EVIDENCE_ROOT             (default: $HOME/evidence)
#   CERT_DATE                 (default: today UTC)
#   WORKER_ID                 (default: real-bootstrap-certifier-<iso>)
#
# Output evidence files (side-loaded by cap. 11 packaging):
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/bootstrap-report.json
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/container-stdout.log
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/container-stderr.log
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/verdict.json
#
# Exit: 0 if verdict=OK + 4 step PASS; 1 otherwise.
# =============================================================================

set -uo pipefail  # NOT -e: continue across checks so all failures report

usage() {
  cat <<USG
usage: $0 [--worker-image REF] [--expected-bundle-hash HEX] [--worker-id ID]
          [--date YYYY-MM-DD] [--evidence-root DIR] [--help]

All flags have env-var equivalents (WORKER_IMAGE, EXPECTED_BUNDLE_HASH,
WORKER_ID, CERT_DATE, EVIDENCE_ROOT).
USG
  exit "${1:-0}"
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-image)         WORKER_IMAGE="$2"; shift 2 ;;
    --expected-bundle-hash) EXPECTED_BUNDLE_HASH="$2"; shift 2 ;;
    --worker-id)            WORKER_ID="$2"; shift 2 ;;
    --date)                 CERT_DATE="$2"; shift 2 ;;
    --evidence-root)        EVIDENCE_ROOT="$2"; shift 2 ;;
    --help|-h)              usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; exit 1 ;;
  esac
done

# ─── Sanity ─────────────────────────────────────────────────────────────────
if [[ -z "${WORKER_IMAGE:-}" ]]; then
  printf '::error::WORKER_IMAGE is required (flag or env)\n' >&2
  usage 1
fi
if ! [[ "$WORKER_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]; then
  printf '::error::WORKER_IMAGE must be a digest pin (got: %s) — never :latest\n' \
    "$WORKER_IMAGE" >&2
  exit 1
fi
if [[ -z "${EXPECTED_BUNDLE_HASH:-}" ]]; then
  printf '::error::EXPECTED_BUNDLE_HASH is required (the SHA-256 hex string of the published BUNDLE_HASH.txt)\n' >&2
  exit 1
fi
if [[ ! "${EXPECTED_BUNDLE_HASH}" =~ ^[a-f0-9]{64}$ ]]; then
  printf '::error::EXPECTED_BUNDLE_HASH must be 64 lowercase hex chars (got %d chars)\n' \
    "${#EXPECTED_BUNDLE_HASH}" >&2
  exit 1
fi

EVIDENCE_ROOT="${EVIDENCE_ROOT:-$HOME/evidence}"
CERT_DATE="${CERT_DATE:-$(date -u +%Y-%m-%d)}"
WORKER_ID="${WORKER_ID:-real-bootstrap-certifier-$(date -u +%Y%m%dT%H%M%SZ)}"
EV_DIR="$EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID"
mkdir -p "$EV_DIR"

if ! command -v docker >/dev/null 2>&1; then
  printf '::error::docker not found on PATH\n' >&2
  exit 2
fi
if ! docker info >/dev/null 2>&1; then
  printf '::error::docker daemon unreachable\n' >&2
  exit 2
fi

# ─── Context: docker might not have the image locally yet ──────────────────
printf '→ ensuring image is present: %s\n' "$WORKER_IMAGE"
if ! docker image inspect "$WORKER_IMAGE" >/dev/null 2>&1; then
  printf '→ image missing locally; pulling\n'
  if ! docker pull "$WORKER_IMAGE" >/dev/null 2>&1; then
    printf '::error::docker pull failed for %s\n' "$WORKER_IMAGE" >&2
    exit 3
  fi
fi

# ─── Run container in --bootstrap-report mode ───────────────────────────────
printf '\n═══ real-bootstrap run for %s ═══\n' "$WORKER_IMAGE"
WORKDIR_HOST="/tmp/velox-real-bootstrap/$WORKER_ID"
mkdir -p "$WORKDIR_HOST"

set -a
EXPECTED_BUNDLE_HASH="$EXPECTED_BUNDLE_HASH" \
VELOX_WORKER_ID="$WORKER_ID" \
VELOX_GRPC_MASTER_URL="${VELOX_GRPC_MASTER_URL:-127.0.0.1:9999}" \
set +a

# 60s budget covers ffmpeg probe + C++ render (RW-PROD-003 A5 says ≤5s;
# we add buffer for first-run container cold start).
if command -v timeout >/dev/null 2>&1; then
  timeout 60s docker run --rm \
    --name "real-bootstrap-${WORKER_ID}" \
    -e VELOX_WORKER_ID="$WORKER_ID" \
    -e VELOX_BUNDLE_HASH="$EXPECTED_BUNDLE_HASH" \
    -e "VELOX_GRPC_MASTER_URL=${VELOX_GRPC_MASTER_URL:-127.0.0.1:9999}" \
    -v "$WORKDIR_HOST:/var/lib/velox-worker" \
    "$WORKER_IMAGE" \
    --bootstrap-report \
    > "$EV_DIR/container-stdout.log" 2> "$EV_DIR/container-stderr.log"
  rc=$?
  if (( rc == 124 )); then
    printf '::error::docker run timed out after 60s\n' >&2
    exit 4
  fi
else
  docker run --rm \
    --name "real-bootstrap-${WORKER_ID}" \
    -e VELOX_WORKER_ID="$WORKER_ID" \
    -e VELOX_BUNDLE_HASH="$EXPECTED_BUNDLE_HASH" \
    -e "VELOX_GRPC_MASTER_URL=${VELOX_GRPC_MASTER_URL:-127.0.0.1:9999}" \
    -v "$WORKDIR_HOST:/var/lib/velox-worker" \
    "$WORKER_IMAGE" \
    --bootstrap-report \
    > "$EV_DIR/container-stdout.log" 2> "$EV_DIR/container-stderr.log"
  rc=$?
fi

printf '→ docker run exit code: %s\n' "$rc"

# ─── Capture contents for post-mortem regardless of verdict ────────────────
{
  echo "═══ docker run ═══"
  echo "WORKER_IMAGE  = $WORKER_IMAGE"
  echo "EXPECTED_HASH = ${EXPECTED_BUNDLE_HASH:0:16}..."
  echo "WORKER_ID     = $WORKER_ID"
  echo "EXIT_CODE     = $rc"
  echo
  echo "═══ stdout ═══"
  cat "$EV_DIR/container-stdout.log"
  echo
  echo "═══ stderr ═══"
  cat "$EV_DIR/container-stderr.log"
} > "$EV_DIR/dump.txt"

# ─── Parse [BOOTSTRAP_REPORT] JSON block ───────────────────────────────────
# bootstrap.DumpReport writes a single line "[BOOTSTRAP_REPORT]" then the
# multi-line JSON, then a newline. We extract by line-marker to avoid
# coupling to the exact JSON shape.
JSON="$(awk '
  /^\[BOOTSTRAP_REPORT\]$/          { capturing=1; next }
  capturing && /^\[/ && !/^\[BOOTSTRAP_REPORT\]$/ { exit }
  capturing                          { print }
' "$EV_DIR/container-stderr.log")"

if [[ -z "$JSON" ]]; then
  printf '::error::no [BOOTSTRAP_REPORT] JSON block in container stderr\n' >&2
  printf '  → see %s/dump.txt\n' "$EV_DIR" >&2
  exit 5
fi

printf '%s' "$JSON" > "$EV_DIR/bootstrap-report.json"

# ─── Assert verdict + 4 step PASS ──────────────────────────────────────────
python3 - "$JSON" "$EXPECTED_BUNDLE_HASH" "$WORKER_ID" "$CERT_DATE" \
        "$WORKER_IMAGE" "$rc" "$EV_DIR/verdict.json" <<'PYEOF'
import json, sys, os
json_text, expected_hash, worker_id, cert_date, worker_image, rc, verdict_path = sys.argv[1:]
try:
    d = json.loads(json_text)
except Exception as e:
    print(f"::error::JSON parse failed: {e}")
    sys.exit(6)

verdict = d.get("verdict", "UNKNOWN")
steps = {s.get("name"): s.get("status") for s in d.get("steps", [])}
required = ("bundle_hash", "ffmpeg", "output_dir", "engine_self_render")
missing = [r for r in required if r not in steps]
bad = [r for r in required if r not in missing and steps.get(r) != "OK"]

ok = (verdict == "OK") and (not missing) and (not bad)
final_status = "PASS" if ok else "FAIL"

result = {
    "worker_id":     worker_id,
    "cert_date":     cert_date,
    "worker_image":  worker_image,
    "expected_bundle_hash": expected_hash,
    "container_exit_code": int(rc),
    "verdict":       verdict,
    "steps":         steps,
    "required_steps": list(required),
    "missing_steps": missing,
    "failed_steps":  bad,
    "final_status":  final_status,
    "evidence_dir":  os.path.dirname(verdict_path),
}
with open(verdict_path, "w") as f:
    json.dump(result, f, indent=2, sort_keys=True)
print(json.dumps(result, indent=2, sort_keys=True))

if not ok:
    sys.exit(7)
PYEOF

printf '\n✓ real-bootstrap PASS — evidence at %s\n' "$EV_DIR"
exit 0
