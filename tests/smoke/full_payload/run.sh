#!/usr/bin/env bash
# =============================================================================
# tests/smoke/full_payload/run.sh — Reusable submitter for the post-2026-07-28
# full-payload smoke matrix.
# =============================================================================
# Submits tests/smoke/full_payload/fixtures/scenario.json (4 scene, ASS+SRT
# subtitle_tracks, special name + important phrase + highlighted word layers,
# destination comedy_test, executor scene.composite.v1@1) to the Velox
# Master's POST /api/v1/jobs endpoint. Source-of-truth for shape:
#   - docs/operations/04-veloxediting-final-smoke-checklist.md §4 (layers example)
#   - DataServer/internal/apiwire/apiwire.go SubmitJobRequest
#   - DataServer/internal/jobs/enqueue/enqueue_normalization_test.go
#     (subtitle_tracks + scene.voiceover nested)
#   - tests/worker-cert/smoke_one.sh (canonical M2M + POST + poll plumbing)
#
# Modes:
#   --mode=submit   (default) full HTTP flow: mint M2M → POST → poll until
#                   SUCCEEDED → write tests/smoke/full_payload/evidence/
#                   run-<EPOCH>-<client_id>.json (collision-safe)
#   --mode=dry      build substituted payload + jq summary to stdout; no HTTP.
#                   Used in CI matrix rows that pre-flight the wire shape
#                   without reaching the master.
#   --mode=selftest build + assert_no_forbidden (forbidden-pattern walk via
#                   jq). no HTTP. Exits 9 on any forbidden-pattern hit. This
#                   is the script's regression shield against accidentally
#                   adding path-form asset refs (velox-asset://<kind>/<file>.<ext>
#                   or file://) — both forbidden by intake_validation.go
#                   §manifestRefURLRegexp.
#
# Exit codes (merged with scripts/api/jobs_smoke.sh semantics for 0/2/3/4/5/6/7/8
#              + 9 NEW for selftest::forbidden):
#   0  success — submit: SUCCEEDED + evidence written; dry/selftest: built OK.
#   2  usage / env (missing admin token, scenario.json unreadable/invalid, no curl/jq).
#   3  network (curl could not reach the master during M2M provision or POST/GET).
#   4  HTTP non-201 on M2M provisioning OR HTTP non-202 on POST.
#   5  POST 202 received but .job_id missing in body.
#   6  terminal-fail state FAILED/CANCELLED reached during poll.
#   7  poll timeout without reaching terminal state (full timeout consumed).
#   8  HTTP non-200 on GET during polling.
#   9  selftest::forbidden-pattern hit (script regression or scenario.json drift).
#
# Environment:
#   VELOX_MASTER_URL                 master base URL (default: http://127.0.0.1:8080)
#   VELOX_ADMIN_TOKEN                admin bearer for /api/v1/admin/m2m/keys
#                                    (set this OR TOKEN_FILE)
#   TOKEN_FILE                       dotenv alternative for VELOX_ADMIN_TOKEN
#   FULLPAYLOAD_IDEM_KEY             idempotency_key override
#                                    (default: full-payload-4-scenes-<EPOCH>)
#   FULLPAYLOAD_DESTINATION_ID       destination_id override (default: comedy_test)
#   FULLPAYLOAD_TARGET_EXECUTOR_ID   target_executor_id override
#                                    (default: scene.composite.v1@1)
#   FULLPAYLOAD_SCENARIO             scenario.json path override
#                                    (default: <self-dir>/fixtures/scenario.json)
#   FULLPAYLOAD_EVIDENCE_DIR         evidence output dir
#                                    (default: <self-dir>/evidence)
#   FULLPAYLOAD_POLL_TIMEOUT_S       polling cap seconds (default: 240)
#
# Usage:
#   tests/smoke/full_payload/run.sh
#   VELOX_MASTER_URL=https://velox.example.com \
#     VELOX_ADMIN_TOKEN=... \
#     tests/smoke/full_payload/run.sh
#   tests/smoke/full_payload/run.sh --mode=dry
#   tests/smoke/full_payload/run.sh --mode=selftest
#   tests/smoke/full_payload/run.sh --help
# =============================================================================

set -uo pipefail  # NOT -e (mirror smoke_one.sh: keep going through polling)

# tests/smoke/full_payload/ → ../.. tests → ../../.. project root.
# The depth is intentional (one more level than smoke_one.sh's tests/worker-cert/)
# and is unit-tested by the selftest mode at the end of the script.
REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ─── Source cross-test + smoke-local helpers ──────────────────────────────
# Order matters: _lib.sh exposes ensure_* / log_* used by pluck.sh.
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
source "${REPO_ROOT}/tests/worker-cert/lib/pluck.sh"

# ─── Args / env ────────────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && usage

MODE="submit"
case "${1:-}" in
  --mode=submit)   MODE="submit"   ; shift ;;
  --mode=dry)      MODE="dry"      ; shift ;;
  --mode=selftest) MODE="selftest" ; shift ;;
  "")              : ;;
  *)
    log_error "unknown argument: ${1:-} (use --mode=submit|dry|selftest, or --help)"
    usage
    ;;
esac

for bin in curl jq; do
  ensure_command_available "$bin" || { log_error "${bin} missing on PATH"; exit 2; }
done

# Defaults (overridable via env).
: "${VELOX_MASTER_URL:=http://127.0.0.1:8080}"
: "${FULLPAYLOAD_DESTINATION_ID:=comedy_test}"
: "${FULLPAYLOAD_TARGET_EXECUTOR_ID:=scene.composite.v1@1}"
: "${FULLPAYLOAD_SCENARIO:=${SCRIPT_DIR}/fixtures/scenario.json}"
: "${FULLPAYLOAD_EVIDENCE_DIR:=${SCRIPT_DIR}/evidence}"
: "${FULLPAYLOAD_POLL_TIMEOUT_S:=240}"

VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
FULLPAYLOAD_DESTINATION_ID="${FULLPAYLOAD_DESTINATION_ID%/}"
SCENARIO_FILE="${FULLPAYLOAD_SCENARIO}"
EVIDENCE_DIR="${FULLPAYLOAD_EVIDENCE_DIR}"
POLL_TIMEOUT_FULL="${FULLPAYLOAD_POLL_TIMEOUT_S}"

# Validate scenario.json is readable + valid JSON up-front; the downstream
# jq read would otherwise fail with a cryptic error mid-pipeline.
[[ -r "$SCENARIO_FILE" ]] || { log_error "scenario file not readable: ${SCENARIO_FILE}"; exit 2; }
jq -e . "$SCENARIO_FILE" >/dev/null 2>&1 || { log_error "scenario file is not valid JSON: ${SCENARIO_FILE}"; exit 2; }

EPOCH=$(date +%s)
IDEM_KEY="${FULLPAYLOAD_IDEM_KEY:-full-payload-4-scenes-${EPOCH}}"

log_info "mode=${MODE} master=${VELOX_MASTER_URL} scenario=${SCENARIO_FILE} destination=${FULLPAYLOAD_DESTINATION_ID} executor=${FULLPAYLOAD_TARGET_EXECUTOR_ID} idem=${IDEM_KEY}"

# ─── Build substituted payload (jq, NEVER sed on JSON) ────────────────────
# Substitute idempotency_key / target_executor_id / delivery_plan[0].destination_id
# via jq field assignment; this keeps the rest of the JSON structurally
# intact even if free-form fields (text-, preset-) contain quotes/backslashes.
SCENARIO_BODY="$(cat "$SCENARIO_FILE")"

build_payload() {
  jq \
    --arg idem "$IDEM_KEY" \
    --arg dest "$FULLPAYLOAD_DESTINATION_ID" \
    --arg exec_id "$FULLPAYLOAD_TARGET_EXECUTOR_ID" \
    '.idempotency_key = $idem
     | .target_executor_id = $exec_id
     | (.delivery_plan[0].destination_id = $dest)' \
    <<<"$SCENARIO_BODY"
}

# ─── Forbidden-pattern self-check (defensive; scenario never emits by construction) ─
# Walks JSON leaves-of-type-string via jq recursive descent; tests each one
# against the authoritative FORBIDDEN_RX. Mirrors build_real_payload.py
# §FORBIDDEN_PATTERNS (without the local-path branch — that runs on the
# server in intake_validation.go §manifestRefURLRegexp). Exits 9 on hit.
FORBIDDEN_RX='velox-asset://(voiceovers|clips|subtitles|images)/[A-Za-z0-9._-]+\\.[A-Za-z0-9]+|file://'

assert_no_forbidden() {
  local payload_json="$1" hits_json count
  hits_json="$(jq \
    --arg rx "$FORBIDDEN_RX" \
    '[.. | select(type == "string")] | map(select(test($rx))) | {count: length, hits: .}' \
    <<<"$payload_json")"
  count=$(printf '%s' "$hits_json" | jq -er '.count')
  if (( count > 0 )); then
    log_error "self-check: ${count} forbidden pattern(s) detected in payload:"
    printf '%s' "$hits_json" | jq -er '.hits[]' | sed 's/^/  - /' >&2
    return 9
  fi
  return 0
}

PAYLOAD="$(build_payload)"
assert_no_forbidden "$PAYLOAD" || exit 9

# ─── selftest mode short-circuit ───────────────────────────────────────────
if [[ "$MODE" == "selftest" ]]; then
  echo "──── FULL-PAYLOAD SELFTEST (mode=selftest, no HTTP) ────"
  printf '%s' "$PAYLOAD" | jq '{schema, idempotency_key, video_name, project_id, target_executor_id,
                                scene_count: (.scenes | length),
                                scenes_kinds: [.scenes[] | .kind],
                                voiceover_paths: .voiceover_paths,
                                subtitle_tracks_count: (.subtitle_tracks | length),
                                subtitle_tracks_formats: [.subtitle_tracks[] | .format],
                                layer_count: (.layers | length),
                                layer_roles: [.layers[] | .role],
                                delivery_plan: .delivery_plan}'
  log_info "selftest OK"
  exit 0
fi

# ─── dry mode short-circuit ───────────────────────────────────────────────
if [[ "$MODE" == "dry" ]]; then
  echo "──── FULL-PAYLOAD DRY RUN (mode=dry, no HTTP) ────"
  printf '%s' "$PAYLOAD" | jq '{idempotency_key, video_name, project_id, target_executor_id,
                               num_scenes: (.scenes | length),
                               num_layers: (.layers | length),
                               num_subtitle_tracks: (.subtitle_tracks | length),
                               num_voiceover_paths: (.voiceover_paths | length),
                               delivery_plan: .delivery_plan}'
  exit 0
fi

# ─── submit mode: full HTTP flow ──────────────────────────────────────────
# Resolve admin token (env > TOKEN_FILE dotenv). Mirrors scripts/api/jobs_smoke.sh
# §resolve_token so an operator can reuse one TOKEN_FILE across smokes.
resolve_token() {
  local v=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    v="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    v=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$v" ]]; then
    log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided / unreadable"
    return 2
  fi
  if [[ "$v" == *$'\r'* || "$v" == *$'\n'* ]]; then
    log_error "VELOX_ADMIN_TOKEN contains CR or LF; refusing"
    return 2
  fi
  printf '%s' "$v"
}
ADMIN_TOKEN="$(resolve_token)" || exit 2

# M2M provisioning (smoke_mint_m2m sets M2M_BEARER + PROVISIONED_CLIENT_ID globals;
# it also handles 409-on-collision with a +1s retry).
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
on_signals() {
  if [[ -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]]; then
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  fi
}
trap 'on_signals' EXIT INT TERM

if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"
  exit 4
fi
log_info "M2M client_id=${PROVISIONED_CLIENT_ID}"

# ─── POST /api/v1/jobs ────────────────────────────────────────────────────
TMP_HDRS="$(mktemp)"; TMP_BODY="$(mktemp)"
trap 'rm -f "$TMP_HDRS" "$TMP_BODY"; on_signals' EXIT INT TERM

# Capture curl_rc explicitly. Without this, a network failure during
# POST would fall through to the HTTP-status check and exit 4
# (HTTP non-202), misclassifying a transport failure as an intake
# rejection. exit 3 is the documented network-error code; exit 4 is
# the documented HTTP non-202 code. See run.sh header for the exit map.
curl_rc=0
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer ${M2M_BEARER}" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: full-payload-${EPOCH}-${PROVISIONED_CLIENT_ID}" \
  --data-raw "$PAYLOAD" \
  "${VELOX_MASTER_URL}/api/v1/jobs" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null || curl_rc=$?
if (( curl_rc != 0 )); then
  log_error "POST /api/v1/jobs network failure (curl_rc=${curl_rc}); could not reach ${VELOX_MASTER_URL}"
  exit 3
fi
POST_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
POST_BODY=$(cat "$TMP_BODY")
if [[ "$POST_STATUS" != "202" ]]; then
  log_error "POST /api/v1/jobs returned HTTP ${POST_STATUS:-?}"
  log_error "  body: $(printf '%s' "$POST_BODY" | head -c 400)"
  exit 4
fi
JOB_ID=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty' 2>/dev/null) || {
  log_error "POST /api/v1/jobs returned 202 but missing .job_id"; exit 5; }
log_info "submitted job_id=${JOB_ID}"

# ─── Poll until SUCCEEDED (exp backoff 1→2→4→8→16s, capped at POLL_TIMEOUT_FULL) ──
elapsed=0
sleep_s=1
last_status=""
last_body=""
while (( elapsed < POLL_TIMEOUT_FULL )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 )); (( sleep_s > 16 )) && sleep_s=16

  if ! curl -sS -m 10 \
        -H "Authorization: Bearer ${M2M_BEARER}" \
        "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}" \
        -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null; then
    # Network blip: reset backoff so a transient drop doesn't burn budget.
    sleep_s=1; continue
  fi
  GET_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
  GET_BODY=$(cat "$TMP_BODY")
  sv=$(printf '%s' "$GET_BODY" | jq -er '.status // empty' 2>/dev/null || true)
  if [[ -n "$sv" ]]; then last_status="$sv"; last_body="$GET_BODY"; fi
  case "$sv" in
    SUCCEEDED) log_info "job SUCCEEDED after ${elapsed}s"; break ;;
    FAILED|CANCELLED)
      log_error "terminal-fail state ${sv} after ${elapsed}s"
      log_error "  body: $(printf '%s' "$GET_BODY" | head -c 400)"
      exit 6
      ;;
  esac
  if [[ "$GET_STATUS" != "200" ]]; then
    log_error "GET /api/v1/jobs/${JOB_ID} returned HTTP ${GET_STATUS:-?}"
    log_error "  body: $(printf '%s' "$GET_BODY" | head -c 400)"
    exit 8
  fi
done
if [[ "$last_status" != "SUCCEEDED" ]]; then
  log_error "poll timeout after ${POLL_TIMEOUT_FULL}s (last observed status=${last_status:-none})"
  exit 7
fi

# ─── Write evidence (tests/smoke/full_payload/evidence/run-<EPOCH>.json) ───
STARTED_AT=$(printf '%s' "$last_body"    | jq -er '.started_at   // empty')
COMPLETED_AT=$(printf '%s' "$last_body"  | jq -er '.completed_at // empty')
ARTIFACT_URL=$(printf '%s' "$last_body"  | jq -er '.artifact_url // .artifact_path // .output_path // empty')
s_epoch=$(smoke_parse_iso8601 "$STARTED_AT")
c_epoch=$(smoke_parse_iso8601 "$COMPLETED_AT")
render_time_ms=0
if [[ -n "$s_epoch" && -n "$c_epoch" ]]; then
  render_time_ms=$(awk -v a="$s_epoch" -v b="$c_epoch" 'BEGIN{printf "%.0f", (b-a)*1000}')
fi
artifact_size_bytes=$(smoke_artifact_size "$ARTIFACT_URL" "$M2M_BEARER")
NOW_ISO="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

ensure_dir "$EVIDENCE_DIR"
# Include the M2M client_id in the evidence filename so back-to-back
# runs in the same epoch second don't clobber each other. PROVISIONED_CLIENT_ID
# is set by smoke_mint_m2m and exported; it is unique per M2M provisioning.
EV_FILE="${EVIDENCE_DIR}/run-${EPOCH}-${PROVISIONED_CLIENT_ID}.json"
TMP_EV="$(mktemp "${EVIDENCE_DIR}/run-XXXXXX.json")"

SCENE_COUNT=$(printf '%s' "$PAYLOAD" | jq '.scenes | length')
LAYER_COUNT=$(printf '%s' "$PAYLOAD" | jq '.layers | length')
SUB_COUNT=$(printf '%s' "$PAYLOAD" | jq '.subtitle_tracks | length')
VO_COUNT=$(printf '%s' "$PAYLOAD" | jq '.voiceover_paths | length')

cat > "$TMP_EV" <<JSON
{
  "schema": "tests/smoke/full_payload@1",
  "job_id": "${JOB_ID}",
  "status": "SUCCEEDED",
  "target_executor_id": "${FULLPAYLOAD_TARGET_EXECUTOR_ID}",
  "destination_id": "${FULLPAYLOAD_DESTINATION_ID}",
  "render_time_ms": ${render_time_ms:-0},
  "artifact_size_bytes": ${artifact_size_bytes:-0},
  "started_at": "${STARTED_AT}",
  "completed_at": "${COMPLETED_AT}",
  "artifact_url": "${ARTIFACT_URL}",
  "scene_count": ${SCENE_COUNT},
  "voiceover_paths_count": ${VO_COUNT},
  "subtitle_tracks_count": ${SUB_COUNT},
  "layer_count": ${LAYER_COUNT},
  "smoke_runner_rev": ${SMOKE_PLUCKER_VARS_REV:-3},
  "written_at": "${NOW_ISO}"
}
JSON
mv "$TMP_EV" "$EV_FILE"
log_info "wrote ${EV_FILE}"

echo "OK: full-payload smoke"
echo "  job_id              : ${JOB_ID}"
echo "  destination_id      : ${FULLPAYLOAD_DESTINATION_ID}"
echo "  target_executor_id  : ${FULLPAYLOAD_TARGET_EXECUTOR_ID}"
echo "  scene_count         : ${SCENE_COUNT}"
echo "  layer_count         : ${LAYER_COUNT}"
echo "  subtitle_tracks_ct  : ${SUB_COUNT}"
echo "  voiceover_paths_ct  : ${VO_COUNT}"
echo "  render_time_ms      : ${render_time_ms:-0}"
echo "  artifact_bytes      : ${artifact_size_bytes:-0}"
echo "  evidence            : ${EV_FILE}"
exit 0
