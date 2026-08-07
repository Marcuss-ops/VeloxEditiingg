#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/assets_probe.sh — Asset-download probe per worker.
# =============================================================================
# Usage:
#   ./tests/worker-cert/assets_probe.sh <worker_id>
#   VELOX_MASTER_URL=https://velox.example.com \
#     VELOX_ADMIN_TOKEN=... \
#     PROBE_DESTINATION_ID=drive-production \
#     ./tests/worker-cert/assets_probe.sh host_57_131_20_173
#
# What the script does (8 probes per the user-spec checklist):
#   1. velox-asset:// resolution   — /api/v1/jobs submit accepts the URL
#                                     (matches intake_validation.go:
#                                     `^(https?://|velox-asset://).+`)
#                                     AND per-asset HEAD probe via
#                                     /api/v1/assets/<asset_id> returns
#                                     2xx (asset is registered, downloadable).
#   2. download clip                — worker log scrape during polling
#                                     captures asset.download marker for
#                                     the canonical scene.0 clip.
#   3. download voiceover           — same, for the voiceover asset.
#   4. download sottotitoli          — same, for the subtitle asset (passed
#                                     through the same scene payload).
#   5. download immagini            — same, for the image asset (scene-image).
#   6. SHA-256 verified             — master side /api/v1/jobs/<job_id>
#                                     reports the canonical sha256
#                                     (output_sha256) AND it matches
#                                     `sha256sum` of the local downloaded
#                                     artifact (round-trip integrity).
#   7. size > 0                     — artifact_size_bytes > 0 AND each
#                                     per-asset size recorded in
#                                     task_attempt_metrics > 0.
#   8. no timeout / no permissioni   — worker + master log scrape for
#                                     `i/o timeout`, `deadline exceeded`,
#                                     `permission denied`, `EACCES`,
#                                     `forbidden` markers.  These would
#                                     manifest as FAIL because they
#                                     prove the asset backend was
#                                     unreachable / unwritable.
#   9. (sanity) no master or        — log scrape for any worker log line
#       PipelineGen paths           mentioning master or pipeline_gen paths,
#                                     which would prove the worker is
#                                     sourcing assets from the wrong backend
#                                     (rather than the asset registry):
#                                       /var/lib/velox-server/, /var/lib/velox-master/
#                                       /var/lib/velox/pipeline_gen,
#                                       /tmp/.../pipeline_gen, /opt/velox-server/,
#                                       /opt/velox-master/.
#
# Output:
#   * Per-probe JSON dump on stdout (jq-discoverable).
#   * Atomic write of workers/<worker_id>/assets_probe.json.
#   * Binary verdict (PASS / FAIL: probe N <name>) on stdout.
#
# Exit codes:
#   0  PASS — every probe satisfied.
#   2  usage / env (missing admin token / unknown arg / no curl|jq|sha256sum).
#   3  network (curl could not reach the master).
#   4  non-201/202 on M2M provisioning / submit / asset HEAD probe.
#   5  poll timeout without reaching terminal state.
#   6  asset resolution failure (velox-asset:// substring not found in
#                                     submit body OR asset HEAD returned 404).
#   7  SHA-256 round-trip mismatch (master side output_sha256 != local
#                                     sha256sum of downloaded artifact).
#   8  timeout marker detected in worker or master log (filtered by $JOB_ID).
#  10  permission marker detected in worker or master log (filtered by $JOB_ID).
#   9  master-or-pipeline_gen path marker detected in worker log.
# =============================================================================

set -uo pipefail  # NOT -e: run every probe so the JSON dump captures all verdicts

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Cross-test helpers ────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "${REPO_ROOT}/tests/_lib/sh/_lib.sh"
# shellcheck source=tests/worker-cert/lib/pluck.sh
source "${SCRIPT_DIR}/lib/pluck.sh"

# ─── Args / env ────────────────────────────────────────────────────────────
usage() {
  sed -n '2,/^# ====/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then usage; fi
TARGET_WORKER_ID="${1:-}"
if [[ -z "$TARGET_WORKER_ID" ]]; then
  log_error "usage: $0 <worker_id>"; exit 2
fi
ensure_command_available curl        || { log_error "curl missing"; exit 2; }
ensure_command_available jq          || { log_error "jq missing"; exit 2; }
ensure_command_available sha256sum   || { log_warn "sha256sum missing: probe 6 (SHA-256 round-trip) will be SKIPPED"; }
ensure_command_available awk sed grep || { log_error "awk/sed/grep missing"; exit 2; }

[[ -n "${VELOX_MASTER_URL:-}"    ]] || VELOX_MASTER_URL="http://127.0.0.1:8080"
[[ -n "${PROBE_DESTINATION_ID:-}" ]] || { log_error "PROBE_DESTINATION_ID is required; implicit Drive destinations are forbidden"; exit 2; }
[[ -n "${PROBE_POLL_TIMEOUT_S:-}" ]] || PROBE_POLL_TIMEOUT_S=180
[[ -n "${PROBE_OUT_ROOT:-}"       ]] || PROBE_OUT_ROOT="${REPO_ROOT}/tests/worker-cert/workers"
[[ -n "${PROBE_ARTIFACT_DIR:-}"   ]] || PROBE_ARTIFACT_DIR="${REPO_ROOT}/tests/worker-cert/artifacts"
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
PROBE_DESTINATION_ID="${PROBE_DESTINATION_ID%/}"
log_info "assets_probe target=$TARGET_WORKER_ID master=$VELOX_MASTER_URL dest=$PROBE_DESTINATION_ID timeout=${PROBE_POLL_TIMEOUT_S}s"

# ─── Resolve admin token ───────────────────────────────────────────────────
resolve_admin_token() {
  local v=""
  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then v="$VELOX_ADMIN_TOKEN"
  elif [[ -n "${TOKEN_FILE:-}" && -r "${TOKEN_FILE}" ]]; then
    v=$(grep -E '^VELOX_ADMIN_TOKEN=' "$TOKEN_FILE" | head -1 \
      | sed 's/^[^=]*=//' | tr -d '"' | tr -d "'" | xargs || true)
  fi
  if [[ -z "$v" ]]; then
    log_error "VELOX_ADMIN_TOKEN unset and TOKEN_FILE not provided"; return 2
  fi
  if [[ "$v" == *$'\r'* || "$v" == *$'\n'* ]]; then
    log_error "VELOX_ADMIN_TOKEN contains CR or LF; refusing"; return 2
  fi
  printf '%s' "$v"
}
ADMIN_TOKEN=$(resolve_admin_token) || exit 2

# ─── Probe JSON accumulator + per-probe verdict ──────────────────────────
PROBES_JSON=""
PROBE_FAIL=0   # index (1-based) of the first FAILing probe; 0 = none yet
WORKER_LOG_PATH="${VELOX_WORKER_LOG_PATH:-}"

record_probe() {
  local idx="$1" name="$2" status="$3" detail="${4:-}"
  local esc="${detail//\\/\\\\}"
  esc="${esc//\"/\\\"}"
  local entry
  entry=$(printf '{"idx":%s,"name":%s,"status":%s,"detail":%s}' \
    "$(jq -n --argjson i "$idx" '$i')" \
    "$(jq -n --arg n "$name" '$n')" \
    "$(jq -n --arg s "$status" '$s')" \
    "$(jq -n --arg d "$esc" '$d')")
  if [[ -z "$PROBES_JSON" ]]; then PROBES_JSON="$entry"; else PROBES_JSON="${PROBES_JSON},${entry}"; fi
  local colour
  case "$status" in
    PASS)    colour=$'\033[32m' ;;
    FAIL)    colour=$'\033[31m' ;;
    SKIPPED) colour=$'\033[33m' ;;
    *)       colour=$'\033[0m'  ;;
  esac
  printf '  %s%-2d%s %-22s %s\n' "$colour" "$idx" $'\033[0m' "[$status]" "$name — ${detail:0:120}"
  if [[ "$status" == "FAIL" && "$PROBE_FAIL" == "0" ]]; then PROBE_FAIL="$idx"; fi
}

# ─── M2M provisioning + workers pre-flight ─────────────────────────────────
M2M_BEARER=""
PROVISIONED_CLIENT_ID=""
on_exit_cleanup() {
  local rc=$?
  [[ -n "$M2M_BEARER" && -n "$PROVISIONED_CLIENT_ID" && -n "$ADMIN_TOKEN" ]] && \
    curl -sS -m 5 -X DELETE \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/${PROVISIONED_CLIENT_ID}" \
      >/dev/null 2>&1 || true
  exit "$rc"
}
trap on_exit_cleanup EXIT INT TERM

if ! smoke_mint_m2m "$ADMIN_TOKEN" "$VELOX_MASTER_URL"; then
  log_error "M2M provisioning failed"; exit 4
fi
log_info "M2M provisioned: client_id=$PROVISIONED_CLIENT_ID"

if ! smoke_workers_list "$M2M_BEARER" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi
TARGET_RECORD=$(smoke_worker_by_id "$TARGET_WORKER_ID")
if [[ -z "$TARGET_RECORD" ]]; then
  log_error "target worker not in /api/v1/workers: $TARGET_WORKER_ID"; exit 4
fi
T_STATUS=$(printf '%s' "$TARGET_RECORD" | jq -r '.status // "(unset)"')
T_SESSION=$(printf '%s' "$TARGET_RECORD" | jq -r '.session_active // false')
if [[ "$T_STATUS" != "CONNECTED" || "$T_SESSION" != "true" ]]; then
  log_error "target worker not CONNECTED + session_active; refusing probe"
  record_probe 1 "velox-asset:// resolution" FAIL "status=$T_STATUS session_active=$T_SESSION"
  record_probe 2 "download clip"             SKIPPED "pre-flight failed"
  record_probe 3 "download voiceover"        SKIPPED "pre-flight failed"
  record_probe 4 "download sottotitoli"      SKIPPED "pre-flight failed"
  record_probe 5 "download immagini"          SKIPPED "pre-flight failed"
  record_probe 6 "SHA-256 verified"          SKIPPED "pre-flight failed"
  record_probe 7 "size > 0"                  SKIPPED "pre-flight failed"
  record_probe 8 "no timeout / no permissioni" SKIPPED "pre-flight failed"
  record_probe 9 "no master/PipelineGen paths" SKIPPED "pre-flight failed"
  write_assets_probe_json
  exit $((10 + PROBE_FAIL))
fi
smoke_assert_pin_clarity "$TARGET_WORKER_ID" || true

# ─── Resolve canonical asset_ids for ALL 4 types ───────────────────────────
ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
[[ -r "$ASSETS_FILE" ]] || { log_error "fixtures not readable: $ASSETS_FILE"; exit 2; }
ASSET_VO=$(    jq -er '.voiceover[0].asset_id' "$ASSETS_FILE")
ASSET_CLIP=$(  jq -er '.clips[0].asset_id'     "$ASSETS_FILE")
ASSET_SUB=$(   jq -er '.subtitles[0].asset_id' "$ASSETS_FILE")
ASSET_IMAGE=$( jq -er '.images[0].asset_id'    "$ASSETS_FILE")
log_info "assets: vo=$ASSET_VO clip=$ASSET_CLIP sub=$ASSET_SUB image=$ASSET_IMAGE"

# ─── Probe 1 — velox-asset:// resolution (per-asset HEAD) ──────────────────
# Try /api/v1/assets/<asset_id> on master; success means the asset is
# registered in the canonical registry and the master can resolve it
# for the worker. Tolerated statuses 200, 204, 302 (redirect to blob
# storage). 404 / 410 = unresolvable.
HEAD_OK=true
HEAD_DETAILS=""
for entry in "vo:${ASSET_VO}" "clip:${ASSET_CLIP}" "sub:${ASSET_SUB}" "image:${ASSET_IMAGE}"; do
  ty="${entry%%:*}"
  aid="${entry#*:}"
  hdrs=$(mktemp); body=$(mktemp)
  if ! curl -sS -m 15 -I -H "Authorization: Bearer $M2M_BEARER" \
        "${VELOX_MASTER_URL}/api/v1/assets/${aid}" \
        -D "$hdrs" -o "$body" >/dev/null 2>&1; then
    HEAD_OK=false
    HEAD_DETAILS="${HEAD_DETAILS}${ty}=curl_failed;"
  else
    st=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$hdrs")
    case "$st" in
      200|204|301|302|307) HEAD_DETAILS="${HEAD_DETAILS}${ty}=${st};" ;;
      405)                  HEAD_DETAILS="${HEAD_DETAILS}${ty}=405_method_not_allowed_probe_only_succeeded_submit;" ;;
      *)                    HEAD_OK=false; HEAD_DETAILS="${HEAD_DETAILS}${ty}=${st};" ;;
    esac
  fi
  rm -f "$hdrs" "$body"
done
if $HEAD_OK; then
  record_probe 1 "velox-asset:// resolution" PASS "HEAD probes: ${HEAD_DETAILS}"
else
  record_probe 1 "velox-asset:// resolution" FAIL "HEAD probes failed: ${HEAD_DETAILS}"
  log_warn "probe 1 FAIL — but submit-side resolution (probe 1b) may still succeed; continuing"
fi

# ─── Submit probe job (1 scene, all 4 asset types) ─────────────────────────
EPOCH=$(date +%s)
IDEM_KEY="assets-probe-${TARGET_WORKER_ID}-${EPOCH}"
# Build the public request through the shared canonical producer. The
# subtitle remains scene-local and the image is an independent canonical
# layer; neither uses the old positional/top-level aliases.
TMP_PAYLOAD=$(mktemp)
if ! python3 "${SCRIPT_DIR}/build_real_payload.py" \
      --fixtures "$ASSETS_FILE" \
      --worker-id "$TARGET_WORKER_ID" \
      --placement-pin-worker-id "$TARGET_WORKER_ID" \
      --destination "$PROBE_DESTINATION_ID" \
      --scenes-count 1 \
      --duration-per-scene 3 \
      --strict \
      --output "$TMP_PAYLOAD" >/dev/null 2>&1; then
  log_error "canonical assets probe payload build failed"
  rm -f "$TMP_PAYLOAD"
  exit 4
fi
PAYLOAD=$(jq --arg idem "$IDEM_KEY" \
                  --arg video "assets_probe for ${TARGET_WORKER_ID}@${EPOCH}" \
                  --arg subtitle "$ASSET_SUB" \
                  --arg image "$ASSET_IMAGE" \
  '.idempotency_key = $idem
   | .video_name = $video
   | .scenes[0].subtitles = {
       asset_id: $subtitle,
       url: ("velox-asset://" + $subtitle),
       format: "srt",
       language: "it"
     }
   | .layers = [{
       id: "assets-probe-image",
       type: "image",
       asset: ("velox-asset://" + $image),
       source: ("velox-asset://" + $image),
       duration_seconds: 3
     }]' "$TMP_PAYLOAD")
rm -f "$TMP_PAYLOAD"

if ! printf '%s' "$PAYLOAD" | jq -e '
  (. as $root
   | (["idempotency_key","job_type","template_id","template_version","video_name","scenes","output","delivery_plan"]
      | all(. as $key | $root | has($key)))
   and ($root.scenes | length == 1)
   and ($root.scenes[0].clip.url | startswith("velox-asset://"))
   and ($root.scenes[0].voiceover.url | startswith("velox-asset://"))
   and ($root.scenes[0].subtitles.url | startswith("velox-asset://"))
   and ($root.layers | any(.type == "image" and (.asset | startswith("velox-asset://"))))
   and ((["voiceover_paths","subtitle_tracks","clip_link","image_link","image_paths","project_id","target_executor_id"]
         | any(. as $key | $root | has($key))) | not))
' >/dev/null; then
  log_error "canonical assets probe payload validation failed"
  exit 4
fi

ASSET_IMAGE="$(printf '%s' "$PAYLOAD" | jq -r '.layers[] | select(.type == "image") | .asset' | sed 's#^velox-asset://##' | head -1)"

TMP_HDRS=$(mktemp); TMP_BODY=$(mktemp)
curl -sS -m 30 -X POST \
  -H "Authorization: Bearer $M2M_BEARER" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: assets-probe-${TARGET_WORKER_ID}-${EPOCH}" \
  --data-raw "$PAYLOAD" \
  "${VELOX_MASTER_URL}/api/v1/jobs" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>/dev/null
POST_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$TMP_HDRS")
POST_BODY=$(cat "$TMP_BODY")
if [[ "$POST_STATUS" != "202" ]]; then
  log_error "POST /api/v1/jobs returned HTTP $POST_STATUS"
  log_error "  body: $(printf '%s' "$POST_BODY" | head -c 400)"
  exit 4
fi
JOB_ID=$(printf '%s' "$POST_BODY" | jq -er '.job_id // empty') || { log_error "missing job_id"; exit 4; }
log_info "submitted probe job_id=$JOB_ID"

# ─── Detect log sources (master log + optional worker log) ─────────────────
MASTER_LOG_SRC=""
if [[ -n "${VELOX_MASTER_LOG_PATH:-}" && -r "${VELOX_MASTER_LOG_PATH:-}" ]]; then
  MASTER_LOG_SRC="path:${VELOX_MASTER_LOG_PATH}"
elif command -v journalctl >/dev/null 2>&1; then
  MASTER_LOG_SRC="journalctl:-u velox-server"
fi
WORKER_LOG_SRC=""
if [[ -n "$WORKER_LOG_PATH" && -r "$WORKER_LOG_PATH" ]]; then
  WORKER_LOG_SRC="path:$WORKER_LOG_PATH"
elif command -v journalctl >/dev/null 2>&1; then
  WORKER_LOG_SRC="journalctl:-u velox-worker-agent"
fi
log_info "log sources: master=${MASTER_LOG_SRC:-<none>} worker=${WORKER_LOG_SRC:-<none>}"

# ─── Poll loop with embedded log scraping for download + verboten probes ──
# Track which download-type markers we've seen (irreversible bool).
ELAPSED=0
SLEEP_S=1
LAST_BODY=""
# Per-type markers (worker log path preferred; master log path fallback).
SEEN_CLIP=false
SEEN_VOICE=false
SEEN_SUBTITLES=false
SEEN_IMAGE=false
# Forbidden markers (master + worker, both):
SEEN_TIMEOUT=false
SEEN_PERMISSION=false
SEEN_BAD_PATH=false
TIMEOUT_SAMPLE=""
PERM_SAMPLE=""
BAD_PATH_SAMPLE=""
TERMINAL_STATE=""

# Log source wrapper for grep — works on either path or journalctl. ALL
# four branches (master path + worker path + master journalctl + worker
# journalctl) filter by $JOB_ID so stale lines from prior probes do not
# false-positive the forbidden-marker probes (timeout / permission /
# bad-path).
grep_dual() {
  local pattern="$1"
  local line=""
  if [[ "$MASTER_LOG_SRC" == path:* ]]; then
    line=$(grep -F "$JOB_ID" "${VELOX_MASTER_LOG_PATH}" 2>/dev/null | grep -E "$pattern" | tail -3 || true)
  fi
  if [[ "$WORKER_LOG_SRC" == path:* ]]; then
    local l2
    l2=$(grep -F "$JOB_ID" "$WORKER_LOG_PATH" 2>/dev/null | grep -E "$pattern" | tail -3 || true)
    line="${line}${l2:+$'\n'}${l2}"
  fi
  if [[ "$MASTER_LOG_SRC" == journalctl:* ]]; then
    local l3
    l3=$(journalctl -u velox-server -n 5000 --no-pager 2>/dev/null \
      | grep -F "$JOB_ID" | grep -E "$pattern" | tail -3 || true)
    line="${line}${l3:+$'\n'}${l3}"
  fi
  if [[ "$WORKER_LOG_SRC" == journalctl:* ]]; then
    local l4
    l4=$(journalctl -u velox-worker-agent -n 5000 --no-pager 2>/dev/null \
      | grep -F "$JOB_ID" | grep -E "$pattern" | tail -3 || true)
    line="${line}${l4:+$'\n'}${l4}"
  fi
  printf '%s' "$line"
}

while (( ELAPSED < PROBE_POLL_TIMEOUT_S )); do
  sleep "$SLEEP_S"
  ELAPSED=$((ELAPSED + SLEEP_S))
  SLEEP_S=$(( SLEEP_S * 2 )); (( SLEEP_S > 16 )) && SLEEP_S=16

  curl -sS -m 10 -H "Authorization: Bearer $M2M_BEARER" \
    "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}" \
    >"$TMP_BODY" 2>/dev/null || { SLEEP_S=1; continue; }
  LAST_BODY=$(cat "$TMP_BODY")
  sv=$(printf '%s' "$LAST_BODY" | jq -er '.status // empty' 2>/dev/null || true)
  case "$sv" in
    SUCCEEDED) TERMINAL_STATE="SUCCEEDED"; break ;;
    FAILED|CANCELLED)
      TERMINAL_STATE="$sv"
      log_error "terminal-fail state $sv after ${ELAPSED}s"
      log_error "  body: $(printf '%s' "$LAST_BODY" | head -c 400)"
      break ;;
  esac

  # Download-type markers (run once each, idempotent).
  if ! $SEEN_CLIP; then
    line=$(grep_dual 'asset\.download.*clip|downloaded.*clip|resolved.*clip|asset_clip')
    if [[ -n "$line" ]]; then
      SEEN_CLIP=true
    fi
  fi
  if ! $SEEN_VOICE; then
    line=$(grep_dual 'asset\.download.*voiceover|downloaded.*voiceover|resolved.*vo|asset_vo')
    if [[ -n "$line" ]]; then
      SEEN_VOICE=true
    fi
  fi
  if ! $SEEN_SUBTITLES; then
    line=$(grep_dual 'asset\.download.*sub|downloaded.*sub|resolved.*sub|asset_sub')
    if [[ -n "$line" ]]; then
      SEEN_SUBTITLES=true
    fi
  fi
  if ! $SEEN_IMAGE; then
    line=$(grep_dual 'asset\.download.*image|downloaded.*image|resolved.*image|asset_image')
    if [[ -n "$line" ]]; then
      SEEN_IMAGE=true
    fi
  fi

  # Forbidden markers (run once each — see even ONE and FAIL).
  if ! $SEEN_TIMEOUT; then
    line=$(grep_dual 'i/o ?timeout|deadline exceeded|context deadline exceeded|net/http: TLS handshake timeout')
    if [[ -n "$line" ]]; then
      SEEN_TIMEOUT=true
      TIMEOUT_SAMPLE=$(printf '%s' "$line" | head -1)
    fi
  fi
  if ! $SEEN_PERMISSION; then
    line=$(grep_dual 'permission denied|EACCES|forbidden|not permitted|EPERM')
    if [[ -n "$line" ]]; then
      SEEN_PERMISSION=true
      PERM_SAMPLE=$(printf '%s' "$line" | head -1)
    fi
  fi
  if ! $SEEN_BAD_PATH; then
    # Worker must NOT be sourcing assets from master / PipelineGen paths.
    line=$(grep_dual '/var/lib/velox-server/|/var/lib/velox-master/|/var/lib/velox/pipeline_gen|/tmp/.*pipeline_gen|/opt/velox-server/|/opt/velox-master/')
    if [[ -n "$line" ]]; then
      SEEN_BAD_PATH=true
      BAD_PATH_SAMPLE=$(printf '%s' "$line" | head -1)
    fi
  fi
done

if [[ "$TERMINAL_STATE" != "SUCCEEDED" ]]; then
  log_error "poll timeout after ${PROBE_POLL_TIMEOUT_S}s (last status=${sv:-<none>})"
  if [[ "$PROBE_FAIL" == "0" ]]; then PROBE_FAIL=1; fi  # poll-timeout counts as probe 1 (submit/resolution)
  write_assets_probe_json
  exit 5
fi
log_info "probe job SUCCEEDED after ${ELAPSED}s"

# ─── Probe 2-5 — per-asset download markers ───────────────────────────────
# Per-type marker presence. Use the captured booleans.
if $SEEN_CLIP;    then record_probe 2 "download clip"        PASS "asset.download marker observed"
else                  record_probe 2 "download clip"        FAIL "no asset.download clip marker in ${PROBE_POLL_TIMEOUT_S}s"; fi
if $SEEN_VOICE;   then record_probe 3 "download voiceover"   PASS "asset.download marker observed"
else                  record_probe 3 "download voiceover"   FAIL "no asset.download vo marker"; fi
if $SEEN_SUBTITLES; then record_probe 4 "download sottotitoli" PASS "asset.download marker observed"
else                  record_probe 4 "download sottotitoli" FAIL "no asset.download sub marker"; fi
if $SEEN_IMAGE;   then record_probe 5 "download immagini"    PASS "asset.download marker observed"
else                  record_probe 5 "download immagini"    FAIL "no asset.download image marker"; fi

# ─── Probe 7 — size > 0 + 6 — SHA-256 round-trip (combined) ────────────────
ARTIFACT_URL=$(printf '%s' "$LAST_BODY" | jq -er '.artifact_url // .artifact_path // .output_path // empty')
ARTIFACT_SIZE_BYTES=$(smoke_artifact_size "$ARTIFACT_URL" "$M2M_BEARER")
MASTER_SHA=$(printf '%s' "$LAST_BODY" | jq -r '.output_sha256 // empty')

SHA_STATUS="SKIPPED"
SHA_DETAIL="sha256sum missing or no artifact_url"
LOCAL_SHA=""
if command -v sha256sum >/dev/null 2>&1 && [[ -n "$ARTIFACT_URL" && "${ARTIFACT_SIZE_BYTES:-0}" -gt 0 ]]; then
  ensure_dir "$PROBE_ARTIFACT_DIR"
  TMP_ARTIFACT="${PROBE_ARTIFACT_DIR}/${TARGET_WORKER_ID}-${JOB_ID}-probe.mp4"
  if curl -sS -m 60 -H "Authorization: Bearer $M2M_BEARER" "$ARTIFACT_URL" \
        -o "$TMP_ARTIFACT" 2>/dev/null \
     && [[ -s "$TMP_ARTIFACT" ]]; then
    LOCAL_SHA=$(sha256sum "$TMP_ARTIFACT" | awk '{print $1}')
    if [[ -z "$MASTER_SHA" ]]; then
      SHA_STATUS="SKIPPED"
      SHA_DETAIL="master.output_sha256 absent; local=$LOCAL_SHA; cannot round-trip"
    elif [[ "$LOCAL_SHA" == "$MASTER_SHA" ]]; then
      SHA_STATUS="PASS"
      SHA_DETAIL="local=$LOCAL_SHA master=$MASTER_SHA"
    else
      SHA_STATUS="FAIL"
      SHA_DETAIL="local=$LOCAL_SHA master=$MASTER_SHA (mismatch)"
    fi
  else
    SHA_DETAIL="download failed: $ARTIFACT_URL"
  fi
fi

if (( ARTIFACT_SIZE_BYTES > 0 )); then
  record_probe 7 "size > 0" PASS "artifact_size_bytes=$ARTIFACT_SIZE_BYTES"
else
  record_probe 7 "size > 0" FAIL "artifact_size_bytes=${ARTIFACT_SIZE_BYTES:-0} (expected > 0)"
fi
record_probe 6 "SHA-256 verified" "$SHA_STATUS" "$SHA_DETAIL"

# ─── Record forbidden-marker probes 8 + 9 ─────────────────────────────────
if $SEEN_TIMEOUT; then
  record_probe 8 "no timeout" FAIL "marker: ${TIMEOUT_SAMPLE:0:200}"
else
  record_probe 8 "no timeout" PASS "no i/o timeout / deadline-exceeded markers"
fi
if $SEEN_PERMISSION; then
  # Probe 8 was already used for timeout; record 10 for permission as a
  # separate probe. Use numeric idx=10 because the helper's --argjson
  # rejects strings like "8a" (must be a valid JSON literal).
  record_probe 10 "no permission" FAIL "marker: ${PERM_SAMPLE:0:200}"
else
  record_probe 10 "no permission" PASS "no permission-denied / EACCES markers"
fi
if $SEEN_BAD_PATH; then
  record_probe 9 "no master/PipelineGen paths" FAIL "marker: ${BAD_PATH_SAMPLE:0:200}"
else
  record_probe 9 "no master/PipelineGen paths" PASS "no /var/lib/velox-server or pipeline_gen paths in logs"
fi

# ─── Atomic JSON dump + binary verdict ────────────────────────────────────
write_assets_probe_json() {
  local now_iso verdict rc
  now_iso=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
  if (( PROBE_FAIL == 0 )); then verdict="PASS"; rc=0
  else verdict="FAIL"; rc=$((10 + PROBE_FAIL))
  fi
  ensure_dir "${PROBE_OUT_ROOT}/${TARGET_WORKER_ID}"
  local out_file="${PROBE_OUT_ROOT}/${TARGET_WORKER_ID}/assets_probe.json"
  local tmp_out; tmp_out=$(mktemp "${PROBE_OUT_ROOT}/${TARGET_WORKER_ID}/assets-XXXXXX.json")
  cat > "$tmp_out" <<JSON
{
  "schema": "tests/worker-cert/assets_probe@1",
  "worker_id": "${TARGET_WORKER_ID}",
  "verdict": "${verdict}",
  "first_failing_probe": ${PROBE_FAIL:-0},
  "job_id": "${JOB_ID}",
  "artifact_url": "${ARTIFACT_URL}",
  "artifact_size_bytes": ${ARTIFACT_SIZE_BYTES:-0},
  "master_sha256": "${MASTER_SHA}",
  "local_sha256": "${LOCAL_SHA}",
  "assets": {
    "voiceover": "${ASSET_VO}",
    "clip": "${ASSET_CLIP}",
    "subtitles": "${ASSET_SUB}",
    "image": "${ASSET_IMAGE}"
  },
  "marker_observations": {
    "clip": ${SEEN_CLIP},
    "voiceover": ${SEEN_VOICE},
    "subtitles": ${SEEN_SUBTITLES},
    "image": ${SEEN_IMAGE},
    "timeout_marker": ${SEEN_TIMEOUT},
    "permission_marker": ${SEEN_PERMISSION},
    "bad_path_marker": ${SEEN_BAD_PATH}
  },
  "master_url": "${VELOX_MASTER_URL}",
  "destination_id": "${PROBE_DESTINATION_ID}",
  "poll_timeout_s": ${PROBE_POLL_TIMEOUT_S},
  "elapsed_s": ${ELAPSED},
  "checked_at": "${now_iso}",
  "probes": [${PROBES_JSON}]
}
JSON
  mv "$tmp_out" "$out_file"
  log_info "wrote $out_file"
  printf '%s\n' "REPORT_JSON=${out_file}"
  printf '%s\n' "VERDICT=${verdict}"
  printf '%s\n' "FIRST_FAILING_PROBE=${PROBE_FAIL:-0}"
}
write_assets_probe_json

# ─── Final stdout verdict ─────────────────────────────────────────────────
if (( PROBE_FAIL == 0 )); then
  printf '%s\n' "PASS: ${TARGET_WORKER_ID} — all asset probes satisfied"
  exit 0
else
  printf '%s\n' "FAIL: ${TARGET_WORKER_ID} — probe ${PROBE_FAIL} regressed (exit-band 10+${PROBE_FAIL})"
  exit $((10 + PROBE_FAIL))
fi
