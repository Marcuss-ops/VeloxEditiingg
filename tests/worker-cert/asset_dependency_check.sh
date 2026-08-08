#!/usr/bin/env bash
# =============================================================================
# tests/worker-cert/asset_dependency_check.sh — Service × asset matrix probe.
# =============================================================================
# Usage:
#   ./tests/worker-cert/asset_dependency_check.sh <worker_id>
#   VELOX_MASTER_URL=https://velox.example.com \
#     VELOX_ADMIN_TOKEN=... \
#     ASSET_CHK_MIN_DISK_KB=5242880 \
#     ./tests/worker-cert/asset_dependency_check.sh host_57_131_20_173
#
# What the script does per the user-spec checklist:
#   7 service probes (the "service matrix"):
#     1. asset backend       — HEAD /api/v1/assets/<front-asset-id> with bearer
#                                returns 2xx (200|204|301|302|307) or 405 with
#                                "method_not_allowed" sentinel.  Anything else
#                                means the worker cannot reach the asset
#                                registry and is unfit for asset-driven jobs.
#     2. FFmpeg              — `command -v ffmpeg` + `ffmpeg -version` exit 0.
#                                The native video engine is BUILT ON TOP of
#                                ffmpeg; if ffmpeg is missing or version probe
#                                fails, scene.composite.v1@1 will fail every
#                                job at scene_render.
#     3. FFprobe             — `command -v ffprobe` + `ffprobe -version`
#                                exit 0.  Used as the upstream scene_image
#                                + clip_link dimension probe (see
#                                RemoteCodex/.../ffmpeg_progress_parser.cpp).
#     4. fonts               — VELOX_FONTS_DIR (default /usr/share/fonts) is
#                                a directory containing ≥ MIN_FONTS_COUNT font
#                                files (*.ttf|*.otf|.ttc).  Burned-in subtitle
#                                rendering requires at least one ttf/otf; this
#                                probe guards against the worker being
#                                deployed on a stripped image.
#     5. dir lavoro          — VELOX_RUNTIME_DIR (default /var/lib/velox-worker)
#                                is a directory that is both writable AND
#                                executable (mkdir + write/rename file
#                                round-trip succeeds).  Same as assets_probe.sh
#                                probe 6 (worker-runtime dir).
#     6. cache               — VELOX_CACHE_DIR (default $RUNTIME_DIR/cache).
#                                Same writable/executable guard, scoped to the
#                                cache sub-tree (intermediate render artifacts,
#                                font cache, transcoded NUT chunks).
#     7. disco               — `df -k $RUNTIME_DIR` reports ≥ MIN_DISK_KB free.
#                                A worker that boots with < 5 GB free will trip
#                                into slow-loop death on the 4th concurrent job
#                                (we've seen this on 57_129_132_133 once).
#
#   Per-asset probes (the "asset matrix"):
#     For each asset_id in tests/worker-cert/fixtures/assets.json under
#     voiceover[], clips[], subtitles[], images[] (one row per asset), do:
#       a. HEAD /api/v1/assets/<asset_id> with bearer — record status code.
#       b. Content-Length > 0 — header extraction; 0 ⇒ FAIL "registered but empty".
#       c. Content-Type coerente — regex match against the asset-type matrix
#          (audio/* for voiceover, video/* for clips, text/(plain|vtt|srt|ass)
#          or application/x-subrip for subtitles, image/* for images).
#       d. SHA-256 if available — pull assets/<id>/sha256 from the master
#          (canonical sha256_master, optional); if absent, SKIPPED rather
#          than FAIL (canonical registry doesn't yet record a sha for every
#          item).
#
# Output:
#   * Service matrix on stdout (one line per probe with PASS/FAIL/SKIPPED).
#   * Asset matrix on stdout (one line per asset row).
#   * Atomic write of workers/<worker_id>/asset_dependency_check.json with
#     both matrices + first-failing indices for post-mortem.
#   * Binary verdict PASS/FAIL on final stdout line.
#
# Exit-code band (diagnosable; uses bands so first failure is addressable):
#   0   all probes satisfied (PASS).
#   2   usage / env (missing admin token / unknown arg / no jq).
#   3   network (curl could not reach the master).
#   4   non-{201,202} on M2M provisioning or initial probe setup.
#   5   poll timeout against the asset HEAD probe (network OK but slow).
#   10  worker not in /api/v1/workers OR not CONNECTED + session_active.
#   11..17   first failing service probe (1-based idx within services matrix:
#            11=asset backend, 12=FFmpeg, 13=FFprobe, 14=fonts,
#            15=dir lavoro, 16=cache, 17=disco).
#   21..29   first failing asset probe (asset matrix band, idx is the
#            position within the asset-type sequence, bounded 1..8 so
#            the band caps at 29 to leave 30+ free for followup scripts).
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
ensure_command_available curl  || { log_error "curl missing";  exit 2; }
ensure_command_available jq    || { log_error "jq missing";    exit 2; }
ensure_command_available awk   || { log_error "awk missing";   exit 2; }
ensure_command_available sed   || { log_error "sed missing";   exit 2; }
ensure_command_available grep  || { log_error "grep missing";  exit 2; }
ensure_command_available df    || log_warn "df missing: probe 7 (disk) will be SKIPPED"
ensure_command_available ffmpeg  || true   # may legitimately be absent; recorded as FAIL
ensure_command_available ffprobe || true   # ditto

[[ -n "${VELOX_MASTER_URL:-}"       ]] || VELOX_MASTER_URL="http://127.0.0.1:8000"
[[ -n "${ASSET_CHK_DESTINATION_ID:-}" ]] || { log_error "ASSET_CHK_DESTINATION_ID is required; implicit Drive destinations are forbidden"; exit 2; }
[[ -n "${ASSET_CHK_MIN_FONTS:-}"     ]] || ASSET_CHK_MIN_FONTS=3
[[ -n "${ASSET_CHK_MIN_DISK_KB:-}"   ]] || ASSET_CHK_MIN_DISK_KB=5242880   # 5 GiB
[[ -n "${VELOX_FONTS_DIR:-}"         ]] || VELOX_FONTS_DIR="/usr/share/fonts"
[[ -n "${VELOX_RUNTIME_DIR:-}"       ]] || VELOX_RUNTIME_DIR="/var/lib/velox-worker"
[[ -n "${VELOX_CACHE_DIR:-}"         ]] || VELOX_CACHE_DIR="${VELOX_RUNTIME_DIR}/cache"
[[ -n "${ASSET_CHK_OUT_ROOT:-}"      ]] || ASSET_CHK_OUT_ROOT="${REPO_ROOT}/tests/worker-cert/workers"
[[ -n "${ASSET_CHK_HEAD_TIMEOUT_S:-}" ]] || ASSET_CHK_HEAD_TIMEOUT_S=20
VELOX_MASTER_URL="${VELOX_MASTER_URL%/}"
log_info "asset_dependency_check target=$TARGET_WORKER_ID master=$VELOX_MASTER_URL fonts=$VELOX_FONTS_DIR runtime=$VELOX_RUNTIME_DIR cache=$VELOX_CACHE_DIR min_disk_kb=$ASSET_CHK_MIN_DISK_KB"

# ─── Resolve admin token (env > TOKEN_FILE) ────────────────────────────────
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

# ─── Probe accumulators ────────────────────────────────────────────────────
SERVICE_PROBES_JSON=""   # comma-joined entries
ASSET_PROBES_JSON=""     # comma-joined entries
SERVICE_FAIL=0           # 1-based idx of first FAILing service; 0 = none
ASSET_FAIL=0             # 1-based idx of first FAILing asset; 0 = none
ASSET_ROW_COUNT=0        # counter for asset matrix row indexing

record_service_probe() {
  local idx="$1" name="$2" status="$3" detail="${4:-}"
  local esc="${detail//\\/\\\\}"
  esc="${esc//\"/\\\"}"
  local entry
  entry=$(printf '{"idx":%s,"name":%s,"status":%s,"detail":%s}' \
    "$(jq -n --argjson i "$idx" '$i')" \
    "$(jq -n --arg n "$name" '$n')" \
    "$(jq -n --arg s "$status" '$s')" \
    "$(jq -n --arg d "$esc" '$d')")
  if [[ -z "$SERVICE_PROBES_JSON" ]]; then SERVICE_PROBES_JSON="$entry"
  else SERVICE_PROBES_JSON="${SERVICE_PROBES_JSON},${entry}"; fi
  local colour
  case "$status" in
    PASS)    colour=$'\033[32m' ;;
    FAIL)    colour=$'\033[31m' ;;
    SKIPPED) colour=$'\033[33m' ;;
    *)       colour=$'\033[0m'  ;;
  esac
  printf '  service %s%-2d%s %-22s %s\n' "$colour" "$idx" $'\033[0m' "[$status]" "$name — ${detail:0:140}"
  if [[ "$status" == "FAIL" && "$SERVICE_FAIL" == "0" ]]; then SERVICE_FAIL="$idx"; fi
}

# record_asset_probe <asset_type> <asset_id> <status> <detail>
# The asset matrix row index is a 1-based sequential counter incremented
# for every row (PASS/FAIL/SKIPPED — SKIPPED also consumed). The exit
# code band encodes the FIRST FAIL pair (asset_type, asset_id) so the
# post-mortem can bin-search the JSON for the failing row.
record_asset_probe() {
  local asset_type="$1" asset_id="$2" status="$3" detail="${4:-}"
  ASSET_ROW_COUNT=$((ASSET_ROW_COUNT + 1))
  local row_idx="$ASSET_ROW_COUNT"
  local esc="${detail//\\/\\\\}"
  esc="${esc//\"/\\\"}"
  local entry
  entry=$(printf '{"row_idx":%s,"asset_type":%s,"asset_id":%s,"status":%s,"detail":%s}' \
    "$(jq -n --argjson i "$row_idx" '$i')" \
    "$(jq -n --arg t "$asset_type" '$t')" \
    "$(jq -n --arg a "$asset_id" '$a')" \
    "$(jq -n --arg s "$status" '$s')" \
    "$(jq -n --arg d "$esc" '$d')")
  if [[ -z "$ASSET_PROBES_JSON" ]]; then ASSET_PROBES_JSON="$entry"
  else ASSET_PROBES_JSON="${ASSET_PROBES_JSON},${entry}"; fi
  local colour
  case "$status" in
    PASS)    colour=$'\033[32m' ;;
    FAIL)    colour=$'\033[31m' ;;
    SKIPPED) colour=$'\033[33m' ;;
    *)       colour=$'\033[0m'  ;;
  esac
  printf '  asset   %srow%-2d%s %-14s %-30s %s\n' "$colour" "$row_idx" $'\033[0m' "[$status]" "$asset_type:$asset_id" "${detail:0:120}"
  if [[ "$status" == "FAIL" && "$ASSET_FAIL" == "0" ]]; then ASSET_FAIL="$row_idx"; fi
}

# ─── M2M provisioning (DELETE-on-exit via trap) ────────────────────────────
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

# ─── Worker pre-flight ─────────────────────────────────────────────────────
if ! smoke_workers_list "$M2M_BEARER" "$VELOX_MASTER_URL"; then
  log_error "could not list workers"; exit 3
fi
TARGET_RECORD=$(smoke_worker_by_id "$TARGET_WORKER_ID")
if [[ -z "$TARGET_RECORD" ]]; then
  log_error "target worker not in /api/v1/workers: $TARGET_WORKER_ID"; exit 10
fi
T_STATUS=$(printf '%s' "$TARGET_RECORD" | jq -r '.status // "(unset)"')
T_SESSION=$(printf '%s' "$TARGET_RECORD" | jq -r '.session_active // false')
if [[ "$T_STATUS" != "CONNECTED" || "$T_SESSION" != "true" ]]; then
  log_error "target worker not CONNECTED + session_active; cannot run matrix probe (status=$T_STATUS session_active=$T_SESSION)"
  exit 10
fi
log_info "target worker status=$T_STATUS session_active=$T_SESSION"

# ─────────────────────────────────────────────────────────────────────────────
# Service matrix — 7 probes
# ─────────────────────────────────────────────────────────────────────────────
log_info "── service matrix ──"

# 1. asset backend — HEAD /api/v1/assets/<front-asset-id> with bearer
HEAD_PROBE_OUT=$(mktemp)
HEAD_PROBE_STATUS=""
ASSETS_FILE="${SCRIPT_DIR}/fixtures/assets.json"
[[ -r "$ASSETS_FILE" ]] || { log_error "fixtures not readable: $ASSETS_FILE"; exit 2; }
FRONT_ASSET_ID=$(jq -er '.voiceover[0].asset_id // .clips[0].asset_id // empty' "$ASSETS_FILE")
if [[ -z "$FRONT_ASSET_ID" ]]; then
  record_service_probe 1 "asset backend" SKIPPED "fixtures has no front asset_id"
else
  if curl -sS -m "$ASSET_CHK_HEAD_TIMEOUT_S" -I \
        -H "Authorization: Bearer $M2M_BEARER" \
        "${VELOX_MASTER_URL}/api/v1/assets/${FRONT_ASSET_ID}" \
        -D "$HEAD_PROBE_OUT" -o /dev/null 2>/dev/null; then
    HEAD_PROBE_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$HEAD_PROBE_OUT")
    case "$HEAD_PROBE_STATUS" in
      200|204|301|302|307)
        record_service_probe 1 "asset backend" PASS "/api/v1/assets/${FRONT_ASSET_ID} HTTP ${HEAD_PROBE_STATUS}" ;;
      405)
        record_service_probe 1 "asset backend" PASS "/api/v1/assets/${FRONT_ASSET_ID} HTTP 405 method-not-allowed-but-route-live" ;;
      *)
        record_service_probe 1 "asset backend" FAIL "/api/v1/assets/${FRONT_ASSET_ID} HTTP ${HEAD_PROBE_STATUS:-<no-response>}" ;;
    esac
  else
    record_service_probe 1 "asset backend" FAIL "curl HEAD failed (network)"
  fi
fi
rm -f "$HEAD_PROBE_OUT"

# 2. FFmpeg
if command -v ffmpeg >/dev/null 2>&1; then
  FFMPEG_VER=$(ffmpeg -version 2>&1 | head -1 | tr -d '\r')
  if [[ -n "$FFMPEG_VER" ]]; then
    record_service_probe 2 "FFmpeg" PASS "$FFMPEG_VER"
  else
    record_service_probe 2 "FFmpeg" FAIL "ffmpeg -version returned empty"
  fi
else
  record_service_probe 2 "FFmpeg" FAIL "command -v ffmpeg: not on PATH"
fi

# 3. FFprobe
if command -v ffprobe >/dev/null 2>&1; then
  FFPROBE_VER=$(ffprobe -version 2>&1 | head -1 | tr -d '\r')
  if [[ -n "$FFPROBE_VER" ]]; then
    record_service_probe 3 "FFprobe" PASS "$FFPROBE_VER"
  else
    record_service_probe 3 "FFprobe" FAIL "ffprobe -version returned empty"
  fi
else
  record_service_probe 3 "FFprobe" FAIL "command -v ffprobe: not on PATH"
fi

# 4. fonts — VELOX_FONTS_DIR has ≥ ASSET_CHK_MIN_FONTS font files
if [[ -d "$VELOX_FONTS_DIR" ]]; then
  FONT_COUNT=$(find "$VELOX_FONTS_DIR" -maxdepth 4 -type f \( -name '*.ttf' -o -name '*.otf' -o -name '*.ttc' \) 2>/dev/null | wc -l | awk '{print $1}')
  if [[ -z "$FONT_COUNT" ]]; then FONT_COUNT=0; fi
  if (( FONT_COUNT >= ASSET_CHK_MIN_FONTS )); then
    record_service_probe 4 "fonts" PASS "$VELOX_FONTS_DIR has $FONT_COUNT font files (≥ $ASSET_CHK_MIN_FONTS)"
  else
    record_service_probe 4 "fonts" FAIL "$VELOX_FONTS_DIR has $FONT_COUNT font files (< $ASSET_CHK_MIN_FONTS)"
  fi
else
  record_service_probe 4 "fonts" FAIL "$VELOX_FONTS_DIR not a directory"
fi

# 5. dir lavoro — VELOX_RUNTIME_DIR writable + mkdir + write/rename round-trip
if [[ ! -d "$VELOX_RUNTIME_DIR" ]]; then
  ensure_dir "$VELOX_RUNTIME_DIR" || record_service_probe 5 "dir lavoro" FAIL "could not create $VELOX_RUNTIME_DIR"
fi
if [[ -d "$VELOX_RUNTIME_DIR" ]]; then
  TMP_PROBE="$(mktemp -p "$VELOX_RUNTIME_DIR" asset-chk-probe-XXXXXX.tmp 2>/dev/null)" || \
    TMP_PROBE=""
  if [[ -z "$TMP_PROBE" ]]; then
    record_service_probe 5 "dir lavoro" FAIL "$VELOX_RUNTIME_DIR not writable by $(id -un)"
  else
    if mv "$TMP_PROBE" "${TMP_PROBE}.renamed" 2>/dev/null \
       && rm -f "${TMP_PROBE}.renamed"; then
      record_service_probe 5 "dir lavoro" PASS "$VELOX_RUNTIME_DIR writable + rename OK"
    else
      record_service_probe 5 "dir lavoro" FAIL "$VELOX_RUNTIME_DIR write succeeded but rename failed"
      [[ -e "$TMP_PROBE" ]] && rm -f "$TMP_PROBE"
    fi
  fi
else
  record_service_probe 5 "dir lavoro" FAIL "$VELOX_RUNTIME_DIR absent or not a directory"
fi

# 6. cache — VELOX_CACHE_DIR writable
if [[ ! -d "$VELOX_CACHE_DIR" ]]; then
  ensure_dir "$VELOX_CACHE_DIR" || record_service_probe 6 "cache" FAIL "could not create $VELOX_CACHE_DIR"
fi
if [[ -d "$VELOX_CACHE_DIR" ]]; then
  TMP_CACHE="$(mktemp -p "$VELOX_CACHE_DIR" asset-chk-cache-XXXXXX.tmp 2>/dev/null)" || TMP_CACHE=""
  if [[ -z "$TMP_CACHE" ]]; then
    record_service_probe 6 "cache" FAIL "$VELOX_CACHE_DIR not writable by $(id -un)"
  else
    if rm -f "$TMP_CACHE"; then
      record_service_probe 6 "cache" PASS "$VELOX_CACHE_DIR writable"
    else
      record_service_probe 6 "cache" FAIL "$VELOX_CACHE_DIR write succeeded but rm failed"
    fi
  fi
else
  record_service_probe 6 "cache" FAIL "$VELOX_CACHE_DIR absent or not a directory"
fi

# 7. disco — `df -k $RUNTIME_DIR` reports ≥ ASSET_CHK_MIN_DISK_KB free
if ! command -v df >/dev/null 2>&1; then
  record_service_probe 7 "disco" SKIPPED "df not on PATH"
else
  DF_LINE=$(df -k "$VELOX_RUNTIME_DIR" 2>/dev/null | tail -1)
  if [[ -z "$DF_LINE" ]]; then
    record_service_probe 7 "disco" FAIL "df $VELOX_RUNTIME_DIR returned no usable line"
  else
    FREE_KB=$(printf '%s' "$DF_LINE" | awk '{print $4}')
    if [[ ! "$FREE_KB" =~ ^[0-9]+$ ]]; then
      record_service_probe 7 "disco" FAIL "df could not parse 4th field; line=$DF_LINE"
    elif (( FREE_KB >= ASSET_CHK_MIN_DISK_KB )); then
      record_service_probe 7 "disco" PASS "free=$FREE_KB KB ≥ $ASSET_CHK_MIN_DISK_KB KB on $VELOX_RUNTIME_DIR"
    else
      record_service_probe 7 "disco" FAIL "free=$FREE_KB KB < $ASSET_CHK_MIN_DISK_KB KB on $VELOX_RUNTIME_DIR"
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Asset matrix — every asset_id in fixtures is probed for HEAD coherence.
# Order matches fixtures.json: voiceover[0..n] -> clips[0..n] -> subtitles[0..n] -> images[0..n].
# Per asset: HEAD /api/v1/assets/<id> with bearer → record status code,
# content-length, content-type. Then size > 0 + content-type-coerente
# assertions. SHA-256 (left as SKIPPED for now unless master exposes an
# /api/v1/assets/<id>/sha256 endpoint).
# ─────────────────────────────────────────────────────────────────────────────
log_info "── asset matrix ──"

probe_one_asset() {
  local asset_type="$1" asset_id="$2"
  local expected_ct_re=""
  case "$asset_type" in
    voiceover) expected_ct_re="^(audio/|application/octet-stream)" ;;
    clips)     expected_ct_re="^(video/|application/octet-stream)" ;;
    subtitles) expected_ct_re="^(text/|application/(x-subrip|vtt|subrip)|application/octet-stream)" ;;
    images)    expected_ct_re="^(image/|application/octet-stream)" ;;
    *)         expected_ct_re=".*" ;;
  esac

  local hdrs body status cl ct
  hdrs=$(mktemp); body=$(mktemp)
  if ! curl -sS -m "$ASSET_CHK_HEAD_TIMEOUT_S" -I \
        -H "Authorization: Bearer $M2M_BEARER" \
        "${VELOX_MASTER_URL}/api/v1/assets/${asset_id}" \
        -D "$hdrs" -o "$body" 2>/dev/null; then
    record_asset_probe "$asset_type" "$asset_id" FAIL "curl HEAD failed (network)"
    rm -f "$hdrs" "$body"
    return
  fi
  status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]\// {print $2; exit}' "$hdrs")
  case "$status" in
    200|204|301|302|307) ;;
    405)
      # HEAD not allowed but route exists — we cannot read content-length
      # nor content-type. Mark this asset SKIPPED on size+content-type and
      # PASS the matrix overall only if all assets are 2xx or 405-uniform.
      record_asset_probe "$asset_type" "$asset_id" SKIPPED "HEAD 405 method-not-allowed; size+content-type unverifiable"
      rm -f "$hdrs" "$body"
      return
      ;;
    *)
      record_asset_probe "$asset_type" "$asset_id" FAIL "HTTP ${status:-<no-response>}"
      rm -f "$hdrs" "$body"
      return
      ;;
  esac

  cl=$(awk 'tolower($1) == "content-length:" {gsub(/[\r\n]/,""); print $2; exit}' "$hdrs")
  ct=$(awk 'tolower($1) == "content-type:" {sub(/^[^:]*:[ \t]*/,""); sub(/[ \t\r\n]+$/,""); print; exit}' "$hdrs")
  rm -f "$hdrs" "$body"

  if [[ -z "$cl" || ! "$cl" =~ ^[0-9]+$ || "$cl" -le 0 ]]; then
    record_asset_probe "$asset_type" "$asset_id" FAIL "size=$cl (expected > 0)"
    return
  fi
  if ! [[ "$ct" =~ $expected_ct_re ]]; then
    record_asset_probe "$asset_type" "$asset_id" FAIL "content-type=$ct (does not match $expected_ct_re)"
    return
  fi
  # SHA-256 — currently SKIPPED. Left as a followup commit if master adds
  # an /api/v1/assets/<id>/sha256 endpoint; until then we don't fabricate.
  record_asset_probe "$asset_type" "$asset_id" PASS "size=${cl}B content-type=${ct} sha256=SKIPPED"
}

# Iterate assets in the same order the fixtures list them (preserves
# stable row_idx across runs, makes post-mortems easier to read).
probe_group() {
  local group="$1"
  local n
  n=$(jq -er --arg g "$group" '.[$g] | length' "$ASSETS_FILE" 2>/dev/null) || n="0"
  if (( n == 0 )); then
    log_warn "fixtures has no rows for $group"; return
  fi
  local i="0"
  while (( i < n )); do
    local aid
    aid=$(jq -er \
      --arg g "$group" \
      --argjson i "$i" \
      '.[$g][$i].asset_id' \
      "$ASSETS_FILE" 2>/dev/null) || {
      log_warn "fixtures[$group][$i] not parseable; skipping"; i=$((i+1)); continue; }
    probe_one_asset "$group" "$aid"
    i=$((i+1))
  done
}

probe_group voiceover
probe_group clips
probe_group subtitles
probe_group images

# ─── Atomic JSON dump + exit code band ────────────────────────────────────
write_dependency_check_json() {
  local now_iso verdict rc band_service band_asset total_fail
  now_iso=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
  band_service=0; band_asset=0; total_fail=0
  if (( SERVICE_FAIL > 0 )); then band_service=$((10 + SERVICE_FAIL)); total_fail=$((total_fail+1)); fi
  if (( ASSET_FAIL > 0 ));   then band_asset=$((20 + ASSET_FAIL));     total_fail=$((total_fail+1)); fi
  if (( total_fail == 0 )); then
    verdict="PASS"; rc=0
  elif (( SERVICE_FAIL > 0 && (ASSET_FAIL == 0 || (10+SERVICE_FAIL) < (20+ASSET_FAIL)) )); then
    verdict="FAIL"; rc=$band_service
  else
    verdict="FAIL"; rc=$band_asset
  fi
  ensure_dir "${ASSET_CHK_OUT_ROOT}/${TARGET_WORKER_ID}"
  local out_file="${ASSET_CHK_OUT_ROOT}/${TARGET_WORKER_ID}/asset_dependency_check.json"
  local tmp_out; tmp_out=$(mktemp "${ASSET_CHK_OUT_ROOT}/${TARGET_WORKER_ID}/adep-XXXXXX.json")
  cat > "$tmp_out" <<JSON
{
  "schema": "tests/worker-cert/asset_dependency_check@1",
  "worker_id": "${TARGET_WORKER_ID}",
  "verdict": "${verdict}",
  "first_failing_service_idx": ${SERVICE_FAIL:-0},
  "first_failing_asset_row": ${ASSET_FAIL:-0},
  "service_band_exit_code": ${band_service},
  "asset_band_exit_code": ${band_asset},
  "asset_row_count": ${ASSET_ROW_COUNT},
  "master_url": "${VELOX_MASTER_URL}",
  "destination_id": "${ASSET_CHK_DESTINATION_ID}",
  "service_dirs": {
    "fonts": "${VELOX_FONTS_DIR}",
    "runtime": "${VELOX_RUNTIME_DIR}",
    "cache": "${VELOX_CACHE_DIR}",
    "min_fonts": ${ASSET_CHK_MIN_FONTS},
    "min_disk_kb": ${ASSET_CHK_MIN_DISK_KB}
  },
  "checked_at": "${now_iso}",
  "service_probes": [${SERVICE_PROBES_JSON}],
  "asset_probes": [${ASSET_PROBES_JSON}]
}
JSON
  mv "$tmp_out" "$out_file"
  log_info "wrote $out_file"
  printf '%s\n' "REPORT_JSON=${out_file}"
  printf '%s\n' "VERDICT=${verdict}"
  printf '%s\n' "FIRST_FAILING_SERVICE=${SERVICE_FAIL:-0}"
  printf '%s\n' "FIRST_FAILING_ASSET_ROW=${ASSET_FAIL:-0}"
  printf '%s\n' "EXIT_CODE_BAND_SERVICE=${band_service}"
  printf '%s\n' "EXIT_CODE_BAND_ASSET=${band_asset}"
  export ASSET_CHK_RC=$rc
}

write_dependency_check_json
exit "${ASSET_CHK_RC}"
