#!/usr/bin/env bash
# deploy/runtime/submit-canary.sh
# ─────────────────────────────────────────────────────────────────────────────
# End-to-end canary: prove that a freshly-deployed Velox worker can render a
# minimal, deterministic scene.composite.v1 job to a verified artifact.
#
# Workflow:
#   1. Pre-flight tools + env + read master reachability + worker container.
#   2. Generate fixtures INSIDE the worker container via docker exec (the
#      worker container has /tmp as a per-container tmpfs and CANNOT see
#      host /tmp — generating on host would 404 inside the render pipeline).
#   3. POST a minimal RenderPlan to the master orchestrator endpoint.
#   4. Poll velox.db for terminal status (SUCCEEDED/FAILED/CANCELLED).
#   5. Fetch artifact metadata, compute local sha256, run ffprobe, assert
#      all 4 invariants (width / height / codec / duration) match.
#   6. Emit ONE of: "PASS: ...", "FAIL: ...", "SKIP: ..." on stdout/stderr.
#      Exit codes: 0 PASS, 1 FAIL, 255 SKIP (so checklist-verify.sh can
#      distinguish "the canary was inapplicable" from "the canary failed").
#
# Required env (the CHECKLIST caller supplies these; see
# deploy/runtime/checklist-verify.sh §15 + worker.env.example):
#
#   VELOX_MASTER_URL       e.g. http://127.0.0.1:8000
#                          (master's HTTP base URL; the script appends
#                          /api/v1/orchestrator/jobs to it).
#   VELOX_ADMIN_TOKEN      bearer token for AdminAuthMiddleware on POST.
#                          Must NOT match the GitHub-PAT regex (ghp_|ghs_|
#                          gho_|ghu_|github_pat_) so it doesn't trip
#                          checklist-verify.sh §7 if it lives in
#                          worker.env.
#   VELOX_DB_PATH          path to master's SQLite DB.
#                          E.g. /var/lib/velox-server/velox.db on a
#                          co-located topology. Remote master topologies
#                          must expose a read replica or a download
#                          endpoint (v2 work).
#
# Optional env:
#   VELOX_WORKER_ID        container suffix (defaults to docker ps detect).
#   VELOX_CANARY_WIDTH     expected video width  (default 64)
#   VELOX_CANARY_HEIGHT    expected video height (default 64)
#   VELOX_CANARY_DURATION  expected duration     (default 1.0, ±10% tolerance)
#
# Exit codes:
#   0   canary rendered and verified
#   1   canary FAILED — could not be rendered OR verification failed.
#       Two sub-classes:
#         1a. pre-flight FAIL (render tool missing / JOB_ID validation
#             failed / docker exec fixture error / POST failed) → FAIL
#             emit, RC=1.
#         1b. verification FAIL (sha256 mismatch / one of ffprobe's
#             width|height|codec|duration invariants broken) → FAIL
#             emit, RC=1.
#   255 canary was SKIPPED — opt-in prerequisites absent (missing env
#       / missing worker container). SECTIONS §15 SHOULD NEVER SILENTLY
#       PASS-OR-SKIP just because the operator hasn't wired canary
#       config; this exit code lets checklist-verify.sh label it SKIP.
#
# Two-tier tool pre-flight:
#   infrastructure (docker, jq, curl)  → missing = SKIP (rc=255).
#       These are pre-conditions for §5-§14; absence is a host issue.
#   render-required (ffmpeg, ffprobe, sqlite3) → missing = FAIL (rc=1).
#       §15's whole purpose is to prove the render pipeline works;
#       silently SKIPing when ffmpeg is gone would let a "clean VPS"
#       pass without ever exercising the render path. FAIL-fast here
#       surfaces the missing dependency to the operator immediately.
#
# Designed for checklist-verify.sh §15 — the verifier parses the
# "PASS:" / "FAIL:" / "SKIP:" sentinel and bridges to record().

set -euo pipefail

emit_pass() { printf 'PASS: %s\n' "$*\n"; }
emit_fail() { printf 'FAIL: %s\n' "$*" >&2; }
emit_skip() { printf 'SKIP: %s\n' "$*" >&2; }

# ── 1. Pre-flight: tools (two-tier) ────────────────────────────────────────
# Infrastructure tools — absence is a host issue, not a canary failure.
# openssl is here because the hardened `IDEM` generation below needs
# `openssl rand -hex 8`. Lacking openssl falls back to a brittle
# date+PID+RANDOM combo; rather than downgrade silently, we SKIP.
for tool in docker curl jq openssl; do
    command -v "$tool" >/dev/null 2>&1 \
        || { emit_skip "infrastructure tool not in PATH: $tool (openssl is required for hardened idempotency_key; install or accept reduced entropy)"; exit 255; }
done
# Render-required tools — §15 cannot prove the pipeline works without
# these. Missing = FAIL (rc=1), NOT SKIP, so a "clean VPS" without
# ffmpeg does NOT silently pass the audit.
for tool in ffmpeg ffprobe sqlite3; do
    command -v "$tool" >/dev/null 2>&1 \
        || { emit_fail "render tool not in PATH: $tool (install ffmpeg + sqlite3 CLI on the worker host; §15 cannot exercise the pipeline without them)"; exit 1; }
done

# ── 2. Pre-flight: env ─────────────────────────────────────────────────────
: "${VELOX_MASTER_URL:?VELOX_MASTER_URL not set (caller should derive from §14 or set explicitly)}"
: "${VELOX_ADMIN_TOKEN:?VELOX_ADMIN_TOKEN not set (must be set in worker.env or canary.env)}"
: "${VELOX_DB_PATH:?VELOX_DB_PATH not set (must be set in worker.env or canary.env)}"

[[ -r "${VELOX_DB_PATH}" ]] \
    || { emit_skip "VELOX_DB_PATH not readable: ${VELOX_DB_PATH}"; exit 255; }

# ── 3. Worker container: detect if not explicitly set ──────────────────────
WORKER_ID="${VELOX_WORKER_ID:-}"
if [[ -z "$WORKER_ID" ]]; then
    WORKER_ID="$(docker ps --filter 'label=com.docker.compose.project=velox-worker' \
        --format '{{.Names}}' 2>/dev/null \
        | sed 's/^velox-worker-//' | head -1 || true)"
    [[ -z "$WORKER_ID" ]] && {
        emit_skip "no running velox-worker-* container detected and VELOX_WORKER_ID unset"
        exit 255
    }
fi
CONTAINER="velox-worker-${WORKER_ID}"
docker inspect "$CONTAINER" >/dev/null 2>&1 \
    || { emit_skip "container ${CONTAINER} not found via docker inspect"; exit 255; }

# ── 4. Generate fixtures INSIDE the worker container ───────────────────────
# The worker's /tmp is a per-container tmpfs (compose.yml `tmpfs: /tmp:size=4g`).
# Host /tmp is NOT mounted into the worker, so we cannot rely on host-local
# fixture paths being readable by the executor. Generating them inside via
# docker exec guarantees the render pipeline finds them.
DOCKER_EXEC_RC=0
docker exec "$CONTAINER" bash -c '
    set -euo pipefail
    mkdir -p /tmp/velox-canary
    # 64x64 teal PNG, single-frame still image.
    ffmpeg -y -f lavfi -i "color=c=0x008080:s=64x64:d=1" \
        -frames:v 1 /tmp/velox-canary/scene.png 2>/dev/null
    # 1-second silent AAC (mono 48kHz, 64 kbps) — pipeline.Runner needs an
    # audio.source stream to route through hybrid.v1.
    ffmpeg -y -f lavfi -i "anullsrc=r=48000:cl=mono" \
        -t 1 -c:a aac -b:a 64k /tmp/velox-canary/silent.m4a 2>/dev/null
' >/dev/null 2>&1 || DOCKER_EXEC_RC=$?
(( DOCKER_EXEC_RC == 0 )) || {
    emit_fail "docker exec fixture generation failed inside ${CONTAINER} (rc=${DOCKER_EXEC_RC})"
    exit 1
}

# Sanity: PNG is readable inside the container (binary header check, no
# network round-trip; ffprobe exits 0 only on parseable media).
docker exec "$CONTAINER" ffprobe -v error -show_streams -of json /tmp/velox-canary/scene.png >/dev/null 2>&1 \
    || { emit_fail "fixture PNG not readable inside ${CONTAINER}"; exit 1; }

# ── 5. Build minimal canary payload ────────────────────────────────────────
EXPECTED_W="${VELOX_CANARY_WIDTH:-64}"
EXPECTED_H="${VELOX_CANARY_HEIGHT:-64}"
EXPECTED_DUR="${VELOX_CANARY_DURATION:-1.0}"

# idempotency_key MUST be unique per run; orchestrator handler returns 409
# conflict otherwise (creatorflow.CreateJobWithPlan CAS gates on this).
# 16 hex chars from openssl rand = 64 bits of entropy — collision-safe
# even in chained CI runs where $$+RANDOM (15 bits) would collide.
IDEM="canary-$(date +%s)-$$-$(openssl rand -hex 8)"

# Reference INSIDE-container file:// paths so the worker's render can read
# them. scenes_json is a JSON-encoded string (not an array) so multi-line
# shapes survive the orchestrator handler's JSON round-trip.
SCENE_JSON=$(jq -nc \
    --arg img "file:///tmp/velox-canary/scene.png" \
    --argjson dur "$EXPECTED_DUR" \
    '[{text: "canary", image: $img, duration_sec: $dur}]')

PAYLOAD=$(jq -n \
    --arg idem "$IDEM" \
    --arg scenes "$SCENE_JSON" \
    --arg audio "/tmp/velox-canary/silent.m4a" \
    --arg outpath "/tmp/velox-canary/canary.mp4" \
    '{
        video_name:     "Velox Canary Render",
        project_id:      "canary",
        executor_id:     "scene.composite.v1",
        run_id:          "canary-run",
        idempotency_key: $idem,
        max_retries:     0,
        priority:        100,
        payload: {
            scenes_json:    $scenes,
            voiceover_paths: [$audio],
            output_path:    $outpath
        }
    }')

# ── 6. Submit ──────────────────────────────────────────────────────────────
SUBMIT_RESP="$(curl -sS --max-time 10 \
    -X POST "${VELOX_MASTER_URL}/api/v1/orchestrator/jobs" \
    -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data "$PAYLOAD" 2>/dev/null)" \
    || { emit_fail "curl POST failed (network/DNS/timeout to ${VELOX_MASTER_URL})"; exit 1; }

JOB_ID="$(printf '%s' "$SUBMIT_RESP" | jq -r '.job_id // empty' 2>/dev/null)"
[[ -z "$JOB_ID" ]] && {
    emit_fail "submit returned no job_id (response: ${SUBMIT_RESP:0:240})"
    exit 1
}

# Whitelist JOB_ID before interpolating it into sqlite3 queries below.
# The master only generates UUID-shaped IDs, but we MUST defensively
# refuse anything else — a compromised master returning a string with
# `'; DROP TABLE jobs; --` would otherwise break the canary's audit
# integrity. Pattern widened from strict lowercase hex (UUID v4) to
# `[A-Za-z0-9._-]{8,128}` so test fixtures that use uppercase hex or
# underscores still get accepted. Single quote `'` is intentionally
# excluded: it would close the SQL literal immediately.
[[ "$JOB_ID" =~ ^[A-Za-z0-9._-]{8,128}$ ]] \
    || { emit_fail "JOB_ID has unexpected shape; refused to interpolate into sqlite3 (got: ${JOB_ID:0:16}...)"; exit 1; }

# ── 7. Poll master SQLite for terminal state ───────────────────────────────
POLL_DEADLINE=$(( $(date +%s) + 120 ))
POLL_INTERVAL=5
LAST_STATUS=""
while (( $(date +%s) < POLL_DEADLINE )); do
    STATUS="$(sqlite3 "$VELOX_DB_PATH" \
        "SELECT status FROM jobs WHERE job_id = '${JOB_ID}';" 2>/dev/null || true)"
    LAST_STATUS="$STATUS"
    case "$STATUS" in
        SUCCEEDED)            break ;;
        FAILED|CANCELLED)
            emit_fail "job ${JOB_ID} reached terminal failure: ${STATUS}"
            exit 1
            ;;
    esac
    sleep "$POLL_INTERVAL"
done

[[ "$LAST_STATUS" == "SUCCEEDED" ]] \
    || { emit_fail "poll timeout (>120s); last status: ${LAST_STATUS:-<none>}"; exit 1; }

# ── 8. Fetch artifact metadata + verify sha256 ────────────────────────────
# DB row shape: id | status | sha256 | local_path
ROW="$(sqlite3 -separator '|' "$VELOX_DB_PATH" \
    "SELECT id, status, sha256, local_path
     FROM artifacts WHERE job_id = '${JOB_ID}' AND status = 'READY'
     ORDER BY verified_at DESC LIMIT 1;" 2>/dev/null)"
[[ -z "$ROW" ]] && { emit_fail "no READY artifact for job ${JOB_ID}"; exit 1; }

# Preserve the SHA from leak-prone FAIL detail rendering: emit the full
# SHA only when we know it matches; otherwise emit a fingerprint suffix
# to keep logs clean against shoulder-surfing.
DB_SHA="$(printf '%s' "$ROW" | awk -F'|' '{print $3}')"
LOCAL_PATH="$(printf '%s' "$ROW" | awk -F'|' '{print $4}' | sed 's|^file://||')"

[[ -f "$LOCAL_PATH" ]] \
    || { emit_fail "artifact missing on disk: ${LOCAL_PATH}"; exit 1; }

DISK_SHA="$(sha256sum "$LOCAL_PATH" | awk '{print $1}')"
[[ "$DISK_SHA" == "$DB_SHA" ]] \
    || { emit_fail "sha256 mismatch (db=${DB_SHA:0:16}... disk=${DISK_SHA:0:16}...)"; exit 1; }

# ── 9. ffprobe invariants ──────────────────────────────────────────────────
# Capture stderr separately so a non-media file surfaces its real
# failure reason (`Invalid data found when processing input`) instead
# of being masked by an "empty output" sentinel. 500-char tail
# accommodates long error lines for pathologically invalid media.
PROBE_ERR="$(mktemp /tmp/velox-canary-ffprobe-err.XXXXXX)"
PROBE="$(ffprobe -v error -print_format json \
    -show_format -show_streams "$LOCAL_PATH" 2>"$PROBE_ERR")" \
    || PROBE_RC=$?
PROBE_RC="${PROBE_RC:-0}"
PROBE_ERR_TAIL="$(tail -c 500 "$PROBE_ERR" 2>/dev/null || true)"
rm -f "$PROBE_ERR"
if (( PROBE_RC != 0 )); then
    emit_fail "ffprobe exited rc=${PROBE_RC} on ${LOCAL_PATH}: ${PROBE_ERR_TAIL}"
    exit 1
fi
[[ -n "$PROBE" ]] || { emit_fail "ffprobe returned empty output"; exit 1; }

# Select the FIRST video stream (artifacts uploaded after h264 may have
# image/jpeg previews indexed before the main mp4 stream — only the lowest
# codec_type==video entry is canonical for the canary).
W="$(printf '%s' "$PROBE"  | jq -r '[.streams[] | select(.codec_type=="video")][0].width  // empty')"
H="$(printf '%s' "$PROBE"  | jq -r '[.streams[] | select(.codec_type=="video")][0].height // empty')"
CODEC="$(printf '%s' "$PROBE" | jq -r '[.streams[] | select(.codec_type=="video")][0].codec_name // empty')"
DUR="$(printf '%s' "$PROBE"  | jq -r '.format.duration // empty')"

# Composite FAIL detail — single fix cycle for the operator.
MISMATCH=""
[[ "$W"      == "$EXPECTED_W"   ]] || MISMATCH="$MISMATCH width=${W:-?}(want ${EXPECTED_W})"
[[ "$H"      == "$EXPECTED_H"   ]] || MISMATCH="$MISMATCH height=${H:-?}(want ${EXPECTED_H})"
[[ "$CODEC"  == "h264"          ]] || MISMATCH="$MISMATCH codec=${CODEC:-?}(want h264)"
if [[ -n "$DUR" ]]; then
    # Pure awk exit-code triage — much safer than a bash case regex on
    # the printed ratio string. Out-of-band ratios exit nonzero → only
    # the FAIL line is added. Replaces an earlier case-statement that
    # glitched on 1.15 (matched `1.1[0-9]`) → false PASS at +15%.
    #
    # awk exit-code contract:
    #   0  → in-band (0.9 ≤ ratio ≤ 1.1)        → PASS
    #   1  → out-of-band                          → FAIL "duration mismatch"
    #   2  → invalid EXPECTED_DUR (e <= 0)        → FAIL "expected duration invalid"
    #                                              (the prior `exit 0` here
    #                                              silently passed when the
    #                                              operator mis-configured
    #                                              VELOX_CANARY_DURATION=0,
    #                                              which is wrong.)
    awk -v d="$DUR" -v e="$EXPECTED_DUR" \
        'BEGIN{ e+=0; if (e<=0) exit 2; r=(d+0)/e; exit (r<0.9 || r>1.1) }'
    awk_rc=$?
    case "$awk_rc" in
        0) : ;;                          # in-band; pass
        2) MISMATCH="$MISMATCH expected_duration=${EXPECTED_DUR}(must be >0)" ;;
        *) MISMATCH="$MISMATCH duration=${DUR}s(want ~${EXPECTED_DUR}±10%)" ;;
    esac
else
    MISMATCH="$MISMATCH duration=<missing>"
fi

[[ -n "$MISMATCH" ]] && { emit_fail "ffprobe check failed:${MISMATCH}"; exit 1; }

# ── 10. PASS ───────────────────────────────────────────────────────────────
unset PROBE_RC  # tidy; not used outside §9
emit_pass "sha=${DB_SHA:0:16}... ${W}x${H} ${CODEC} dur=${DUR}s job_id=${JOB_ID}"
