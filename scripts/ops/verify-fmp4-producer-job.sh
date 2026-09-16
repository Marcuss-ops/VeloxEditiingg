#!/usr/bin/env bash
# scripts/ops/verify-fmp4-producer-job.sh
# ─────────────────────────────────────────────────────────────────────────────
# End-to-end acceptance for the fMP4 (fragmented MP4) final-mux migration,
# driven by a REAL producer-submitted job — not the canary.
#
# It submits exactly what the producer (PipelineGen) is required to send
# through the canonical M2M intake documented in
# docs/operations/REMOTE-M2M-JOB-OPERATIONS.md: a producer-owned
# CompiledRenderPlanV2 in compiled_render_plan_json + compiled_render_plan_sha256.
# The Master never compiles or infers the plan, and VELOX_FMP4_STREAM_PROFILE=1
# on the worker is only an admission gate — it does NOT convert legacy jobs.
#
# THREE ACCEPTANCE CHECKS
#   A. output.profile_id is velox-h264-fmp4-stream-v1 on the submitted plan
#      (the profile the worker fast-path and video.assemble.copy.v1 require).
#   B. artifact.safe_offset_bytes > 0 was observed WHILE the render was still
#      running (artifact.finalized != 1) — the progressive-upload win that
#      justified fMP4. Requires VELOX_ADMIN_TOKEN (operator read side).
#   C. the final published artifact contains `moof` (fragmented container) and
#      is non-empty.
#
# Mutating: with --plan this script SUBMITS a real job. Use --dry-run to print
# the request body without sending it, or --job-id to verify a job that was
# already submitted (check B then needs the job to still be running).
#
# Usage:
#   scripts/ops/verify-fmp4-producer-job.sh --plan <compiled-plan-v2.json>
#   scripts/ops/verify-fmp4-producer-job.sh --plan plan.json --dry-run
#   scripts/ops/verify-fmp4-producer-job.sh --plan plan.json --job-id job_xxx
#   scripts/ops/verify-fmp4-producer-job.sh --plan plan.json --artifact /tmp/out.mp4
#   scripts/ops/verify-fmp4-producer-job.sh --plan plan.json --allow-artifact-only
#
# Required env:
#   VELOX_MASTER_URL    master REST base URL (e.g. https://master.example:8000)
#   VELOX_M2M_SECRET    producer M2M secret (scope jobs.submit)
#   VELOX_CLIENT_ID     producer client id (e.g. computer-editor-77-01)
# Optional env:
#   VELOX_ADMIN_TOKEN   operator token; REQUIRED for check B (live safe_offset)
#
# Options:
#   --plan PATH             CompiledRenderPlanV2 JSON (required)
#   --job-id ID             verify this job instead of submitting a new one
#   --artifact PATH         check a local file for check C (skips download)
#   --delivery-destination ID   delivery_plan destination_id (default local-fallback)
#   --video-name NAME       video_name (default: fMP4 acceptance <timestamp>)
#   --idempotency-key KEY   reuse a key for retries of the same logical job
#   --wait SECONDS          terminal-state budget (default 3600)
#   --interval SECONDS      poll interval (default 5)
#   --emit-body PATH        write the generated submit body to PATH (forensics)
#   --dry-run               do not submit; print what would be sent
#   --allow-artifact-only   pass when check B could not be observed (never silent)
#   --json                  emit the evidence summary as JSON on stdout
#   --help
#
# Exit codes:
#   0  PASS        all three checks green
#   1  FAIL        a check failed (wrong profile, no moof, job failed)
#   2  PRECONDITION usage error, missing tool/env, plan/manifest mismatch
#   3  INCONCLUSIVE a check could not be observed (missing admin token, job
#                   already terminal when polling started, artifact not ready)
set -uo pipefail

FMP4_PROFILE_ID="velox-h264-fmp4-stream-v1"

PLAN=""
JOB_ID=""
ARTIFACT=""
DESTINATION="local-fallback"
VIDEO_NAME=""
IDEMPOTENCY_KEY=""
WAIT_S=3600
INTERVAL_S=5
DRY_RUN=0
EMIT_BODY=""
ALLOW_ARTIFACT_ONLY=0
JSON=0

usage() {
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan) [[ $# -ge 2 ]] || usage; PLAN="$2"; shift 2 ;;
    --job-id) [[ $# -ge 2 ]] || usage; JOB_ID="$2"; shift 2 ;;
    --artifact) [[ $# -ge 2 ]] || usage; ARTIFACT="$2"; shift 2 ;;
    --delivery-destination) [[ $# -ge 2 ]] || usage; DESTINATION="$2"; shift 2 ;;
    --video-name) [[ $# -ge 2 ]] || usage; VIDEO_NAME="$2"; shift 2 ;;
    --idempotency-key) [[ $# -ge 2 ]] || usage; IDEMPOTENCY_KEY="$2"; shift 2 ;;
    --wait) [[ $# -ge 2 ]] || usage; WAIT_S="$2"; shift 2 ;;
    --interval) [[ $# -ge 2 ]] || usage; INTERVAL_S="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --emit-body) [[ $# -ge 2 ]] || usage; EMIT_BODY="$2"; shift 2 ;;
    --allow-artifact-only) ALLOW_ARTIFACT_ONLY=1; shift ;;
    --json) JSON=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

fail_precondition() { printf 'PRECONDITION: %s\n' "$*" >&2; exit 2; }
fail_check() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

for tool in curl jq mktemp; do
  command -v "$tool" >/dev/null 2>&1 || fail_precondition "required tool missing from PATH: $tool"
done
[[ -n "$PLAN" ]] || usage
[[ -r "$PLAN" ]] || fail_precondition "plan file is not readable: $PLAN"
[[ "$WAIT_S" =~ ^[0-9]+$ && "$WAIT_S" -gt 0 ]] || fail_precondition "--wait must be a positive integer"
[[ "$INTERVAL_S" =~ ^[0-9]+$ && "$INTERVAL_S" -gt 0 ]] || fail_precondition "--interval must be a positive integer"

# ── Check A: the plan must select the fMP4 profile explicitly ───────────────
plan_profile="$(jq -r '.output.profile_id // ""' "$PLAN" 2>/dev/null)"
[[ -n "$plan_profile" ]] || fail_precondition "plan has no output.profile_id: the Master never infers the profile from the worker flag"
if [[ "$plan_profile" != "$FMP4_PROFILE_ID" ]]; then
  fail_check "check A: output.profile_id = \"$plan_profile\", want \"$FMP4_PROFILE_ID\""
fi
printf 'Check A  OK        plan selects output.profile_id=%s\n' "$plan_profile"

MASTER="${VELOX_MASTER_URL:-}"
MASTER="${MASTER%/}"
M2M_AUTH=("Authorization: Bearer ${VELOX_M2M_SECRET-}")
ADMIN_AUTH=("Authorization: Bearer ${VELOX_ADMIN_TOKEN-}")

plan_sha=""
if command -v sha256sum >/dev/null 2>&1; then
  plan_sha="$(sha256sum "$PLAN" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  plan_sha="$(shasum -a 256 "$PLAN" | awk '{print $1}')"
else
  fail_precondition "neither sha256sum nor shasum is available"
fi

if [[ -z "$VIDEO_NAME" ]]; then
  VIDEO_NAME="fMP4 acceptance $(date -u +%Y%m%dT%H%M%SZ)"
fi
if [[ -z "$IDEMPOTENCY_KEY" ]]; then
  IDEMPOTENCY_KEY="fmp4-acceptance-$(date -u +%Y%m%dT%H%M%SZ)"
fi

TMP_DIR="$(mktemp -d -t velox-fmp4-acceptance.XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT
BODY="$TMP_DIR/submit.json"

jq -n \
  --rawfile plan "$PLAN" \
  --arg plan_sha "$plan_sha" \
  --arg video_name "$VIDEO_NAME" \
  --arg destination "$DESTINATION" \
  --arg idem "$IDEMPOTENCY_KEY" \
  '{
    job_type: "scene.composite.v1",
    video_name: $video_name,
    script_text: "Producer-owned compiled render plan (fMP4 acceptance)",
    compiled_render_plan_json: $plan,
    compiled_render_plan_sha256: $plan_sha,
    delivery_plan: [{destination_id: $destination}],
    idempotency_key: $idem
  }' >"$BODY" || fail_precondition "failed to build the submit request body"

if [[ -n "$EMIT_BODY" ]]; then
  cp "$BODY" "$EMIT_BODY" || fail_precondition "cannot write --emit-body $EMIT_BODY"
fi

if (( DRY_RUN == 1 )); then
  printf 'DRY RUN: would POST %s/api/v1/jobs (plan_sha256=%s, job_type=scene.composite.v1)\n' "$MASTER" "$plan_sha"
  printf 'Check A  OK        plan selects output.profile_id=%s\n' "$plan_profile"
  if [[ -n "$EMIT_BODY" ]]; then
    printf 'DRY RUN: submit body written to %s\n' "$EMIT_BODY"
  else
    printf 'DRY RUN: submit body was staged in a temp dir (use --emit-body PATH to keep it)\n'
  fi
  printf 'DRY RUN: checks B and C require a real submission.\n'
  exit 0
fi

[[ -n "$MASTER" ]] || fail_precondition "VELOX_MASTER_URL is not set"
[[ -n "${VELOX_M2M_SECRET:-}" ]] || fail_precondition "VELOX_M2M_SECRET is not set (producer M2M credential)"
[[ -n "${VELOX_CLIENT_ID:-}" ]] || fail_precondition "VELOX_CLIENT_ID is not set (producer M2M client id)"

# ── Submit through the canonical producer intake ────────────────────────────
if [[ -z "$JOB_ID" ]]; then
  response="$(curl --fail-with-body -sS -X POST "$MASTER/api/v1/jobs" \
    -H "${M2M_AUTH[0]}" \
    -H 'Content-Type: application/json' \
    -H "X-Request-ID: ${VELOX_CLIENT_ID}-${IDEMPOTENCY_KEY}" \
    --data-binary @"$BODY" 2>"$TMP_DIR/submit.err")" || {
    cat "$TMP_DIR/submit.err" >&2
    fail_check "producer submit failed (see body above)"
  }
  JOB_ID="$(jq -r '.job_id // ""' <<<"$response")"
  [[ -n "$JOB_ID" ]] || fail_check "submit response carried no job_id: $response"
  printf 'Submitted job_id=%s (idempotency_key=%s)\n' "$JOB_ID" "$IDEMPOTENCY_KEY"
else
  printf 'Verifying existing job_id=%s\n' "$JOB_ID"
fi

# ── Poll: check B (safe_offset before finalize) while the render runs ───────
safe_offset_peak=0
safe_offset_before_finalize=0
safe_offset_seen_finalized=0
metrics_observable=1
attempts=0
deadline=$(( $(date +%s) + WAIT_S ))
status="UNKNOWN"

while :; do
  attempts=$((attempts + 1))
  status_resp="$(curl -sS --max-time 15 -H "${M2M_AUTH[0]}" -H 'Accept: application/json' \
    "$MASTER/api/v1/jobs/$JOB_ID" 2>/dev/null || true)"
  status="$(jq -r '.status // "UNKNOWN"' <<<"${status_resp:-{\}}" 2>/dev/null || echo UNKNOWN)"

  if [[ -n "${VELOX_ADMIN_TOKEN:-}" ]]; then
    metrics_doc="$(curl -sS --max-time 15 -H "${ADMIN_AUTH[0]}" \
      "$MASTER/api/v1/admin/jobs/$JOB_ID" 2>/dev/null || true)"
    if [[ -n "$metrics_doc" ]]; then
      read -r safe finalized <<<"$(jq -r '[.. | objects | .cumulative_metrics? // empty] | add // {}
        | [ ((.["artifact.safe_offset_bytes"] // 0) | tonumber | floor), ((.["artifact.finalized"] // 0) | tonumber | floor) ] | @tsv' <<<"$metrics_doc" 2>/dev/null || true)"
      [[ "$safe" =~ ^[0-9]+$ ]] || safe=0
      [[ "$finalized" =~ ^[0-9]+$ ]] || finalized=0
      if (( safe > safe_offset_peak )); then safe_offset_peak="$safe"; fi
      if (( safe > 0 && finalized == 0 )); then
        safe_offset_before_finalize=1
      elif (( finalized == 1 )); then
        safe_offset_seen_finalized=1
      fi
    else
      metrics_observable=0
    fi
  else
    metrics_observable=0
  fi

  case "$status" in
    SUCCEEDED|FAILED|CANCELLED) break ;;
  esac
  if (( $(date +%s) >= deadline )); then
    status="TIMEOUT"
    break
  fi
  sleep "$INTERVAL_S"
done

if [[ "$status" != "SUCCEEDED" ]]; then
  fail_check "job $JOB_ID ended status=$status (attempts=$attempts); no acceptance evidence"
fi

# ── Check C: the published artifact must carry moof ─────────────────────────
artifact_path="$ARTIFACT"
if [[ -z "$artifact_path" ]]; then
  for _ in $(seq 1 30); do
    status_resp="$(curl -sS --max-time 15 -H "${M2M_AUTH[0]}" -H 'Accept: application/json' \
      "$MASTER/api/v1/jobs/$JOB_ID" 2>/dev/null || true)"
    artifact_url="$(jq -r '.artifact_url // ""' <<<"${status_resp:-{\}}" 2>/dev/null || true)"
    [[ -n "$artifact_url" ]] && break
    sleep "$INTERVAL_S"
  done
  [[ -n "${artifact_url:-}" ]] || fail_check "check C: the job exposes no artifact_url"
  artifact_path="$TMP_DIR/artifact.mp4"
  curl -fsS --max-time 600 -H "${M2M_AUTH[0]}" -o "$artifact_path" "$artifact_url" \
    || fail_check "check C: artifact download failed ($artifact_url)"
fi

[[ -s "$artifact_path" ]] || fail_check "check C: artifact is empty or missing: $artifact_path"
artifact_bytes="$(wc -c <"$artifact_path" | tr -d '[:space:]')"
if LC_ALL=C grep -aq 'moof' "$artifact_path"; then
  moof_present=1
else
  moof_present=0
fi
if (( moof_present == 0 )); then
  fail_check "check C: artifact has no moof box (progressive MP4?): $artifact_path"
fi
printf 'Check C  OK        artifact=%s bytes=%s moof=present\n' "$artifact_path" "$artifact_bytes"

# ── Check B verdict ────────────────────────────────────────────────────────
status_b="OK"
if (( safe_offset_before_finalize == 1 )); then
  printf 'Check B  OK        safe_offset_bytes>0 observed before finalize (peak=%s)\n' "$safe_offset_peak"
elif (( ALLOW_ARTIFACT_ONLY == 1 )); then
  status_b="WAIVED"
  printf 'Check B  WAIVED    safe_offset not observed (metrics_observable=%s, finalized_seen=%s, peak=%s); --allow-artifact-only\n' \
    "$metrics_observable" "$safe_offset_seen_finalized" "$safe_offset_peak"
else
  printf 'Check B  INCONCLUSIVE  safe_offset before finalize was not observed (metrics_observable=%s, finalized_seen=%s, peak=%s)\n' \
    "$metrics_observable" "$safe_offset_seen_finalized" "$safe_offset_peak" >&2
  if (( JSON == 1 )); then
    printf '{"job_id":"%s","plan_profile_id":"%s","safe_offset_bytes_peak":%s,"safe_offset_before_finalize":false,"artifact_bytes":%s,"moof_present":true,"verdict":"INCONCLUSIVE"}\n' \
      "$JOB_ID" "$plan_profile" "$safe_offset_peak" "$artifact_bytes"
  fi
  printf 'Hint: export VELOX_ADMIN_TOKEN so check B can read /api/v1/admin/jobs/%s,\n' "$JOB_ID" >&2
  printf 'and start the harness BEFORE the render finishes (or use --allow-artifact-only).\n' >&2
  exit 3
fi

VERDICT="PASS"
(( safe_offset_before_finalize == 0 )) && VERDICT="PASS-$status_b"
if (( JSON == 1 )); then
  printf '{"job_id":"%s","plan_profile_id":"%s","plan_sha256":"%s","safe_offset_bytes_peak":%s,"safe_offset_before_finalize":%s,"artifact_bytes":%s,"moof_present":true,"verdict":"%s"}\n' \
    "$JOB_ID" "$plan_profile" "$plan_sha" "$safe_offset_peak" \
    "$([[ $safe_offset_before_finalize == 1 ]] && echo true || echo false)" "$artifact_bytes" "$VERDICT"
else
  printf '\nFMP4 PRODUCER-JOB ACCEPTANCE: %s (job_id=%s, profile=%s, safe_offset_peak=%s, artifact=%s bytes, moof=present)\n' \
    "$VERDICT" "$JOB_ID" "$plan_profile" "$safe_offset_peak" "$artifact_bytes"
fi
exit 0
