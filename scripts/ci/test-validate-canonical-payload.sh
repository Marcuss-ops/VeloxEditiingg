#!/usr/bin/env bash
# scripts/ci/test-validate-canonical-payload.sh
#
# Self-test for the Step 8/8 closure semantic validator
# (shared/contract/cmd/validate-canonical-payload). Validates that the
# Go binary correctly:
#
#   1. Loads a JSON fixture, parses it into a map[string]interface{},
#      and runs contract.ValidatePayload over the result.
#   2. Exits 0 on a canonical payload (no legacy aliases, no shape
#      anomalies) — green-light signal for verify.sh.
#   3. Exits 1 on a payload that emits any of the 5 forbidden legacy
#      aliases (id, run_id, title, voiceover_path, audio_path) — the
#      binding writer-bug denylist.
#   4. Exits 1 on a payload with shape anomalies (non-string job_id,
#      non-array scenes, etc.) — the secondary gate.
#   5. Honors the --strict flag (passes --strict → switches to
#      StrictValidatePayload which additionally rejects drift keys).
#   6. Walks the default ops/jobs/*.json surface when no extras are
#      given (matches the verify.sh wiring pattern).
#
# CLI convention: the validator's first positional arg is the REPO ROOT
# (drives the default ops/jobs/*.json walk). Subsequent positional args
# are EXTRA fixture paths (files OR directories). The self-test calls
# the binary as:
#     validate-canonical-payload <repo-root> [extra.json ...]
# so the test's $TMP acts as the repo root and individual fixture
# files are passed as extras.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BIN_DIR="$REPO_ROOT/shared"
BIN_PATH="$BIN_DIR/contract/cmd/validate-canonical-payload"

# Ensure this self-test is executable (idempotent).
chmod +x "$SCRIPT_DIR/test-validate-canonical-payload.sh" 2>/dev/null || true

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf '%s\n' "$*"; }

# 1. The binary path must exist.
[ -d "$BIN_PATH" ] || fail "$BIN_PATH does not exist — the Step 8/8 closure is broken"

TMP="$(mktemp -d /tmp/validate-canonical-test.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

# run_validator REPO_ROOT [flags] [extra.json ...]
# Runs the Go binary; the first arg is treated as repo root, flags
# (anything matching --*) are hoisted to the front, and the rest are
# extra fixture paths. Returns the validator's exit code.
#
# Why hoist flags? Go's stdlib `flag` package (used by the validator
# binary) requires flags to appear BEFORE positional args in argv;
# `flag.Parse` silently leaves post-positional --flags as positional
# strings. The hoist is a UX bridge so callers can write
#   run_validator "$TMP" --strict "$fixture"
# naturally instead of having to know the Go-flag ordering rule.
run_validator() {
  local repo_root="$1"; shift
  local flags=()
  local positional=()
  for arg in "$@"; do
    if [[ "$arg" == --* ]]; then
      flags+=("$arg")
    else
      positional+=("$arg")
    fi
  done
  (cd "$BIN_DIR" && go run ./contract/cmd/validate-canonical-payload "${flags[@]}" "$repo_root" "${positional[@]}")
}

# ── Positive fixture: canonical payload must pass ────────────────────────
cat >"$TMP/canonical.json" <<'EOF'
{
  "contract_version": 2,
  "job_id": "job_canonical",
  "job_run_id": "run_abc",
  "correlation_id": "corr_xyz",
  "job_type": "process_video",
  "version": "v2",
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-01T00:00:00Z",
  "video_name": "My Video",
  "script_text": "Hello world",
  "voiceover_paths": ["vo.mp3"],
  "scenes": [{"text": "S1", "duration_seconds": 5.0}],
  "items": [{"role": "scene", "text": "S1"}],
  "audio_language_for_srt": "en",
  "video_mode": "scene_image",
  "output_path": "/tmp/out.mp4",
  "priority": 1,
  "timeout_secs": 3600,
  "status": "PENDING"
}
EOF

if ! out=$(run_validator "$TMP" "$TMP/canonical.json" 2>&1); then
  fail "validator should EXIT 0 on canonical fixture, got FAIL. Output:
$out"
fi
echo "$out" | grep -q "PASS  $TMP/canonical.json" \
  || fail "PASS line not reported for canonical fixture:
$out"

# ── Negative fixture: "no fixtures discovered" branch exits 1 ────────────
# A non-existent repo-root yields zero fixtures; the binary must exit 1
# and emit a "no fixtures discovered" message. Closes the gap where a
# silent green would hide a misconfigured verify.sh invocation.
EMPTY_ROOT="$TMP/empty_repo_root"
mkdir -p "$EMPTY_ROOT"
if out=$(run_validator "$EMPTY_ROOT" 2>&1); then
  fail "validator should EXIT 1 on empty/non-existent repo root, got PASS. Output:\n$out"
fi
echo "$out" | grep -q "no fixtures discovered" \
  || fail "expected 'no fixtures discovered' on empty repo root, got:\n$out"

# ── Positive fixture: scenes[].id (nested) is allowed ────────────────────
# The scenes[] array legitimately contains scene IDs (id: "scene-0",
# etc.). These are NESTED inside the top-level scenes array, NOT
# top-level keys, so ValidatePayload must not flag them as the
# "id" legacy alias.
cat >"$TMP/scenes_with_internal_id.json" <<'EOF'
{
  "job_id": "j",
  "video_name": "v",
  "script_text": "s",
  "voiceover_paths": ["vo.mp3"],
  "scenes": [
    {"id": "scene-0", "text": "S0", "duration_seconds": 4},
    {"id": "scene-1", "text": "S1", "duration_seconds": 4}
  ]
}
EOF

if ! out=$(run_validator "$TMP" "$TMP/scenes_with_internal_id.json" 2>&1); then
  fail "validator should EXIT 0 when only scenes[].id (nested) is present, got FAIL. Output:
$out"
fi

# ── Negative fixture: legacy alias must fail (one per denylist entry) ───
for alias in id run_id title voiceover_path audio_path; do
  cat >"$TMP/legacy_$alias.json" <<EOF
{
  "job_id": "j",
  "video_name": "v",
  "script_text": "s",
  "voiceover_paths": ["vo.mp3"],
  "$alias": "legacy_value"
}
EOF
  if out=$(run_validator "$TMP" "$TMP/legacy_$alias.json" 2>&1); then
    fail "validator should EXIT 1 on legacy alias \"$alias\", got PASS. Output:
$out"
  fi
  echo "$out" | grep -q "FAIL  $TMP/legacy_$alias.json" \
    || fail "FAIL line not reported for legacy alias \"$alias\":
$out"
  echo "$out" | grep -q "\"$alias\"" \
    || fail "expected error to mention \"$alias\", got:
$out"
done

# ── Negative fixture: shape anomaly must fail ───────────────────────────
cat >"$TMP/bad_shape.json" <<'EOF'
{
  "job_id": 123,
  "video_name": "v",
  "script_text": "s",
  "voiceover_paths": "not-an-array"
}
EOF
if out=$(run_validator "$TMP" "$TMP/bad_shape.json" 2>&1); then
  fail "validator should EXIT 1 on shape anomaly (job_id=int + voiceover_paths=string), got PASS. Output:
$out"
fi
echo "$out" | grep -q 'FAIL' \
  || fail "FAIL line not reported for shape anomaly:
$out"

# ── Negative fixture: --strict mode rejects drift keys ──────────────────
cat >"$TMP/drift_keys.json" <<'EOF'
{
  "job_id": "j",
  "video_name": "v",
  "script_text": "s",
  "voiceover_paths": ["vo.mp3"],
  "skip_creator": true,
  "fit": "contain"
}
EOF
# Default mode: tolerates drift keys (skip_creator, fit are operator
# helpers the master enqueue preflight tolerates). Validator should PASS.
if ! out=$(run_validator "$TMP" "$TMP/drift_keys.json" 2>&1); then
  fail "validator default mode should PASS drift keys, got FAIL. Output:
$out"
fi
# --strict mode: rejects drift keys. Validator should FAIL.
if out=$(run_validator "$TMP" --strict "$TMP/drift_keys.json" 2>&1); then
  fail "validator --strict mode should FAIL drift keys, got PASS. Output:
$out"
fi
echo "$out" | grep -q 'FAIL' \
  || fail "--strict mode did not report FAIL for drift keys:
$out"

# ── Positive fixture: --strict mode still accepts canonical ─────────────
if ! out=$(run_validator "$TMP" --strict "$TMP/canonical.json" 2>&1); then
  fail "validator --strict mode should PASS canonical, got FAIL. Output:
$out"
fi

# ── Positive fixture: default ops/jobs/*.json surface walk ──────────────
# Stage a canonical fixture under $TMP/ops/jobs/ so the default scan
# (which walks <repoRoot>/ops/jobs/*.json) discovers it.
mkdir -p "$TMP/ops/jobs"
cp "$TMP/canonical.json" "$TMP/ops/jobs/sample.json"
if ! out=$(run_validator "$TMP" 2>&1); then
  fail "default-surface walk against $TMP/ops/jobs/* failed. Output:
$out"
fi
echo "$out" | grep -qE 'scanning [0-9]+ fixture' \
  || fail "expected 'scanning N fixture(s)' line, got:
$out"
echo "$out" | grep -q "PASS  $TMP/ops/jobs/sample.json" \
  || fail "PASS line not reported for default-surface fixture:
$out"

# ── Positive fixture: noise keys (_-prefix, x- prefix) are filtered ─────
cat >"$TMP/noise_keys.json" <<'EOF'
{
  "job_id": "j",
  "video_name": "v",
  "voiceover_paths": ["vo.mp3"],
  "_etag": "abc",
  "x-traceid": "xyz"
}
EOF
if ! out=$(run_validator "$TMP" --strict "$TMP/noise_keys.json" 2>&1); then
  fail "validator --strict mode should PASS filtered-noise (_etag, x-traceid) keys, got FAIL. Output:
$out"
fi

pass "[test-validate-canonical-payload] OK"
