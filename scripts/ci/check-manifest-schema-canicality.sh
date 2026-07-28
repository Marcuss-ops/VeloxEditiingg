#!/usr/bin/env bash
# scripts/ci/check-manifest-schema-canicality.sh
#
# CI guard for docs/manifest-spec.md + the canonical
# velox.render-manifest.v1 fixture. Closes the spec-vs-fixture drift
# loop: a future change to either side that breaks the contract (e.g.
# adding a required field to the spec without updating the fixture, or
# rewriting the canonical-JSON form to sorted-keys vs unsorted) fails
# this script with a non-zero exit code.
#
# What it asserts (each assertion must pass; the script prints PASS
# lines + a final summary; exit non-zero on first FAIL):
#
#   1. Spec coverage              — every top-level field name from
#                                   docs/manifest-spec.md §1
#                                   (schema_version, manifest_id,
#                                   created_at, source, video, script,
#                                   scenes, delivery_plan, integrity)
#                                   appears verbatim in the spec doc.
#   2. Fixture parses             — the good fixture parses as JSON.
#   3. schema_version closed enum — equals `"velox.render-manifest.v1"`.
#   4. Required-field presence    — every top-level required key is
#                                   present on the fixture; every
#                                   scene carries the 5 required
#                                   per-scene fields
#                                   (scene_id, index, kind, text,
#                                   duration_ms).
#   5. integrity.manifest_sha256  — sha256 over the canonical JSON of
#                                   `body minus integrity block` matches
#                                   the stated value (LOWERCASE-HEX
#                                   64 chars). Mismatch ⇒ FAIL.
#   6. integrity.scene_count      — equals `len(scenes)`.
#   7. integrity.total_duration_ms — equals `sum(scenes[*].duration_ms)`.
#   8. Negative-path coverage     — the BAD fixture has an
#                                   intentionally-wrong sha256, so the
#                                   validator's failure path is
#                                   self-pinning (a future "fix" that
#                                   makes the validator always pass
#                                   breaks this assertion).
#
# Canonical-JSON form (the only form the master accepts):
#   json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
# mirrors the spec §10 reference Python implementation.
#
# Heading matching (`grep -qiE`) is case-insensitive so future
# title-case variations in docs/manifest-spec.md (e.g. `## Integrity`
# vs `## integrity`) don't break the validator.
#
# Exit codes:
#   0  — every assertion PASSED.
#   1  — at least one assertion FAILED (printed inline).
#   2  — environment / tool missing (e.g. python3 not on PATH).
#
# Run on every push (additive: green CI = spec & fixture are in sync
# with the runtime contract guarded by jobs/enqueue/manifest_ref's
# Shape + FutureRenderManifestResolver).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SPEC_DOC="${REPO_ROOT}/docs/manifest-spec.md"
GOOD_FIXTURE="${REPO_ROOT}/scripts/ci/fixtures/manifest.v1.fixture.json"
BAD_FIXTURE="${REPO_ROOT}/scripts/ci/fixtures/manifest.v1.bad-fixture.json"

# ── Preflight: tooling + artifacts exist ──────────────────────────────────────
command -v python3 >/dev/null 2>&1 || {
    printf 'check-manifest-schema-canicality: python3 not on PATH (required for sha256 + JSON canonicalization)\n' >&2
    exit 2
}
[ -f "${SPEC_DOC}" ]   || { printf 'missing spec: %s\n' "${SPEC_DOC}"   >&2; exit 1; }
[ -f "${GOOD_FIXTURE}" ] || { printf 'missing good fixture: %s\n' "${GOOD_FIXTURE}" >&2; exit 1; }
[ -f "${BAD_FIXTURE}" ]  || { printf 'missing bad fixture: %s\n'  "${BAD_FIXTURE}"  >&2; exit 1; }

# ── Assertion helpers ─────────────────────────────────────────────────────────
errs=0
section() { printf '\n=== %s ===\n' "$*"; }
ok()      { printf '  PASS: %s\n' "$*"; }
no()      { printf '  FAIL: %s\n' "$*" >&2; errs=$((errs + 1)); }

# ── 1. Spec coverage ─────────────────────────────────────────────────────────
section "1. spec coverage"
for f in 'schema_version' 'manifest_id' 'created_at' 'source' 'video' 'script' 'scenes' 'delivery_plan' 'integrity'; do
    # Case-insensitive + word-boundary grep: matches the spec's
    # title-case headings (`Integrity` / `Source`) AND snake-case
    # body mentions (`integrity.manifest_sha256`), without
    # false-matching `source_provider` for `source`.
    if grep -qiE "(^|[^a-zA-Z0-9_])${f}([^a-zA-Z0-9_]|$)" "${SPEC_DOC}"; then
        ok "${f} present in spec doc"
    else
        no "${f} MISSING in spec doc"
    fi
done
# Per-region headings — case-insensitive (heading text uses title
# case: `## 5. Source object` etc.) so a future case variation in
# the spec doesn't break this assertion.
for region in 'Source object' 'Video object' 'Script object' 'Scene object' 'Integrity object'; do
    if grep -qiE "^## .*${region}" "${SPEC_DOC}"; then
        ok "spec §${region}"
    else
        no "spec §${region} heading MISSING"
    fi
done

# ── 2-7. Good fixture integrity + negative-path coverage ────────────────────
section "2-7. fixture integrity + bad-fixture coverage"

python3 - "${GOOD_FIXTURE}" "${BAD_FIXTURE}" <<'PY' || errs=$((errs + 1))
import hashlib
import json
import re
import sys


def canonical(body: dict) -> bytes:
    # The spec §10 "Canonical form" — the only form the master
    # accepts. Validator MUST compute the sha256 over the body
    # excluding the `integrity` block entirely (so the value of
    # integrity.manifest_sha256 doesn't recursively include itself).
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def fail(msg: str) -> None:
    raise SystemExit("FAIL: " + msg)


good_path, bad_path = sys.argv[1], sys.argv[2]


# --- Good fixture ---
with open(good_path) as fh:
    body = json.load(fh)
print("  PASS: good fixture parses")


# schema_version closed enum
sv = body.get("schema_version")
if sv != "velox.render-manifest.v1":
    fail(f"schema_version={sv!r}, want 'velox.render-manifest.v1'")
print("  PASS: schema_version closed enum")


# Required top-level keys
for k in ("schema_version", "manifest_id", "created_at", "source", "video", "script",
          "scenes", "delivery_plan", "integrity"):
    if k not in body:
        fail(f"missing required top-level field: {k}")
print("  PASS: every required top-level field present")


# Per-scene required keys
scenes = body.get("scenes", [])
if not isinstance(scenes, list) or not scenes:
    fail("scenes must be a non-empty array")
for i, s in enumerate(scenes):
    if not isinstance(s, dict):
        fail(f"scenes[{i}] must be a JSON object")
    for k in ("scene_id", "index", "kind", "text", "duration_ms"):
        if k not in s:
            fail(f"scenes[{i}] missing required field: {k}")
print(f"  PASS: every per-scene required field present (n_scenes={len(scenes)})")


# integrity.manifest_sha256 self-consistency
integ = body.get("integrity", {})
if not isinstance(integ, dict):
    fail("integrity must be a JSON object")
stated = integ.get("manifest_sha256")
if not isinstance(stated, str) or not re.match(r"^[0-9a-f]{64}$", stated):
    fail(f"integrity.manifest_sha256 must match ^[0-9a-f]{{64}}$; got {stated!r}")
body_minus_integrity = {k: v for k, v in body.items() if k != "integrity"}
computed = hashlib.sha256(canonical(body_minus_integrity)).hexdigest()
if computed != stated:
    fail(
        f"integrity.manifest_sha256 mismatch — stated={stated} computed={computed}. "
        f"This usually means the fixture was edited without recomputing the sha256, "
        f"or the canonical-JSON form (sort_keys + separators) drifted from the spec."
    )
print("  PASS: integrity.manifest_sha256 self-consistent (canonical-form sha256)")


# integrity.scene_count == len(scenes)
stated_count = integ.get("scene_count")
actual_count = len(scenes)
if stated_count != actual_count:
    fail(f"integrity.scene_count={stated_count} != len(scenes)={actual_count}")
print(f"  PASS: integrity.scene_count = {actual_count}")


# integrity.total_duration_ms == sum(scenes[*].duration_ms)
stated_total = integ.get("total_duration_ms")
actual_total = sum(int(s.get("duration_ms", 0)) for s in scenes)
if stated_total != actual_total:
    fail(f"integrity.total_duration_ms={stated_total} != sum(scenes[*].duration_ms)={actual_total}")
print(f"  PASS: integrity.total_duration_ms = {actual_total}")


# --- Bad fixture (must FAIL the sha256 check; otherwise the
# validator's failure path is itself untested and a future change
# could silently make the validator always-pass) ---
with open(bad_path) as fh:
    bad_body = json.load(fh)
print("  PASS: bad-fixture parses")

bad_minus = {k: v for k, v in bad_body.items() if k != "integrity"}
bad_computed = hashlib.sha256(canonical(bad_minus)).hexdigest()
bad_stated = bad_body.get("integrity", {}).get("manifest_sha256")
if bad_stated == bad_computed:
    fail(
        f"bad-fixture has integrity.manifest_sha256={bad_stated!r} that EQUALS the canonical-body sha256. "
        f"The negative-path fixture must contain an intentionally-wrong sha256 so the "
        f"validator's failure path is itself pinned. If you intentionally changed this, "
        f"flip the bad-fixture's integrity.manifest_sha256 to a 64-char zero string and re-run."
    )
print(f"  PASS: bad-fixture integrity.manifest_sha256 != canonical sha256 (mismatch pinned)")
PY

# ── Summary ──────────────────────────────────────────────────────────────────
section "summary"
if [ "${errs}" -gt 0 ]; then
    printf 'check-manifest-schema-canicality: FAIL (%s assertion(s) failed)\n' "${errs}" >&2
    exit 1
fi
printf 'check-manifest-schema-canicality: PASS (spec + good-fixture + bad-fixture all green)\n'
exit 0
