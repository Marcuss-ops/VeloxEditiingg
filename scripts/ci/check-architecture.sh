#!/usr/bin/env bash
# scripts/ci/check-architecture.sh
#
# Structural invariants of the repository: this guards the SHAPE of the
# codebase, not its behaviour. Adding new rules here is encouraged; adding
# behaviour tests belongs elsewhere (Go test, ansible-lint, etc).
#
# Exit codes: 0 ok -- 1 violation detected.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

fail() {
  printf 'ARCHITECTURE ERROR: %s\n' "$*" >&2
  exit 1
}

# 1. The pre-restructure double-root must stay dead.
# If a contributor resurrects refactored/ (e.g. via
# `git mv DataServer refactored/DataServer`), every package import path
# drifts and builds break silently. Catch the resurrection at PR-time.
# We use `[ -e ]` directly so the failure path is unambiguous if someone
# hides refactored/ behind a symlink later.
if [[ -e refactored || -L refactored ]]; then
  ls -la refactored >&2 || true
  fail "refactored/ exists -- forbidden (single-root rule)"
fi

# 2. Exactly one VERSION.txt at project root.
version_count="$(
  find . \
    -path './.git' -prune -o \
    -name VERSION.txt -print | wc -l | tr -d ' '
)"
[[ "$version_count" == "1" ]] \
  || fail "expected exactly one VERSION.txt at root, found $version_count"

# 3. Exactly one shared/go.mod.
shared_count="$(
  find . \
    -path './.git' -prune -o \
    -path '*/shared/go.mod' -print | wc -l | tr -d ' '
)"
[[ "$shared_count" == "1" ]] \
  || fail "expected exactly one shared/go.mod, found $shared_count"

# 4. All GitHub workflow YAML lives under ./.github/workflows/.
# We anchor on `./.github/workflows/*` (NOT `*/workflows/*`) so that no
# stray `foo/workflows/stage.yml` from another tool slipped in.
#
# npm transitive deps (e.g. `VeloxFrontend/web/node_modules/reusify/`
# shipping its own `.github/workflows/ci.yml`) are NOT real project
# workflows and are excluded here:
#   - `find -prune` skips traversing any `*/node_modules/` subtree entirely
#     so deps never enter the candidate set (consistent with how `.git` is
#     pruned above and in rule #2);
#   - the defensive case-branch below documents the allow-list inline and
#     ensures the rule still skips node_modules paths even if the prune
#     is later reordered/removed in a refactor.
found_off_root=0
while IFS= read -r workflow; do
  case "$workflow" in
    ./.github/workflows/*) ;;
    */node_modules/*/.github/workflows/*) ;;
    *) printf '  off-root workflow: %s\n' "$workflow" >&2; found_off_root=1 ;;
  esac
done < <(find . \
    -path './.git' -prune -o \
    -path '*/node_modules' -prune -o \
    -type f \( -name '*.yml' -o -name '*.yaml' \) \
    -path '*/workflows/*' -print)
[[ "$found_off_root" -eq 0 ]] \
  || fail "workflow YAML files outside ./.github/workflows/ -- see above"

# 5. No *_legacy / *_old / *.deprecated files anywhere.
if find . \
     -path './.git' -prune -o \
     -path '*/build' -prune -o \
     -path '*/build-*' -prune -o \
     -path '*/Testing' -prune -o \
     -type f \( \
       -iname '*.deprecated' -o \
       -iname '*_legacy.*'  -o \
       -iname '*_old.*'  \
     \) -print -quit | grep -q .; then
  find . \
    -path './.git' -prune -o \
    -path '*/build' -prune -o \
    -path '*/build-*' -prune -o \
    -path '*/Testing' -prune -o \
    -type f \( \
      -iname '*.deprecated' -o \
      -iname '*_legacy.*'  -o \
      -iname '*_old.*'  \
    \) -print >&2
  fail "legacy/deprecated source files are forbidden -- see above"
fi

# 6. Removed queue package must stay dead.
# After PR "refactor(jobs): remove queue compatibility facade",
# internal/queue was deleted. Reintroducing it would resurrect the
# facade types (queue.Job, queue.QueueItem, queue.JobStatus,
# *queue.FileQueue) that were swept in that PR.
if [[ -d DataServer/internal/queue ]]; then
  fail "DataServer/internal/queue/ exists -- forbidden (queue facade was removed; use internal/jobs instead)"
fi

# 7. PR-3.9 guard: forbid reintroduction of hardcoded worker
# dispatch maps. Every job type must resolve through the executor
# registry inside internal/executor + internal/taskrunner. The worker
# package is permitted exactly ONE switch arm in runJobTask: a
# health_check carve-out kept for master-side health semantics. Any
# other per-job-type switch arm, or any of the legacy duplicate-
# routing helpers (executeWorkflowJob, runRenderJob, runVideoJob,
# runAudioJob, newVideoWorkflow) effectively re-creates a parallel
# dispatch table — exactly the regression PR-3.9 removed.
#
# Scope: only non-test files inside the worker package. Tests are
# allowed to mock the old surface for regression coverage; production
# code MUST NOT contain these patterns any more.
#
# Comment-aware filter: package doc comments and doc-comment blocks
# legitimately reference the deleted legacy helpers (e.g. "the
# helpers ... are GONE in PR-3.9" or "matching the legacy
# executeWorkflowJob contract"). Without a comment filter the grep
# would flag those doc-only references as false positives. The
# `grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)'` filter drops any
# line whose content (after the path:lineno: prefix and whitespace)
# starts with a Go comment token: line comment `//`, block comment
# opener `/*`, or block-comment continuation `*`. Pure code lines
# (e.g. `case "render": ...`) are preserved so genuine regressions
# still trip the guard.
worker_dispatch_violations="$(
  grep -RInE \
    -e 'case[[:space:]]+"render"[[:space:]]*:' \
    -e 'case[[:space:]]+"process_video"[[:space:]]*:' \
    -e 'case[[:space:]]+"process_audio"[[:space:]]*:' \
    -e 'executeWorkflowJob' \
    -e 'runRenderJob' \
    -e 'runVideoJob' \
    -e 'runAudioJob' \
    -e 'newVideoWorkflow' \
    RemoteCodex/native/worker-agent-go/internal/worker \
    --include='*.go' --exclude='*_test.go' \
    2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$worker_dispatch_violations" ]]; then
  printf 'PR-3.9: hardcoded worker dispatch detected (regression — every job type must resolve through executor.Registry):\n'
  printf '%s\n' "$worker_dispatch_violations" >&2
  exit 1
fi

# 8a. PR-04.4 guard: forbid hand-rolled boolean-AND selector filters
# inside velox-server/internal/workers. The cost model
# (internal/costmodel) is the canonical admission gate; any line
# that ANDs `Schedulable` with `Drain` (in either order, within a
# short window to suppress cross-function false positives) trips
# this rule. Single-source-of-truth rule for selector placement.
# _test.go is exempt because tests legitimately exercise legacy
# state to verify the historical boolean AND is no longer in
# production code.
worker_selector_violations="$(
  grep -RInE 'Schedulable.{1,80}Drain|Drain.{1,80}Schedulable' \
    DataServer/internal/workers \
    --include='*.go' --exclude='*_test.go' \
    2>/dev/null || true
)"
if [[ -n "$worker_selector_violations" ]]; then
  printf 'PR-04.4: hand-rolled boolean-AND selector filter in workers package — admission must route through costmodel.Score:\n' >&2
  printf '%s\n' "$worker_selector_violations" >&2
  exit 1
fi

# 9. BUILD_INFO.json ↔ VERSION.txt SSOT drift guard.
#
# Single-source-of-truth rule (scope: VERSION-LEVEL integrity only).
# This rule catches the headline SSOT invariant: the `version` field in
# RemoteCodex/BUILD_INFO.json must mirror VERSION.txt (prefixed with `v`).
#
# NOT covered by this guard (out of scope, intentionally):
#   * engine_version drift    — `engine_version` is informational for the
#     remote worker; bump it independently when the C++ engine protocol
#     changes. Enforced separately by the worker-image cosign step which
#     tags with the resolved semver.
#   * source_hash drift       — versioned by sha256sum on VERSION.txt;
#     emerges naturally from VERSION.txt edits + ./scripts/generate-build-info.sh.
#   * git_commit drift        — informational only; HEAD at build time.
#   * built_at drift          — derived from SOURCE_DATE_EPOCH or wall clock.
#
# Deepening this guard into a full canonical-shape comparison is
# deliberately deferred: the BUILD_INFO.json file is owned by the worker
# release pipeline (worker-image.yml) and the master image never reads it
# directly, so the only drift class with end-to-end impact is the version
# field. Promote to full-shape check once a producer-side bug surfaces.
if [[ -f RemoteCodex/BUILD_INFO.json ]]; then
  build_info_version="$(python3 -c "import json,sys; print(json.load(open('RemoteCodex/BUILD_INFO.json')).get('version',''))" 2>/dev/null || echo "")"
  version_txt="$(tr -d '[:space:]' < VERSION.txt)"
  expected="${version_txt}"
  if [[ "$build_info_version" != "$expected" ]]; then
    cat >&2 <<VIOLATION
BUILD_INFO.json version drift:
  RemoteCodex/BUILD_INFO.json   version=${build_info_version}
  VERSION.txt                   VERSION=${version_txt} (expected version=${expected})
Run ./scripts/generate-build-info.sh to regenerate BUILD_INFO.json from VERSION.txt.
VIOLATION
    exit 1
  fi
fi

# 9b. Worker protocol SSOT guard.
# The wire protocol is owned by shared/controltransport. Deployment templates
# must not carry a stale protocol identifier that the master will reject.
canonical_worker_protocol="$({
  sed -n 's/.*ProtocolVersionCurrent[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
    shared/controltransport/transport.go
} | head -n 1)"
[[ -n "$canonical_worker_protocol" ]] \
  || fail "cannot resolve ProtocolVersionCurrent from shared/controltransport/transport.go"

python3 - "$canonical_worker_protocol" <<'PY'
import json
import pathlib
import sys

expected = sys.argv[1]
config_path = pathlib.Path("deploy/runtime/worker_config.example.json")
with config_path.open() as config_file:
    actual = json.load(config_file).get("protocol_version")
if actual != expected:
    raise SystemExit(
        f"worker config protocol drift: {config_path} has {actual!r}, expected {expected!r}"
    )
PY

grep -Fq "PROTOCOL_VERSION=\"${canonical_worker_protocol}\"" \
  deploy/scripts/apply-local-worker-config.sh \
  || fail "local worker config default does not match ProtocolVersionCurrent (${canonical_worker_protocol})"

if grep -qE '^[[:space:]]*var[[:space:]]+SupportedProtocolVersions' shared/controltransport/transport.go; then
  fail "SupportedProtocolVersions must not be an exported mutable global; use SupportedProtocols()"
fi
grep -qE '^[[:space:]]*func[[:space:]]+SupportedProtocols\(\)[[:space:]]+\[\]string' shared/controltransport/transport.go \
  || fail "SupportedProtocols() read-only API is missing"

if grep -qE '^[[:space:]]*var[[:space:]]+AllCapabilities' shared/controltransport/capabilities.go; then
  fail "AllCapabilities must not be an exported mutable global; use KnownCapabilities()"
fi
grep -qE '^[[:space:]]*func[[:space:]]+KnownCapabilities\(\)[[:space:]]+\[\]string' shared/controltransport/capabilities.go \
  || fail "KnownCapabilities() read-only API is missing"

# 9c. Retired livestream surface must stay absent. Migration 155 is the only
# permitted historical reference; no application package may recreate the
# handler or store API.
[[ ! -e DataServer/internal/handlers/remote/livestream ]] \
  || fail "retired livestream handler package was reintroduced"
[[ ! -e DataServer/internal/store/sqlite_livestream.go ]] \
  || fail "retired livestream store was reintroduced"

# 10. Canonical worker playbook — structural syntax check.
# The normalize_worker_systemd.yml playbook guards canonical runtime
# purity via its STEP 7 strict idempotency assert task: only
# velox-worker.service plus the two named siblings (watchdog, auto-update)
# are tolerated, AND the canonical
# unit must be present in the post-state enumeration. We verify here
# at PR-time that the YAML parses cleanly (and its include_tasks
# resolves) so a regression in the assert lands here rather than at
# the next worker deploy. Pure syntax-check: no remote connections
# are opened, no remote state is read or written.
#
# Fail-loud convention (matches rules 1–9): if ansible-playbook
# cannot be located the script exits 1 instead of silently skipping
# — letting a regression pass in CI would defeat the entire gate.
ansible_bin=""
if command -v ansible-playbook >/dev/null 2>&1; then
  ansible_bin="$(command -v ansible-playbook)"
elif [[ -x "${VELOX_VENV:-${HOME}/Projects/company/.venv}/bin/ansible-playbook" ]]; then
  ansible_bin="${VELOX_VENV:-${HOME}/Projects/company/.venv}/bin/ansible-playbook"
fi
[[ -n "$ansible_bin" ]] \
  || fail "ansible-playbook not on PATH and venv fallback missing; install ansible-core in PATH or in /home/pierone/venv (cannot verify canonical-worker playbook)"

ANSIBLE_LOG="$(mktemp /tmp/check-arch-ansible.XXXXXX.log)"
ANSIBLE_COLLECTIONS_PATH="${ANSIBLE_COLLECTIONS_PATH:-/home/pierone/.ansible/collections}" \
  "$ansible_bin" --syntax-check -i 'localhost,' -c local \
    DataServer/data/ansible/playbooks/normalize_worker_systemd.yml \
    >"$ANSIBLE_LOG" 2>&1 \
  || {
      cat "$ANSIBLE_LOG" >&2
      rm -f "$ANSIBLE_LOG"
      fail "normalize_worker_systemd.yml failed syntax check"
    }
rm -f "$ANSIBLE_LOG"

# 11. SQL-ownership ratchet. Replaces the previous directory-allowlist
# gates (check-no-sql-outside-store.sh and check-sql-ownership.sh)
# with a per-file baseline. The total can only decrease and no new
# files outside internal/store/** may introduce SQL coupling.
bash "${BASH_SOURCE[0]%/*}/ratchet-sql.sh" >/dev/null \
  || fail "SQL ratchet violation detected -- see ratchet-sql.sh output"

# 12. Cross-module dependency direction (no import cycles across the
# go.work workspace). velox-shared is the leaf: velox-server and
# velox-worker-agent consume it but must never import each other, and
# shared must never import either consumer. Go's compiler forbids cycles
# WITHIN a module but not BETWEEN modules in a workspace, so this is the
# one cycle class that can regress without a build failure.
check_module_imports() {
  local importer_dir="$1" importer="$2" imported="$3"
  local hits
  hits="$(
    (cd "$REPO_ROOT/$importer_dir" \
      && go list -f '{{range .Imports}}{{println .}}{{end}}' ./... 2>/dev/null) \
      | grep -E "^${imported}/" \
      | sort -u \
      || true
  )"
  if [[ -n "$hits" ]]; then
    printf '  %s imports %s (forbidden cross-module dependency):\n%s\n' \
      "$importer" "$imported" "$hits" >&2
    fail "cross-module dependency cycle risk: $importer -> $imported"
  fi
}
check_module_imports shared velox-shared velox-server
check_module_imports shared velox-shared velox-worker-agent
check_module_imports DataServer velox-server velox-worker-agent
check_module_imports RemoteCodex/native/worker-agent-go velox-worker-agent velox-server

# 13. Intra-module layering (velox-server): the persistence/foundation
# layer must never import the API/application/transport layer, nor do
# network I/O directly (net/http).
#
# Go forbids LITERAL cycles within a module (build would fail), but it
# does NOT forbid an "upward" import that re-couples a data-layer
# package to the HTTP/gRPC handlers or the composition root. Such an
# import is the seed of a future cycle and defeats isolated testing
# (`go test ./internal/store/...` should not drag in the API surface).
# The net/http prohibition is the I/O side of the same boundary: the
# persistence layer reaches the network only through adapters, never
# by importing the HTTP client directly.
#
# Rule #12 guards cross-module direction; this guards the same invariant
# INSIDE velox-server, where only a cyclic edge would trip the compiler.
check_upward_import() {
  local importer="$1" forbidden_re="$2"
  local hits
  hits="$(
    (cd "$REPO_ROOT/DataServer" \
      && go list -f '{{range .Imports}}{{println .}}{{end}}' "$importer" 2>/dev/null) \
      | grep -E "$forbidden_re" \
      | sort -u \
      || true
  )"
  if [[ -n "$hits" ]]; then
    printf '  %s imports API/application layer or net/http (isolation break):\n%s\n' \
      "$importer" "$hits" >&2
    return 1
  fi
  return 0
}
FORBIDDEN_UP='^velox-server/internal/(handlers|app|fleet|grpcserver)(/|$)|^net/http$'
foundation_violation=0
foundation_pkgs="$(
  cd "$REPO_ROOT/DataServer" \
    && go list \
      velox-server/internal/store \
      velox-server/internal/config \
      velox-server/internal/repository \
      velox-server/internal/storecore \
      velox-server/internal/platform/... 2>/dev/null
)"
while IFS= read -r pkg; do
  [[ -z "$pkg" ]] && continue
  check_upward_import "$pkg" "$FORBIDDEN_UP" || foundation_violation=1
done <<<"$foundation_pkgs"
[[ "$foundation_violation" -eq 0 ]] \
  || fail "foundation-layer package imports API/application layer or net/http — breaks isolated testing"

# 14. Single canonical video-profile authority (P0 media-profile unification).
#
# shared/contract.CanonicalVideoProfileV1 owns the encoded-stream IDENTITY:
# dimensions, frame rate, pixel format, codec profile/level, GOP, B-frames,
# closed GOP, time base. shared/contract.PreparationQualityPolicy owns ONLY
# the quality knobs (bitrate / VBV / audio), because compatibility is an
# identity while bitrate is a tuning choice.
#
# The drift this rule retires: DataServer/internal/assets carried a
# trimmer-local `VideoNormalization` declaring 1920x1080 @ 30 fps while the
# canonical profile promised 1920x1080 @ 24 fps — two owners of the same
# identity. That makes content-addressed reuse (W5) unsound, because the same
# input could produce two different "canonical" streams, and it silently broke
# the packet-copy fast path that assumes a single stream identity.
#
# Scope: production Go only (_test.go is exempt because the anti-drift test in
# internal/assets deliberately asserts the exact literal ffmpeg arguments, and
# a literal there is the pin, not the drift). The literal-argument patterns are
# scoped to DataServer/internal + shared (the master preparation surface);
# RemoteCodex/.../cmd/velox-*-fixture-gen are synthetic TEST-fixture producers
# with deliberately non-canonical shapes (e.g. timescale 15360) and are NOT
# part of the canonical preparation path.
vp_violations=0

# 14a. The removed duplicate authority symbol must not reappear in production
# Go. Documenting comments that name it historically are tolerated.
vp_symbol_hits="$(
  grep -RInE 'VideoNormalization' \
    --include='*.go' --exclude='*_test.go' \
    . 2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$vp_symbol_hits" ]]; then
  printf 'VideoNormalization must not reappear: compatibility belongs to shared/contract.CanonicalVideoProfileV1:\n%s\n\n' "$vp_symbol_hits" >&2
  vp_violations=$((vp_violations + 1))
fi

# 14b. No literal compatibility values in video filter strings. A literal
# `fps=30/1`, `scale=1920:1080` or `pad=1920:1080` is a second authority; the
# filter must interpolate the profile (canonicalScaleFilter).
vp_filter_hits="$(
  grep -RInE 'fps=[0-9]+(/[0-9]+)?|scale=[0-9]+:[0-9]+|pad=[0-9]+:[0-9]+' \
    DataServer/internal shared \
    --include='*.go' --exclude='*_test.go' \
    2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$vp_filter_hits" ]]; then
  printf 'literal ffmpeg filter compatibility values detected — derive fps/scale/pad from CanonicalVideoProfileV1:\n%s\n\n' "$vp_filter_hits" >&2
  vp_violations=$((vp_violations + 1))
fi

# 14c. The pinned H.264 arguments (profile / level / GOP / B-frames / x264
# params) must be built from the profile fields, never spelled out as literals.
vp_arg_hits="$(
  grep -RInE '\"-g\",[[:space:]]*\"[0-9]|\"-bf\",[[:space:]]*\"[0-9]|\"-profile:v\",[[:space:]]*\"|\"-level:v\",[[:space:]]*\"[0-9]|\"-x264-params\",[[:space:]]*\"' \
    DataServer/internal shared \
    --include='*.go' --exclude='*_test.go' \
    2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$vp_arg_hits" ]]; then
  printf 'literal H.264 pinning arguments detected — read profile/level/GOP/B-frames from CanonicalVideoProfileV1:\n%s\n\n' "$vp_arg_hits" >&2
  vp_violations=$((vp_violations + 1))
fi

# 14d. The removed manifest label must not come back: the asset manifest
# records canonical_profile_id / canonical_stream_profile_id instead.
vp_manifest_hits="$(
  grep -RIn 'normalization_version' \
    DataServer internal shared \
    --include='*.go' --exclude='*_test.go' \
    2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$vp_manifest_hits" ]]; then
  printf 'normalization_version manifest label detected — record the canonical profile identity instead:\n%s\n\n' "$vp_manifest_hits" >&2
  vp_violations=$((vp_violations + 1))
fi

# 14e. The single-owner pair must stay declared: the canonical profile and the
# quality policy are the ONLY two video-preparation authorities, and the
# quality policy must not grow a compatibility field (pinned at runtime by
# TestPreparationQualityPolicyCarriesNoCompatibilityFields).
[[ -f shared/contract/canonical_video_profile.go ]] \
  || fail "shared/contract/canonical_video_profile.go is missing (canonical video-profile authority)"
[[ -f shared/contract/preparation_quality_policy.go ]] \
  || fail "shared/contract/preparation_quality_policy.go is missing (documented quality/compatibility split)"
grep -qE '^type[[:space:]]+PreparationQualityPolicy[[:space:]]+struct' shared/contract/preparation_quality_policy.go \
  || fail "shared/contract.PreparationQualityPolicy is missing"
grep -qE '^type[[:space:]]+CanonicalVideoProfileV1[[:space:]]+struct' shared/contract/canonical_video_profile.go \
  || fail "shared/contract.CanonicalVideoProfileV1 is missing"

[[ "$vp_violations" -eq 0 ]] \
  || fail "single video-profile authority violated — see above"

# 15. No test-harness HTTP types in the production runtime.
#
# The batch intake endpoint (POST /api/v1/jobs/batch) previously reused the
# canonical single-job handler by building an httptest.NewRecorder() +
# gin.CreateTestContext() per item — i.e. the production runtime simulated
# HTTP in order to reach its own application logic. It now dispatches through
# an internal adapter (batchResponseCapture over a copied gin.Context).
#
# This rule keeps the test harness out of production: importing
# net/http/httptest, or calling gin.CreateTestContext, in a non-test Go file
# means application logic is once again being reached by faking the HTTP
# layer instead of through a canonical seam. Documenting comments are
# tolerated (mirrors rule 7's comment filter).
httptest_hits="$(
  grep -RInE '^[[:space:]]*([a-zA-Z_][a-zA-Z0-9_]*[[:space:]]+)?"net/http/httptest"|gin[.]CreateTestContext[(]' \
    DataServer --include='*.go' --exclude='*_test.go' \
    2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$httptest_hits" ]]; then
  printf 'test-harness HTTP types in production code — reach application logic through its canonical seam, not by faking HTTP:\n%s\n\n' "$httptest_hits" >&2
  fail "production runtime must not import net/http/httptest or call gin.CreateTestContext"
fi

# 16. The batch intake envelope must call the intake CORE, not replay the
# single-job HTTP handler.
#
# POST /api/v1/jobs/batch reaches the canonical intake through
# submitJobCore (internal/handlers/server/pipeline/job_submit_core.go), the same
# transport-neutral function the single-job adapter (POST /api/v1/jobs) uses.
# The retired shape — "clone the gin.Context, point a synthetic ResponseWriter
# at it, invoke SubmitJob(), then parse the JSON response back" — made the batch
# surface depend on the HTTP serialization round-trip of its own handler and put
# the real application seam (creatorflow.CanonicalJobSubmitter) behind a fake
# HTTP exchange. Re-introducing it is a regression even without httptest.
#
# The pattern is the invocation `SubmitJob()(` (the route registration and the
# SubmitJobBatch declaration do not match it), scoped to the batch file so the
# single-job adapter is unaffected.
batch_replay_hits="$(
  grep -RHInE 'SubmitJob\(\)\(' \
    DataServer/internal/handlers/server/pipeline/batch_submit.go \
    2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$batch_replay_hits" ]]; then
  printf 'batch intake replays the single-job HTTP handler — call submitJobCore instead:\n%s\n\n' "$batch_replay_hits" >&2
  fail "POST /api/v1/jobs/batch must dispatch through the intake core, not by invoking the single-job handler"
fi

# 16b. The intake core must stay transport-neutral: it must not write to a
# gin.Context (which would re-couple it to one surface's HTTP response) and it
# must not be bypassed by a second submitter invocation from the HTTP layer.
if grep -qE 'c[.]JSON|WriteHeader|c[.]Set[(]' DataServer/internal/handlers/server/pipeline/job_submit_core.go 2>/dev/null; then
  printf 'intake core writes to a gin.Context — it must return a transport-neutral outcome:\n' >&2
  grep -nE 'c[.]JSON|WriteHeader|c[.]Set[(]' DataServer/internal/handlers/server/pipeline/job_submit_core.go >&2 || true
  fail "job_submit_core.go must not touch a gin.Context"
fi
# Comment-aware: the core's doc comment legitimately explains WHY it holds no
# gin.Context dependency (that explanation is the point); only real code is a
# violation.
core_gin_hits="$(
  grep -RHInE 'gin[.]Context' DataServer/internal/handlers/server/pipeline/job_submit_core.go 2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|/\*|\*)' \
    || true
)"
if [[ -n "$core_gin_hits" ]]; then
  printf 'intake core references gin.Context — it must stay transport-neutral:\n%s\n\n' "$core_gin_hits" >&2
  fail "job_submit_core.go must not depend on gin.Context (transport-neutral core)"
fi


echo "check-architecture: OK"
