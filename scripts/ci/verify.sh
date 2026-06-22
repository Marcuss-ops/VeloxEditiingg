#!/usr/bin/env bash
# scripts/ci/verify.sh
#
# Single canonical entry point for repository-wide verification. BOTH local
# developers and GitHub Actions must invoke this script via `make verify`.
# Replicating these steps inline anywhere else is a regression by definition.
#
# Hardening opts (via env):
#   SKIP_HEAVY=1  skip cmake + docker image builds (fast local feedback)
#   SKIP_LIGHT=1  skip gofmt/vet/test suite (used for image-only CI jobs)
#
# Exit non-zero (any failure) bubbles up through `set -euo pipefail`.
set -euo pipefail

# Resolve repo root from the script location (works under `make verify`
# and direct invocation alike).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

SKIP_LIGHT=${SKIP_LIGHT:-0}
SKIP_HEAVY=${SKIP_HEAVY:-0}

log()  { printf '\u2192 %s\n' "$*"; }
fail() { printf '\u2717 %s\n' "$*" >&2; exit 1; }

# ── 0. Working-tree safety ──────────────────────────────────────────────────
# BASE_REF-scoped checks only see files committed between BASE_REF and
# HEAD. Uncommitted/untracked working-tree edits would silently slip
# past every check, giving developers a false-green on dirty runs.
# Catch this loudly. ALLOW_DIRTY=1 opts in for power-users who
# understand the implication.
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
  if [[ "${ALLOW_DIRTY:-0}" != "1" ]]; then
    printf '\u26a0 WORKING TREE DIRTY: uncommitted/untracked changes\n' >&2
    printf '  Mechanism: BASE_REF-scoped checks ONLY see committed files\n' >&2
    printf '  between BASE_REF...HEAD. Your dirty edits remain invisible\n' >&2
    printf '  to the architecture / migration / single-writer / db-access\n' >&2
    printf '  / registry / no-legacy / secrets checks. Only the file-\n' >&2
    printf '  system sniffer (refactored/, *_legacy.go, etc.) would see\n' >&2
    printf '  them. Commit first, then re-run `make verify`.\n' >&2
    printf '  Optional: ALLOW_DIRTY=1 to continue -- you accept the\n' >&2
    printf '  silent-pass on the dirty subset.\n' >&2
    exit 1
  fi
  log 'WARN: working tree dirty + ALLOW_DIRTY=1 -- continuing (dirty edits invisible to BASE_REF-scoped checks)'
fi

# ── 1. Architectural invariants ─────────────────────────────────────────────
log "check-architecture"
./scripts/ci/check-architecture.sh
log "check-no-legacy"
./scripts/ci/check-no-legacy.sh
log "check-secrets"
./scripts/ci/check-secrets.sh
log "check-migrations"
./scripts/ci/check-migrations.sh
log "check-single-writer"
./scripts/ci/check-single-writer.sh
log "check-task-runtime-invariants"
./scripts/ci/check-task-runtime-invariants.sh
log "check-db-access"
./scripts/ci/check-db-access.sh
log "check-registry"
./scripts/ci/check-registry.sh

# ── 2. Go modules: gofmt + vet + test (-race) ──────────────────────────────
#
# `gofmt -w .` is destructive on purpose: when devs pull main, all files end
# up formatted immediately. The subsequent `git diff --exit-code` makes the
# build fail loudly if anything was reformatted but not committed \u2014 the
# canonical signal that someone bypassed `make verify` before pushing.
if [[ "$SKIP_LIGHT" -ne 1 ]]; then
  # shared is now part of the loop, so its gofmt failures are surfaced
  # exactly like DataServer's or worker-agent-go's. The redundant
  # post-loop `go-build+vet: shared` block was removed in the round-2
  # cleanup; `go test` against ./... compiles every package even when
  # no internal *_test.go exists, so the same loop covers shared.
  for mod in DataServer RemoteCodex/native/worker-agent-go shared; do
    log "go-fmt: ${mod}"
    (
      set -euo pipefail
      cd "$REPO_ROOT/$mod"
      gofmt -w .
      # `git diff --exit-code` errors out if any tracked file differs from HEAD
      # post-format. CI: this should never trigger (committed = formatted).
      # Local dev: format was auto-applied; run `git commit -am gofmt` and retry.
      cd "$REPO_ROOT"
      git diff --exit-code -- "$mod"
      log "go-vet: ${mod}"
      go vet ./...
      log "go-test -race: ${mod}"
      go test -race -count=1 -timeout 180s ./...
    )
  done
fi

# ── 3. Pre-existing mutation guard (DataServer-specific legacy removal) ────
if [[ -x "$REPO_ROOT/DataServer/ci/guard_legacy_mutation.sh" ]]; then
  log "guard-legacy-mutation (DataServer)"
  bash "$REPO_ROOT/DataServer/ci/guard_legacy_mutation.sh"
fi

# ── 4. Heavy steps: native engine + docker image builds ─────────────────────
if [[ "$SKIP_HEAVY" -ne 1 ]]; then
  log "cmake: configure + build video engine"
  cmake \
    -S "$REPO_ROOT/RemoteCodex/native/video-engine-cpp" \
    -B /tmp/velox-engine \
    -DCMAKE_BUILD_TYPE=Release
  cmake --build /tmp/velox-engine --parallel

  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    log "docker build: velox-server"
    # context = repo root because DataServer/go.mod's
    #     replace velox-shared => ../shared
    # resolves to ./shared at the repo root.
    docker build -f "$REPO_ROOT/DataServer/Dockerfile" -t velox-server:verify .

    log "docker build: velox-worker (pre-build Go binary via make agent)"
    # Worker Dockerfile's build contract (see header) requires the Go
    # binary to already exist at <build-context>/native/worker-agent-go/
    # bin/velox-worker-agent. Mirror .github/workflows/worker-image.yml's
    # `Build worker-agent binary (pre-Docker step)` step here so that
    # `make verify` exercises the SAME production path as a release
    # build -- not a parallel "looks-like" approximation that drifts.
    make -C "$REPO_ROOT/RemoteCodex/native/worker-agent-go" agent

    # Build context = RemoteCodex (NOT repo root): the worker Dockerfile
    # COPYs scripts/{build-video-engine,worker-entrypoint}.sh,
    # native/video-engine-cpp, and native/worker-agent-go/bin/, all of
    # which are anchored one level under RemoteCodex/. Using `.` would
    # silently leave /app/native/video-engine-cpp missing.
    docker build \
      -f "$REPO_ROOT/RemoteCodex/native/worker-agent-go/Dockerfile" \
      -t velox-worker:verify \
      "$REPO_ROOT/RemoteCodex"
  else
    if [[ "${CI:-}" == "true" ]]; then
      fail "Docker daemon unreachable or Docker not installed (required in CI)"
    else
      log "WARN: docker daemon unreachable — skipping image builds"
      log "(this is fine for local dev; CI must run with a working daemon)"
    fi
  fi
fi

log "VERIFY OK"
