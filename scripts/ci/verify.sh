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
  for mod in DataServer RemoteCodex/native/worker-agent-go; do
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
      GOFLAGS="-mod=mod" go vet ./...
      log "go-test -race: ${mod}"
      GOFLAGS="-mod=mod" go test -race -count=1 -timeout 180s ./...
    )
  done

  # shared has no internal tests; build + vet is enough to fail-fast on
  # contract drift between DataServer and RemoteCodex consumers.
  log "go-build+vet: shared"
  (
    set -euo pipefail
    cd "$REPO_ROOT/shared"
    GOFLAGS="-mod=mod" go build ./...
    GOFLAGS="-mod=mod" go vet ./...
  )
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
    docker build -f "$REPO_ROOT/DataServer/Dockerfile" -t velox-server:verify .
    log "docker build: velox-worker"
    docker build \
      -f "$REPO_ROOT/RemoteCodex/native/worker-agent-go/Dockerfile" \
      -t velox-worker:verify .
  else
    log "WARN: docker daemon unreachable \u2014 skipping image builds"
    log "(this is fine for local dev; CI must run with a working daemon)"
  fi
fi

log "VERIFY OK"
