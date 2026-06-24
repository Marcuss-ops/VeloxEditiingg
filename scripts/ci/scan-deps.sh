#!/usr/bin/env bash
# scripts/ci/scan-deps.sh
#
# PR-5 5f — comprehensive dependency scanning. Three layers:
#
#   1. govulncheck on the three Go modules in go.work (DataServer,
#      RemoteCodex/native/worker-agent-go, shared).
#   2. (Optional) syft + grype SBOM + CVE scan against any locally-built
#      Docker images the operator has tagged.
#   3. (Optional) npm audit + npm outdated on frontend/.  (Frontend extracted to separate VeloxFrontend repo.)
#
# Tooling matrix — every missing tool skips cleanly with a one-line log;
# the script never blocks on missing dependencies:
#
#   govulncheck                          Go module vulns (Go std + dep vulns)
#   syft                                 SBOM generation from image
#   grype                                CVE scan against SBOM
#   npm + frontend/package-lock.json  JS dep vulns (extracted to VeloxFrontend)
#
# Failure modes:
#   0   scan ran clean (or skipped cleanly with no tools available)
#   1   at least one vulnerability found (caller's job to triage)
#   2   usage error / sanity fail
#
# Operators running this locally get fast feedback. CI binds to the same
# exit codes; a non-zero result fails the workflow.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

log()    { printf '\u2192 %s\n' "$*"; }
warn()   { printf '\u26a0 %s\n' "$*" >&2; }
fail()   { printf '\u2717 %s\n' "$*" >&2; exit 1; }
skip()   { printf '\u2299 %s (skipped -- tool not installed)\n' "$1"; }

violations=0

# ── 1. govulncheck across all 3 Go modules ────────────────────────────────
if command -v govulncheck >/dev/null 2>&1; then
    log 'govulncheck: DataServer'
    if ! (cd "$REPO_ROOT/DataServer" && govulncheck ./...); then
        violations=$((violations + 1))
    fi
    log 'govulncheck: RemoteCodex/native/worker-agent-go'
    if ! (cd "$REPO_ROOT/RemoteCodex/native/worker-agent-go" && govulncheck ./...); then
        violations=$((violations + 1))
    fi
    log 'govulncheck: shared'
    if ! (cd "$REPO_ROOT/shared" && govulncheck ./...); then
        violations=$((violations + 1))
    fi
else
    skip 'govulncheck'
    warn 'install: go install golang.org/x/vuln/cmd/govulncheck@latest'
fi

# ── 2. syft + grype image SBOM + CVE scan ─────────────────────────────────
# Only runs against locally-built images the operator already tagged. CI
# invokes this with --image=velox-server:verify --image=velox-worker:verify
# right after the docker build step. Locally, leave --image empty to skip.
SCAN_IMAGE_LIST="${SCAN_IMAGES:-}"
for image in $SCAN_IMAGE_LIST; do
    if command -v syft >/dev/null 2>&1; then
        log "syft: $image"
        if ! syft "$image" -o spdx-json 2>/dev/null | tee /tmp/velox-sbom.spdx.json >/dev/null; then
            violations=$((violations + 1))
            continue
        fi
    else
        skip "syft ($image)"
    fi
    if command -v grype >/dev/null 2>&1; then
        log "grype: $image"
        # grype exit codes: 0 clean, 1 vulns found, >1 error
        if ! grype "sbom:/tmp/velox-sbom.spdx.json" --fail-on medium; then
            rc=$?
            if [[ $rc -eq 1 ]]; then
                violations=$((violations + 1))
            else
                warn "grype returned $rc on $image (treated as scan error)"
                violations=$((violations + 1))
            fi
        fi
    else
        skip "grype ($image)"
    fi
done

# ── 3. npm audit (VeloxFrontend) ──────────────────────────────────────────
# The frontend was extracted to its own repo (VeloxFrontend/); npm audit
# on the frontend is the responsibility of that repo's CI pipeline.
# This stanza is intentionally a skip — kept as a placeholder for a future
# `cd VeloxFrontend/web && npm audit` if operators want in-repo scanning.
skip 'npm audit (frontend extracted to VeloxFrontend)'

# ── Summary ───────────────────────────────────────────────────────────────
if (( violations > 0 )); then
    fail "scan-deps: $violations tier(s) reported vulnerabilities"
fi

log 'scan-deps: OK'
