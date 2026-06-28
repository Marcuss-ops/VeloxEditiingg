# =============================================================================
# Velox -- root orchestrator
#
# This Makefile is intentionally minimal: development + CI run ONE command,
# `make verify`, which dispatches to scripts/ci/verify.sh. Per-component
# build logic lives in:
#   * DataServer/go.mod                           (server)
#   * RemoteCodex/native/worker-agent-go/go.mod   (agent)
#   * RemoteCodex/native/video-engine-cpp/CMakeLists.txt (C++ engine)
# Anything more elaborate than an env-dispatched script-call here will be
# rejected: the goal is "one canonical button", not a make-graph for humans.
# =============================================================================

# Force /bin/sh-unsafe constructs (e.g. `[[ -n "$x" ]]` in fmt-check) to
# resolve to bash on every host, including minimal containers whose
# /bin/sh is dash. Without this, `make fmt-check` breaks on Alpine-style
# systems where /bin/sh is not bash. Mirrors the same convention used by
# RemoteCodex/native/worker-agent-go/Makefile.
SHELL := /usr/bin/env bash

.PHONY: verify verify-fast verify-heavy fmt fmt-check vet pilot e2e-grpc e2e-workload e2e-workload-mtls \
        enable-branch-protection disable-branch-protection inspect-branch-protection \
        local-verify-mirror certify-worker certify-worker-bootstrap-mtls \
        real-bootstrap pin-worker-digest help

help:
	@echo "Velox repo orchestrator"
	@echo ""
	@echo "  make verify                       -- full architectural + integrity suite (CI default)"
	@echo "  make verify-fast                  -- architecture + Go steps only (skip docker & cmake)"
	@echo "  make fmt                          -- gofmt -w on every Go module (auto-format tree)"
	@echo "  make fmt-check                    -- gofmt -d (dry run); fails if any file is dirty"
	@echo "  make vet                          -- go vet ./... on every Go module"
	@echo "  make pilot                        -- full pilot pipeline (build + start + submit + work + poll)"
	@echo "  make e2e-grpc                     -- PR 3 gRPC control-plane E2E matrix (6 cases, ~90s)"
	@echo "  make e2e-workload                 -- PR 5 full workload E2E (Hello → artifact, ~3-5 min)"
	@echo "  make local-verify-mirror          -- reproduce the GitHub Actions pyramid locally"
	@echo ""
	@echo "  Phase 0 (branch protection, required checks):"
	@echo "  make enable-branch-protection     -- apply the four required checks on \`main\` (CI gate)"
	@echo "  make disable-branch-protection    -- escape hatch (incident response ONLY)"
	@echo "  make inspect-branch-protection    -- read-only audit of current protection state"
	@echo ""
	@echo "  Phase 1 (image certification, cap. 2):"
	@echo "  make real-bootstrap               -- REAL bootstrap pass on a published digest (4-step PASS required)"
	@echo "  make pin-worker-digest            -- record a cosign-verified baseline for a digest under evidence/baselines/"
	@echo ""
	@echo "  Phase 2 (worker certification, cap. 3):"
	@echo "  make certify-worker               -- certify a single worker host+deploy (sub-phases 2A+2B)"
	@echo "  make certify-worker-bootstrap-mtls-- certify sub-phases 2C (real bootstrap) + 2D (mTLS handshake)"
	@echo "All heavy targets defer to scripts/ci/verify.sh. Do not replicate steps."

fmt:           ## gofmt -w on every Go module
	@for mod in DataServer RemoteCodex/native/worker-agent-go shared; do \
	  echo "-> gofmt -w $$mod"; \
	  (cd $$mod && gofmt -w .) || exit 1; \
	done

fmt-check:     ## gofmt -d on every Go module (CI-style dry run; fails on dirty)
	@status=0; \
	for mod in DataServer RemoteCodex/native/worker-agent-go shared; do \
	  echo "-> gofmt -d $$mod"; \
	  out="$$(cd $$mod && gofmt -d .)"; \
	  if [[ -n "$$out" ]]; then \
	    echo "$$out"; \
	    status=1; \
	  fi; \
	done; \
	exit $$status

vet:           ## go vet ./... on every Go module
	@for mod in DataServer RemoteCodex/native/worker-agent-go shared; do \
	  echo "-> go vet $$mod"; \
	  (cd $$mod && go vet ./...) || exit 1; \
	done

pilot:         ## Full pilot pipeline (build + start + submit + work + poll)
	./scripts/pilot.sh

verify:        ## Architecture + Go (-race) + cmake + docker (full suite)
	./scripts/ci/verify.sh

verify-fast:   ## Architecture + Go (-race) only; skip cmake + docker
	SKIP_HEAVY=1 ./scripts/ci/verify.sh

verify-heavy:  ## Synonym for full verify (kept for legacy callers)
	./scripts/ci/verify.sh

e2e-grpc:      ## PR 3 — 6-case gRPC control-plane matrix on a host-native master + workers
	@bash tests/e2e/grpc-control-plane/run.sh

e2e-workload:  ## PR 5 — full workload E2E (Hello → HelloAck → Task → Artifact → SUCCEEDED)
	@bash tests/e2e/workload/run.sh

e2e-workload-mtls:  ## PR 7 — full workload E2E over mTLS (channel=staging, environment=staging, fail-closed: NO insecure fallback)
	@bash tests/e2e/workload-mtls/run.sh

# ─── Phase 0 — Branch protection + required-check wiring ────────────────────────────
# Apply the canonical Phase 0 protection to \`main\` (strict, code-owner review
# required, four canonical checks). Idempotent — re-running is a no-op.
enable-branch-protection:  ## Phase 0 — wire the 4 required checks on \`main\`
	@bash scripts/ci/enable-branch-protection.sh

# Escape hatch: removes protection. Only for incident response. The script
# itself prompts "yes" within 10s; in CI use VELOX_FORCE_REMOVE_PROTECTION=1.
disable-branch-protection: ## Phase 0 — ESCAPE HATCH (incident response ONLY)
	@bash scripts/ci/disable-branch-protection.sh

# Read-only inspector: prints current state + audits the four canonical checks.
inspect-branch-protection: ## Phase 0 — audit current branch protection
	@bash scripts/ci/inspect-branch-protection.sh

# Reproduce the GitHub Actions pyramid locally: git pull + make verify + the
# 3 e2e targets. Mirrors what the four required checks run on PR-open. See
# docs/100-percent-plan/ci-required-checks.md for SHA-pinning details.
local-verify-mirror: ## Phase 0 — reproduce CI pyramid locally
	@bash scripts/ci/local-verify-mirror.sh

# Operator runbook for Phase 0 setup (CI required checks + secret wiring).
phase0-docs: ## Phase 0 — open the operator runbook
	@echo "→ docs/100-percent-plan/ci-required-checks.md"
	@ls -la docs/100-percent-plan/ci-required-checks.md

# Operator certifier for phases 2A+2B of the 100% Velox certification plan
# (cap. 3 of docs/100-percent-plan/). Run on the VPS as root or with sudo.
# Required env: WORKER_ID. Optional: EXPECTED_WORKER_IMAGE_DIGEST, MASTER_URL,
# CERT_DATE, EVIDENCE_ROOT, CONTAINER_NAME, CONFIG_FILE, HEALTH_PORT.
certify-worker:  ## Phase 2A+2B — certify a single worker (host + deploy)
	@if [ -z "$$WORKER_ID" ]; then \
	  echo "WORKER_ID is required (export WORKER_ID=velox-worker-N or pass --worker-id)" ; \
	  exit 1 ; \
	fi
	@echo "→ certifying worker $$WORKER_ID"
	@echo "→ evidence → $${EVIDENCE_ROOT:-$$HOME/evidence}/$$(date -u +%Y-%m-%d)/$$WORKER_ID"
	@bash scripts/cert/certify-worker-2a-2b.sh "$$@"

# Phase 1 / cap. 2 — real-bootstrap certifier. Pull the actual published
# worker image, run `velox-worker-agent --bootstrap-report` under production
# deps baked into the image, parse [BOOTSTRAP_REPORT] from stderr, and
# assert verdict=OK + all 4 canonical step PASS. Saves evidence under
# $EVIDENCE_ROOT/<date>/<worker_id>/. NOT a fake-FFmpeg/ffprobe smoke —
# this boots the real binary against the real engine + real FFmpeg.
real-bootstrap:  ## Phase 1 / cap. 2 — REAL bootstrap pass on a published digest
	@if [ -z "$$WORKER_IMAGE" ]; then \
	  echo "WORKER_IMAGE is required (e.g. export WORKER_IMAGE=ghcr.io/<owner>/velox-worker@sha256:<64hex>)" ; \
	  exit 1 ; \
	fi
	@if [ -z "$$EXPECTED_BUNDLE_HASH" ]; then \
	  echo "EXPECTED_BUNDLE_HASH is required (the 64-lowercase-hex SHA-256 of the published BUNDLE_HASH.txt)" ; \
	  exit 1 ; \
	fi
	@bash scripts/cert/real-bootstrap.sh "$$@"

# Phase 1 / cap. 2 — operator pinner. Records baseline manifest per published
# digest (registry, tags, version, cosign signature envelope, pinning
# timestamp, pinning operator) under $EVIDENCE_ROOT/baselines/. Unsigned
# or wrong-signatory digests are FAIL-CLOSED. Required env: DIGEST.
pin-worker-digest:  ## Phase 1 / cap. 2 — record a cosign-verified baseline for a digest
	@if [ -z "$$DIGEST" ]; then \
	  echo "DIGEST is required (e.g. export DIGEST=ghcr.io/<owner>/velox-worker@sha256:<64hex>)" ; \
	  exit 1 ; \
	fi
	@bash scripts/cert/pin-worker-digest.sh "$$@"

# Phase 2C+2D / cap. 3 — orchestrator that runs (a) 2C: real-bootstrap
# certifier on the published worker image, asserting verdict=OK + the 4
# canonical step PASS; (b) 2D-1: static cert checks (CN==worker_id, CA
# chain, expiry, EKU); (c) 2D-2: dynamic handshake probe via
# DataServer/cmd/dev-hello-client against MASTER_URL; (d) 2D-3: master
# state probe via /api/v1/workers. Saves canonical cap-3 evidence files
# under $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/.
# Required env: WORKER_ID, WORKER_IMAGE, EXPECTED_BUNDLE_HASH,
# WORKER_CERT_FILE, WORKER_KEY_FILE, WORKER_CA_FILE. Optional: MASTER_URL
# (host:port for gRPC), MASTER_RESTSERVER (https URL for REST state probe).
# Fail-closed unless --allow-skip-dynamic is passed.
certify-worker-bootstrap-mtls:  ## Phase 2C+2D — real bootstrap + mTLS handshake certifier
	@if [ -z "$$WORKER_ID" ]; then \
	  echo "WORKER_ID is required (export WORKER_ID=velox-worker-N or pass --worker-id)" ; \
	  exit 1 ; \
	fi
	@if [ -z "$$WORKER_IMAGE" ]; then \
	  echo "WORKER_IMAGE is required (export WORKER_IMAGE=ghcr.io/<owner>/velox-worker@sha256:<64hex>)" ; \
	  exit 1 ; \
	fi
	@if [ -z "$$EXPECTED_BUNDLE_HASH" ]; then \
	  echo "EXPECTED_BUNDLE_HASH is required (the 64-lowercase-hex SHA-256 of the published BUNDLE_HASH.txt)" ; \
	  exit 1 ; \
	fi
	@if [ -z "$$WORKER_CERT_FILE" ] || [ -z "$$WORKER_KEY_FILE" ] || [ -z "$$WORKER_CA_FILE" ]; then \
	  echo "WORKER_CERT_FILE, WORKER_KEY_FILE, WORKER_CA_FILE are all required" ; \
	  exit 1 ; \
	fi
	@echo "→ certifying worker $$WORKER_ID (2C bootstrap + 2D mTLS handshake)"
	@echo "→ evidence → $${EVIDENCE_ROOT:-$$HOME/evidence}/$$(date -u +%Y-%m-%d)/$$WORKER_ID"
	@bash scripts/cert/certify-worker-2c-2d.sh "$$@"

# Operator runbook for Phase 0 setup (CI required checks + secret wiring).
phase0-docs: ## Phase 0 — open the operator runbook
	@echo "→ docs/100-percent-plan/ci-required-checks.md"
	@ls -la docs/100-percent-plan/ci-required-checks.md
