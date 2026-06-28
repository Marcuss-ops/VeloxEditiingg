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

# Per-cap evidence roots. Cap-9 + cap-10 store their hermetic
# per-cell JSONL + SQLite DBs + verdict.json under these paths;
# cap-7 / cap-8 reuse $EVIDENCE_ROOT directly. Override at make
# invocation (EVIDENCE_ROOT_CAP9=/var/log/... make cap-9-capacity).
EVIDENCE_ROOT_CAP9      ?= /tmp/velox-cap9-evidence
EVIDENCE_ROOT_CAP10     ?= /tmp/velox-cap10-evidence

.PHONY: verify verify-fast verify-heavy fmt fmt-check vet pilot e2e-grpc e2e-workload e2e-workload-mtls \
        enable-branch-protection disable-branch-protection inspect-branch-protection \
        local-verify-mirror certify-worker certify-worker-bootstrap-mtls \
        real-bootstrap pin-worker-digest recovery-matrix recovery-matrix-dry \
        cap-7-reboot-recovery cap-7-reboot-recovery-dry \
        cap-8-upgrade-rollback cap-8-upgrade-rollback-dry \
        cap-8-upgrade-rollback-upgrade cap-8-upgrade-rollback-rollback \
        cap-9-capacity cap-9-capacity-dry cap-9-c150-engine cap-9-small-determinism \
        cap-10-soak cap-10-soak-dry cap-10-soak-48 cap-10-soak-72 cap-10-soak-operator help

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
	@echo ""
	@echo "  Phase 3 (recovery matrix, cap. 6):"
	@echo "  make recovery-matrix              -- run the 15-scenario fault-injection + 7 invariant checks"
	@echo "  make recovery-matrix-dry          -- bash -n syntax sweep only (no DB or process actions)"
	@echo ""
	@echo "  Phase 6 (reboot recovery, cap. 7):"
	@echo "  make cap-7-reboot-recovery        -- pre-reboot → reboot → post-reboot walk; NR-8/9/11/12"
	@echo "  make cap-7-reboot-recovery-dry    -- bash -n sweep only"
	@echo ""
	@echo "  Phase 7 (upgrade + rollback, cap. 8):"
	@echo "  make cap-8-upgrade-rollback       -- bidirectional digest-A↔digest-B walk; NR-10/13/14/15"
	@echo "  make cap-8-upgrade-rollback-upgrade  -- upgrade scenario (A → B) only"
	@echo "  make cap-8-upgrade-rollback-rollback -- rollback scenario (B → A) only"
	@echo "  make cap-8-upgrade-rollback-dry  -- bash -n sweep only"
	@echo ""
	@echo "  Phase 8 (capacity curve, cap. 9):"
	@echo "  make cap-9-capacity               -- 12-cell capacity matrix + 150-frame engine bench (NR-16..NR-25)"
	@echo "  make cap-9-c150-engine            -- 150-frame C++ engine bench only (NR-22..NR-24)"
	@echo "  make cap-9-small-determinism      -- small-profile 5-rerun byte-determinism only (NR-16..NR-17)"
	@echo "  make cap-9-capacity-dry           -- bash -n + python3 preflight only"
	@echo ""
	@echo "  Phase 9 (24h soak, cap. 10):"
	@echo "  make cap-10-soak                  -- 24h soak simulator (288 ticks, NR-26..NR-38)"
	@echo "  make cap-10-soak-48               -- 48h soak simulator (576 ticks)"
	@echo "  make cap-10-soak-72               -- 72h soak simulator (864 ticks)"
	@echo "  make cap-10-soak-dry              -- bash -n + python3 preflight only"
	@echo "  make cap-10-soak-operator         -- operator mode (refuses auto-run; see cap-10-soak.md)"
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

# Phase 3 / cap. 6 — 15-scenario fault-injection recovery matrix with
# 7 canonical invariants (NR-1..NR-7). Pure sqlite3 + bash. Each scenario
# pre-recovery-mutates the SQLite state to simulate the fault surface and
# asserts that the SQL CAS / handler check would have prevented the
# invariant violation. Negative scenarios (TaskResult duplicate, etc.) PASS
# when the system CORRECTLY REJECTS the bad input.
# Default evidence root: $EVIDENCE_ROOT (override via make arg).
# Refuses to run as root by default (ENOSPC sim is unreliable); set
# VELOX_RECOVERY_ALLOW_ROOT=1 to override.
recovery-matrix:  ## Phase 3 / cap. 6 — 15-scenario recovery matrix (NR-1..NR-7)
	@if ! command -v sqlite3 >/dev/null 2>&1; then \
	  echo "sqlite3 not found — install sqlite3 before running make recovery-matrix" ; \
	  exit 1 ; \
	fi
	@bash tests/e2e/recovery-matrix/run.sh

recovery-matrix-dry:  ## Phase 3 / cap. 6 — bash -n syntax sweep only
	@bash tests/e2e/recovery-matrix/run.sh --dry-run

# Phase 6 / cap. 7 — reboot-recovery simulator. Drives the pre-reboot
# / mid-reboot-gap / post-reboot cadence from the operator runbook
# (scripts/cert/cap-7-reboot-recovery.sh) but uses local stub master +
# worker + sqlite3 against $DATA_DIR/velox.db so CI can run the same
# invariants on every commit. Asserts NR-8 (config sha256 preserved),
# NR-9 (image digest preserved), NR-11 (certs sha256 preserved), and
# NR-12 (orphan-task recovery via LEASE_EXPIRED + fresh attempt).
# The dpsim orchestrator does NOT pull containers or reboot the host.
cap-7-reboot-recovery:  ## Phase 6 / cap. 7 — reboot-recovery simulator (NR-8/9/11/12)
	@if ! command -v sqlite3 >/dev/null 2>&1; then \
	  echo "sqlite3 not found — install sqlite3 before running make cap-7-reboot-recovery" ; \
	  exit 1 ; \
	fi
	@bash tests/e2e/cap-7-reboot-recovery/simulator.sh

cap-7-reboot-recovery-dry:  ## Phase 6 / cap. 7 — bash -n + python3 preflight only
	@bash tests/e2e/cap-7-reboot-recovery/simulator.sh --dry-run

# Phase 7 / cap. 8 — upgrade + rollback simulator. Mocks docker pull +
# cosign verify with $DATA_DIR/.active-digest + $EVIDENCE_ROOT/baselines/
# _index.json so the same invariants NR-10 (digest changed post-upgrade),
# NR-13 (post-rollback digest IS the prior baselined A), NR-14 (drain
# column flipped), NR-15 (no double finalization across the swap boundary)
# hold end-to-end without pulling 250 MB of container layers.
# —upgrade and —rollback sub-targets exercise a single scenario in isolation.
cap-8-upgrade-rollback:  ## Phase 7 / cap. 8 — full upgrade + rollback simulator (NR-10/13/14/15)
	@if ! command -v sqlite3 >/dev/null 2>&1; then \
	  echo "sqlite3 not found — install sqlite3 before running make cap-8-upgrade-rollback" ; \
	  exit 1 ; \
	fi
	@bash tests/e2e/cap-8-upgrade-rollback/simulator.sh

cap-8-upgrade-rollback-upgrade:  ## Phase 7 / cap. 8 — upgrade scenario only (A → B, NR-10/15)
	@SCEN_CAP8_ONLY=upgrade bash tests/e2e/cap-8-upgrade-rollback/simulator.sh

cap-8-upgrade-rollback-rollback:  ## Phase 7 / cap. 8 — rollback scenario only (B → A, NR-13/15)
	@SCEN_CAP8_ONLY=rollback bash tests/e2e/cap-8-upgrade-rollback/simulator.sh

cap-8-upgrade-rollback-dry:  ## Phase 7 / cap. 8 — bash -n + python3 preflight only
	@bash tests/e2e/cap-8-upgrade-rollback/simulator.sh --dry-run

# Phase 8 / cap. 9 — capacity curve + C++ engine benchmark. Drives the
# 12-cell matrix (3 profiles × 4 capacity multipliers) and the 150-frame
# C++ engine bench against the canonical NR-16..NR-25 invariants:
#   * NR-16..NR-17: small profile is byte-deterministic across 5 reruns
#   * NR-18..NR-20: capacity curve scaling (queue, dispatcher-warm, throughput)
#   * NR-21:        Large profile bounded-RAM growth (no linear leak)
#   * NR-22..NR-24: C++ engine clearnode restore + no full-dirty fallback
#   * NR-25:        per-executor retry bounds
# CI-runnable; engine binary is optional (Python mock stand-in if absent).
# Default evidence root: /tmp/velox-cap9-evidence (override via make env).
cap-9-capacity:  ## Phase 8 / cap. 9 — capacity curve + C150 engine (NR-16..NR-25)
	@if ! command -v sqlite3 >/dev/null 2>&1; then \
	  echo "sqlite3 not found — install sqlite3 before running make cap-9-capacity" ; \
	  exit 1 ; \
	fi
	@if ! command -v python3 >/dev/null 2>&1; then \
	  echo "python3 not found — required for synthetic frame generator" ; \
	  exit 1 ; \
	fi
	@bash tests/e2e/cap-9-capacity/run.sh

cap-9-c150-engine:  ## Phase 8 / cap. 9 — C++ engine 150-frame bench only (NR-22..NR-24)
	@bash tests/e2e/cap-9-capacity/run.sh --engine-only

cap-9-small-determinism:  ## Phase 8 / cap. 9 — Small profile 5-rerun byte-determinism (NR-16..NR-17)
	@bash tests/e2e/cap-9-capacity/run.sh --capacity-only

cap-9-capacity-dry:  ## Phase 8 / cap. 9 — bash -n + python3 preflight only
	@bash tests/e2e/cap-9-capacity/run.sh --dry-run

# Phase 9 / cap. 10 — 24h–72h soak with chaos engineering. Compresses a
# real soak (cosign-pinned image, real mTLS, real chaos) to a CI-runnable
# 288- to 864-tick simulator (1 tick = 5 sim-min). Asserts 13 hard
# acceptance thresholds (NR-26..NR-38) split across two dimensions:
#   * NR-26..NR-35 — stability / chaos-soak: 0 jobs lost, 0 duplicate
#     active tasks, 0 duplicate artifacts, 0 corrupt artifacts, 0
#     unauthorized mTLS connections (bounded by AUTH_REJECT_THRESHOLD ≤
#     20), 0 stuck workers after reconnect (within WATCHDOG_GRACE), 0
#     jobs RUNNING beyond TTL+reaper, 0 linear RAM growth, 0
#     uncontrolled staging-cache growth (≤ MAX_ACTIVE_JOBS ×
#     AVG_JOB_STAGING_BYTES × 2), 100% coherent outcomes (status ==
#     expected_terminal).
#   * NR-36..NR-38 — scale / RPC profile: RPC reconnect / rotation
#     latency p99 ≤ 10 ticks (~50 sim-min), Jain-index fairness ≥ 0.85
#     across active workers, cross-worker load-balance max/min ratio
#     ≤ 2.5×.
# The 24/48/72h variants set DURATION_HOURS=24|48|72 on the underlying
# bash simulator.
#   * cap-10-soak-operator is intentionally NOT auto-run: it requires
#     cosign-pinned VPS image + mTLS allowlist + systemd units; operators
#     invoke scripts/cert/cap-10-soak.sh manually after confirming prereqs.
cap-10-soak:  ## Phase 9 / cap. 10 — 24h soak simulator (288 ticks, NR-26..NR-38)
	@if ! command -v sqlite3 >/dev/null 2>&1; then \
	  echo "sqlite3 not found — install sqlite3 before running make cap-10-soak" ; \
	  exit 1 ; \
	fi
	@if ! command -v python3 >/dev/null 2>&1; then \
	  echo "python3 not found — required for invariant verifier" ; \
	  exit 1 ; \
	fi
	@EVIDENCE_ROOT=$(EVIDENCE_ROOT_CAP10) bash tests/e2e/cap-10-soak/run.sh --24h

cap-10-soak-48:  ## Phase 9 / cap. 10 — 48h soak simulator (576 ticks)
	@EVIDENCE_ROOT=$(EVIDENCE_ROOT_CAP10) bash tests/e2e/cap-10-soak/run.sh --48h

cap-10-soak-72:  ## Phase 9 / cap. 10 — 72h soak simulator (864 ticks)
	@EVIDENCE_ROOT=$(EVIDENCE_ROOT_CAP10) bash tests/e2e/cap-10-soak/run.sh --72h

cap-10-soak-dry:  ## Phase 9 / cap. 10 — bash -n + python3 preflight only
	@bash tests/e2e/cap-10-soak/run.sh --dry-run

cap-10-soak-operator:  ## Phase 9 / cap. 10 — REAL 24-72h VPS soak (operator mode only)
	@echo "::warning::operator mode requires cosign-pinned VPS image + mTLS allowlist + systemd"
	@echo "::warning::refusing to auto-run; invoke scripts/cert/cap-10-soak.sh manually after confirming prereqs"
	@echo "see docs/100-percent-plan/cap-10-soak.md for operator checklist"
