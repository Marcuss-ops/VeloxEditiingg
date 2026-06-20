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

.PHONY: verify verify-fast verify-heavy fmt fmt-check vet help

help:
	@echo "Velox repo orchestrator"
	@echo ""
	@echo "  make verify         -- full architectural + integrity suite (CI default)"
	@echo "  make verify-fast    -- architecture + Go steps only (skip docker & cmake)"
	@echo "  make fmt            -- gofmt -w on every Go module (auto-format tree)"
	@echo "  make fmt-check      -- gofmt -d (dry run); fails if any file is dirty"
	@echo "  make vet            -- go vet ./... on every Go module"
	@echo ""
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
	  (cd $$mod && GOFLAGS=-mod=mod go vet ./...) || exit 1; \
	done

verify:        ## Architecture + Go (-race) + cmake + docker (full suite)
	./scripts/ci/verify.sh

verify-fast:   ## Architecture + Go (-race) only; skip cmake + docker
	SKIP_HEAVY=1 ./scripts/ci/verify.sh

verify-heavy:  ## Synonym for full verify (kept for legacy callers)
	./scripts/ci/verify.sh
