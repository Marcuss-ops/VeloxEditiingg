# Velox Worker Agent - Build System

# Version is read VERBATIM from ../../../VERSION.txt (canonical project
# root). The build FAILS HARD if that file is missing — there is no
# Git-tag / "development" fallback by design: every release artifact must be
# produced from an explicitly-bumped VERSION.txt so the worker bundle hash
# the master emits is reproducible.
#
# To pin a specific file, callers can still override the variable on the
# command line, e.g. `make VERSION_FILE=./local.version agent`.
VERSION_FILE := ../../../VERSION.txt
VERSION := $(strip $(shell if [ -f "$(VERSION_FILE)" ]; then \
    tr -d '[:space:]' < "$(VERSION_FILE)"; \
else \
    echo "FATAL: $(VERSION_FILE) not found. Bump the project root VERSION.txt before building." >&2; \
    exit 1; \
fi))

# Reproducible-builds BuildTime.
#
# Priority order (highest first):
#   1. SOURCE_DATE_EPOCH (Reproducible Builds env contract)
#      -> date -u -d @<epoch> '+%Y-%m-%dT%H:%M:%SZ'
#   2. HEAD commit timestamp from `git log -1 --format=%cI`
#      -> normalized to '%Y-%m-%dT%H:%M:%SZ'
#   3. "unknown" -- the literal fallback, NOT a wall-clock timestamp.
#      A wall-clock timestamp in a release binary would invalidate the
#      Reproducible Builds contract (two builds of the same commit MUST
#      produce byte-identical Layer 0+ of the container image's
#      binary when the input tree is identical). Operators building for
#      release MUST export one of (1) or (2).
BUILD_TIME ?= $(shell \
  if [ -n "$${SOURCE_DATE_EPOCH:-}" ]; then \
    date -u -d "@$${SOURCE_DATE_EPOCH}" '+%Y-%m-%dT%H:%M:%SZ'; \
  elif git log -1 --format=%cI >/dev/null 2>&1; then \
    git log -1 --format=%cI | sed -E 's/([0-9-]+)T([0-9:]+)[.+].*/\1T\2Z/'; \
  else \
    echo unknown; \
  fi)
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOMOD := $(GOCMD) mod

AGENT_BINARY := velox-worker-agent
INSTALLER_BINARY := velox-installer

PREFIX ?= /usr/local
BINDIR := $(PREFIX)/bin

BUILD_DIR := build
BIN_DIR := bin

.PHONY: all build clean test install uninstall agent installer release
.PHONY: release-linux release-darwin release-windows test-coverage
.PHONY: fmt lint deps verify-deps docs run-agent docker help
.PHONY: bootstrap-smoke bootstrap-selftest-regenerate

all: build

build: agent installer

agent:
	@echo "Building $(AGENT_BINARY)..."
	@mkdir -p $(BIN_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BIN_DIR)/$(AGENT_BINARY) ./cmd/velox-worker-agent

installer:
	@echo "Building $(INSTALLER_BINARY)..."
	@mkdir -p $(BIN_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BIN_DIR)/$(INSTALLER_BINARY) ./cmd/installer

release: release-linux release-darwin release-windows

release-linux:
	@echo "Building for Linux (amd64)..."
	@mkdir -p $(BUILD_DIR)/linux-amd64
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/linux-amd64/$(AGENT_BINARY) ./cmd/velox-worker-agent
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/linux-amd64/$(INSTALLER_BINARY) ./cmd/installer

	@echo "Building for Linux (arm64)..."
	@mkdir -p $(BUILD_DIR)/linux-arm64
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/linux-arm64/$(AGENT_BINARY) ./cmd/velox-worker-agent
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/linux-arm64/$(INSTALLER_BINARY) ./cmd/installer

release-darwin:
	@echo "Building for macOS (amd64)..."
	@mkdir -p $(BUILD_DIR)/darwin-amd64
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/darwin-amd64/$(AGENT_BINARY) ./cmd/velox-worker-agent

	@echo "Building for macOS (arm64)..."
	@mkdir -p $(BUILD_DIR)/darwin-arm64
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/darwin-arm64/$(AGENT_BINARY) ./cmd/velox-worker-agent

release-windows:
	@echo "Building for Windows (amd64)..."
	@mkdir -p $(BUILD_DIR)/windows-amd64
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/windows-amd64/$(AGENT_BINARY).exe ./cmd/velox-worker-agent

test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

install: build
	@echo "Installing binaries to $(BINDIR)..."
	install -m 755 $(BIN_DIR)/$(AGENT_BINARY) $(BINDIR)/
	install -m 755 $(BIN_DIR)/$(INSTALLER_BINARY) $(BINDIR)/
	@echo "Installation complete"

uninstall:
	@echo "Removing binaries from $(BINDIR)..."
	rm -f $(BINDIR)/$(AGENT_BINARY)
	rm -f $(BINDIR)/$(INSTALLER_BINARY)
	@echo "Uninstallation complete"

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -rf $(BIN_DIR)
	rm -f coverage.out coverage.html

fmt:
	@echo "Formatting code..."
	gofmt -s -w .

lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(BINDIR) v1.55.2)
	golangci-lint run ./...

deps:
	@echo "Updating dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

verify-deps:
	@echo "Verifying dependencies..."
	$(GOMOD) verify

docs:
	@echo "Generating documentation..."
	$(GOCMD) doc -all ./pkg/api > docs/pkg-api.txt || true

run-agent: agent
	./$(BIN_DIR)/$(AGENT_BINARY) -master http://localhost:8080 -work-dir .

docker:
	@echo "Building Docker image (PR-3 self-sufficient, no pre-build required)..."
	# PR-3: the worker Dockerfile now compiles both the C++ engine
	# (cpp-builder stage) and the Go worker binary (go-builder stage)
	# INLINE. No `$(MAKE) agent` pre-step is needed; building from a
	# clean checkout with `docker build` and nothing else works.
	#
	# Build context MUST be the repo root (cd ../.. from worker-agent-go
	# lands at root, NOT at RemoteCodex/ — PR-3 moved everything to
	# repo-root paths inside the Dockerfile):
	#   * RemoteCodex/scripts/build-video-engine.sh (cpp-builder COPY)
	#   * RemoteCodex/native/video-engine-cpp/        (cpp-builder COPY)
	#   * RemoteCodex/native/worker-agent-go/         (go-builder COPY)
	#   * shared/                                    (go-builder COPY,
	#                                                  satisfies the
	#                                                  `replace velox-shared`
	#                                                  in worker-agent-go/go.mod)
	#   * RemoteCodex/scripts/worker-entrypoint.sh   (runtime COPY)
	# The bin/velox-worker-agent that `make agent` would have left is
	# ignored by the Dockerfile (the COPY --from=go-builder stage
	# supplies the freshly-built binary instead).
	cd ../.. && docker build \
		-f native/worker-agent-go/Dockerfile \
		-t velox-worker-agent:$(VERSION) \
		--build-arg "VERSION=$(VERSION)" \
		--build-arg "BUILD_TIME=$(BUILD_TIME)" \
		.

# RW-PROD-003 §5: smoke test the bootstrap gate WITHOUT going through
# the full main() flow. Used by CI as a single-host canary before any
# real production canary (RW-PROD-007) is even attempted.
#
#   - run `bash tests/bootstrap-smoke.sh` from the module root
#   - exit 0  → every step OK, JSON boot report on stdout
#   - exit 1  → at least one step FAIL, JSON dumps to stderr
#
# The script itself uses a fake cfg.BundleHash that matches a touched
# BUNDLE_HASH.txt so we do not depend on the production artefact. The
# `bootstrap-selftest-regenerate` target writes a fresh baseline SHA
# after a confirmed-render against a verified C++ engine build.
bootstrap-smoke: agent
	@echo "Bootstrap smoke (RW-PROD-003 §5)…"
	@bash tests/bootstrap-smoke.sh
	@echo "Bootstrap smoke OK"

# RW-PROD-003 §5 regeneration. Run ONLY after a confirmed C++ engine
# build bump that produced a verified-byte-stable render. CI MUST
# gate this via a manual approval — the operator pins
# VERSION_FILE_BASELINE_TARBALL=<path-to-tarball> and the script
# rejects any render whose surrounding metadata disagrees.
#
# Requires the C++ render binary at $VELOX_VIDEO_ENGINE_CPP_BIN.
bootstrap-selftest-regenerate: agent
	@echo "Regenerating tests/fixtures/engine_selftest_baseline.sha256 (RW-PROD-003 §3 A1+A2)…"
	@bash tests/bootstrap-selftest-regenerate.sh
	@echo "Baseline regenerated; commit alongside VERSION.txt bump"

help:
	@echo "Velox Worker Agent - Build System"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all            Clean and build all binaries (default)"
	@echo "  build          Build all binaries"
	@echo "  agent          Build the worker agent binary"
	@echo "  installer      Build the installer binary"
	@echo "  release        Build for all platforms (Linux, macOS, Windows)"
	@echo "  test           Run all tests"
	@echo "  test-coverage  Run tests with coverage report"
	@echo "  install        Install binaries to $(BINDIR)"
	@echo "  uninstall      Remove binaries from $(BINDIR)"
	@echo "  clean          Remove build artifacts"
	@echo "  fmt            Format source code"
	@echo "  lint           Run linter"
	@echo "  deps           Update dependencies"
	@echo "  run-agent      Run worker agent locally (dev)"
	@echo "  docker         Build Docker image"
	@echo "  help           Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION        Version string (default: git tag or 'dev')"
	@echo "  PREFIX         Installation prefix (default: /usr/local)"
	@echo ""
	@echo "Examples:"
	@echo "  make build                    # Build all binaries"
	@echo "  make test                     # Run tests"
	@echo "  make install                  # Install to /usr/local/bin"
	@echo "  make VERSION=v1.0.0 release   # Build release version"
