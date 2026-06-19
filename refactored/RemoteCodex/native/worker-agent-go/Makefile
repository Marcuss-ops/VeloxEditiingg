# Velox Worker Agent - Build System

# Pick the first existing VERSION.txt from a fallback chain. The Monorepo
# layout assumes VERSION.txt at the project root, but the chain also
# tolerates:
#   - a VERSION.txt alongside this Makefile (standalone build of the
#     worker without the rest of the repo); or
#   - one inside RemoteCodex/ (if the team ever promotes RemoteCodex/
#     to its own checkout).
# If none of the candidates exist, the `?=` later falls back to a
# `git describe` of the current tree, and finally to literal "dev".
#
# To pin a specific file, callers can still override the variable on the
# command line, e.g. `make VERSION_FILE=./local.version agent`.
VERSION_FILE ?= $(firstword \
    $(wildcard VERSION.txt \
             ../VERSION.txt \
             ../../VERSION.txt \
             ../../../VERSION.txt))
VERSION ?= $(shell \
    if [ -n "$(VERSION_FILE)" ] && [ -f "$(VERSION_FILE)" ]; then \
        tr -d '[:space:]' < "$(VERSION_FILE)"; \
    else \
        git describe --tags --always --dirty 2>/dev/null || echo "dev"; \
    fi)
BUILD_TIME ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S')
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
	@echo "Building Docker image..."
	docker build -t velox-worker-agent:$(VERSION) -t velox-worker-agent:latest .

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
