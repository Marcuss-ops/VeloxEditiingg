package workers

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"
)

// WorkerUpdateHandler handles worker bundle lifecycle operations:
// manifest generation (called by the supervisor at boot) and the
// bundle-hash surface consumed by worker status reporting. The former
// HTTP update/rollout/ack surface was removed as dead code — workers
// are updated via the mounted admin command routes and the gRPC
// command stream (worker_commands is the single source of truth).
type WorkerUpdateHandler struct {
	cfg         *config.Config
	reg         *workersreg.Registry
	cmdMgr      *workersreg.CommandManager
	dataDir     string
	bundleDir   string
	codeVersion string
}

func bundleDirCandidates(dataDir string) []string {
	add := func(out *[]string, seen map[string]struct{}, path string) {
		if path == "" {
			return
		}
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		*out = append(*out, path)
	}

	candidates := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	if dataDir == "" {
		add(&candidates, seen, "worker_downloads")
		return candidates
	}

	runtimeDir := filepath.Dir(dataDir)
	repoRoot := filepath.Dir(runtimeDir)

	for _, root := range []string{
		dataDir,
		runtimeDir,
		repoRoot,
	} {
		add(&candidates, seen, filepath.Join(root, "worker_downloads"))
		add(&candidates, seen, filepath.Join(root, "BundleRemote", "worker_downloads"))
		add(&candidates, seen, filepath.Join(root, "BundleRemote"))
		add(&candidates, seen, filepath.Join(root, "DataServer", "data", "worker_downloads"))
		add(&candidates, seen, filepath.Join(root, "refactored", "DataServer", "data", "worker_downloads"))
	}

	return candidates
}

func resolveBundlePath(bundleDir, platform, arch string) (string, os.FileInfo, error) {
	bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
	bundlePath := filepath.Join(bundleDir, bundleName)
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		bundlePath = filepath.Join(bundleDir, "worker_code.zip")
	}
	info, err := os.Stat(bundlePath)
	if err != nil {
		return "", nil, err
	}
	return bundlePath, info, nil
}

func findRepoRootFrom(start string) string {
	dir, err := filepath.Abs(start)
	if err != nil {
		dir = start
	}
	for i := 0; i < 6; i++ {
		candidate := filepath.Join(dir, "DataServer")
		if stat, err := os.Stat(candidate); err == nil && stat.IsDir() {
			return dir
		}
		candidateAlternative := filepath.Join(dir, "refactored", "DataServer")
		if stat, err := os.Stat(candidateAlternative); err == nil && stat.IsDir() {
			return filepath.Join(dir, "refactored")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// NewWorkerUpdateHandler creates the worker bundle handler. The
// former tokenMgr/outboxStore parameters were removed with the dead
// HTTP update/ack/rebuild surface; CommandManager still holds the
// process-wide singleton (asserted by cmd/server/bootstrap_test.go).
func NewWorkerUpdateHandler(cfg *config.Config, reg *workersreg.Registry, cmdMgr *workersreg.CommandManager, dataDir string) *WorkerUpdateHandler {
	bundleDir := cfg.Workers.BundleDir
	if bundleDir != "" {
		if _, err := os.Stat(filepath.Join(bundleDir, "worker_code.zip")); err != nil {
			bundleDir = ""
		}
	}
	if bundleDir == "" {
		for _, d := range bundleDirCandidates(dataDir) {
			if _, err := os.Stat(filepath.Join(d, "worker_code.zip")); err == nil {
				bundleDir = d
				break
			}
		}
		if bundleDir == "" {
			bundleDir = filepath.Join(dataDir, "worker_downloads")
		}
	}

	log.Printf("[UPDATE] Using bundle directory: %s", bundleDir)

	return &WorkerUpdateHandler{
		cfg:         cfg,
		reg:         reg,
		cmdMgr:      cmdMgr,
		dataDir:     dataDir,
		bundleDir:   bundleDir,
		codeVersion: cfg.Workers.CodeVersion,
	}
}

// CommandManager returns the command manager used to push commands to workers.
func (h *WorkerUpdateHandler) CommandManager() *workersreg.CommandManager {
	return h.cmdMgr
}

// Config returns the runtime config used by worker update handlers.
func (h *WorkerUpdateHandler) Config() *config.Config {
	return h.cfg
}
