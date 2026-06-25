package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"velox-worker-agent/pkg/config"
)

// DirsValidator checks that all working directories are writable:
// WorkDir, OutputDir, TempDir, and the cache + blob roots env vars.
// It performs mkdir + write + remove on each directory.
// RW-PROD-002 §2 item 5.
type DirsValidator struct{}

func (v *DirsValidator) ID() string { return "dirs" }

func (v *DirsValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	dirs := map[string]string{
		"work_dir":   cfg.WorkDir,
		"output_dir": cfg.OutputDir,
		"temp_dir":   cfg.TempDir,
	}

	// Add optional directories from env vars.
	if cd := trim(os.Getenv("VELOX_WORKER_CACHE_DIR")); cd != "" {
		dirs["cache_dir"] = cd
	} else {
		dirs["cache_dir"] = "/opt/velox/cache"
	}
	if bd := trim(os.Getenv("VELOX_WORKER_BLOB_DIR")); bd != "" {
		dirs["blob_dir"] = bd
	} else {
		dirs["blob_dir"] = "/opt/velox/blobs"
	}

	var failures []string
	for name, path := range dirs {
		if path == "" {
			failures = append(failures, fmt.Sprintf("%s: empty path", name))
			continue
		}
		// Ensure directory exists.
		if err := os.MkdirAll(path, 0755); err != nil {
			failures = append(failures, fmt.Sprintf("%s (%s): cannot create: %v", name, path, err))
			continue
		}
		// Write test file.
		testFile := filepath.Join(path, ".doctor_write_test")
		if err := os.WriteFile(testFile, []byte("ok"), 0644); err != nil {
			failures = append(failures, fmt.Sprintf("%s (%s): cannot write: %v", name, path, err))
			continue
		}
		// Remove test file.
		if err := os.Remove(testFile); err != nil {
			failures = append(failures, fmt.Sprintf("%s (%s): cannot remove test file: %v", name, path, err))
			continue
		}
	}

	if len(failures) > 0 {
		detail := ""
		for i, f := range failures {
			if i > 0 {
				detail += "; "
			}
			detail += f
		}
		return fail("dirs", detail, "ensure all directories exist and are writable by the worker process")
	}

	dirList := make([]string, 0, len(dirs))
	for name, path := range dirs {
		dirList = append(dirList, fmt.Sprintf("%s=%s", name, path))
	}
	return pass("dirs", fmt.Sprintf("all %d directories writable (%v)", len(dirs), dirList))
}
