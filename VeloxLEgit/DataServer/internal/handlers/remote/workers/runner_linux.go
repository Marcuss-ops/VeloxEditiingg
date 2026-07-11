//go:build !windows

package workers

import "path/filepath"

// getBundlerPath returns the path to the velox-bundler binary.
func getBundlerPath(repoRoot string) string {
	return filepath.Join(repoRoot, "DataServer", "bin", "velox-bundler")
}
