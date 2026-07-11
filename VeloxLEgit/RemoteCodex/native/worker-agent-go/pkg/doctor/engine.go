package doctor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"velox-worker-agent/pkg/config"
)

// EngineBinaryValidator checks that the C++ video engine binary exists
// and is executable. It first tries cfg.VideoEngineCppBin as an absolute
// path, then falls back to exec.LookPath.
// RW-PROD-002 §2 item 8.
type EngineBinaryValidator struct{}

func (v *EngineBinaryValidator) ID() string { return "engine.binary" }

func (v *EngineBinaryValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	bin := trim(cfg.VideoEngineCppBin)
	if bin == "" {
		bin = "velox-render-cpp"
	}

	// Try absolute path.
	if strings.Contains(bin, "/") {
		info, err := os.Stat(bin)
		if err != nil {
			return fail("engine.binary",
				fmt.Sprintf("video engine binary not found at %s: %v", bin, err),
				"install the C++ video engine or set VELOX_VIDEO_ENGINE_CPP_BIN to the correct path")
		}
		if info.IsDir() {
			return fail("engine.binary",
				fmt.Sprintf("%s is a directory, not a binary", bin),
				"point to the executable binary, not a directory")
		}
		if info.Mode()&0111 == 0 {
			return fail("engine.binary",
				fmt.Sprintf("%s exists but is not executable", bin),
				"chmod +x the binary or fix permissions")
		}
		return pass("engine.binary", fmt.Sprintf("binary exists and executable at %s", bin))
	}

	// Fallback: look up in PATH.
	path, err := exec.LookPath(bin)
	if err != nil {
		return fail("engine.binary",
			fmt.Sprintf("video engine binary %q not found in PATH: %v", bin, err),
			"install velox-render-cpp or set VELOX_VIDEO_ENGINE_CPP_BIN to the absolute path")
	}
	return pass("engine.binary", fmt.Sprintf("found at %s", path))
}
