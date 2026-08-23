package collectors

import (
	"os"
	"path/filepath"
	"strings"
)

// GPUProbe is the host GPU capability boundary used by SampleHost and the
// root telemetry facade. Detection is deliberately best-effort and reports
// only visibility of a supported GPU device/vendor.
type GPUProbe struct{}

// DetectGPU reports whether a supported GPU is visible to the worker.
func (GPUProbe) DetectGPU() bool {
	return detectGPU()
}

// detectGPU returns true if any supported GPU is visible to the worker.
// Device probes are checked before the DRM vendor walk because they are
// cheaper and cover the production NVIDIA/CUDA path.
// detectGPU preserves the sampler-local test seam while keeping the
// implementation in the dedicated GPU collector.
func (s *Sampler) detectGPU() bool {
	return detectGPU()
}

func detectGPU() bool {
	for _, path := range []string{"/dev/nvidia0", "/dev/nvidiactl", "/dev/dri/renderD128"} {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}

	matches, err := filepath.Glob("/sys/class/drm/card*/device/vendor")
	if err != nil {
		return false
	}
	for _, path := range matches {
		data, err := readFile(path)
		if err != nil {
			continue
		}
		switch strings.TrimSpace(string(data)) {
		case "0x10de", "0x1002", "0x8086":
			return true
		}
	}
	return false
}
