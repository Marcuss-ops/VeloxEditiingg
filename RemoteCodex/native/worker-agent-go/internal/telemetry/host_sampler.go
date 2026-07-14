// Package telemetry / host_sampler.go
//
// Host-level sampling split out of resource_sampler.go as part of the
// per-domain refactor (cpu / memory / disk / network / process / host
// + resource_sampler.go facade). The host envelope (SampledHost) is
// the boot-time / one-shot layer used by api.HostInfo and the
// capability markers. The sampler refreshes it on a slow cadence
// (1 minute) so HasGPU / RAMBytes / DiskFreeBytes reflect current
// reality (e.g. nvidia module loaded mid-flight).
//
// GPU detection is a two-step probe: cheap /dev/nvidia* + /dev/dri/*
// file existence, then a /sys/class/drm walk to surface NVIDIA/AMD/
// Intel vendor IDs.
package telemetry

import (
	"os"
	"path/filepath"
	"strings"
)

// SampledHost is the boot-time / one-shot host layer used by
// api.HostInfo (future markers at worker.go:177-183).
// SampledHost is cheap to recompute; the worker stores it on *Worker
// and refreshes it on a slow cadence (1 minute) so HasGPU / RAMBytes
// DiskFreeBytes reflect current reality (e.g. nvidia module loaded
// mid-flight).
type SampledHost struct {
	RAMBytes      int64
	DiskFreeBytes int64
	HasGPU        bool
}

// SampleHost reads the one-shot boot-time host layer. Includes HasGPU
// detection (cheap /dev/nvidia* + DRM walk), MemTotal from
// /proc/meminfo, and statvfs free bytes for the work directory.
func (s *Sampler) SampleHost() (*SampledHost, error) {
	out := &SampledHost{}

	mem, err := s.readProcMeminfo()
	if err == nil {
		out.RAMBytes = mem.total
	}

	free, err := s.statvfsFreeBytes()
	if err == nil {
		out.DiskFreeBytes = free
	}

	out.HasGPU = s.detectGPU()

	return out, nil
}

// detectGPU returns true if there's any GPU visible to the worker.
// Strategy (ordered cheapest-first):
//  1. /dev/nvidia0 → /dev/nvidiaN existence (NVIDIA CUDA).
//  2. /sys/class/drm/card*/device/vendor → known GPU vendors.
//  3. Else false.
func (s *Sampler) detectGPU() bool {
	// 1. /dev/nvidia* existence.
	for _, p := range []string{"/dev/nvidia0", "/dev/nvidiactl", "/dev/dri/renderD128"} {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	// 2. /sys/class/drm/card* with GPU-style vendor IDs.
	matches, err := filepath.Glob("/sys/class/drm/card*/device/vendor")
	if err != nil {
		return false
	}
	for _, m := range matches {
		data, rerr := readFile(m)
		if rerr != nil {
			continue
		}
		// Format: "0x10de\n"
		vendor := strings.TrimSpace(string(data))
		switch vendor {
		case "0x10de": // NVIDIA
			return true
		case "0x1002": // AMD/ATI
			return true
		case "0x8086": // Intel (only certain SKUs are real GPUs)
			return true
		}
	}
	return false
}
