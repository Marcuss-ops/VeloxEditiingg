package telemetry

// collectors_facade.go — compatibility facade for the sampler family.
//
// Target structure (plan #17): the /proc + /sys sampler implementation
// lives in internal/telemetry/collectors (package collectors). This root
// package keeps re-exporting every public sampler symbol so existing
// callers (attempt session, worker lifecycle, taskrunner, cmd) compile
// unchanged — the facade is the public contract, the subpackage is the
// implementation.
//
// Dependency rule: collectors never imports the root package (the sampler
// is stdlib-only); the facade is the only edge from root → collectors.

import (
	"time"

	"velox-worker-agent/internal/telemetry/collectors"
)

// Type aliases preserve the full method set and exported fields.
type (
	Sampler          = collectors.Sampler
	SampledResources = collectors.SampledResources
	SampledHost      = collectors.SampledHost
	CPUCapacity      = collectors.CPUCapacity
	DiskGC           = collectors.DiskGC
	GPUProbe         = collectors.GPUProbe
)

// Constructor/function wrappers keep the root package's public API
// source-compatible with the pre-split telemetry package.

// NewResourceSampler is the canonical sampler constructor. See
// collectors.NewResourceSampler for the semantics of the /proc, /sys and
// workDir roots and the tick/emitEvery cadence.
func NewResourceSampler(procRoot, sysRoot, workDir string, tick time.Duration, emitEvery int) *Sampler {
	return collectors.NewResourceSampler(procRoot, sysRoot, workDir, tick, emitEvery)
}

// DetectCPUCapacity reports the worker's CPU quota/count from cgroup v2
// (fallback v1, then logical cores). See collectors.DetectCPUCapacity.
func DetectCPUCapacity() CPUCapacity { return collectors.DetectCPUCapacity() }

// DiskFreeAt returns free bytes at path via statvfs. See
// collectors.DiskFreeAt.
func DiskFreeAt(path string) (int64, error) { return collectors.DiskFreeAt(path) }

// NewDiskGC constructs the scratch-tree garbage collector. See
// collectors.NewDiskGC.
func NewDiskGC(workDir string) *DiskGC { return collectors.NewDiskGC(workDir) }

// DetectGPU reports whether a supported GPU is visible to the worker. The
// implementation lives in the GPU collector; this wrapper preserves the
// root telemetry facade as the stable caller boundary.
func DetectGPU() bool { return collectors.GPUProbe{}.DetectGPU() }
