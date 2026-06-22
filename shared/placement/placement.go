// Package placement defines the stable placement contracts shared between
// master (DataServer) and worker (RemoteCodex). PR #7 moves these out of the
// duplicated worker-side costmodel so both modules can import from a single
// source of truth.
//
// The master remains the sole owner of placement decisions (Score formula,
// eligibility gates). This package carries ONLY the type contracts — no
// scoring logic, no cost computation, no WorkerProfile.
package placement

// ResourceClass identifies the dominant kind of resource a task type uses.
type ResourceClass string

const (
	ResourceCPU   ResourceClass = "cpu"
	ResourceGPU   ResourceClass = "gpu"
	ResourceMixed ResourceClass = "mixed"
	ResourceIO    ResourceClass = "io"
)

// Valid returns true for known resource classes.
func (r ResourceClass) Valid() bool {
	switch r {
	case ResourceCPU, ResourceGPU, ResourceMixed, ResourceIO:
		return true
	}
	return false
}

// TemporalMode describes how an executor moves through time.
type TemporalMode string

const (
	TemporalFrameLocal TemporalMode = "frame_local"
	TemporalWindowed   TemporalMode = "windowed"
	TemporalStateful   TemporalMode = "stateful"
	TemporalGlobal     TemporalMode = "global"
)

// Valid returns true for known temporal modes.
func (t TemporalMode) Valid() bool {
	switch t {
	case TemporalFrameLocal, TemporalWindowed, TemporalStateful, TemporalGlobal:
		return true
	}
	return false
}

// TaskRequirements describes what a task needs from a worker. Empty fields
// mean "no constraint" — the master-side eligibility layer treats absent
// requirements as permissive pass-through.
//
// This is the stable contract shared between master and worker. The master
// owns placement decisions; the worker may read this for self-assessment
// or capability reporting.
type TaskRequirements struct {
	ResourceClass    ResourceClass
	TemporalMode     TemporalMode
	Deterministic    bool
	Cacheable        bool
	MinBandwidthMbps float64
}
