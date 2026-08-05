package costmodel

import "strings"

// WorkerProfile is the master-side projection of a worker's
// capability state. Transient fields (IsDraining, IsOffline,
// ActiveJobs, MaxParallel) come from the heartbeat and the registry;
// the four canonical fields (ResourceClass, TemporalMode,
// Deterministic, Cacheable, plus SupportsAlpha) come from the
// executor registry surfaced through Hello/Heartbeat capabilities.
//
// Built by BuildWorkerProfile — callers should compose this struct
// once per heartbeat-notify boundary and pass the resulting value
// to Score in the eligibility layer. The struct is intentionally
// value-type so it can travel through scoring without heap escape.
type WorkerProfile struct {
	WorkerID string

	// Executor-registry surface derived from the `executors` array inside
	// the capabilities map. Legacy heartbeats without executors fall back
	// to cpu/frame_local/false/false.
	ResourceClass ResourceClass
	TemporalMode  TemporalMode
	Deterministic bool
	Cacheable     bool
	SupportsAlpha bool

	// LinkBandwidthMbps: Reported by the worker through
	// capabilities["link_bandwidth_mbps"] (per executor or root);
	// merges most-permissive (max) across the executors array. 0
	// means the worker has not yet published the field (legacy) and
	// Score.BandwidthFit treats it as "unknown" = pass-through so
	// legacy workers are not penalized by the new component.
	LinkBandwidthMbps float64

	// Transient state, sourced from heartbeat / registry.
	IsDraining  bool
	IsOffline   bool
	ActiveJobs  int
	MaxParallel int
	Resources   ResourceSnapshot
	Pressure    PressureState
}

// BuildWorkerProfile maps a master-side schedulability state + a
// capabilities map into a WorkerProfile ready for Score.
//
// Legacy fall-through: when capabilities map has no
// `executors` entry (CapabilityReport schema <2 OR empty array), the
// function synthesizes {cpu, frame_local, false, false}. This
// preserves existing queue routing for legacy workers.
func BuildWorkerProfile(
	workerID string,
	schedulable bool,
	drain bool,
	isOffline bool,
	activeJobs int,
	maxParallel int,
	caps map[string]interface{},
) WorkerProfile {
	w := WorkerProfile{
		WorkerID:    workerID,
		IsDraining:  drain || !schedulable,
		IsOffline:   isOffline,
		ActiveJobs:  activeJobs,
		MaxParallel: maxParallel,
	}
	mergeExecutorsInto(&w, caps)

	if w.ResourceClass == "" {
		w.ResourceClass = ResourceCPU
	}
	if w.TemporalMode == "" {
		w.TemporalMode = TemporalFrameLocal
	}
	return w
}

// ResourceSnapshotFromMaps translates the worker heartbeat maps into the
// canonical scheduling snapshot. Both metric locations are accepted because
// older workers publish resources at the root while newer workers nest them.
func ResourceSnapshotFromMaps(capabilities, metrics map[string]interface{}) ResourceSnapshot {
	r := ResourceSnapshot{}
	read := func(name string) interface{} {
		if metrics != nil {
			if v, ok := metrics[name]; ok {
				return v
			}
		}
		if capabilities != nil {
			if v, ok := capabilities[name]; ok {
				return v
			}
			if resources, ok := capabilities["resources"].(map[string]interface{}); ok {
				if v, ok := resources[name]; ok {
					return v
				}
			}
		}
		return nil
	}
	r.CPUCores = intValue(read("cpu_cores"), intValue(read("num_cpu"), 0))
	r.CPUThreadsInUse = intValue(read("cpu_threads_in_use"), 0)
	r.MemoryBytes = int64Value(read("memory_total_bytes"), int64Value(read("memory_bytes"), 0))
	r.MemoryUsedBytes = int64Value(read("memory_used_bytes"), 0)
	r.DiskFreeBytes = int64Value(read("disk_free_bytes"), 0)
	r.TempReservedBytes = int64Value(read("temp_reserved_bytes"), 0)
	r.ActiveTasks = intValue(read("active_tasks"), 0)
	r.TaskSlots = intValue(read("task_slots"), 0)
	r.SwapUsedBytes = int64Value(read("swap_used_bytes"), 0)
	r.IOWaitRatio = floatValue(read("cpu_iowait_ratio"), floatValue(read("iowait_ratio"), 0))
	return r
}

func intValue(v interface{}, fallback int) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	}
	return fallback
}
func int64Value(v interface{}, fallback int64) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	}
	return fallback
}
func floatValue(v interface{}, fallback float64) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return fallback
}

// DerivePressure applies the canonical admission policy to a worker snapshot.
func DerivePressure(r ResourceSnapshot, p AdmissionPolicy) PressureState {
	freeMemory := r.MemoryBytes - r.MemoryUsedBytes
	freeDisk := r.DiskFreeBytes - r.TempReservedBytes
	return PressureState{
		Memory: r.MemoryBytes > 0 && freeMemory < p.MinFreeMemoryBytes,
		Disk:   r.DiskFreeBytes > 0 && freeDisk < p.MinFreeDiskBytes,
		Swap:   r.SwapUsedBytes > 0 && !p.AllowSwap,
		IOWait: p.MaxIOWaitRatio > 0 && r.IOWaitRatio > p.MaxIOWaitRatio,
	}
}

// mergeExecutorsInto applies the per-executor merge policy. Policy
// produces a single aggregate WorkerProfile from N executors:
//
//   - ResourceClass: most-permissive wins (gpu > mixed > cpu/io).
//   - TemporalMode:  most-permissive wins (global > windowed >
//     frame_local > stateful).
//   - Deterministic: AND across executors (strict — if any executor
//     reports non-deterministic, advertise false).
//   - Cacheable:     OR across executors (liberal — any executor
//     cacheable ⇒ advertise true).
//   - SupportsAlpha: OR across executors.
func mergeExecutorsInto(w *WorkerProfile, caps map[string]interface{}) {
	if caps == nil {
		return
	}
	raw, ok := caps["executors"].([]interface{})
	if !ok || len(raw) == 0 {
		return
	}
	seenDeterministic := false
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if rc := strings.TrimSpace(stringOf(m["resource_class"])); rc != "" {
			w.ResourceClass = mergeResourceClass(w.ResourceClass, ResourceClass(rc))
		}
		if tm := strings.TrimSpace(stringOf(m["temporal_mode"])); tm != "" {
			w.TemporalMode = mergeTemporalMode(w.TemporalMode, TemporalMode(tm))
		}
		if d, ok := m["deterministic"].(bool); ok {
			if !seenDeterministic {
				w.Deterministic = d
				seenDeterministic = true
			} else {
				w.Deterministic = w.Deterministic && d
			}
		}
		if c, ok := m["cacheable"].(bool); ok && c {
			w.Cacheable = true
		}
		if a, ok := m["supports_alpha"].(bool); ok && a {
			w.SupportsAlpha = true
		}
		// Per-executor link_bandwidth_mbps (Mbps). Merge
		// policy mirrors ResourceClass / TemporalMode: most-
		// permissive wins (max across executors). A worker that does
		// not publish the field on any executor keeps
		// LinkBandwidthMbps == 0 ("unknown"); Score treats it as
		// pass-through so today's routing is preserved.
		if bw, ok := m["link_bandwidth_mbps"].(float64); ok && bw > 0 {
			if bw > w.LinkBandwidthMbps {
				w.LinkBandwidthMbps = bw
			}
		}
	}
}

// mergeResourceClass: most-permissive wins.
func mergeResourceClass(current, candidate ResourceClass) ResourceClass {
	rank := func(r ResourceClass) int {
		switch r {
		case ResourceGPU:
			return 3
		case ResourceMixed:
			return 2
		case ResourceCPU, ResourceIO:
			return 1
		default:
			return 0
		}
	}
	if rank(candidate) > rank(current) {
		return candidate
	}
	return current
}

// mergeTemporalMode: most-permissive wins.
func mergeTemporalMode(current, candidate TemporalMode) TemporalMode {
	rank := func(t TemporalMode) int {
		switch t {
		case TemporalGlobal:
			return 4
		case TemporalWindowed:
			return 3
		case TemporalFrameLocal:
			return 2
		case TemporalStateful:
			return 1
		default:
			return 0
		}
	}
	if rank(candidate) > rank(current) {
		return candidate
	}
	return current
}

func stringOf(v interface{}) string {
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}
