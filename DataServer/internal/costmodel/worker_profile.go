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

	// Executor-registry surface (PR-3.5/PR-4): derived from the
	// `executors` array inside the capabilities map. Defaults
	// (cpu / frame_local / false / false) are applied by
	// BuildWorkerProfile for legacy heartbeats that pre-date the
	// CapabilityReport schema (PR-3.5+).
	ResourceClass ResourceClass
	TemporalMode  TemporalMode
	Deterministic bool
	Cacheable     bool
	SupportsAlpha bool

	// LinkBandwidthMbps: PR-04.6. Reported by the worker through
	// capabilities["link_bandwidth_mbps"] (per executor or root);
	// merges most-permissive (max) across the executors array. 0
	// means the worker has not yet published the field (legacy) and
	// Score.BandwidthFit treats it as "unknown" = pass-through so
	// pre-PR-04.6 workers are not penalized by the new component.
	LinkBandwidthMbps float64

	// Transient state, sourced from heartbeat / registry.
	IsDraining  bool
	IsOffline   bool
	ActiveJobs  int
	MaxParallel int
}

// BuildWorkerProfile maps a master-side schedulability state + a
// capabilities map into a WorkerProfile ready for Score.
//
// Legacy fall-through (PR-04.4 §4.4): when capabilities map has no
// `executors` entry (CapabilityReport schema <2 OR empty array), the
// function synthesizes {cpu, frame_local, false, false}. This
// preserves existing queue routing for pre-PR-04 workers.
func BuildWorkerProfile(
	workerID string,
	schedulable bool,
	drain bool,
	status string,
	activeJobs int,
	maxParallel int,
	caps map[string]interface{},
) WorkerProfile {
	w := WorkerProfile{
		WorkerID:    workerID,
		IsDraining:  drain || !schedulable,
		IsOffline:   strings.EqualFold(strings.TrimSpace(status), "offline"),
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
		// PR-04.6: per-executor link_bandwidth_mbps (Mbps). Merge
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
