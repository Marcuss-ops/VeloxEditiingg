package workers

// WorkerCapacity is the canonical scheduling capacity projection.
//
// MaxSlots is the worker-declared parallelism limit. ActiveSlots is counted
// exclusively from non-expired task leases in the master store. AvailableSlots
// is always derived as max(MaxSlots-ActiveSlots, 0). Heartbeat active_tasks and
// task_slots are telemetry/input compatibility fields, never authoritative
// occupancy once the registry has hydrated this projection from the lease store.
type WorkerCapacity struct {
	MaxSlots       int `json:"max_slots"`
	ActiveSlots    int `json:"active_slots"`
	AvailableSlots int `json:"available_slots"`

	// Authoritative is internal read-path metadata. It is false only for
	// compatibility fixtures or an in-memory registry without a lease store;
	// production registries backed by SQLite set it true on hydration.
	Authoritative bool `json:"-"`
}

func deriveWorkerCapacity(maxSlots, activeSlots int) WorkerCapacity {
	return deriveWorkerCapacityWithAuthority(maxSlots, activeSlots, true)
}

func deriveWorkerCapacityWithAuthority(maxSlots, activeSlots int, authoritative bool) WorkerCapacity {
	if maxSlots < 0 {
		maxSlots = 0
	}
	if activeSlots < 0 {
		activeSlots = 0
	}
	available := maxSlots - activeSlots
	if available < 0 {
		available = 0
	}
	if !authoritative {
		// Keep the declared limit visible for diagnostics, but never
		// advertise capacity while the lease-store read is unavailable.
		available = 0
	}
	return WorkerCapacity{
		MaxSlots:       maxSlots,
		ActiveSlots:    activeSlots,
		AvailableSlots: available,
		Authoritative:  authoritative,
	}
}

func declaredWorkerSlotsFromCapabilities(raw interface{}) int {
	caps, ok := raw.(map[string]interface{})
	if !ok || caps == nil {
		return 0
	}
	host, ok := caps["host"].(map[string]interface{})
	if !ok || host == nil {
		return 0
	}
	return intValue(host["max_parallel_jobs"])
}

func intValue(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float32:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
