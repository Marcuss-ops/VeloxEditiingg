package workers

// WorkerCapacity is the canonical scheduling capacity projection.
//
// MaxSlots is the worker-declared parallelism limit. ActiveSlots is counted
// exclusively from non-expired task leases in the master store. AvailableSlots
// is always derived as max(MaxSlots-ActiveSlots, 0). Heartbeat active_tasks and
// task_slots are telemetry/input compatibility fields, never authoritative
// occupancy once the registry has hydrated this projection from the lease store.
//
// Per-phase slots (RenderSlots, PrefetchSlots, PublisherSlots) are computed
// by the CapacityScorecard from live resource metrics and per-job cost
// estimates. They replace the flat MaxSlots limit for placement when
// available. When zero, the flat MaxSlots is the fallback.
type WorkerCapacity struct {
	MaxSlots       int `json:"max_slots"`
	ActiveSlots    int `json:"active_slots"`
	AvailableSlots int `json:"available_slots"`

	// Per-phase slots from the capacity scorecard. Zero means "not yet
	// computed" — callers fall back to the flat MaxSlots limit.
	RenderSlots      int `json:"render_slots"`
	PrefetchSlots    int `json:"prefetch_slots"`
	PublisherSlots   int `json:"publisher_slots"`
	ActiveRender     int `json:"active_render"`     // current render-occupying leases
	ActivePrefetch   int `json:"active_prefetch"`   // current prefetch-occupying leases
	ActivePublisher  int `json:"active_publisher"`  // current publisher-occupying leases
	LimitingResource string `json:"limiting_resource,omitempty"`

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

// deriveWorkerCapacityWithPhaseScores derives a capacity projection using the
// scorecard-recommended per-phase slots. flatMaxSlots remains as the fallback
// ceiling; per-phase active counts are derived from phase-aware lease queries.
func deriveWorkerCapacityWithPhaseScores(
	flatMaxSlots int,
	activeSlots int,
	renderSlots, activeRender int,
	prefetchSlots, activePrefetch int,
	publisherSlots, activePublisher int,
	limitingResource string,
) WorkerCapacity {
	cap := deriveWorkerCapacityWithAuthority(flatMaxSlots, activeSlots, true)
	cap.RenderSlots = renderSlots
	cap.PrefetchSlots = prefetchSlots
	cap.PublisherSlots = publisherSlots
	cap.ActiveRender = activeRender
	cap.ActivePrefetch = activePrefetch
	cap.ActivePublisher = activePublisher
	cap.LimitingResource = limitingResource
	return cap
}

// AvailableRenderSlots returns how many additional render tasks this worker
// can accept. Uses per-phase limits when the scorecard has computed them;
// falls back to flat AvailableSlots otherwise.
func (wc WorkerCapacity) AvailableRenderSlots() int {
	if wc.RenderSlots > 0 {
		a := wc.RenderSlots - wc.ActiveRender
		if a < 0 {
			a = 0
		}
		return a
	}
	return wc.AvailableSlots
}

// AvailablePrefetchSlots returns how many additional prefetch tasks this
// worker can accept.
func (wc WorkerCapacity) AvailablePrefetchSlots() int {
	if wc.PrefetchSlots > 0 {
		a := wc.PrefetchSlots - wc.ActivePrefetch
		if a < 0 {
			a = 0
		}
		return a
	}
	return wc.AvailableSlots
}

// AvailablePublisherSlots returns how many additional publisher tasks this
// worker can accept.
func (wc WorkerCapacity) AvailablePublisherSlots() int {
	if wc.PublisherSlots > 0 {
		a := wc.PublisherSlots - wc.ActivePublisher
		if a < 0 {
			a = 0
		}
		return a
	}
	return wc.AvailableSlots
}
