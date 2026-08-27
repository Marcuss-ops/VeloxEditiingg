// Package worker — ResourceAdmissionController provides RSS-based admission
// control for the three resource categories: RENDER, PREFETCH, and PUBLISH.
//
// The controller samples the process RSS (from /proc/self/statm on Linux)
// and applies a tiered throttling policy with hysteresis recovery:
//
//	RSS > 80% of total RAM → no new PREFETCH admitted
//	RSS > 88% of total RAM → reduce PUBLISH concurrency (backpressure signal)
//	RSS > 93% of total RAM → no new RENDER admitted (never interrupt a running render)
//
// Recovery thresholds are lower than throttle thresholds to prevent
// oscillation (hysteresis):
//
//	80% throttle → recovers at 70%
//	88% throttle → recovers at 78%
//	93% throttle → recovers at 83%
//
// The controller is thread-safe and designed for concurrent admission
// checks from multiple goroutines (render dispatcher, prefetch scheduler,
// publisher pool).
package worker

import (
	"sync"
	"sync/atomic"

	"velox-worker-agent/internal/telemetry"
)

// ResourceKind identifies the three admission-controlled resource categories.
type ResourceKind int

const (
	// ResourceRender is the video render pipeline (ffmpeg / native engine).
	// Highest priority: once a render starts, it must never be preempted.
	ResourceRender ResourceKind = iota

	// ResourcePrefetch is the FutureAssetPlan download pipeline.
	// Lowest priority: can be fully blocked under memory pressure.
	ResourcePrefetch

	// ResourcePublish is the artifact upload pipeline.
	// Medium priority: concurrency can be reduced under pressure.
	ResourcePublish
)

// String returns a human-readable label for the resource kind.
func (rk ResourceKind) String() string {
	switch rk {
	case ResourceRender:
		return "RENDER"
	case ResourcePrefetch:
		return "PREFETCH"
	case ResourcePublish:
		return "PUBLISH"
	default:
		return "UNKNOWN"
	}
}

// ResourceClaim describes the resource requirements for a single admission
// request. Only RAMBytes is used for RSS-based throttling; DiskBytes is
// reserved for future disk-pressure admission.
type ResourceClaim struct {
	Kind      ResourceKind
	RAMBytes  int64
	DiskBytes int64
}

// AdmissionDecision is the result of an admission check.
type AdmissionDecision int

const (
	// Admit means the resource claim is approved.
	Admit AdmissionDecision = iota

	// RejectMemory means admission is denied due to high RSS.
	RejectMemory

	// RejectStopped means the controller has been stopped.
	RejectStopped
)

// String returns a human-readable label for the admission decision.
func (d AdmissionDecision) String() string {
	switch d {
	case Admit:
		return "ADMIT"
	case RejectMemory:
		return "REJECT_MEMORY"
	case RejectStopped:
		return "REJECT_STOPPED"
	default:
		return "UNKNOWN"
	}
}

// RSSSampler is a function that returns the current process RSS in bytes.
// On Linux this reads /proc/self/statm; tests inject a deterministic value.
type RSSSampler func() int64

// TotalRAMBytesFunc is a function that returns the total physical RAM in bytes.
// On Linux this reads /proc/meminfo; tests inject a deterministic value.
type TotalRAMBytesFunc func() int64

// Throttle thresholds (percentage of total RAM).
const (
	throttleRenderPct   = 93 // no new renders above this
	throttlePrefetchPct = 80 // no new prefetch above this
	throttlePublishPct  = 88 // reduce publisher concurrency above this

	// Hysteresis recovery thresholds — must be below the throttle thresholds
	// to prevent oscillation.
	recoverRenderPct   = 83
	recoverPrefetchPct = 70
	recoverPublishPct  = 78
)

// ResourceAdmissionController evaluates admission decisions based on
// the process RSS relative to total physical RAM.
type ResourceAdmissionController struct {
	sampleRSS     RSSSampler
	totalRAM      TotalRAMBytesFunc
	isStopped     atomic.Bool
	stopCh        chan struct{}

	// Hysteresis state: once a category is throttled, it stays throttled
	// until the RSS drops below the recovery threshold.
	renderThrottled   atomic.Bool
	prefetchThrottled atomic.Bool
	publishThrottled  atomic.Bool

	// Metrics (atomic counters).
	peakRSSBytes          atomic.Int64
	admissionRejections   atomic.Int64
	backpressureEvents    atomic.Int64

	// mu protects the throttle state transitions (read-modify-write on
	// the atomic bools happens under this to prevent double-counting
	// backpressure events).
	mu sync.Mutex
}

// NewResourceAdmissionController creates a new controller with the given
// RSS and total-RAM samplers. Both must be non-nil.
func NewResourceAdmissionController(sampler RSSSampler, totalRAM TotalRAMBytesFunc) *ResourceAdmissionController {
	return &ResourceAdmissionController{
		sampleRSS: sampler,
		totalRAM:  totalRAM,
		stopCh:    make(chan struct{}),
	}
}

// CanAdmit checks whether a resource claim can be admitted given the
// current RSS pressure. It returns Admit or RejectMemory.
//
// The check is side-effect-free: it does NOT mutate throttle state.
// Call RecordAdmissionResult after the operation completes to update
// hysteresis state.
func (rac *ResourceAdmissionController) CanAdmit(claim ResourceClaim) AdmissionDecision {
	if rac.isStopped.Load() {
		return RejectStopped
	}

	rss := rac.sampleRSS()
	total := rac.totalRAM()
	if total <= 0 {
		return Admit // Unknown total — fail open.
	}

	peak := rac.peakRSSBytes.Load()
	if rss > peak {
		rac.peakRSSBytes.Store(rss)
	}

	pct := float64(rss) / float64(total) * 100

	// Check hysteresis state first: once throttled, admission is blocked
	// until RSS drops below the recovery threshold (checked in
	// RecordAdmissionResult). This prevents oscillation between admit/
	// reject when RSS hovers near the threshold.
	switch claim.Kind {
	case ResourceRender:
		if rac.renderThrottled.Load() || pct >= throttleRenderPct {
			return RejectMemory
		}
	case ResourcePrefetch:
		if rac.prefetchThrottled.Load() || pct >= throttlePrefetchPct {
			return RejectMemory
		}
	case ResourcePublish:
		if rac.publishThrottled.Load() || pct >= throttlePublishPct {
			return RejectMemory
		}
	}

	return Admit
}

// RecordAdmissionResult updates hysteresis state after an admission
// decision and operation outcome. Call this when an operation finishes
// (success or failure) so the throttle state can recover.
//
// For backpressure: when the RSS exceeds the throttle threshold, the
// caller should reduce concurrency for PUBLISH or skip PREFETCH. This
// method emits the backpressure event metric exactly once per threshold
// crossing.
func (rac *ResourceAdmissionController) RecordAdmissionResult(claim ResourceClaim, admitted bool) {
	if !admitted {
		rac.admissionRejections.Add(1)
	}

	rss := rac.sampleRSS()
	total := rac.totalRAM()
	if total <= 0 {
		return
	}

	pct := float64(rss) / float64(total) * 100

	rac.mu.Lock()
	defer rac.mu.Unlock()

	switch claim.Kind {
	case ResourceRender:
		wasThrottled := rac.renderThrottled.Load()
		if pct >= throttleRenderPct {
			if !wasThrottled {
				rac.renderThrottled.Store(true)
				rac.backpressureEvents.Add(1)
				telemetry.GetPrometheusMetrics().RecordBackpressureEvent("render")
			}
		} else if pct <= recoverRenderPct && wasThrottled {
			rac.renderThrottled.Store(false)
		}
	case ResourcePrefetch:
		wasThrottled := rac.prefetchThrottled.Load()
		if pct >= throttlePrefetchPct {
			if !wasThrottled {
				rac.prefetchThrottled.Store(true)
				rac.backpressureEvents.Add(1)
				telemetry.GetPrometheusMetrics().RecordBackpressureEvent("prefetch")
			}
		} else if pct <= recoverPrefetchPct && wasThrottled {
			rac.prefetchThrottled.Store(false)
		}
	case ResourcePublish:
		wasThrottled := rac.publishThrottled.Load()
		if pct >= throttlePublishPct {
			if !wasThrottled {
				rac.publishThrottled.Store(true)
				rac.backpressureEvents.Add(1)
				telemetry.GetPrometheusMetrics().RecordBackpressureEvent("publish")
			}
		} else if pct <= recoverPublishPct && wasThrottled {
			rac.publishThrottled.Store(false)
		}
	}
}

// IsThrottled returns whether the given resource kind is currently
// throttled by the hysteresis state. Useful for diagnostic reporting
// and for the publisher pool to decide its effective concurrency.
func (rac *ResourceAdmissionController) IsThrottled(kind ResourceKind) bool {
	switch kind {
	case ResourceRender:
		return rac.renderThrottled.Load()
	case ResourcePrefetch:
		return rac.prefetchThrottled.Load()
	case ResourcePublish:
		return rac.publishThrottled.Load()
	default:
		return false
	}
}

// PeakRSSBytes returns the high-water mark of process RSS observed
// by the controller since construction or the last ResetPeakRSS call.
func (rac *ResourceAdmissionController) PeakRSSBytes() int64 {
	return rac.peakRSSBytes.Load()
}

// AdmissionRejections returns the total number of rejected admission
// requests since construction.
func (rac *ResourceAdmissionController) AdmissionRejections() int64 {
	return rac.admissionRejections.Load()
}

// BackpressureEvents returns the total number of hysteresis state
// transitions (throttle activations) since construction.
func (rac *ResourceAdmissionController) BackpressureEvents() int64 {
	return rac.backpressureEvents.Load()
}

// CurrentRSSBytes returns the latest sampled RSS. Useful for heartbeat
// payloads and diagnostic endpoints.
func (rac *ResourceAdmissionController) CurrentRSSBytes() int64 {
	return rac.sampleRSS()
}

// RSSPressurePercent returns the current RSS as a percentage of total RAM.
// Returns -1 if total RAM is unknown.
func (rac *ResourceAdmissionController) RSSPressurePercent() float64 {
	total := rac.totalRAM()
	if total <= 0 {
		return -1
	}
	return float64(rac.sampleRSS()) / float64(total) * 100
}

// Stop marks the controller as stopped. All subsequent CanAdmit calls
// return RejectStopped.
func (rac *ResourceAdmissionController) Stop() {
	rac.isStopped.Store(true)
	close(rac.stopCh)
}

// StopCh returns a channel that is closed when Stop is called. Selectable
// from goroutines that need to bail out.
func (rac *ResourceAdmissionController) StopCh() <-chan struct{} {
	return rac.stopCh
}
