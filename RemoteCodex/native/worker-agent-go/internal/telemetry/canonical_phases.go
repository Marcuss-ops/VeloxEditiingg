// canonical_phases.go — Scorecard v1 canonical phase model.
//
// PR-3.3 ships a 5-phase runner; Scorecard v1 (F3+) collapses the 5
// legacy phases into 12 canonical, finer-grained phases that survive
// independently on the wire and in Prometheus graphs:
//
//   queue, asset_wait, cache_lookup, download,
//   decode, compile, simulate, render, composite, encode,
//   upload, finalize.
//
// The TaskRunner today emits only the 5 legacy boundary phases
// (cache_lookup, prefetch, execute, upload, report). The 5→12 mapping
// lives in MapLegacyPhase and is exposed so downstream consumers
// (master, prometheus, audit rollups) can rewrite legacy names onto
// canonical ones without depending on the worker's exact phase
// vocabulary.
//
// Thread-safety contract: PhaseTimer is NOT safe for concurrent
// mutation. The TaskRunner is single-goroutine per Run call; the
// PhaseTimer instance is owned by that goroutine for the run's
// lifetime. Freeze() — which zeros the mutable state and renders the
// timer immutable for inspection — MUST be called once the run is
// over before any other goroutine reads PhaseDurations().
package telemetry

import (
	"time"
)

// Canonical 12 phase names. Lower-case, snake_case, with stable
// ordering. SCORECARD-CANONICAL-PHASES-V1 — do NOT rename without
// updating Scorecard dashboards, audit rollups, and the master-side
// 5→12 mapper. String constants rather than typed enum so wire shape
// stays a plain string (already true on PhaseMarker.Name).
const (
	PhaseQueue      = "queue"
	PhaseAssetWait  = "asset_wait"
	PhaseCacheLookup = "cache_lookup"
	PhaseDownload   = "download"
	PhaseDecode     = "decode"
	PhaseCompile    = "compile"
	PhaseSimulate   = "simulate"
	PhaseRender     = "render"
	PhaseComposite  = "composite"
	PhaseEncode     = "encode"
	PhaseUpload     = "upload"
	PhaseFinalize   = "finalize"
)

// CanonicalPhaseOrder is the stable ordering for the 12 canonical
// phases. Exposed so tests, dashboards, and rollups can rely on a
// deterministic iteration order without resorting to map-print
// randomness.
var CanonicalPhaseOrder = []string{
	PhaseQueue,
	PhaseAssetWait,
	PhaseCacheLookup,
	PhaseDownload,
	PhaseDecode,
	PhaseCompile,
	PhaseSimulate,
	PhaseRender,
	PhaseComposite,
	PhaseEncode,
	PhaseUpload,
	PhaseFinalize,
}

// IsCanonical reports whether name is one of the 12 canonical phases.
// Implemented with a tiny internal set so the PhaseTimer hot path
// (StartPhase) is O(1) without a per-call slice scan.
func IsCanonical(name string) bool {
	_, ok := canonicalSet[name]
	return ok
}

// canonicalSet is the canonical-name allow-list. Initialized once at
// package init for cheap O(1) lookups.
var canonicalSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(CanonicalPhaseOrder))
	for _, p := range CanonicalPhaseOrder {
		m[p] = struct{}{}
	}
	return m
}()

// legacyToCanonical maps the 5 legacy PR-3.3 runner phase names onto
// the canonical 12-phase vocabulary. A legacy phase maps to a SMALLER
// or EQUAL SET of canonical phases; the executor depends on the
// coarse boundary the runner provides and fills sub-phase timings
// inside Executor.Execute.
//
// Mapping rationale (Scorecard v1 PR-3.3 → PR-3.6 cutover):
//   - "cache_lookup"  → cache_lookup           (1:1)
//   - "prefetch"      → asset_wait + cache_lookup + download
//                        (prefetch covers asset pre-stage + cache hit
//                        + the actual download chunk)
//   - "execute"       → decode + compile + simulate + render +
//                        composite + encode     (the whole pipeline)
//   - "upload"        → upload + finalize      (upload emits the
//                        artifacts, finalize closes the report)
//   - "report"        → finalize               (legacy "report" was
//                        the meta-phase that produced the typed proto;
//                        in v1 it collapses into finalize)
//
// IMPORTANT: consumers SHOULD prefer the canonical 12-phase vocabulary
// for new graphs; legacy 5-phase names MUST still parse in dashboards
// for the F3 transition window.
//
// Map values are intentionally slices in stable order so callers can
// test the ordering deterministically (CanonicalPhaseOrder is the
// reference sequence).
var legacyToCanonical = map[string][]string{
	"cache_lookup": {PhaseCacheLookup},
	"prefetch":     {PhaseAssetWait, PhaseCacheLookup, PhaseDownload},
	"execute":      {PhaseDecode, PhaseCompile, PhaseSimulate, PhaseRender, PhaseComposite, PhaseEncode},
	"upload":       {PhaseUpload, PhaseFinalize},
	"report":       {PhaseFinalize},
}

// MapLegacyPhase returns the canonical phases a legacy phase name
// collapses onto. Unknown legacy names return nil (no rewrite).
// Slice is a defensive copy of the package's table; callers may
// mutate it freely.
func MapLegacyPhase(legacyName string) []string {
	src, ok := legacyToCanonical[legacyName]
	if !ok {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// PhaseTimer is a single-goroutine per-run phase accumulator. A fresh
// PhaseTimer is zero-value usable; callers typically create one
// instance per Run call and freeze it before returning.
//
// StartPhase and EndPhase expect canonical names only. Unknown names
// are silently ignored (StartPhase becomes a noop), preserving the
// legacy 5-phase path which may call into the timer with names like
// "prefetch" during the transition window — MapLegacyPhase() can be
// called externally to rewrite legacy names before StartPhase.
//
// PhaseTimer is NOT safe for concurrent use.
type PhaseTimer struct {
	starts    map[string]time.Time // running → started-at
	durations map[string]time.Duration // completed → total
	clock     func() time.Time
	frozen    bool
}

// NewPhaseTimer returns a PhaseTimer with the default wall clock.
func NewPhaseTimer() *PhaseTimer {
	return &PhaseTimer{
		starts:    make(map[string]time.Time, len(CanonicalPhaseOrder)),
		durations: make(map[string]time.Duration, len(CanonicalPhaseOrder)),
		clock:     time.Now,
	}
}

// NewPhaseTimerWithClock allows injecting a fixed clock in tests.
// Production code should use NewPhaseTimer().
func NewPhaseTimerWithClock(clock func() time.Time) *PhaseTimer {
	return &PhaseTimer{
		starts:    make(map[string]time.Time, len(CanonicalPhaseOrder)),
		durations: make(map[string]time.Duration, len(CanonicalPhaseOrder)),
		clock:     clock,
	}
}

// StartPhase marks the beginning of the canonical phase `name`.
// Unknown names are a noop (allows legacy callers that pass
// non-canonical names without crashing the run). Calling StartPhase
// twice on the same canonical name without EndPhase in between
// re-stamps the start — last-write-wins is appropriate for a
// timer owned by a single goroutine.
//
// StartPhase requires the timer NOT to be frozen.
func (p *PhaseTimer) StartPhase(name string) {
	if p == nil || p.frozen {
		return
	}
	if !IsCanonical(name) {
		return
	}
	if p.clock == nil {
		p.clock = time.Now
	}
	p.starts[name] = p.clock()
}

// EndPhase marks the end of the canonical phase `name`. If the phase
// was never started (no StartPhase call), EndPhase records zero
// duration. Repeated EndPhase calls accumulate durations — useful
// for sub-phases that begin/end multiple times within a coarse
// 5-phase boundary.
//
// EndPhase requires the timer NOT to be frozen.
func (p *PhaseTimer) EndPhase(name string) {
	if p == nil || p.frozen {
		return
	}
	if !IsCanonical(name) {
		return
	}
	if p.clock == nil {
		p.clock = time.Now
	}
	end := p.clock()
	if start, ok := p.starts[name]; ok {
		p.durations[name] += end.Sub(start)
		delete(p.starts, name)
		// else: zero-record; do not silently add to existing duration
	}
}

// PhaseDurations returns a defensive copy of the accumulated phase
// durations, ordered by CanonicalPhaseOrder for deterministic
// iteration. Phases with zero duration are still present (zero-valued
// entries) so callers can rely on a complete 12-key shape.
//
// Nil-receiver safe: returns a fresh map with all 12 canonical keys
// at zero rather than nil. Dashboards depend on a stable shape even
// when a timer was not constructed.
func (p *PhaseTimer) PhaseDurations() map[string]time.Duration {
	out := make(map[string]time.Duration, len(CanonicalPhaseOrder))
	for _, name := range CanonicalPhaseOrder {
		if p == nil {
			out[name] = time.Duration(0)
			continue
		}
		d, ok := p.durations[name]
		if !ok {
			d = time.Duration(0)
		}
		out[name] = d
	}
	return out
}

// Freeze marks the timer as immutable. Subsequent StartPhase /
// EndPhase calls are no-ops; PhaseDurations still returns the
// accumulated state. Safe to call multiple times.
//
// One-shot guard: under the runner's single-goroutine contract,
// callers MUST call Freeze() exactly once after Run completes and
// before any other goroutine touches PhaseDurations().
func (p *PhaseTimer) Freeze() {
	if p == nil {
		return
	}
	p.frozen = true
}

// Frozen returns whether Freeze has been called. Useful in tests.
func (p *PhaseTimer) Frozen() bool {
	if p == nil {
		return false
	}
	return p.frozen
}

// Reset clears all state and unfreezes the timer. Mostly a test
// affordance; production callers should create a fresh PhaseTimer
// per Run call rather than reset.
func (p *PhaseTimer) Reset() {
	if p == nil {
		return
	}
	p.starts = make(map[string]time.Time, len(CanonicalPhaseOrder))
	p.durations = make(map[string]time.Duration, len(CanonicalPhaseOrder))
	p.frozen = false
}
