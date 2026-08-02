// Package metrics is the master-side Prometheus text-format exporter
// for the Project Performance Scorecard.
//
// Why hand-rolled and not prometheus/client_golang? Hand-roll keeps
// the dependency tree small (this package requires nothing outside
// the standard library) and matches the existing PR-internal pattern
// in RemoteCodex/native/worker-agent-go/internal/telemetry. The
// wire-format we emit is the canonical Prometheus exposition format
// (text/plain; version=0.0.4); content negotiation is intentionally
// minimal (the master serves text only — Prometheus rooms can scrape
// directly without further headers).
//
// Label discipline:
//
//	SAFE:   executor_id, executor_version, worker_class, phase,
//	        codec, preset, resolution_bucket, cache_source, worker_id
//	UNSAFE: job_id, task_id, attempt_id, artifact_id, sha256,
//	        video_title, channel_id, hash
//
// Counters/gauges/histograms below reject unsafe label keys at
// registration time; mismatched label sets at call time are an
// explicit panic. This is the load-bearing guard rail that keeps the
// TSDB cardinality bounded as the fleet grows.
//
// File split by responsibility:
//   - metrics.go          → Family/Registry core (constructors, Inc/GaugeSet/Observe)
//   - metrics_write.go    → Prometheus exposition helpers
//   - metrics_histogram.go → histogramData backing type
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

// unsafeLabelKeys are rejected at registration time. Operators adding
// a new label MUST keep this list in mind.
var unsafeLabelKeys = map[string]struct{}{
	"job_id":      {},
	"task_id":     {},
	"attempt_id":  {},
	"artifact_id": {},
	"sha256":      {},
	"hash":        {},
	"video_title": {},
	"channel_id":  {},
}

// safeLabelKey reports whether `name` is permitted as a Prometheus
// label. Unknown keys pass; known-unsafe keys are rejected.
func safeLabelKey(name string) bool {
	if _, ok := unsafeLabelKeys[name]; ok {
		return false
	}
	return true
}

// Label is a single (key,value) pair.
type Label struct {
	Name  string
	Value string
}

// FamilyType is the typed domain of a metric family.
type FamilyType int

const (
	CounterFamily FamilyType = iota
	GaugeFamily
	HistogramFamily
)

// Family is the canonical Prometheus exposition-unit. A single typed
// metric (counter / gauge / histogram) carrying a Help text + a set
// of child instances keyed by their label-tuple.
//
// Concurrency: all children are dispatched to per-key atomics guarded
// by labelMu. The lookup-or-create path acquires the write lock
// unconditionally so concurrent first-writers do NOT lose increments
// (the buggy double-checked-lock pattern is removed; correctness
// wins over a per-look fast-path that we measure as irrelevant
// under the supervisor poll load).
type Family struct {
	Name    string
	Help    string
	Kind    FamilyType
	labels  []string   // canonical label key list (label order matters!)
	buckets []float64  // histogram-family only; nil for counter/gauge
	labelMu sync.Mutex // guards all children-maps below

	counterVals map[string]*atomic.Uint64 // CounterFamily only
	gaugeVals   map[string]*atomic.Int64  // GaugeFamily only
	histVals    map[string]*histogramData // HistogramFamily only
}

// NewCounterFamily builds a counter-family. Names with unsafe label
// keys panic at registration.
func NewCounterFamily(name, help string, labels []string) *Family {
	for _, k := range labels {
		if !safeLabelKey(k) {
			panic(fmt.Sprintf("metrics: refused unsafe label %q on counter family %q", k, name))
		}
	}
	return &Family{
		Name:        name,
		Help:        help,
		Kind:        CounterFamily,
		labels:      labels,
		counterVals: make(map[string]*atomic.Uint64),
	}
}

// NewGaugeFamily builds a gauge-family.
func NewGaugeFamily(name, help string, labels []string) *Family {
	for _, k := range labels {
		if !safeLabelKey(k) {
			panic(fmt.Sprintf("metrics: refused unsafe label %q on gauge family %q", k, name))
		}
	}
	return &Family{
		Name:      name,
		Help:      help,
		Kind:      GaugeFamily,
		labels:    labels,
		gaugeVals: make(map[string]*atomic.Int64),
	}
}

// NewHistogramFamily builds a histogram-family. `buckets` is the closed
// upper-bound list (Prometheus convention). Buckets must be strictly
// increasing; the implicit +Inf bucket is appended automatically at
// exposition time. The buckets list is OWNED by the family (copied).
func NewHistogramFamily(name, help string, labels []string, buckets []float64) *Family {
	for _, k := range labels {
		if !safeLabelKey(k) {
			panic(fmt.Sprintf("metrics: refused unsafe label %q on histogram family %q", k, name))
		}
	}
	if len(buckets) == 0 {
		panic("metrics: histogram family must have non-empty bucket list")
	}
	for i := 1; i < len(buckets); i++ {
		if buckets[i-1] >= buckets[i] {
			panic(fmt.Sprintf("metrics: histogram buckets must be strictly increasing, got %v", buckets))
		}
	}
	copied := append([]float64(nil), buckets...)
	return &Family{
		Name:     name,
		Help:     help,
		Kind:     HistogramFamily,
		labels:   labels,
		buckets:  copied,
		histVals: make(map[string]*histogramData),
	}
}

// Inc adds `delta` to a counter-family child. Panics if labels len
// doesn't match the family's registered label list OR if delta is
// negative (counters are monotonic — pass-through would silently
// produce wrong Prometheus metrics).
func (f *Family) Inc(labelVals []string, delta uint64) {
	if f.Kind != CounterFamily {
		panic(fmt.Sprintf("metrics: Inc called on non-counter family %q", f.Name))
	}
	if len(labelVals) != len(f.labels) {
		panic(fmt.Sprintf("metrics: counter %q label len mismatch: got %d want %d", f.Name, len(labelVals), len(f.labels)))
	}
	key := labelKey(labelVals)
	f.labelMu.Lock()
	c, ok := f.counterVals[key]
	if !ok {
		c = &atomic.Uint64{}
		f.counterVals[key] = c
	}
	f.labelMu.Unlock()
	c.Add(delta)
}

// GaugeSet overwrites a gauge-family child's value.
func (f *Family) GaugeSet(labelVals []string, value int64) {
	if f.Kind != GaugeFamily {
		panic(fmt.Sprintf("metrics: GaugeSet called on non-gauge family %q", f.Name))
	}
	if len(labelVals) != len(f.labels) {
		panic(fmt.Sprintf("metrics: gauge %q label len mismatch: got %d want %d", f.Name, len(labelVals), len(f.labels)))
	}
	key := labelKey(labelVals)
	f.labelMu.Lock()
	g, ok := f.gaugeVals[key]
	if !ok {
		g = &atomic.Int64{}
		f.gaugeVals[key] = g
	}
	f.labelMu.Unlock()
	g.Store(value)
}

// Observe adds one observation `v` to a histogram-family child.
func (f *Family) Observe(labelVals []string, v float64) {
	if f.Kind != HistogramFamily {
		panic(fmt.Sprintf("metrics: Observe called on non-histogram family %q", f.Name))
	}
	if len(labelVals) != len(f.labels) {
		panic(fmt.Sprintf("metrics: histogram %q label len mismatch: got %d want %d", f.Name, len(labelVals), len(f.labels)))
	}
	key := labelKey(labelVals)
	f.labelMu.Lock()
	h, ok := f.histVals[key]
	if !ok {
		h = newHistogramData(f.buckets)
		f.histVals[key] = h
	}
	f.labelMu.Unlock()
	h.observe(v)
}

// Registry holds the typed metric families the master exposes.
type Registry struct {
	mu       sync.RWMutex
	families []*Family
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// Register adds a family. Re-registering the same name panics — it
// almost always signals a programmer bug (double-init) rather than
// legitimate same-name reuse.
func (r *Registry) Register(f *Family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, g := range r.families {
		if g.Name == f.Name {
			panic(fmt.Sprintf("metrics: register called twice for %q", f.Name))
		}
	}
	r.families = append(r.families, f)
}

// WritePrometheus writes every registered family to `w` in the
// canonical text/plain; version=0.0.4 exposition format. Stable
// ordering: families sorted by name; children sorted by their
// label-tuple key.
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.RLock()
	families := make([]*Family, len(r.families))
	copy(families, r.families)
	r.mu.RUnlock()
	sort.Slice(families, func(i, j int) bool { return families[i].Name < families[j].Name })
	for _, f := range families {
		if err := f.write(w); err != nil {
			return err
		}
	}
	return nil
}

// HTTPHandler returns an http.HandlerFunc that serves the registry on
// GET requests with the canonical text/plain; version=0.0.4 content
// type. Non-GET requests get a 405.
func (r *Registry) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := r.WritePrometheus(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// Handler is the alias for HTTPHandler returning http.Handler for
// consumers that need the canonical interface (gin / http.ServeMux
// wrappers, etc.).
func (r *Registry) Handler() http.Handler { return r.HTTPHandler() }
