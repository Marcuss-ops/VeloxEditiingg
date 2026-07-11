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
package metrics

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
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

// ── internals ─────────────────────────────────────────────────────────────

// labelKey is the canonical label-tuple key (deterministic). We
// join label values with `\x00` to avoid collisions on labels like
// "a,b" + "c" vs "a" + "b,c".
func labelKey(vals []string) string {
	return strings.Join(vals, "\x00")
}

// splitLabelKey maps a label key list to its value list for exposition.
// Splits labelKey back to a slice.
func splitLabelKey(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}

// quote escapes \ and " for the Prometheus exposition label-value format.
func quote(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "\"", "\\\"")
}

// formatLabelInline formats a (labels, label-values) pair as
// `{name="value",name2="value2"}`. Empty label list ⇒ "".
func formatLabelInline(names, vals []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("{")
	for i, n := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(quote(vals[i]))
		b.WriteString(`"`)
	}
	b.WriteString("}")
	return b.String()
}

// formatHistogramLabelInline extends formatLabelInline with a single
// extra `extraKey="extraVal"` entry (e.g. `le="0.5"` on a histogram bucket).
func formatHistogramLabelInline(names, vals []string, extraKey, extraVal string) string {
	if len(names) == 0 {
		return "{" + extraKey + `="` + extraVal + `"}`
	}
	var b strings.Builder
	b.WriteString("{")
	for i, n := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(quote(vals[i]))
		b.WriteString(`"`)
	}
	b.WriteString(",")
	b.WriteString(extraKey)
	b.WriteString(`="`)
	b.WriteString(extraVal)
	b.WriteString(`"`)
	b.WriteString("}")
	return b.String()
}

// formatBucket formats a bucket boundary using the canonical Prometheus
// representation. We use 'g' with -1 precision so 1.0 stays "1",
// 0.5 stays "0.5", and 0.05 stays "0.05". Stable across runs.
func formatBucket(v float64) string {
	return strconvFormatFloat(v, 'g', -1, 64)
}

// write emits one family to `w`.
func (f *Family) write(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "# HELP %s %s\n", f.Name, strings.ReplaceAll(f.Help, "\n", " ")); err != nil {
		return err
	}
	typeName := "untyped"
	switch f.Kind {
	case CounterFamily:
		typeName = "counter"
	case GaugeFamily:
		typeName = "gauge"
	case HistogramFamily:
		typeName = "histogram"
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", f.Name, typeName); err != nil {
		return err
	}
	switch f.Kind {
	case CounterFamily:
		return f.writeCounter(w)
	case GaugeFamily:
		return f.writeGauge(w)
	case HistogramFamily:
		return f.writeHistogram(w)
	}
	return errors.New("metrics: unknown family type")
}

func (f *Family) writeCounter(w io.Writer) error {
	f.labelMu.Lock()
	keys := make([]string, 0, len(f.counterVals))
	mapping := make(map[string]uint64, len(f.counterVals))
	labelList := append([]string(nil), f.labels...)
	for k, v := range f.counterVals {
		keys = append(keys, k)
		mapping[k] = v.Load()
	}
	f.labelMu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		lblVals := splitLabelKey(k)
		if _, err := fmt.Fprintf(w, "%s%s %d\n", f.Name, formatLabelInline(labelList, lblVals), mapping[k]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Family) writeGauge(w io.Writer) error {
	f.labelMu.Lock()
	keys := make([]string, 0, len(f.gaugeVals))
	mapping := make(map[string]int64, len(f.gaugeVals))
	labelList := append([]string(nil), f.labels...)
	for k, v := range f.gaugeVals {
		keys = append(keys, k)
		mapping[k] = v.Load()
	}
	f.labelMu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		lblVals := splitLabelKey(k)
		if _, err := fmt.Fprintf(w, "%s%s %d\n", f.Name, formatLabelInline(labelList, lblVals), mapping[k]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Family) writeHistogram(w io.Writer) error {
	f.labelMu.Lock()
	keys := make([]string, 0, len(f.histVals))
	snapshots := make(map[string]*histogramData, len(f.histVals))
	labelList := append([]string(nil), f.labels...)
	buckets := append([]float64(nil), f.buckets...)
	for k, v := range f.histVals {
		keys = append(keys, k)
		snapshots[k] = v.snapshot()
	}
	f.labelMu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		h := snapshots[k]
		if len(labelList) == 0 {
			for _, b := range buckets {
				fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", f.Name, formatBucket(b), h.bucketLE(b))
			}
			fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", f.Name, h.count)
			fmt.Fprintf(w, "%s_sum %s\n", f.Name, strconvFormatFloat(h.sum, 'g', -1, 64))
			fmt.Fprintf(w, "%s_count %d\n", f.Name, h.count)
			continue
		}
		lblVals := splitLabelKey(k)
		for _, b := range buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", f.Name,
				formatHistogramLabelInline(labelList, lblVals, "le", formatBucket(b)),
				h.bucketLE(b))
		}
		// +Inf comes BEFORE _sum/_count in canonical exposition format.
		fmt.Fprintf(w, "%s_bucket%s %d\n", f.Name,
			formatHistogramLabelInline(labelList, lblVals, "le", "+Inf"),
			h.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", f.Name,
			formatLabelInline(labelList, lblVals),
			strconvFormatFloat(h.sum, 'g', -1, 64))
		fmt.Fprintf(w, "%s_count%s %d\n", f.Name,
			formatLabelInline(labelList, lblVals),
			h.count)
	}
	return nil
}

// ── histogram helpers ─────────────────────────────────────────────────────

type histogramData struct {
	mu      sync.RWMutex
	count   uint64
	sum     float64
	buckets []float64
	counts  []uint64 // cumulative bucket counts (≤ b)
}

func newHistogramData(buckets []float64) *histogramData {
	return &histogramData{
		buckets: append([]float64(nil), buckets...),
		counts:  make([]uint64, len(buckets)),
	}
}

func (h *histogramData) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

// bucketLE returns the cumulative count with `v <= b`. Iterates and
// returns the first bucket whose upper bound is ≥ b (Prometheus
// convention: cumulative counts are reported against `le`). Falls
// through to `count` (the implicit +Inf bucket) when `b` exceeds all
// explicit bucket boundaries.
func (h *histogramData) bucketLE(b float64) uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for i, ub := range h.buckets {
		if b <= ub {
			return h.counts[i]
		}
	}
	return h.count
}

func (h *histogramData) snapshot() *histogramData {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := &histogramData{
		count:   h.count,
		sum:     h.sum,
		buckets: append([]float64(nil), h.buckets...),
		counts:  append([]uint64(nil), h.counts...),
	}
	return out
}
