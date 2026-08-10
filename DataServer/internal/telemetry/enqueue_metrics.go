package telemetry

import (
	"context"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// EnqueueMetrics is a request-scoped accumulator for enqueue observations.
// It is deliberately separate from the process-wide Prometheus registry:
// these values describe one enqueue trace and are emitted as span attributes.
// The mutex keeps the accumulator safe when a resolver or asset operation is
// instrumented from a helper that may run concurrently.
type EnqueueMetrics struct {
	mu sync.Mutex

	phaseDuration  map[string]time.Duration
	phaseAllocByte map[string]uint64

	jsonMarshal   uint64
	jsonUnmarshal uint64

	resolverQueries     uint64
	resolverPlanQueries uint64

	measureAllocations bool
}

type enqueueMetricsContextKey struct{}

// WithEnqueueMetrics installs a request-scoped accumulator in ctx. Allocation
// measurement is explicit because runtime.ReadMemStats is materially more
// expensive than the counters and timers. All other observations are always
// collected when this accumulator is present.
func WithEnqueueMetrics(ctx context.Context, measureAllocations bool) (context.Context, *EnqueueMetrics) {
	if ctx == nil {
		ctx = context.Background()
	}
	metrics := &EnqueueMetrics{
		phaseDuration:      make(map[string]time.Duration),
		phaseAllocByte:     make(map[string]uint64),
		measureAllocations: measureAllocations,
	}
	return context.WithValue(ctx, enqueueMetricsContextKey{}, metrics), metrics
}

// EnsureEnqueueMetrics reuses an existing accumulator or creates one. The
// environment switch intentionally applies only to allocation measurement;
// timing and operation counts remain cheap and always available to traces.
func EnsureEnqueueMetrics(ctx context.Context) (context.Context, *EnqueueMetrics) {
	if ctx == nil {
		ctx = context.Background()
	}
	if metrics := EnqueueMetricsFromContext(ctx); metrics != nil {
		return ctx, metrics
	}
	return WithEnqueueMetrics(ctx, globalConfig().MeasureEnqueueAllocations)
}

// EnqueueMetricsFromContext returns the request accumulator, if installed.
func EnqueueMetricsFromContext(ctx context.Context) *EnqueueMetrics {
	if ctx == nil {
		return nil
	}
	metrics, _ := ctx.Value(enqueueMetricsContextKey{}).(*EnqueueMetrics)
	return metrics
}

// BeginEnqueuePhase starts a phase timer. The returned function is safe to
// defer and records wall duration. When allocation measurement is enabled it
// also records a diagnostic TotalAlloc delta from runtime.MemStats. TotalAlloc
// is process-wide, so concurrent requests can contribute to the same delta;
// allocation bytes are therefore approximate and must only be used for
// exploratory profiling, never as an exact per-request optimization signal.
// The measurement does not force a GC, but ReadMemStats still has overhead and
// is intentionally opt-in.
func BeginEnqueuePhase(ctx context.Context, phase string) func() {
	metrics := EnqueueMetricsFromContext(ctx)
	if metrics == nil {
		return func() {}
	}
	started := time.Now()
	var allocStart uint64
	if metrics.measureAllocations {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		allocStart = stats.TotalAlloc
	}
	return func() {
		var allocDelta uint64
		if metrics.measureAllocations {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.TotalAlloc >= allocStart {
				allocDelta = stats.TotalAlloc - allocStart
			}
		}
		metrics.mu.Lock()
		metrics.phaseDuration[phase] += time.Since(started)
		metrics.phaseAllocByte[phase] += allocDelta
		metrics.mu.Unlock()
	}
}

func (m *EnqueueMetrics) addJSONMarshal() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.jsonMarshal++
	m.mu.Unlock()
}

func (m *EnqueueMetrics) addJSONUnmarshal() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.jsonUnmarshal++
	m.mu.Unlock()
}

// RecordEnqueueJSONMarshal records one json.Marshal operation in ctx.
func RecordEnqueueJSONMarshal(ctx context.Context) {
	if metrics := EnqueueMetricsFromContext(ctx); metrics != nil {
		metrics.addJSONMarshal()
	}
}

// RecordEnqueueJSONUnmarshal records one json.Unmarshal operation in ctx.
func RecordEnqueueJSONUnmarshal(ctx context.Context) {
	if metrics := EnqueueMetricsFromContext(ctx); metrics != nil {
		metrics.addJSONUnmarshal()
	}
}

// RecordEnqueueResolverQuery records an explicit per-job plan query.
func RecordEnqueueResolverQuery(ctx context.Context, kind string) {
	metrics := EnqueueMetricsFromContext(ctx)
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.resolverQueries++
	if kind == "plans" {
		metrics.resolverPlanQueries++
	}
	metrics.mu.Unlock()
}

// RecordOnSpan emits a stable, low-cardinality snapshot on the enqueue root
// span. Phase names are controlled internal constants, not user input. JSON
// counts cover the canonical enqueue normalization/projection operations that
// receive the request context; legacy standalone builders without a context
// accumulator are intentionally outside this request-scoped measurement.
// Allocation attributes, when present, carry the approximate diagnostic
// TotalAlloc deltas described above.
func (m *EnqueueMetrics) RecordOnSpan(span trace.Span) {
	if m == nil || span == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	attrs := []attribute.KeyValue{
		attribute.Int64("velox.enqueue.json_marshal_count", int64(m.jsonMarshal)),
		attribute.Int64("velox.enqueue.json_unmarshal_count", int64(m.jsonUnmarshal)),
		attribute.Int64("velox.enqueue.delivery_plan_resolver_queries", int64(m.resolverQueries)),
		attribute.Int64("velox.enqueue.delivery_plan_plan_queries", int64(m.resolverPlanQueries)),
	}
	for phase, duration := range m.phaseDuration {
		attrs = append(attrs, attribute.Int64("velox.enqueue.phase."+phase+".duration_ms", duration.Milliseconds()))
		if m.measureAllocations {
			attrs = append(attrs, attribute.Int64("velox.enqueue.phase."+phase+".alloc_bytes", int64(m.phaseAllocByte[phase])))
		}
	}
	span.SetAttributes(attrs...)
}

// EnqueueMetricsSnapshot is a test/debug-friendly immutable view of the
// request-scoped observations.
type EnqueueMetricsSnapshot struct {
	PhaseDuration           map[string]time.Duration
	PhaseAllocBytes         map[string]uint64
	JSONMarshalCount        uint64
	JSONUnmarshalCount      uint64
	ResolverQueries         uint64
	ResolverPlanQueries     uint64
	AllocationMeasurementOn bool
}

// Snapshot returns a copy that can be inspected without holding the metrics
// mutex. It is intentionally not used as a production export path.
func (m *EnqueueMetrics) Snapshot() EnqueueMetricsSnapshot {
	if m == nil {
		return EnqueueMetricsSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	phaseDuration := make(map[string]time.Duration, len(m.phaseDuration))
	for phase, duration := range m.phaseDuration {
		phaseDuration[phase] = duration
	}
	phaseAllocBytes := make(map[string]uint64, len(m.phaseAllocByte))
	for phase, bytes := range m.phaseAllocByte {
		phaseAllocBytes[phase] = bytes
	}
	return EnqueueMetricsSnapshot{
		PhaseDuration:           phaseDuration,
		PhaseAllocBytes:         phaseAllocBytes,
		JSONMarshalCount:        m.jsonMarshal,
		JSONUnmarshalCount:      m.jsonUnmarshal,
		ResolverQueries:         m.resolverQueries,
		ResolverPlanQueries:     m.resolverPlanQueries,
		AllocationMeasurementOn: m.measureAllocations,
	}
}
