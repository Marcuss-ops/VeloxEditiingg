package telemetry

// sinks.go — SinkRegistry and the four canonical attempt sinks.
//
// A sink is the ONLY writer of derived/observable state. Producers never
// call a sink: they emit RAW facts (events, session resources, cache
// counters) and the pipeline assembled by AttemptTelemetrySession hands
// them the canonical AttemptSnapshot at Stop. Sinks are read-only
// consumers of the snapshot — none of them mutates the recorder or the
// producers.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Sink projects the canonical AttemptSnapshot onto one observable surface.
// Publish must be idempotent-safe (a failed attempt still publishes its
// raw facts) and must never mutate the snapshot or the recorder.
type Sink interface {
	Name() string
	Publish(ctx context.Context, snapshot *AttemptSnapshot) error
}

// SinkRegistry owns the ordered sink set of one attempt. All sinks
// receive the SAME snapshot; a sink failure does not stop the remaining
// sinks (a Prometheus error must not drop the diagnostic dump).
type SinkRegistry struct {
	mu    sync.Mutex
	sinks []Sink
}

func NewSinkRegistry() *SinkRegistry {
	return &SinkRegistry{}
}

func (r *SinkRegistry) Register(s Sink) {
	if s == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks = append(r.sinks, s)
}

// Publish delivers the snapshot to every sink in registration order and
// returns the first error while still publishing to the remaining sinks.
func (r *SinkRegistry) Publish(ctx context.Context, snapshot *AttemptSnapshot) error {
	r.mu.Lock()
	sinks := append([]Sink(nil), r.sinks...)
	r.mu.Unlock()
	var firstErr error
	for _, s := range sinks {
		if err := s.Publish(ctx, snapshot); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("sink %s: %w", s.Name(), err)
		}
	}
	return firstErr
}

// Names returns the registered sink names in order (diagnostics/tests).
func (r *SinkRegistry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.sinks))
	for _, s := range r.sinks {
		out = append(out, s.Name())
	}
	return out
}

func (r *SinkRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sinks)
}

// ── PrometheusSink ────────────────────────────────────────────────────
// The ONLY Prometheus projection for per-attempt snapshot facts. The
// cache producer no longer calls Prometheus directly: the CacheCollector
// gathers the raw deltas and this sink publishes them. Resource/media/
// process families land here when their dedicated Prometheus families
// exist; until then the snapshot facts travel via the receipt/diagnostic
// sinks.

type PrometheusSink struct {
	// Metrics is the worker's global Prometheus metrics instance.
	Metrics *PrometheusMetrics
}

func (s *PrometheusSink) Name() string { return "prometheus" }

func (s *PrometheusSink) Publish(_ context.Context, snapshot *AttemptSnapshot) error {
	if s.Metrics == nil || snapshot == nil {
		return nil
	}
	raw := snapshot.RawMetrics()
	// Cache counters come from the raw typed envelope stamped by
	// CacheCollector. The structured CacheFacts view remains available for
	// point-in-time gauges and diagnostic compatibility, but it is not the
	// counter source for this projection.
	if raw.AssetCacheHitCount > 0 {
		s.Metrics.RecordCacheRequestN("hit", raw.AssetCacheHitCount)
	}
	if raw.AssetCacheMissCount > 0 {
		s.Metrics.RecordCacheRequestN("miss", raw.AssetCacheMissCount)
	}
	if snapshot.Cache.Evictions > 0 {
		s.Metrics.RecordCacheEvictions("pressure", int(snapshot.Cache.Evictions))
	}
	// Point-in-time gauges are idempotent: later attempts simply overwrite.
	// Corruptions have no dedicated family yet and remain typed snapshot
	// facts for the receipt/diagnostic projections.
	s.Metrics.SetCacheSize(int(snapshot.Cache.Entries), snapshot.Cache.BytesUsed)
	if renderMS, ok := snapshot.RenderDurationMS(); ok {
		s.Metrics.RecordRender(time.Duration(renderMS) * time.Millisecond)
	}
	return nil
}

// ── ReceiptSink ───────────────────────────────────────────────────────
// Persists the canonical PerformanceReceiptV1 JSON. The Build closure is
// wired at the worker boundary: it owns the single assembler
// (pkg/performance.AssembleFromSnapshot), which keeps telemetry free of a
// pkg/performance import. The sink only writes the assembled bytes.

type ReceiptSink struct {
	// Build assembles the receipt JSON from the canonical snapshot.
	// Required. Must be pure: same snapshot ⇒ same bytes.
	Build func(snapshot *AttemptSnapshot) ([]byte, error)
	// OutDir is the directory the receipt is written to. Created if
	// missing. The filename embeds the attempt id.
	OutDir string
}

func (s *ReceiptSink) Name() string { return "receipt" }

func (s *ReceiptSink) Publish(_ context.Context, snapshot *AttemptSnapshot) error {
	if s.Build == nil {
		return nil
	}
	data, err := s.Build(snapshot)
	if err != nil {
		return fmt.Errorf("assemble receipt: %w", err)
	}
	// Stable name: the docs and the benchmark tooling reference
	// performance_receipt.json (the latest attempt's receipt). Per-attempt
	// raw evidence rides the attempt-suffixed diagnostic dump.
	return writeSinkFile(s.OutDir, "performance_receipt.json", data)
}

// ── BenchmarkSink ─────────────────────────────────────────────────────
// Writes the raw-facts benchmark artifact (identity, resources, cache,
// process, media, wall) consumed by the phase-0 benchmark tooling. It
// carries RAW facts only: the derived KPIs live in the receipt, which
// remains the single derived artifact.

type BenchmarkSink struct {
	OutDir string
}

func (s *BenchmarkSink) Name() string { return "benchmark" }

func (s *BenchmarkSink) Publish(_ context.Context, snapshot *AttemptSnapshot) error {
	view := struct {
		Identity    AttemptIdentity     `json:"identity"`
		WallMs      int64               `json:"wall_ms"`
		StartedAt   string              `json:"started_at"`
		CompletedAt string              `json:"completed_at"`
		Resources   RawExecutionMetrics `json:"resources"`
		Process     ProcessFacts        `json:"process"`
		Media       MediaFacts          `json:"media"`
		Cache       CacheFacts          `json:"cache"`
	}{
		Identity:    snapshot.Identity,
		WallMs:      snapshot.WallMs,
		StartedAt:   snapshot.StartedAt.UTC().Format(time.RFC3339),
		CompletedAt: snapshot.CompletedAt.UTC().Format(time.RFC3339),
		Resources:   snapshot.Resources,
		Process:     snapshot.Process,
		Media:       snapshot.Media,
		Cache:       snapshot.Cache,
	}
	data, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal benchmark: %w", err)
	}
	name := "benchmark.json"
	if snapshot.Identity.AttemptID != "" {
		name = fmt.Sprintf("benchmark_%s.json", snapshot.Identity.AttemptID)
	}
	return writeSinkFile(s.OutDir, name, data)
}

// ── DiagnosticJSONSink ────────────────────────────────────────────────
// Dumps the FULL canonical snapshot (events included) for offline
// diagnostics. This is the complete raw evidence base; the receipt is the
// compact derived projection.

type DiagnosticJSONSink struct {
	OutDir string
}

func (s *DiagnosticJSONSink) Name() string { return "diagnostic_json" }

func (s *DiagnosticJSONSink) Publish(_ context.Context, snapshot *AttemptSnapshot) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal diagnostic: %w", err)
	}
	name := "diagnostic.json"
	if snapshot.Identity.AttemptID != "" {
		name = fmt.Sprintf("diagnostic_%s.json", snapshot.Identity.AttemptID)
	}
	return writeSinkFile(s.OutDir, name, data)
}

func writeSinkFile(dir, name string, data []byte) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create sink dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}
