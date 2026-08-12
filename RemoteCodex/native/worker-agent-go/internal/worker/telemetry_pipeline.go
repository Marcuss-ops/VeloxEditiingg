package worker

// telemetry_pipeline.go — per-attempt CollectorRegistry + SinkRegistry
// wiring.
//
// AttemptTelemetrySession.Start()/Stop() is the single telemetry entry
// point; this file only assembles the registries the session drives.
// Producers (cache, engine, sampler) never reference a sink: they emit
// RAW facts and the sinks project from the canonical AttemptSnapshot.

import (
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/cache"
	"velox-worker-agent/pkg/performance"
)

// cacheFactsAdapter adapts the persistent local cache's Stats() surface
// onto the telemetry CacheFactsSource contract (producer → recorder).
// The cache package itself never references a sink.
type cacheFactsAdapter struct{ c *cache.PersistedLocalCache }

func (a cacheFactsAdapter) CacheFacts() telemetry.CacheFacts {
	if a.c == nil {
		return telemetry.CacheFacts{}
	}
	s := a.c.Stats()
	return telemetry.CacheFacts{
		Hits:        s.Hits,
		Misses:      s.Misses,
		Evictions:   s.Evictions,
		Corruptions: s.Corruptions,
		BytesUsed:   s.BytesUsed,
		Entries:     int64(s.Entries),
	}
}

// newAttemptPipeline assembles the per-attempt collector + sink
// registries the session drives:
//
//	collectors — AttemptResource (session result), Process and Media
//	             (projected from the canonical journal), Cache (provider
//	             lifetime counters diffed against the attempt baseline)
//	sinks      — Prometheus projection, canonical PerformanceReceiptV1
//	             (via the single assembler), benchmark and diagnostic
//	             JSON artifacts under the worker state dir
func (w *Worker) newAttemptPipeline(pte *PendingTaskExecution, attemptID string, session *telemetry.AttemptTelemetrySession) *telemetry.AttemptPipeline {
	pipeline := telemetry.NewAttemptPipeline()
	pipeline.SetIdentity(telemetry.AttemptIdentity{
		JobID:      pte.JobID,
		AttemptID:  attemptID,
		WorkerID:   w.workerID(),
		ExecutorID: pte.ExecutorID,
	})
	pipeline.AddCollector(&telemetry.AttemptResourceCollector{Session: session})
	pipeline.AddCollector(&telemetry.ProcessCollector{})
	pipeline.AddCollector(&telemetry.MediaCollector{})
	pipeline.AddCollector(&telemetry.CacheCollector{})
	if w.cache != nil {
		pipeline.BindCacheFactsSource(cacheFactsAdapter{c: w.cache})
	}
	pipeline.AddSink(&telemetry.PrometheusSink{Metrics: telemetry.GetPrometheusMetrics()})
	// The receipt/benchmark/diagnostic JSON sinks are OPT-IN diagnostics
	// (VELOX_TELEMETRY_JSON_DIR): the Prometheus projection always runs,
	// but no per-attempt files are written unless an operator opts in.
	// This keeps the default task-completion hot path free of file IO.
	if telemetryDir := w.telemetryJSONDir(); telemetryDir != "" {
		pipeline.AddSink(&telemetry.ReceiptSink{
			// The receipt is assembled by the SINGLE assembler
			// (pkg/performance); the sink only persists its bytes.
			Build: func(snapshot *telemetry.AttemptSnapshot) ([]byte, error) {
				return performance.AssembleFromSnapshot(snapshot).ToJSON()
			},
			OutDir: telemetryDir,
		})
		pipeline.AddSink(&telemetry.BenchmarkSink{OutDir: telemetryDir})
		pipeline.AddSink(&telemetry.DiagnosticJSONSink{OutDir: telemetryDir})
	}
	return pipeline
}

func (w *Worker) workerID() string {
	if w.config != nil {
		return w.config.WorkerID
	}
	return "unknown"
}

// telemetryJSONDir resolves the opt-in per-attempt JSON artifact
// directory (VELOX_TELEMETRY_JSON_DIR). Empty means the JSON sinks are
// not wired at all.
func (w *Worker) telemetryJSONDir() string {
	if w.config != nil {
		return w.config.TelemetryJSONDir
	}
	return ""
}
