package telemetry

// attempt_pipeline.go — AttemptPipeline: the single orchestration that
// turns one attempt's RAW facts into every observable projection.
//
// Lifecycle (driven exclusively by AttemptTelemetrySession):
//
//	Start()  → pipeline.StartBaseline()   (collectors capture baselines:
//	                                        e.g. cache lifetime counters)
//	Stop()   → pipeline.Run(snapshot)     (collect → sinks publish)
//
// Producers never see the pipeline: they emit facts (events, session
// resources, cache counters) and the sinks are projected from the
// resulting snapshot. Adding a sink (ClickHouse, OTel, Loki, …) is a
// pipeline-level change and never touches a producer.

import "context"

// AttemptPipeline owns the collector and sink registries of one attempt.
// It is not safe for concurrent use beyond the Start/Stop pair the
// session already serializes; registries themselves are lock-safe.
type AttemptPipeline struct {
	collectors CollectorRegistry
	sinks      SinkRegistry
	identity   AttemptIdentity
	recorder   *EventRecorder
	cacheFacts CacheFactsSource

	started bool
}

func NewAttemptPipeline() *AttemptPipeline {
	return &AttemptPipeline{}
}

// SetIdentity stamps the snapshot identity (worker-known context the
// producers cannot carry).
func (p *AttemptPipeline) SetIdentity(identity AttemptIdentity) {
	p.identity = identity
}

// BindRecorder attaches the attempt's canonical journal. The recorder is
// created after Start (dispatch path), so the binding is deferred: the
// pipeline snapshots it at Run time.
func (p *AttemptPipeline) BindRecorder(rec *EventRecorder) {
	p.recorder = rec
}

// BindCacheFactsSource attaches the producer-owned cache fact surface
// (worker adapter over its concrete cache).
func (p *AttemptPipeline) BindCacheFactsSource(source CacheFactsSource) {
	p.cacheFacts = source
}

func (p *AttemptPipeline) AddCollector(c Collector) { p.collectors.Register(c) }
func (p *AttemptPipeline) AddSink(s Sink)           { p.sinks.Register(s) }
func (p *AttemptPipeline) CollectorNames() []string { return p.collectors.Names() }
func (p *AttemptPipeline) SinkNames() []string      { return p.sinks.Names() }

// StartBaseline runs the attempt-start collector hooks exactly once
// (invoked by AttemptTelemetrySession.Start).
func (p *AttemptPipeline) StartBaseline() {
	if p.started {
		return
	}
	p.started = true
	p.collectors.StartBaseline()
}

// Run collects the RAW facts into the canonical AttemptSnapshot and
// publishes it to every sink. It is invoked by AttemptTelemetrySession
// at Stop — the single entry point of the per-attempt telemetry flow.
// A collector or sink failure never panics: sinks receive the snapshot
// best-effort and failures are aggregated and reported.
func (p *AttemptPipeline) Run(ctx context.Context) error {
	snapshot := &AttemptSnapshot{Identity: p.identity}
	if p.recorder != nil {
		snapshot.Events = p.recorder.Snapshot()
		snapshot.DroppedEvents = p.recorder.DroppedEventCount()
	}
	p.collectors.Collect(ctx, snapshot)
	if err := p.sinks.Publish(ctx, snapshot); err != nil {
		return err
	}
	return nil
}
