package telemetry

// attempt_session.go owns the worker-side per-attempt resource accounting
// and lifecycle.  The AttemptTelemetrySession is the single authority for
// per-attempt telemetry: sampling, observation, and finalization.
//
// Supporting vocabulary lives in:
//   - attempt_session_types.go      — type definitions, constants, pure helpers
//   - attempt_session_sampling.go   — cgroup detection, process CPU, observe()
//   - attempt_session_finalize.go   — terminal metrics computation

import (
	"context"
	"log"
	"sync"
	"time"
)

type AttemptTelemetrySession struct {
	samplers       []*Sampler
	cgroupRoot     string
	startedAt      time.Time
	startResources *SampledResources
	startCgroup    cgroupUsage
	startProcess   processCPUUsage
	lastResources  *SampledResources
	lastCgroup     cgroupUsage
	lastProcess    processCPUUsage
	peakCPUPercent float64
	peakRSSBytes   int64
	peakOpenFDs    int64
	sampleCount    int
	mu             sync.Mutex
	stopOnce       sync.Once
	done           chan struct{}
	wg             sync.WaitGroup
	result         *AttemptTelemetry
	// pipeline is the optional collector+sink orchestration driven by
	// Start/Stop (the single entry point). When unset, the session keeps
	// its legacy behavior: resource accounting only, no sink projection.
	pipeline *AttemptPipeline
	// executorRaw is the executor-owned raw fact envelope supplied at the
	// attempt boundary immediately before Stop. It is copied into the
	// canonical AttemptSnapshot; the session resource collector overlays only
	// the resource fields it owns.
	executorRaw   *RawExecutionMetrics
	executorRawMu sync.RWMutex
}

// NewAttemptTelemetrySession creates a session. It is safe to pass nil when a
// worker was constructed by a legacy/unit-test path without a sampler.
func NewAttemptTelemetrySession(host *Sampler) *AttemptTelemetrySession {
	var clone *Sampler
	if host != nil {
		clone = host.CloneForAttempt()
	}
	return &AttemptTelemetrySession{
		samplers:   []*Sampler{clone},
		cgroupRoot: detectCgroupRoot(),
		done:       make(chan struct{}),
	}
}

// Start begins the background sampling loop and captures baselines.
func (s *AttemptTelemetrySession) Start(ctx context.Context) {
	if s == nil {
		return
	}
	if s.pipeline != nil {
		s.pipeline.StartBaseline()
	}
	s.startedAt = time.Now().UTC()
	s.startResources, _ = s.sampleResources(ctx)
	s.startCgroup = s.sampleCgroup()
	s.startProcess = sampleProcessCPU()
	s.lastResources = s.startResources
	s.lastCgroup = s.startCgroup
	s.lastProcess = s.startProcess
	s.observe(s.startResources, s.startCgroup, s.startProcess, s.startedAt)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				now := time.Now().UTC()
				resources, _ := s.sampleResources(ctx)
				s.observe(resources, s.sampleCgroup(), sampleProcessCPU(), now)
			case <-s.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop terminates the sampling loop, takes a final sample, computes the
// terminal metrics via finalizeMetrics, and publishes the pipeline sinks.
func (s *AttemptTelemetrySession) Stop(ctx context.Context) AttemptTelemetry {
	if s == nil {
		return AttemptTelemetry{}
	}
	s.stopOnce.Do(func() { close(s.done) })
	s.wg.Wait()
	end := time.Now().UTC()
	resources, _ := s.sampleResources(ctx)
	cgroup := s.sampleCgroup()
	process := sampleProcessCPU()
	s.observe(resources, cgroup, process, end)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result != nil {
		return *s.result
	}

	metrics, coverage, wall := s.finalizeMetrics(end, resources, cgroup, process)

	s.result = &AttemptTelemetry{
		Metrics:          metrics,
		Coverage:         coverage,
		Complete:         coverage["cpu"] && coverage["memory"] && coverage["disk"] && coverage["network"],
		WallClockSeconds: wall,
		StartedAt:        s.startedAt,
		CompletedAt:      end,
	}

	// Single entry point: after the resource facts are finalized, collect
	// every RAW fact (resources, process/media events, cache deltas) into
	// the canonical AttemptSnapshot and publish every registered sink.
	// Sink failures never fail the attempt: they are logged by the caller
	// (the result envelope is already complete).
	if s.pipeline != nil {
		if err := s.pipeline.Run(ctx); err != nil {
			log.Printf("[TELEMETRY] attempt sink publish failed: %v", err)
		}
	}
	return *s.result
}

// ── Pipeline binding ──────────────────────────────────────────────────────

// BindPipeline attaches the attempt's collector+sink pipeline. With a
// pipeline bound, Start/Stop become the single telemetry entry point:
// Start captures collector baselines, Stop collects the RAW facts and
// publishes every sink. Producers never call the pipeline directly.
func (s *AttemptTelemetrySession) BindPipeline(p *AttemptPipeline) {
	if s == nil {
		return
	}
	s.pipeline = p
}

// SetExecutorRawMetrics supplies the executor's observed raw facts to the
// canonical attempt pipeline. It is deliberately a boundary operation:
// producers do not publish sinks and the snapshot remains the only input to
// receipt/Prometheus/benchmark projections.
func (s *AttemptTelemetrySession) SetExecutorRawMetrics(metrics *RawExecutionMetrics) {
	if s == nil {
		return
	}
	s.executorRawMu.Lock()
	defer s.executorRawMu.Unlock()
	if metrics == nil {
		s.executorRaw = nil
		return
	}
	copy := *metrics
	s.executorRaw = &copy
}

func (s *AttemptTelemetrySession) ExecutorRawMetrics() (RawExecutionMetrics, bool) {
	if s == nil {
		return RawExecutionMetrics{}, false
	}
	s.executorRawMu.RLock()
	defer s.executorRawMu.RUnlock()
	if s.executorRaw == nil {
		return RawExecutionMetrics{}, false
	}
	return *s.executorRaw, true
}

// BindRecorder attaches the attempt's canonical journal to the pipeline.
// The recorder is created by the dispatch path after Start, so this
// binding is deferred (snapshotted at Stop).
func (s *AttemptTelemetrySession) BindRecorder(rec *EventRecorder) {
	if s == nil || s.pipeline == nil {
		return
	}
	s.pipeline.BindRecorder(rec)
}

// BindCacheFactsSource attaches the producer-owned cache fact surface
// (the worker's cache adapter) to the pipeline.
func (s *AttemptTelemetrySession) BindCacheFactsSource(src CacheFactsSource) {
	if s == nil || s.pipeline == nil {
		return
	}
	s.pipeline.BindCacheFactsSource(src)
}

// SetAttemptIdentity stamps the snapshot identity (worker-known context
// the producers cannot carry).
func (s *AttemptTelemetrySession) SetAttemptIdentity(identity AttemptIdentity) {
	if s == nil || s.pipeline == nil {
		return
	}
	s.pipeline.SetIdentity(identity)
}

// Result returns the finalized attempt telemetry. It returns a zero
// value before Stop and is nil-safe for legacy callers.
func (s *AttemptTelemetrySession) Result() AttemptTelemetry {
	if s == nil || s.result == nil {
		return AttemptTelemetry{}
	}
	return *s.result
}

// ── Phase resource tracking ───────────────────────────────────────────────

func (s *AttemptTelemetrySession) StartPhase() PhaseResourceSnapshot {
	if s == nil {
		return PhaseResourceSnapshot{at: time.Now().UTC()}
	}
	r, _ := s.sampleResources(context.Background())
	return PhaseResourceSnapshot{resources: r, cgroup: s.sampleCgroup(), process: sampleProcessCPU(), at: time.Now().UTC()}
}

func (s *AttemptTelemetrySession) EndPhase(start PhaseResourceSnapshot) PhaseResourceDelta {
	if s == nil {
		return PhaseResourceDelta{}
	}
	r, _ := s.sampleResources(context.Background())
	c := s.sampleCgroup()
	p := sampleProcessCPU()
	cpuUsec := c.CPUUsec - start.cgroup.CPUUsec
	if s.cgroupRoot == "" {
		cpuUsec = p.totalUsec() - start.process.totalUsec()
	}
	diskReadBytes := positiveDelta(c.DiskReadBytes - start.cgroup.DiskReadBytes)
	diskWriteBytes := positiveDelta(c.DiskWriteBytes - start.cgroup.DiskWriteBytes)
	if !cgroupIOAvailable(s.cgroupRoot) {
		diskReadBytes = positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.DiskReadBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.DiskReadBytesTotal }))
		diskWriteBytes = positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.DiskWriteBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.DiskWriteBytesTotal }))
	}
	return PhaseResourceDelta{
		CPUTimeMs:      positiveDelta(cpuUsec) / 1000,
		PeakRSSBytes:   max64(start.cgroup.MemoryCurrent, c.MemoryCurrent),
		DiskReadBytes:  diskReadBytes,
		DiskWriteBytes: diskWriteBytes,
		NetworkRxBytes: positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.NetworkReceiveBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.NetworkReceiveBytesTotal })),
		NetworkTxBytes: positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.NetworkTransmitBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.NetworkTransmitBytesTotal })),
	}
}

func (s *AttemptTelemetrySession) BeginPhase() PhaseResourceSnapshot { return s.StartPhase() }
func (s *AttemptTelemetrySession) EndPhaseResource(start PhaseResourceSnapshot) PhaseResourceDelta {
	return s.EndPhase(start)
}
