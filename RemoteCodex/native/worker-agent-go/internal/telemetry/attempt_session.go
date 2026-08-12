package telemetry

// attempt_session.go owns the worker-side per-attempt resource accounting.
// The heartbeat sampler remains a host snapshot source; this session uses an
// independent clone of it plus cgroup v2 counters so child processes (native
// engine, ffmpeg and ffprobe) are included in CPU/RAM/IO accounting.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const AttemptTelemetrySchemaVersion int32 = 2

type cgroupUsage struct {
	CPUUsec        int64
	MemoryCurrent  int64
	MemoryPeak     int64
	DiskReadBytes  int64
	DiskWriteBytes int64
}

type processCPUUsage struct {
	userUsec   int64
	systemUsec int64
	valid      bool
}

func (u processCPUUsage) totalUsec() int64 { return u.userUsec + u.systemUsec }

// PhaseResourceDelta is the resource portion available to a detailed phase.
// CPU is carried in the typed phase field; the remaining values are included
// in phase metadata until the phase storage schema grows typed columns.
type PhaseResourceDelta struct {
	CPUTimeMs      int64
	PeakRSSBytes   int64
	DiskReadBytes  int64
	DiskWriteBytes int64
	NetworkRxBytes int64
	NetworkTxBytes int64
}

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
}

type AttemptTelemetry struct {
	Metrics          RawExecutionMetrics
	Coverage         map[string]bool
	Complete         bool
	WallClockSeconds float64
	StartedAt        time.Time
	CompletedAt      time.Time
}

type PhaseResourceSnapshot struct {
	resources *SampledResources
	cgroup    cgroupUsage
	process   processCPUUsage
	at        time.Time
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
	wall := end.Sub(s.startedAt).Seconds()
	if wall < 0 {
		wall = 0
	}
	cpuUsec := cgroup.CPUUsec - s.startCgroup.CPUUsec
	if s.cgroupRoot == "" {
		cpuUsec = process.totalUsec() - s.startProcess.totalUsec()
	}
	diskReadBytes := positiveDelta(cgroup.DiskReadBytes - s.startCgroup.DiskReadBytes)
	diskWriteBytes := positiveDelta(cgroup.DiskWriteBytes - s.startCgroup.DiskWriteBytes)
	// cgroup v2 is authoritative for an attempt because it includes the
	// native renderer and every FFmpeg child. Older hosts may expose CPU/RAM
	// cgroup files without io.stat; retain the existing diskstats fallback in
	// that case.
	if !cgroupIOAvailable(s.cgroupRoot) {
		diskReadBytes = positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.DiskReadBytesTotal }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.DiskReadBytesTotal }))
		diskWriteBytes = positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.DiskWriteBytesTotal }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.DiskWriteBytesTotal }))
	}
	metrics := TypedExecutionMetrics{
		CpuTimeMs:        positiveDelta(cpuUsec) / 1000,
		PeakRssBytes:     s.peakRSSBytes,
		CpuPercentPeak:   s.peakCPUPercent,
		WallClockSeconds: wall,
		DiskReadBytes:    diskReadBytes,
		DiskWriteBytes:   diskWriteBytes,
		NetworkRxBytes:   positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.NetworkReceiveBytesTotal }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.NetworkReceiveBytesTotal })),
		NetworkTxBytes:   positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.NetworkTransmitBytesTotal }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.NetworkTransmitBytesTotal })),
		TempBytesWritten: positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.TempBytesWritten }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.TempBytesWritten })),
		IowaitMs:         int64(wall * 1000 * safeRatio(resourcesRatio(resources, func(r *SampledResources) float64 { return r.CPUIOWaitRatio }))),
		OpenFdsPeak:      s.peakOpenFDs,
		LogicalCpuCount:  int32(runtime.NumCPU()),
	}
	cp := DetectCPUCapacity()
	metrics.CpuQuota = cp.CPUQuota
	metrics.EffectiveCpuCount = int32(cp.EffectiveCPUCount)
	coverage := map[string]bool{
		"cpu":          (s.cgroupRoot != "" && cgroup.CPUUsec >= s.startCgroup.CPUUsec) || (s.cgroupRoot == "" && s.startProcess.valid && process.valid && process.totalUsec() >= s.startProcess.totalUsec()),
		"memory":       s.startCgroup.MemoryCurrent > 0 || cgroup.MemoryCurrent > 0 || s.peakRSSBytes > 0,
		"disk":         (s.cgroupRoot != "" && (cgroup.DiskReadBytes >= s.startCgroup.DiskReadBytes || cgroup.DiskWriteBytes >= s.startCgroup.DiskWriteBytes)) || (resources != nil && s.startResources != nil),
		"network":      resources != nil && s.startResources != nil,
		"cgroup":       s.cgroupRoot != "",
		"process_tree": s.cgroupRoot != "",
	}
	if s.cgroupRoot != "" {
		metrics.TelemetryCPUSource = "cgroup_v2"
	} else if len(s.samplers) > 0 && s.samplers[0] != nil {
		metrics.TelemetryCPUSource = "proc"
	} else {
		metrics.TelemetryCPUSource = "missing"
	}
	coverageJSON, _ := json.Marshal(coverage)
	metrics.TelemetryCoverageJSON = string(coverageJSON)
	metrics.TelemetryComplete = coverage["cpu"] && coverage["memory"] && coverage["disk"] && coverage["network"]
	s.result = &AttemptTelemetry{Metrics: metrics, Coverage: coverage, Complete: coverage["cpu"] && coverage["memory"] && coverage["disk"] && coverage["network"], WallClockSeconds: wall, StartedAt: s.startedAt, CompletedAt: end}
	// Single entry point: after the resource facts are finalized, collect
	// every RAW fact (resources, process/media events, cache deltas) into
	// the canonical AttemptSnapshot and publish every registered sink.
	// Sink failures never fail the attempt: they are logged by the caller
	// (the result envelope is already complete).
	if s.pipeline != nil {
		_ = s.pipeline.Run(ctx)
	}
	return *s.result
}

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

func (s *AttemptTelemetrySession) observe(r *SampledResources, c cgroupUsage, p processCPUUsage, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r != nil && s.lastResources != nil && !s.lastResources.SampledAt.IsZero() {
		elapsed := now.Sub(s.lastResources.SampledAt).Seconds()
		var deltaUsec int64
		validCPU := false
		if s.cgroupRoot != "" {
			deltaUsec = c.CPUUsec - s.lastCgroup.CPUUsec
			validCPU = deltaUsec >= 0
		} else if p.valid && s.lastProcess.valid {
			deltaUsec = p.totalUsec() - s.lastProcess.totalUsec()
			validCPU = deltaUsec >= 0
		}
		if elapsed > 0 && validCPU {
			cpu := float64(deltaUsec) / (elapsed * 1e6) * 100
			if cpu > s.peakCPUPercent {
				s.peakCPUPercent = cpu
			}
		}
	}
	// memory.peak is a cgroup lifetime high-water mark and is not
	// attributable to this attempt. Track memory.current at each sample.
	if c.MemoryCurrent > s.peakRSSBytes {
		s.peakRSSBytes = c.MemoryCurrent
	}
	if r != nil {
		if r.ProcessRSSBytes > s.peakRSSBytes {
			s.peakRSSBytes = r.ProcessRSSBytes
		}
		s.peakOpenFDs = max64(s.peakOpenFDs, countOpenFDs())
		s.lastResources = r
	}
	s.lastCgroup = c
	s.lastProcess = p
	s.sampleCount++
}

func sampleProcessCPU() processCPUUsage {
	var self, children syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &self); err != nil {
		return processCPUUsage{}
	}
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &children); err != nil {
		return processCPUUsage{}
	}
	return processCPUUsage{
		userUsec:   timevalUsec(self.Utime) + timevalUsec(children.Utime),
		systemUsec: timevalUsec(self.Stime) + timevalUsec(children.Stime),
		valid:      true,
	}
}

func timevalUsec(tv syscall.Timeval) int64 {
	return tv.Sec*1_000_000 + tv.Usec
}

func (s *AttemptTelemetrySession) sampleResources(ctx context.Context) (*SampledResources, error) {
	if len(s.samplers) == 0 || s.samplers[0] == nil {
		return nil, nil
	}
	return s.samplers[0].Sample(ctx)
}

func (s *AttemptTelemetrySession) sampleCgroup() cgroupUsage {
	if s == nil || s.cgroupRoot == "" {
		return cgroupUsage{}
	}
	return readCgroupUsage(s.cgroupRoot)
}

func detectCgroupRoot() string {
	root := os.Getenv("VELOX_CGROUP_ROOT")
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	if _, err := os.Stat(filepath.Join(root, "cgroup.controllers")); err != nil {
		return ""
	}
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return root
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, "::", 2)
		if len(parts) == 2 {
			path := filepath.Join(root, parts[1])
			if _, err := os.Stat(filepath.Join(path, "cpu.stat")); err == nil {
				return path
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, "cpu.stat")); err == nil {
		return root
	}
	return ""
}

func readCgroupUsage(root string) cgroupUsage {
	var out cgroupUsage
	if data, err := os.ReadFile(filepath.Join(root, "cpu.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && f[0] == "usage_usec" {
				out.CPUUsec, _ = strconv.ParseInt(f[1], 10, 64)
			}
		}
	}
	readInt := func(name string) int64 {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return 0
		}
		n, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		return n
	}
	out.MemoryCurrent = readInt("memory.current")
	out.MemoryPeak = readInt("memory.peak")
	if data, err := os.ReadFile(filepath.Join(root, "io.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			for _, field := range strings.Fields(line) {
				p := strings.SplitN(field, "=", 2)
				if len(p) != 2 {
					continue
				}
				n, _ := strconv.ParseInt(p[1], 10, 64)
				if p[0] == "rbytes" {
					out.DiskReadBytes += n
				}
				if p[0] == "wbytes" {
					out.DiskWriteBytes += n
				}
			}
		}
	}
	return out
}

func cgroupIOAvailable(root string) bool {
	if root == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(root, "io.stat"))
	return err == nil
}

func (s *AttemptTelemetrySession) BeginPhase() PhaseResourceSnapshot { return s.StartPhase() }
func (s *AttemptTelemetrySession) EndPhaseResource(start PhaseResourceSnapshot) PhaseResourceDelta {
	return s.EndPhase(start)
}

func PhaseResourceMetadataJSON(d PhaseResourceDelta) string {
	data, _ := json.Marshal(map[string]interface{}{
		"telemetry_schema_version": AttemptTelemetrySchemaVersion,
		"peak_rss_bytes":           d.PeakRSSBytes,
		"disk_read_bytes":          d.DiskReadBytes,
		"disk_write_bytes":         d.DiskWriteBytes,
		"network_rx_bytes":         d.NetworkRxBytes,
		"network_tx_bytes":         d.NetworkTxBytes,
	})
	return string(data)
}

func positiveDelta(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func resourceCounter(r *SampledResources, f func(*SampledResources) int64) int64 {
	if r == nil {
		return 0
	}
	return f(r)
}
func resourcesRatio(r *SampledResources, f func(*SampledResources) float64) float64 {
	if r == nil {
		return 0
	}
	return f(r)
}
func safeRatio(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
func countOpenFDs() int64 {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return int64(len(entries))
}
