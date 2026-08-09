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
	lastResources  *SampledResources
	lastCgroup     cgroupUsage
	peakCPUPercent float64
	peakRSSBytes   int64
	peakOpenFDs    int64
	sampleCount    int
	mu             sync.Mutex
	stopOnce       sync.Once
	done           chan struct{}
	wg             sync.WaitGroup
	result         *AttemptTelemetry
}

type AttemptTelemetry struct {
	Metrics     TypedExecutionMetrics
	Coverage    map[string]bool
	Complete    bool
	StartedAt   time.Time
	CompletedAt time.Time
}

type PhaseResourceSnapshot struct {
	resources *SampledResources
	cgroup    cgroupUsage
	at        time.Time
}

// NewAttemptTelemetrySession creates a session. It is safe to pass nil when a
// worker was constructed by a legacy/unit-test path without a sampler.
func NewAttemptTelemetrySession(host *Sampler) *AttemptTelemetrySession {
	clone := host.CloneForAttempt()
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
	s.startedAt = time.Now().UTC()
	s.startResources, _ = s.sampleResources(ctx)
	s.startCgroup = s.sampleCgroup()
	s.lastResources = s.startResources
	s.lastCgroup = s.startCgroup
	s.observe(s.startResources, s.startCgroup, s.startedAt)
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
				s.observe(resources, s.sampleCgroup(), now)
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
	s.observe(resources, cgroup, end)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result != nil {
		return *s.result
	}
	wall := end.Sub(s.startedAt).Seconds()
	if wall < 0 {
		wall = 0
	}
	metrics := TypedExecutionMetrics{
		CpuTimeMs:        positiveDelta(cgroup.CPUUsec-s.startCgroup.CPUUsec) / 1000,
		PeakRssBytes:     s.peakRSSBytes,
		CpuPercentPeak:   s.peakCPUPercent,
		WallClockSeconds: wall,
		DiskReadBytes:    positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.DiskReadBytesTotal }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.DiskReadBytesTotal })),
		DiskWriteBytes:   positiveDelta(resourceCounter(resources, func(r *SampledResources) int64 { return r.DiskWriteBytesTotal }) - resourceCounter(s.startResources, func(r *SampledResources) int64 { return r.DiskWriteBytesTotal })),
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
		"cpu":          s.startCgroup.CPUUsec > 0 && cgroup.CPUUsec >= s.startCgroup.CPUUsec,
		"memory":       s.startCgroup.MemoryCurrent > 0 || cgroup.MemoryCurrent > 0 || s.peakRSSBytes > 0,
		"disk":         resources != nil && s.startResources != nil,
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
	s.result = &AttemptTelemetry{Metrics: metrics, Coverage: coverage, Complete: coverage["cpu"] && coverage["memory"] && coverage["disk"] && coverage["network"], StartedAt: s.startedAt, CompletedAt: end}
	return *s.result
}

// ApplyToMap is the single worker→report bridge. It writes canonical dotted
// keys and explicit coverage; consumers must not interpret absent metrics as 0.
func (s *AttemptTelemetrySession) ApplyToMap(m map[string]interface{}, result AttemptTelemetry) {
	if m == nil {
		return
	}
	t := result.Metrics
	m["cpu.ms"] = t.CpuTimeMs
	m["rss.peak.bytes"] = t.PeakRssBytes
	m["cpu.percent.peak"] = t.CpuPercentPeak
	m["disk.read.bytes"] = t.DiskReadBytes
	m["disk.write.bytes"] = t.DiskWriteBytes
	m["network.rx.bytes"] = t.NetworkRxBytes
	m["network.tx.bytes"] = t.NetworkTxBytes
	m["temp.bytes.written"] = t.TempBytesWritten
	m["iowait.ms"] = t.IowaitMs
	m["open.fds.peak"] = t.OpenFdsPeak
	m["wall.clock.seconds"] = t.WallClockSeconds
	m["telemetry.schema.version"] = AttemptTelemetrySchemaVersion
	coverage, _ := json.Marshal(result.Coverage)
	m["telemetry.coverage.json"] = string(coverage)
	m["telemetry.complete"] = result.Complete
	m["telemetry.cpu.source"] = t.TelemetryCPUSource
}

func (s *AttemptTelemetrySession) StartPhase() PhaseResourceSnapshot {
	if s == nil {
		return PhaseResourceSnapshot{at: time.Now().UTC()}
	}
	r, _ := s.sampleResources(context.Background())
	return PhaseResourceSnapshot{resources: r, cgroup: s.sampleCgroup(), at: time.Now().UTC()}
}

func (s *AttemptTelemetrySession) EndPhase(start PhaseResourceSnapshot) PhaseResourceDelta {
	if s == nil {
		return PhaseResourceDelta{}
	}
	r, _ := s.sampleResources(context.Background())
	c := s.sampleCgroup()
	return PhaseResourceDelta{
		CPUTimeMs:      positiveDelta(c.CPUUsec-start.cgroup.CPUUsec) / 1000,
		PeakRSSBytes:   max64(start.cgroup.MemoryCurrent, c.MemoryCurrent),
		DiskReadBytes:  positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.DiskReadBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.DiskReadBytesTotal })),
		DiskWriteBytes: positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.DiskWriteBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.DiskWriteBytesTotal })),
		NetworkRxBytes: positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.NetworkReceiveBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.NetworkReceiveBytesTotal })),
		NetworkTxBytes: positiveDelta(resourceCounter(r, func(v *SampledResources) int64 { return v.NetworkTransmitBytesTotal }) - resourceCounter(start.resources, func(v *SampledResources) int64 { return v.NetworkTransmitBytesTotal })),
	}
}

func (s *AttemptTelemetrySession) observe(r *SampledResources, c cgroupUsage, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r != nil && s.lastResources != nil && !s.lastResources.SampledAt.IsZero() {
		elapsed := now.Sub(s.lastResources.SampledAt).Seconds()
		if elapsed > 0 && c.CPUUsec >= s.lastCgroup.CPUUsec {
			cpu := float64(c.CPUUsec-s.lastCgroup.CPUUsec) / (elapsed * 1e6) * 100
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
	s.sampleCount++
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
