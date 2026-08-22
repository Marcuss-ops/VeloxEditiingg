// gpu_sampler.go — background GPU metrics sampler.
//
// GPUSampler runs a background goroutine that periodically samples GPU
// utilization, NVDEC/NVENC engine utilization, and VRAM usage via nvidia-smi.
// It is designed to run for the duration of a single job and produces
// aggregate statistics (average, peak) when stopped.
//
// Usage:
//
//	sampler := NewGPUSampler(ctx, 100*time.Millisecond)
//	sampler.Start()
//	defer sampler.Stop()
//	// ... run job ...
//	stats := sampler.Stats()
//
// Thread-safety: Start/Stop/Stats may be called from any goroutine.
package telemetry

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// GPUSample is one instantaneous GPU measurement.
type GPUSample struct {
	Timestamp   time.Time
	GPUUtilPct  float64 // GPU utilization 0-100
	NVDECUtilPct float64 // NVDEC engine utilization 0-100
	NVENCUtilPct float64 // NVENC engine utilization 0-100
	VRAMUsedBytes int64  // VRAM used in bytes
	VRAMTotalBytes int64 // VRAM total in bytes
}

// GPUStats aggregates samples into summary statistics.
type GPUStats struct {
	SampleCount int64

	GPUUtilAvgPct   float64
	GPUUtilPeakPct  float64
	NVDECUtilAvgPct  float64
	NVDECUtilPeakPct float64
	NVENCUtilAvgPct  float64
	NVENCUtilPeakPct float64

	VRAMUsedAvgBytes  int64
	VRAMUsedPeakBytes int64
	VRAMTotalBytes    int64

	GPUIdleDuringRenderMs int64 // accumulated time GPU was < 5% utilized
}

// GPUSampler polls nvidia-smi at a configurable interval and aggregates
// results. It is scoped to one job: call Start() at job beginning and
// Stop() at job end.
type GPUSampler struct {
	ctx       context.Context
	cancel    context.CancelFunc
	interval  time.Duration
	started   atomic.Bool

	mu        sync.Mutex
	samples   []GPUSample
	idleTime  time.Duration
	lastAbove time.Time // last time GPU was above idle threshold
}

// NewGPUSampler creates a sampler backed by the parent context. The parent
// cancellation also stops the sampler. interval is how often to poll nvidia-smi;
// a reasonable default is 500ms-1s.
func NewGPUSampler(parent context.Context, interval time.Duration) *GPUSampler {
	ctx, cancel := context.WithCancel(parent)
	return &GPUSampler{
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}

// Start begins background sampling. It is a no-op if already started.
func (s *GPUSampler) Start() {
	if s == nil {
		return
	}
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	go s.loop()
}

// Stop halts background sampling. It is safe to call multiple times.
func (s *GPUSampler) Stop() {
	if s == nil {
		return
	}
	s.cancel()
}

// Stats returns aggregate GPU statistics. Safe to call at any time; after
// Stop() it returns the final summary.
func (s *GPUSampler) Stats() GPUStats {
	if s == nil {
		return GPUStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.computeStatsLocked()
}

// Snapshot returns a defensive copy of all raw samples collected so far.
func (s *GPUSampler) Snapshot() []GPUSample {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]GPUSample, len(s.samples))
	copy(out, s.samples)
	return out
}

// ── internal loop ──────────────────────────────────────────────────────────

const idleThresholdPct = 5.0

func (s *GPUSampler) loop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Take an immediate first sample.
	s.sample()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.sample()
		}
	}
}

func (s *GPUSampler) sample() {
	gpu, err := queryNVidiaSMI()
	if err != nil {
		// Silently skip failed samples; the Stats will report 0 counts.
		return
	}
	s.mu.Lock()
	s.samples = append(s.samples, gpu)
	if gpu.GPUUtilPct < idleThresholdPct {
		// Track idle time: if we were previously above threshold, finalize
		// that active interval; we'll account idle at Stats time.
	} else {
		if s.lastAbove.IsZero() {
			s.lastAbove = gpu.Timestamp
		}
	}
	s.mu.Unlock()
}

func (s *GPUSampler) computeStatsLocked() GPUStats {
	n := int64(len(s.samples))
	if n == 0 {
		return GPUStats{SampleCount: 0}
	}

	var (
		sumGPU, sumNVDEC, sumNVENC float64
		peakGPU, peakNVDEC, peakNVENC float64
		sumVRAM, peakVRAM int64
		totalVRAM          int64
		idleAccum           time.Duration
	)

	for i, sample := range s.samples {
		sumGPU += sample.GPUUtilPct
		sumNVDEC += sample.NVDECUtilPct
		sumNVENC += sample.NVENCUtilPct

		if sample.GPUUtilPct > peakGPU {
			peakGPU = sample.GPUUtilPct
		}
		if sample.NVDECUtilPct > peakNVDEC {
			peakNVDEC = sample.NVDECUtilPct
		}
		if sample.NVENCUtilPct > peakNVENC {
			peakNVENC = sample.NVENCUtilPct
		}

		sumVRAM += sample.VRAMUsedBytes
		if sample.VRAMUsedBytes > peakVRAM {
			peakVRAM = sample.VRAMUsedBytes
		}
		totalVRAM = sample.VRAMTotalBytes

		// Idle tracking: accumulate gaps where GPU is < threshold.
		if sample.GPUUtilPct < idleThresholdPct {
			if i > 0 {
				idleAccum += sample.Timestamp.Sub(s.samples[i-1].Timestamp)
			}
		}
	}

	avgGPU := sumGPU / float64(n)
	avgNVDEC := sumNVDEC / float64(n)
	avgNVENC := sumNVENC / float64(n)
	avgVRAM := sumVRAM / n

	return GPUStats{
		SampleCount:        n,
		GPUUtilAvgPct:      round2(avgGPU),
		GPUUtilPeakPct:     round2(peakGPU),
		NVDECUtilAvgPct:    round2(avgNVDEC),
		NVDECUtilPeakPct:   round2(peakNVDEC),
		NVENCUtilAvgPct:    round2(avgNVENC),
		NVENCUtilPeakPct:   round2(peakNVENC),
		VRAMUsedAvgBytes:   avgVRAM,
		VRAMUsedPeakBytes:  peakVRAM,
		VRAMTotalBytes:     totalVRAM,
		GPUIdleDuringRenderMs: idleAccum.Milliseconds(),
	}
}

// ── nvidia-smi query ────────────────────────────────────────────────────────

// queryNVidiaSMI runs `nvidia-smi --query-gpu=... --format=csv,noheader`
// and parses a single sample. Returns zero GPUSample on error.
func queryNVidiaSMI() (GPUSample, error) {
	// nvidia-smi --query-gpu=utilization.gpu,utilization.decoder,utilization.encoder,memory.used,memory.total --format=csv,noheader,nounits
	cmd := exec.Command("nvidia-smi",
		"--query-gpu=utilization.gpu,utilization.decoder,utilization.encoder,memory.used,memory.total",
		"--format=csv,noheader,nounits",
	)
	out, err := cmd.Output()
	if err != nil {
		return GPUSample{}, err
	}
	return parseNVidiaSMIOutput(strings.TrimSpace(string(out)))
}

// parseNVidiaSMIOutput parses a single CSV line like: "31, 14, 27, 3210, 16384"
// Returns zero GPUSample on parse failure.
func parseNVidiaSMIOutput(line string) (GPUSample, error) {
	if line == "" {
		return GPUSample{}, nil
	}
	// Take only the first GPU line if multiple GPUs.
	if idx := strings.Index(line, "\n"); idx >= 0 {
		line = line[:idx]
	}

	fields := strings.Split(line, ",")
	if len(fields) < 5 {
		return GPUSample{}, nil
	}

	values := make([]int64, 5)
	for i := 0; i < 5; i++ {
		v, err := strconv.ParseInt(strings.TrimSpace(fields[i]), 10, 64)
		if err != nil {
			return GPUSample{}, err
		}
		values[i] = v
	}

	return GPUSample{
		Timestamp:      time.Now(),
		GPUUtilPct:      float64(values[0]),
		NVDECUtilPct:    float64(values[1]),
		NVENCUtilPct:    float64(values[2]),
		VRAMUsedBytes:   values[3] * 1024 * 1024, // MiB → bytes
		VRAMTotalBytes:  values[4] * 1024 * 1024,
	}, nil
}

// IsGPUAvailable returns true if nvidia-smi is present on the system.
func IsGPUAvailable() bool {
	_, err := exec.LookPath("nvidia-smi")
	return err == nil
}

func round2(v float64) float64 {
	// Two-decimal precision without importing math.
	return float64(int64(v*100+0.5)) / 100
}