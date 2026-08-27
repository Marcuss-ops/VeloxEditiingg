package performance

// benchmark_runner.go owns the BenchmarkRunner (plan §9/§21): it runs a
// canonical fixture multiple times (with bounded concurrency), collects
// the run identity (git commit, worker id, host fingerprint, cache
// mode), evaluates the deterministic gate on every observation and
// produces ONE machine-readable BenchmarkRun JSON that future
// compare/CI tooling consumes — never a hand-rolled report.
//
// The actual render is delegated to a RenderRunner implementation: the
// production one drives pipeline.Runner → Assembler.Assemble → manifest
// once the fixture assets are registered; tests inject fakes. The
// runner itself is render-agnostic.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// RenderRunner executes ONE benchmark render of a fixture. Implementations
// return the receipt plus the runner-collected evidence the receipt
// cannot carry (artifact SHA, temp files).
type RenderRunner interface {
	Render(ctx context.Context, fixture BenchmarkFixture) (BenchmarkRenderResult, error)
}

// BenchmarkRenderResult is the outcome of one render.
type BenchmarkRenderResult struct {
	Receipt        *PerformanceReceiptV1
	ArtifactSHA256 string
	Evidence       GateEvidence
}

// BenchmarkRunObservation is ONE render observation inside a run.
type BenchmarkRunObservation struct {
	RunIndex       int                   `json:"run_index"`
	WallMS         int64                 `json:"wall_ms"`
	ArtifactSHA256 string                `json:"artifact_sha,omitempty"`
	Receipt        *PerformanceReceiptV1 `json:"receipt"`
	GateViolations []GateViolation       `json:"gate_violations,omitempty"`
	Error          string                `json:"error,omitempty"`
}

// RunSummary aggregates the successful observations. Percentiles use
// the nearest-rank method; an empty sample yields 0 (never NaN).
type RunSummary struct {
	WallMSP50             float64 `json:"wall_ms_p50"`
	WallMSP95             float64 `json:"wall_ms_p95"`
	AccountedRatioP50     float64 `json:"accounted_ratio_p50"`
	WriteAmplificationP50 float64 `json:"write_amplification_p50"`
	ProcessesPerClipP50   float64 `json:"processes_per_clip_p50"`
	// Output durability p95s (engine sidecar io_counters block,
	// publishAtomic): fsync of the partial output file, the atomic
	// rename, and the parent-directory fsync. After ~100 jobs the
	// file_fsync p95 decides whether durability work is justified
	// (low tens of ms → fine; seconds-range → intervene).
	FileFsyncMSP95      float64 `json:"file_fsync_ms_p95"`
	OutputRenameMSP95   float64 `json:"output_rename_ms_p95"`
	DirectoryFsyncMSP95 float64 `json:"directory_fsync_ms_p95"`
	GateFailures        int     `json:"gate_failures"`
	FailedRuns          int     `json:"failed_runs"`
}

// BenchmarkRun is the machine-readable report of one benchmark run
// (plan §21): identity + host fingerprint + cold/warm cache +
// concurrency + per-run receipts + deterministic artifact SHA +
// aggregated summary.
type BenchmarkRun struct {
	BenchmarkRunID  string                    `json:"benchmark_run_id"`
	FixtureID       BenchmarkFixtureID        `json:"fixture_id"`
	GitCommit       string                    `json:"git_commit"`
	Release         string                    `json:"release,omitempty"`
	EngineDigest    string                    `json:"engine_digest,omitempty"`
	WorkerID        string                    `json:"worker_id"`
	HostFingerprint string                    `json:"host_fingerprint"`
	Kernel          string                    `json:"kernel,omitempty"`
	CPUModel        string                    `json:"cpu_model,omitempty"`
	RAMBytes        int64                     `json:"ram_bytes,omitempty"`
	CacheMode       CacheMode                 `json:"cache_mode"`
	Concurrency     int                       `json:"concurrency"`
	Runs            int                       `json:"runs"`
	Receipts        []BenchmarkRunObservation `json:"receipts"`
	ArtifactSHA256  string                    `json:"artifact_sha,omitempty"`
	Summary         RunSummary                `json:"summary"`
	CreatedAt       time.Time                 `json:"created_at"`
}

// BenchmarkRunner runs a fixture `Runs` times with bounded concurrency
// and assembles the machine-readable BenchmarkRun. All fields are
// optional except Renderer; zero values get sensible fallbacks
// (worker_id → hostname, git_commit → "unknown", concurrency → 1).
type BenchmarkRunner struct {
	Fixture      BenchmarkFixture
	Runs         int
	Concurrency  int
	CacheMode    CacheMode
	WorkerID     string
	GitCommit    string
	Release      string
	EngineDigest string
	Renderer     RenderRunner
}

// Run executes the fixture `Runs` times (bounded by Concurrency),
// collects identity + host fingerprint and returns the report. It fails
// only when EVERY render fails; partial failures are recorded per
// observation and summarized.
func (r *BenchmarkRunner) Run(ctx context.Context) (*BenchmarkRun, error) {
	if r.Renderer == nil {
		return nil, fmt.Errorf("benchmark runner: no RenderRunner configured")
	}
	runs := max(r.Runs, 1)
	concurrency := max(r.Concurrency, 1)
	if concurrency > runs {
		concurrency = runs
	}
	workerID := r.WorkerID
	if workerID == "" {
		workerID, _ = os.Hostname()
	}
	host := collectHostIdentity()

	run := &BenchmarkRun{
		BenchmarkRunID:  newBenchmarkRunID(),
		FixtureID:       r.Fixture.ID,
		GitCommit:       orUnknown(r.GitCommit),
		Release:         r.Release,
		EngineDigest:    r.EngineDigest,
		WorkerID:        workerID,
		HostFingerprint: host.Fingerprint,
		Kernel:          host.Kernel,
		CPUModel:        host.CPUModel,
		RAMBytes:        host.RAMBytes,
		CacheMode:       r.CacheMode,
		Concurrency:     concurrency,
		Runs:            runs,
		CreatedAt:       time.Now().UTC(),
	}

	observations := make([]BenchmarkRunObservation, runs)
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			obs := BenchmarkRunObservation{RunIndex: idx}
			res, err := r.Renderer.Render(ctx, r.Fixture)
			if err != nil {
				obs.Error = err.Error()
			} else {
				if res.Evidence.ArtifactSHA256 != "" {
					obs.ArtifactSHA256 = res.Evidence.ArtifactSHA256
				} else {
					obs.ArtifactSHA256 = res.ArtifactSHA256
				}
				if res.Receipt != nil {
					obs.WallMS = res.Receipt.Timing.WallMs
					obs.Receipt = res.Receipt
					obs.GateViolations = CheckFixtureGate(r.Fixture, res.Receipt, res.Evidence)
				}
			}
			mu.Lock()
			observations[idx] = obs
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	run.Receipts = observations
	run.Summary = summarizeObservations(observations)
	run.ArtifactSHA256 = deterministicArtifactSHA(observations)
	if run.Summary.FailedRuns == runs {
		return nil, fmt.Errorf("benchmark run %s: all %d renders failed", run.BenchmarkRunID, runs)
	}
	return run, nil
}

// hostIdentity is the machine identity collected for the run report.
type hostIdentity struct {
	Fingerprint string
	Kernel      string
	CPUModel    string
	RAMBytes    int64
}

// collectHostIdentity gathers a stable host fingerprint (SHA-256 of
// machine-id | hostname | kernel | cpu model | ram) plus the human
// fields. Every source has a fallback so the runner never fails on an
// unreadable /proc.
func collectHostIdentity() hostIdentity {
	machineID := readFirstLine("/etc/machine-id")
	hostname, _ := os.Hostname()
	kernel := runtime.GOOS + "/" + runtime.GOARCH
	cpuModel := firstCPUModel()
	ramBytes := totalRAMBytes()

	fp := sha256.Sum256([]byte(strings.Join([]string{machineID, hostname, kernel, cpuModel, fmt.Sprintf("%d", ramBytes)}, "|")))
	return hostIdentity{
		Fingerprint: hex.EncodeToString(fp[:]),
		Kernel:      kernel,
		CPUModel:    cpuModel,
		RAMBytes:    ramBytes,
	}
}

func readFirstLine(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	if idx := strings.IndexByte(string(data), '\n'); idx >= 0 {
		return strings.TrimSpace(string(data[:idx]))
	}
	return strings.TrimSpace(string(data))
}

// firstCPUModel extracts the first "model name" line from /proc/cpuinfo.
func firstCPUModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "model name") {
			if idx := strings.Index(line, ":"); idx >= 0 {
				return strings.TrimSpace(line[idx+1:])
			}
		}
	}
	return ""
}

// totalRAMBytes parses MemTotal from /proc/meminfo (kB) into bytes.
func totalRAMBytes() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			var kb int64
			if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &kb); err == nil {
				return kb * 1024
			}
		}
	}
	return 0
}

// summarizeObservations computes the aggregated summary over the
// successful observations.
func summarizeObservations(obs []BenchmarkRunObservation) RunSummary {
	var walls, accounted, writeAmp, ppc []float64
	var fsync, rename, dirFsync []float64
	var gateFailures, failed int
	for _, o := range obs {
		if o.Error != "" {
			failed++
			continue
		}
		if o.Receipt == nil {
			continue
		}
		walls = append(walls, float64(o.WallMS))
		accounted = append(accounted, o.Receipt.Derived.AccountedRatio)
		writeAmp = append(writeAmp, o.Receipt.Derived.WriteAmplification)
		ppc = append(ppc, o.Receipt.Derived.ProcessesPerClip)
		// Output durability timings (sidecar io_counters block); zero on
		// engines that predate the block.
		fsync = append(fsync, float64(o.Receipt.IO.FileFsyncMS))
		rename = append(rename, float64(o.Receipt.IO.OutputRenameMS))
		dirFsync = append(dirFsync, float64(o.Receipt.IO.DirectoryFsyncMS))
		gateFailures += len(o.GateViolations)
	}
	return RunSummary{
		WallMSP50:             percentile(walls, 0.50),
		WallMSP95:             percentile(walls, 0.95),
		AccountedRatioP50:     percentile(accounted, 0.50),
		WriteAmplificationP50: percentile(writeAmp, 0.50),
		ProcessesPerClipP50:   percentile(ppc, 0.50),
		FileFsyncMSP95:        percentile(fsync, 0.95),
		OutputRenameMSP95:     percentile(rename, 0.95),
		DirectoryFsyncMSP95:   percentile(dirFsync, 0.95),
		GateFailures:          gateFailures,
		FailedRuns:            failed,
	}
}

// percentile returns the nearest-rank percentile of a sorted copy of
// the sample; an empty sample yields 0 (never NaN).
func percentile(sample []float64, p float64) float64 {
	if len(sample) == 0 {
		return 0
	}
	sorted := append([]float64(nil), sample...)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// deterministicArtifactSHA returns the artifact SHA when EVERY
// successful observation produced the same non-empty SHA (deterministic
// fixture); an empty or divergent SHA yields "" — the determinism
// invariant is broken and the gate (with pinned SHAs) would flag it.
func deterministicArtifactSHA(obs []BenchmarkRunObservation) string {
	var sha string
	for _, o := range obs {
		if o.ArtifactSHA256 == "" {
			continue
		}
		if sha == "" {
			sha = o.ArtifactSHA256
			continue
		}
		if sha != o.ArtifactSHA256 {
			return ""
		}
	}
	return sha
}

// newBenchmarkRunID returns a random benchmark run identifier.
func newBenchmarkRunID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	return "bench-" + hex.EncodeToString(b)
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "unknown"
	}
	return s
}
