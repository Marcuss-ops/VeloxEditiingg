package performance

// Remote renderer for the production capacity benchmark.  The Master owns
// orchestration, while the worker image owns the native renderer and fixture
// tools.  The command boundary is deliberately tiny and accepts only fixed
// arguments so the fleet SSH client's shell validation remains effective.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RemoteCommandRunner is the narrow fleet boundary needed by the benchmark.
// fleet.BackendSSHClient satisfies it without importing fleet here.
type RemoteCommandRunner interface {
	Run(ctx context.Context, workerID, command string) (string, error)
}

// RemoteWorkerRenderer runs the real worker-side benchmark binary. It never
// uses the worker stub renderer and fails closed when the toolchain is absent.
type RemoteWorkerRenderer struct {
	SSH       RemoteCommandRunner
	Container string
	Root      string

	mu       sync.Mutex
	prepared map[string]bool
	seq      uint64
}

func NewRemoteWorkerRenderer(ssh RemoteCommandRunner) *RemoteWorkerRenderer {
	return &RemoteWorkerRenderer{SSH: ssh, Container: "velox-worker", Root: "/var/lib/velox-worker/benchmark-tracks", prepared: make(map[string]bool)}
}

func (r *RemoteWorkerRenderer) Render(ctx context.Context, req BenchmarkRenderRequest) (BenchmarkRenderResult, error) {
	if r == nil || r.SSH == nil {
		return BenchmarkRenderResult{}, fmt.Errorf("remote benchmark renderer is not configured")
	}
	if strings.TrimSpace(req.WorkerID) == "" || strings.TrimSpace(string(req.Fixture.ID)) == "" {
		return BenchmarkRenderResult{}, fmt.Errorf("remote benchmark requires worker_id and fixture")
	}
	if err := r.prepare(ctx, req.WorkerID, string(req.Fixture.ID)); err != nil {
		return BenchmarkRenderResult{}, err
	}

	r.mu.Lock()
	r.seq++
	seq := r.seq
	r.mu.Unlock()
	outPath := filepath.Join(r.Root, string(req.Fixture.ID), fmt.Sprintf("master-%d.json", seq))
	trackDir := filepath.Join(r.Root, string(req.Fixture.ID))
	cache := "warm"
	if req.CacheMode == CacheModeCold {
		cache = "cold"
	}
	cmd := r.exec("/usr/local/bin/velox-benchmark", "-fixture", string(req.Fixture.ID), "-runs", "1", "-concurrency", "1", "-cache", cache, "-worker-id", req.WorkerID, "-track-dir", trackDir, "-out", outPath)
	started := time.Now()
	if _, err := r.SSH.Run(ctx, req.WorkerID, cmd); err != nil {
		return BenchmarkRenderResult{}, fmt.Errorf("remote benchmark render: %w", err)
	}
	runJSON, err := r.SSH.Run(ctx, req.WorkerID, r.exec("/bin/cat", outPath))
	if err != nil {
		return BenchmarkRenderResult{}, fmt.Errorf("read remote benchmark receipt: %w", err)
	}
	var run struct {
		ArtifactSHA256 string `json:"artifact_sha"`
		Receipts       []struct {
			WallMS  int64 `json:"wall_ms"`
			Receipt struct {
				Timing struct {
					WallMS   int64 `json:"wall_ms"`
					RenderMS int64 `json:"render_ms"`
				} `json:"timing"`
				Memory struct {
					PeakRSSBytes int64 `json:"peak_rss_bytes"`
				} `json:"memory"`
			} `json:"receipt"`
		} `json:"receipts"`
	}
	if err := json.Unmarshal([]byte(runJSON), &run); err != nil {
		return BenchmarkRenderResult{}, fmt.Errorf("parse remote benchmark receipt: %w", err)
	}
	if len(run.Receipts) == 0 {
		return BenchmarkRenderResult{}, fmt.Errorf("remote benchmark receipt contains no observations")
	}
	obs := run.Receipts[0]
	wall := obs.WallMS
	if wall == 0 {
		wall = obs.Receipt.Timing.WallMS
	}
	if wall == 0 {
		wall = time.Since(started).Milliseconds()
	}
	return BenchmarkRenderResult{Receipt: &BenchmarkRenderReceipt{
		PeakRAMBytes:   obs.Receipt.Memory.PeakRSSBytes,
		RenderWallMS:   obs.Receipt.Timing.RenderMS,
		WallMS:         wall,
		ArtifactSHA256: run.ArtifactSHA256,
	}, ArtifactSHA256: run.ArtifactSHA256}, nil
}

func (r *RemoteWorkerRenderer) prepare(ctx context.Context, workerID, fixture string) error {
	key := workerID + "\x00" + fixture
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prepared[key] {
		return nil
	}
	trackDir := filepath.Join(r.Root, fixture)
	if _, err := r.SSH.Run(ctx, workerID, r.exec("/usr/local/bin/velox-fixture-gen", "-out-dir", trackDir)); err != nil {
		return fmt.Errorf("prepare fixture %s: %w", fixture, err)
	}
	manifest := filepath.Join(trackDir, "manifest.json")
	if _, err := r.SSH.Run(ctx, workerID, r.exec("/usr/local/bin/velox-fixture-gen", "-verify-manifest", manifest)); err != nil {
		return fmt.Errorf("verify fixture %s: %w", fixture, err)
	}
	r.prepared[key] = true
	return nil
}

func (r *RemoteWorkerRenderer) exec(binary string, args ...string) string {
	container := r.Container
	if container == "" {
		container = "velox-worker"
	}
	parts := []string{"sudo", "-n", "docker", "exec", "--user", "velox", container, binary}
	parts = append(parts, args...)
	return strings.Join(parts, " ")
}
