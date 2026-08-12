package performance

// benchmark_renderer.go owns the production RenderRunner for the benchmark
// loop (plan §9/§21). The copy-only fixture drives the zero-spawn C++ backend;
// the complex fixture drives the existing V1 render path as an explicit
// baseline. Both assemble the receipt from the engine sidecar.
//
// Zero-spawn contract enforced here (and re-asserted by the tier-1 gate
// CheckFixtureGate): no ffmpeg/ffprobe/shell execs, no cache-to-tmp
// materialization, no temp segment files — the engine opens the clip
// files in place and writes exactly one artifact.
//
// KNOWN LIMITATION (honesty note): on sub-second renders the /proc
// process-tree sampler can miss the engine's I/O window, leaving
// TotalBytesRead/TotalBytesWritten (and therefore the amplification
// KPIs) at 0 = "not measured" — the receipt contract treats a zero
// amplification as not-measured, never as a measured 1.0x, and the
// tier-2 gate skips unmeasured KPIs. The engine-declared sidecar
// counters (mux bytes, total_size, asset bytes) stay authoritative;
// closing the sampler gap for very fast renders is a sampler-layer
// follow-up, not a receipt-schema change.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/services/native"
)

// NativeRendererConfig configures the production benchmark renderer.
type NativeRendererConfig struct {
	// TrackDir holds the generated fixture track: clip_*.mp4 +
	// final_audio.m4a + manifest.json (velox-fixture-gen output). It is
	// verified against the pinned fixture spec before EVERY render.
	TrackDir string
	// WorkDir holds per-render output subdirectories; the evidence sweep
	// (unexpected temp files) runs inside each render's own subdirectory
	// so concurrent renders never pollute each other. Empty = system temp.
	WorkDir string
	// BinaryPath pins the velox_video_engine binary; empty resolves via
	// the standard resolver (VELOX_VIDEO_ENGINE_CPP_BIN, /usr/local/bin).
	BinaryPath string
	// Logger is optional; a warn-level stderr logger is the fallback.
	Logger *logger.Logger
	// WorkerID / GitCommit stamp the receipt identity (the same values
	// the BenchmarkRunner records on the run report).
	WorkerID  string
	GitCommit string
}

// NativeRenderer is the production RenderRunner: fixture track →
// frame-exact V2 plan → zero-spawn engine → sidecar → receipt.
type NativeRenderer struct {
	client       *native.RenderClient
	trackDir     string
	workDir      string
	engineSHA256 string
	workerID     string
	gitCommit    string
}

// NewNativeRenderer builds the production renderer. It fails fast when
// the track directory has no manifest or the engine binary is missing —
// a benchmark must never silently degrade to a stub.
func NewNativeRenderer(cfg NativeRendererConfig) (*NativeRenderer, error) {
	if strings.TrimSpace(cfg.TrackDir) == "" {
		return nil, errors.New("native renderer: TrackDir is required")
	}
	if _, err := os.Stat(filepath.Join(cfg.TrackDir, "manifest.json")); err != nil {
		return nil, fmt.Errorf("native renderer: track manifest: %w", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.New(logger.WarnLevel, os.Stderr)
	}
	var client *native.RenderClient
	var err error
	if strings.TrimSpace(cfg.BinaryPath) != "" {
		client, err = native.NewRenderClientWithBinary(cfg.BinaryPath, cfg.Logger)
	} else {
		client, err = native.NewRenderClient(cfg.Logger)
	}
	if err != nil {
		return nil, fmt.Errorf("native renderer: engine: %w", err)
	}
	workDir := strings.TrimSpace(cfg.WorkDir)
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return nil, fmt.Errorf("native renderer: work dir: %w", err)
	}
	workerID := cfg.WorkerID
	if workerID == "" {
		workerID, _ = os.Hostname()
	}
	return &NativeRenderer{
		client:       client,
		trackDir:     cfg.TrackDir,
		workDir:      workDir,
		engineSHA256: sha256File(client.BinaryPath()),
		workerID:     workerID,
		gitCommit:    cfg.GitCommit,
	}, nil
}

// Render executes ONE zero-spawn render of the fixture and returns the
// receipt plus the runner-collected evidence (artifact SHA, temp-file
// sweep). It never falls back to a stub: an invalid track or a failed
// engine render is an error.
func (r *NativeRenderer) Render(ctx context.Context, fixture BenchmarkFixture) (BenchmarkRenderResult, error) {
	var result BenchmarkRenderResult
	if r == nil || r.client == nil {
		return result, errors.New("native renderer: not configured")
	}
	if fixture.ID == FixtureComplexCanonical5MV1 {
		return r.renderComplex(ctx, fixture)
	}
	// The copy-only renderer drives the canonical V2 track only: the track
	// dir + manifest are defined by CanonicalFixtureSpecV1. Complex V1 is
	// dispatched above, and any other fixture fails closed here.
	if fixture.ID != FixtureCopyOnlyCanonical5MV1 {
		return result, fmt.Errorf("native renderer: only the canonical fixture %s can be rendered over a track dir, got %s", FixtureCopyOnlyCanonical5MV1, fixture.ID)
	}

	// 1. The track must match the pinned fixture (Formula 1 contract): a
	// manifest whose spec digest does not match the fixture's AssetSHA256
	// was built from a different track definition — reject it instead of
	// benchmarking apples against oranges.
	spec := CanonicalFixtureSpecV1()
	manifest, err := LoadFixtureManifest(filepath.Join(r.trackDir, "manifest.json"))
	if err != nil {
		return result, fmt.Errorf("native renderer: %w", err)
	}
	if problems := ValidateManifest(*manifest, fixture, spec); len(problems) > 0 {
		return result, fmt.Errorf("native renderer: fixture track invalid: %s", strings.Join(problems, "; "))
	}
	if fixture.AssetSHA256 != "" && !manifest.SpecMatches(fixture.AssetSHA256) {
		return result, fmt.Errorf("native renderer: track spec digest %s does not match pinned fixture %s", manifest.SpecSHA256, fixture.AssetSHA256)
	}

	// 2. Per-render isolated subdir: the output artifact and the evidence
	// sweep never see another concurrent render's files.
	runDir, err := os.MkdirTemp(r.workDir, "bench-render-*")
	if err != nil {
		return result, fmt.Errorf("native renderer: run dir: %w", err)
	}
	defer os.RemoveAll(runDir)
	outPath := filepath.Join(runDir, "out.mp4")

	jobID := "bench-" + newBenchmarkRunID()
	doc, err := BuildCopyOnlyPlanV2(spec, manifest, r.trackDir)
	if err != nil {
		return result, fmt.Errorf("native renderer: %w", err)
	}
	doc.JobID = jobID
	doc.OutputPath = outPath
	planJSON, err := doc.MarshalJSON()
	if err != nil {
		return result, fmt.Errorf("native renderer: %w", err)
	}

	// 3. The zero-spawn render: ONE engine process, in-process
	// libavformat packet pipeline, atomic output. No external execs — the
	// tier-1 gate asserts the execve/encode/decode/temp invariants.
	renderMetrics, err := r.client.RenderCompiledPlanV2(ctx, planJSON, outPath)
	if err != nil {
		return result, fmt.Errorf("native renderer: %w", err)
	}

	// 4. Artifact identity + evidence.
	evidence := sweepRunDir(runDir, outPath)
	evidence.ArtifactSHA256 = sha256File(outPath)

	// 5. Assemble the canonical receipt from the sidecar telemetry.
	identity := PerformanceIdentity{
		JobID:              jobID,
		WorkerID:           r.workerID,
		GitCommit:          r.gitCommit,
		EngineSHA256:       r.engineSHA256,
		BenchmarkFixtureID: string(fixture.ID),
	}
	workload := WorkloadFromCompiledRenderPlan(doc.Plan)
	workload.JobType = string(fixture.Kind)
	workload.CopyOnly = fixture.Kind == FixtureKindCopyOnly || fixture.Kind == FixtureKindFinalAudio

	receipt := NewAssembler().Assemble(
		pipeline.RunMetrics{RenderMetrics: renderMetrics, TotalMs: renderMetrics.TotalMs},
		AssemblyContext{
			Identity:         identity,
			Workload:         workload,
			CPUWallMS:        renderMetrics.CPUUserMs + renderMetrics.CPUSystemMs,
			UsefulPipelineMS: UsefulPipelineMSFromRenderMetrics(renderMetrics),
		},
	)
	return BenchmarkRenderResult{Receipt: receipt, ArtifactSHA256: evidence.ArtifactSHA256, Evidence: evidence}, nil
}

// renderComplex runs the canonical V1 complex path. It intentionally keeps
// the existing segment-by-segment FFmpeg implementation as the baseline: the
// receipt exposes the exact segment, phase, process, audio and CPU facts that
// the next optimization must improve. This path must never silently fall back
// to the copy-only V2 renderer.
func (r *NativeRenderer) renderComplex(ctx context.Context, fixture BenchmarkFixture) (BenchmarkRenderResult, error) {
	var result BenchmarkRenderResult
	spec := ComplexCanonicalFixtureSpecV1()
	manifest, err := LoadComplexFixtureManifest(filepath.Join(r.trackDir, ComplexFixtureManifestName))
	if err != nil {
		return result, fmt.Errorf("complex renderer: %w", err)
	}
	if problems := ValidateComplexManifest(*manifest, fixture, spec); len(problems) > 0 {
		return result, fmt.Errorf("complex renderer: fixture track invalid: %s", strings.Join(problems, "; "))
	}
	runDir, err := os.MkdirTemp(r.workDir, "bench-complex-render-*")
	if err != nil {
		return result, fmt.Errorf("complex renderer: run dir: %w", err)
	}
	defer os.RemoveAll(runDir)
	outPath := filepath.Join(runDir, "out.mp4")
	jobID := "bench-" + newBenchmarkRunID()
	p, err := BuildComplexRenderPlanV1(spec, *manifest, r.trackDir, jobID, outPath)
	if err != nil {
		return result, fmt.Errorf("complex renderer: %w", err)
	}
	renderMetrics, err := r.client.RenderWithMetrics(ctx, p)
	if err != nil {
		return result, fmt.Errorf("complex renderer: %w", err)
	}
	evidence := sweepRunDir(runDir, outPath)
	evidence.ArtifactSHA256 = sha256File(outPath)
	identity := PerformanceIdentity{
		JobID: jobID, WorkerID: r.workerID, GitCommit: r.gitCommit,
		EngineSHA256: r.engineSHA256, BenchmarkFixtureID: string(fixture.ID),
	}
	workload := WorkloadFromRenderPlanV1(p)
	workload.JobType = string(fixture.Kind)
	workload.AudioCodec = spec.Audio.Codec
	receipt := NewAssembler().Assemble(
		pipeline.RunMetrics{RenderMetrics: renderMetrics, TotalMs: renderMetrics.TotalMs},
		AssemblyContext{
			Identity: identity, Workload: workload,
			CPUWallMS:        renderMetrics.CPUUserMs + renderMetrics.CPUSystemMs,
			UsefulPipelineMS: UsefulPipelineMSFromRenderMetrics(renderMetrics),
		},
	)
	return BenchmarkRenderResult{Receipt: receipt, ArtifactSHA256: evidence.ArtifactSHA256, Evidence: evidence}, nil
}

// sweepRunDir reports every file in the render subdir other than the
// output artifact and its sidecar — the zero-spawn invariant: nothing
// staged, nothing materialized, nothing left behind.
func sweepRunDir(runDir, outPath string) GateEvidence {
	evidence := GateEvidence{}
	sidecar := outPath + ".progress.json"
	entries, err := os.ReadDir(runDir)
	if err != nil {
		return evidence
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		full := filepath.Join(runDir, entry.Name())
		if full == outPath || full == sidecar {
			continue
		}
		evidence.TempFiles = append(evidence.TempFiles, entry.Name())
	}
	evidence.TempSegmentFiles = len(evidence.TempFiles)
	return evidence
}

// sha256File returns the hex SHA-256 of a file, or "" on any error.
func sha256File(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
