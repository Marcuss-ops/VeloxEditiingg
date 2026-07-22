package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/video/plan"
)

// fakeRenderClient produces a deterministic 1 KiB byte sequence on
// the output path passed via the plan. The bootstrap computes the
// SHA-256 of those bytes and compares to opts.BaselineSHA256Path.
//
// Deterministic content lets us pre-write the expected baseline file
// into t.TempDir and get a reproducible smoke pass.
type fakeRenderClient struct {
	payload []byte
}

func (f *fakeRenderClient) Render(_ context.Context, p *plan.RenderPlan) error {
	if p == nil || p.OutputPath == "" {
		return errFake("nil plan or empty output path")
	}
	if err := os.MkdirAll(filepath.Dir(p.OutputPath), 0o755); err != nil {
		return errFake("mkdir: " + err.Error())
	}
	if err := os.WriteFile(p.OutputPath, f.payload, 0o644); err != nil {
		return errFake("write: " + err.Error())
	}
	return nil
}

type errFake string

func (e errFake) Error() string { return string(e) }

// fakeRunner is the one-method adapter required by RunnerView.
type fakeRunner struct {
	rc RenderClientIface
}

func (f *fakeRunner) RenderClient() RenderClientIface { return f.rc }

func writeBaseline(t *testing.T, dir string, payload []byte) string {
	t.Helper()
	h := sha256.Sum256(payload)
	hex := hex.EncodeToString(h[:])
	path := filepath.Join(dir, "baseline.sha256")
	if err := os.WriteFile(path, []byte(hex+"  tests/fixtures/engine_selftest_baseline.sha256\n"), 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	return path
}

func TestRun_AllOK_Smoke(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skipf("ffprobe not available in PATH: %v", err)
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skipf("ffmpeg not available in PATH: %v", err)
	}

	// Arrange: workspace with fake render payload + matching baseline
	// + stub BUNDLE_HASH.txt == cfg.BundleHash.
	dir := t.TempDir()
	payload := []byte("fake-render-payload-with-stable-bytes")
	baseline := writeBaseline(t, dir, payload)
	const hash = "smoke-hash-ok"
	if err := os.WriteFile(filepath.Join(dir, "BUNDLE_HASH.txt"), []byte(hash+"\n"), 0o644); err != nil {
		t.Fatalf("write bundle hash: %v", err)
	}
	// output dir under t.TempDir so the smoke does not touch /tmp
	outputDir := filepath.Join(dir, "output")

	// Reset the package gate (tests under the same process must not
	// inherit a previously-flipped Ok state).
	Reset()

	rc := &fakeRenderClient{payload: payload}
	report, err := Run(context.Background(),
		makeCfg(hash),
		&fakeRunner{rc: rc},
		Options{
			WorkDir:            dir,
			OutputDir:          outputDir,
			StateDir:           dir,
			TempDir:            dir,
			BaselineSHA256Path: baseline,
			FFmpegMinMajor:     0, // 0 → default applied
		},
	)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if report == nil {
		t.Fatalf("Run returned nil report")
	}
	if report.Verdict != "OK" {
		t.Fatalf("verdict = %q want OK; steps=%+v", report.Verdict, report.Steps)
	}
	if !Ok() {
		t.Fatalf("Ok() == false after OK run; HardGate would reject future Start calls")
	}
	if HardGate() != nil {
		t.Fatalf("HardGate() != nil after OK run")
	}
	// All four canonical step names must appear.
	seenSteps := map[string]bool{}
	for _, s := range report.Steps {
		seenSteps[s.Name] = true
	}
	for _, want := range []string{"bundle_hash", "ffmpeg", "output_dir", "state_dir", "temp_dir", "engine_self_render"} {
		if !seenSteps[want] {
			t.Errorf("missing step %q in report: %+v", want, report.Steps)
		}
	}
}

func TestRun_RenderClientNil(t *testing.T) {
	dir := t.TempDir()
	const hash = "smoke-hash-nil"
	_ = os.WriteFile(filepath.Join(dir, "BUNDLE_HASH.txt"), []byte(hash+"\n"), 0o644)
	_ = writeBaseline(t, dir, []byte("anything"))

	Reset()
	report, err := Run(context.Background(),
		makeCfg(hash),
		nil, // runner is nil → engine_self_render fails with engine_missing + bootstrap.go also adds FAIL row
		Options{
			WorkDir:            dir,
			OutputDir:          dir,
			BaselineSHA256Path: filepath.Join(dir, "baseline.sha256"),
		},
	)
	if err == nil {
		t.Fatalf("expected error on nil runner; got report=%+v", report)
	}
	if report.Verdict != "FAIL" {
		t.Fatalf("verdict = %q want FAIL", report.Verdict)
	}
	var found bool
	for _, s := range report.Steps {
		if s.Name == "engine_self_render" && s.Code == "engine_missing" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected engine_self_render/engine_missing step; got %+v", report.Steps)
	}
	if Ok() {
		t.Fatalf("Ok() should remain false after FAIL run")
	}
}

func TestRun_BundleMismatch(t *testing.T) {
	dir := t.TempDir()
	// Disk has 'disk-value'; cfg.BundleHash has 'cfg-value' → mismatch.
	_ = os.WriteFile(filepath.Join(dir, "BUNDLE_HASH.txt"), []byte("disk-value\n"), 0o644)
	_ = writeBaseline(t, dir, []byte("something"))

	Reset()
	_, err := Run(context.Background(),
		makeCfg("cfg-value"),
		&fakeRunner{rc: &fakeRenderClient{payload: []byte("x")}},
		Options{
			WorkDir:            dir,
			OutputDir:          dir,
			BaselineSHA256Path: filepath.Join(dir, "baseline.sha256"),
		},
	)
	if err == nil {
		t.Fatalf("expected error on bundle mismatch")
	}
	if HardGate() == nil {
		t.Fatalf("HardGate() should still fail after FAIL run")
	}
}

func TestRun_BundleMissing(t *testing.T) {
	dir := t.TempDir()
	// No BUNDLE_HASH.txt under dir.
	_ = writeBaseline(t, dir, []byte("something"))

	Reset()
	_, err := Run(context.Background(),
		makeCfg("doesnt-matter"),
		&fakeRunner{rc: &fakeRenderClient{payload: []byte("x")}},
		Options{
			WorkDir:            dir,
			OutputDir:          dir,
			BaselineSHA256Path: filepath.Join(dir, "baseline.sha256"),
		},
	)
	if err == nil {
		t.Fatalf("expected error on bundle missing")
	}
}

func TestOk_HardGate_NotRun(t *testing.T) {
	Reset()
	if Ok() {
		t.Fatalf("Ok() should be false after Reset")
	}
	if err := HardGate(); err == nil {
		t.Fatalf("HardGate() should not be nil when bootstrap has not run")
	}
}

// TestRun_CreatorProfile_SkipsEngine verifies that the creator profile
// skips ffmpeg and engine self-render while still checking bundle and
// runtime directories.
func TestRun_CreatorProfile_SkipsEngine(t *testing.T) {
	dir := t.TempDir()
	const hash = "creator-hash"
	_ = os.WriteFile(filepath.Join(dir, "BUNDLE_HASH.txt"), []byte(hash+"\n"), 0o644)
	_ = writeBaseline(t, dir, []byte("creator-baseline"))

	Reset()
	cfg := makeCfg(hash)
	cfg.WorkerProfile = "creator"
	report, err := Run(context.Background(),
		cfg,
		nil, // creator does not pass a runner
		Options{
			WorkDir:            dir,
			OutputDir:          dir,
			StateDir:           dir,
			TempDir:            dir,
			BaselineSHA256Path: filepath.Join(dir, "baseline.sha256"),
		},
	)
	if err != nil {
		t.Fatalf("Run returned error for creator profile: %v", err)
	}
	if report == nil {
		t.Fatalf("Run returned nil report")
	}
	if report.Verdict != "OK" {
		t.Fatalf("verdict = %q want OK; steps=%+v", report.Verdict, report.Steps)
	}

	seenSteps := map[string]bool{}
	for _, s := range report.Steps {
		seenSteps[s.Name] = true
	}
	if seenSteps["ffmpeg"] {
		t.Errorf("creator profile should skip ffmpeg step")
	}
	if seenSteps["engine_self_render"] {
		t.Errorf("creator profile should skip engine_self_render step")
	}
	if !seenSteps["bundle_hash"] {
		t.Errorf("creator profile should still run bundle_hash step")
	}
	if !seenSteps["state_dir"] {
		t.Errorf("creator profile should still run state_dir step")
	}
	if !seenSteps["temp_dir"] {
		t.Errorf("creator profile should still run temp_dir step")
	}
}

// makeCfg builds a minimal *config.WorkerConfig for tests. We don't
// care about most fields — Run() consults cfg.WorkerID, cfg.WorkDir,
// cfg.BundleHash, cfg.OutputDir, and reads them via the worker-agent's
// canonical normalisation rules.
//
// We intentionally do NOT pull pkg/config here because some test
// environments do not initialise identity correctly without VERSION.txt.
// A near-empty config struct is sufficient because Run() only reads
// fields; tests never call cfg.Validate().
// makeCfg builds a minimal *config.WorkerConfig for tests. Run()
// only reads cfg.WorkerID, cfg.WorkerDir (via opts), cfg.BundleHash, cfg.OutputDir
// (via opts), so an empty-Validate struct is sufficient. A full Validate
// is intentionally NOT called here — the bundle step is the gate we want
// to test, not cfg.Validate.
func makeCfg(bundleHash string) *config.WorkerConfig {
	return &config.WorkerConfig{
		WorkerID:   "test-worker-id-A1",
		BundleHash: bundleHash,
	}
}
