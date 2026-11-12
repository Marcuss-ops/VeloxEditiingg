package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

type derivedCacheTestDouble struct {
	mu      sync.Mutex
	entries map[string]workercache.Entry
	stores  int
}

func derivedCacheTestKey(source assetref.ContentHash, profile workercache.NormalizationProfile) string {
	raw, _ := profile.CanonicalJSON()
	return string(source) + "\x00" + string(raw)
}

func (c *derivedCacheTestDouble) FindDerived(_ context.Context, source assetref.ContentHash, profile workercache.NormalizationProfile) (workercache.Entry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[derivedCacheTestKey(source, profile)]
	return entry, ok, nil
}

func (c *derivedCacheTestDouble) StoreDerived(_ context.Context, source assetref.ContentHash, profile workercache.NormalizationProfile, entry workercache.Entry) (assetref.AssetKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]workercache.Entry)
	}
	c.entries[derivedCacheTestKey(source, profile)] = entry
	c.stores++
	return assetref.AssetKey("derived:test"), nil
}

type concurrentNormalizedRunner struct{ calls atomic.Int32 }

func (r *concurrentNormalizedRunner) Run(_ context.Context, request ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	r.calls.Add(1)
	return ffmpegrunner.FFmpegResult{}, os.WriteFile(request.Args[len(request.Args)-1], []byte("normalized-source"), 0o640)
}

func TestNormalizedSourceRebuildsCorruptCacheOnceUnderConcurrency(t *testing.T) {
	runner := &concurrentNormalizedRunner{}
	cache := &derivedCacheTestDouble{}
	e := &videoPrepareWindowExecutor{outputRoot: t.TempDir(), runner: runner, derivedCache: cache, profile: contract.CanonicalVideoProfileV1Default, quality: contract.CanonicalPreparationQualityPolicyDefault,
		normalizedSourceValidator: func(_ context.Context, path string, _ contract.CanonicalVideoProfileV1) bool {
			b, err := os.ReadFile(path)
			return err == nil && string(b) == "normalized-source"
		},
	}
	sha := strings.Repeat("a", 64)
	key, err := workercache.DerivedAssetKey(assetref.ContentHash(sha), e.normalizationProfile())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(key))
	dir := filepath.Join(e.outputRoot, "derived-sources")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, hex.EncodeToString(sum[:])+".mp4")
	if err := os.WriteFile(output, []byte("truncated mp4"), 0o640); err != nil {
		t.Fatal(err)
	}
	_, err = cache.StoreDerived(context.Background(), assetref.ContentHash(sha), e.normalizationProfile(), workercache.Entry{LocalPath: output})
	if err != nil {
		t.Fatal(err)
	}
	const n = 16
	start := make(chan struct{})
	results := make(chan string, n)
	for i := 0; i < n; i++ {
		go func() {
			<-start
			path, ok := e.normalizedSourcePath(context.Background(), "/tmp/source.mp4", sha)
			if !ok {
				results <- ""
				return
			}
			results <- path
		}()
	}
	close(start)
	for i := 0; i < n; i++ {
		if got := <-results; got != output {
			t.Fatalf("result=%q want=%q", got, output)
		}
	}
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("encodes=%d want 1", got)
	}
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "normalized-source" {
		t.Fatalf("output=%q err=%v", data, err)
	}
	partials, err := filepath.Glob(filepath.Join(dir, "*.partial.mp4"))
	if err != nil || len(partials) != 0 {
		t.Fatalf("partial files=%v err=%v", partials, err)
	}
}

func TestValidNormalizedSourceRejectsCorruptMP4(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is unavailable")
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is unavailable")
	}
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.mp4")
	if err := os.WriteFile(bad, []byte("nonempty truncated mp4"), 0o600); err != nil {
		t.Fatal(err)
	}
	profile := contract.CanonicalVideoProfileV1Default
	if validNormalizedSource(context.Background(), bad, profile) {
		t.Fatal("accepted corrupt MP4")
	}
	good := filepath.Join(dir, "good.mp4")
	geometry := fmt.Sprintf("color=c=black:s=%dx%d:r=24", profile.Width, profile.Height)
	command := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", geometry, "-frames:v", "1", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-y", good)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create valid MP4: %v: %s", err, output)
	}
	if !validNormalizedSource(context.Background(), good, profile) {
		t.Fatal("rejected valid normalized MP4")
	}
}

type normalizedSourceRunner struct {
	calls int
}

func (r *normalizedSourceRunner) Run(_ context.Context, request ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	r.calls++
	output := request.Args[len(request.Args)-1]
	if err := os.WriteFile(output, []byte("normalized-source"), 0o640); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	return ffmpegrunner.FFmpegResult{}, nil
}

// TestNormalizedSourceCacheReusesOneEncodeAcrossWindows is the stock-cache
// measurement the architecture audit asks for: three windows of one immutable
// source execute one whole-source normalization, and the other two reuse it.
// A different source must still normalize independently.
func TestNormalizedSourceCacheReusesOneEncodeAcrossWindows(t *testing.T) {
	cache := &derivedCacheTestDouble{}
	runner := &normalizedSourceRunner{}
	metrics := telemetry.NewPrometheusMetrics()
	exec := &videoPrepareWindowExecutor{
		outputRoot:   t.TempDir(),
		runner:       runner,
		derivedCache: cache,
		normalizedSourceValidator: func(_ context.Context, path string, _ contract.CanonicalVideoProfileV1) bool {
			info, err := os.Stat(path)
			return err == nil && info.Mode().IsRegular() && info.Size() > 0
		},
		metrics: metrics,
		profile: contract.CanonicalVideoProfileV1Default,
		quality: contract.CanonicalPreparationQualityPolicyDefault,
	}
	ctx := context.Background()
	sourceSHA := strings.Repeat("a", 64)
	var paths []string
	for window := 0; window < 3; window++ {
		path, ok := exec.normalizedSourcePath(ctx, "/tmp/source.mp4", sourceSHA)
		if !ok || strings.TrimSpace(path) == "" {
			t.Fatalf("window %d did not resolve a normalized source", window)
		}
		paths = append(paths, path)
	}
	if runner.calls != 1 {
		t.Fatalf("whole-source encodes = %d, want 1 (3 windows, 2 avoided)", runner.calls)
	}
	if cache.stores != 1 {
		t.Fatalf("derived cache stores = %d, want 1", cache.stores)
	}
	for window, path := range paths {
		if path != paths[0] {
			t.Fatalf("window %d normalized path = %q, want %q", window, path, paths[0])
		}
		if info, err := os.Stat(path); err != nil || info.Size() <= 0 {
			t.Fatalf("window %d normalized source is missing: %v", window, err)
		}
	}

	otherPath, ok := exec.normalizedSourcePath(ctx, "/tmp/other.mp4", strings.Repeat("b", 64))
	if !ok || otherPath == paths[0] {
		t.Fatalf("different source reused the first normalization: %q vs %q", otherPath, paths[0])
	}
	if runner.calls != 2 {
		t.Fatalf("whole-source encodes after second source = %d, want 2", runner.calls)
	}
	export := metrics.ExportPrometheus()
	for _, series := range []string{
		`velox_normalized_source_cache_hits_total{label="total"} 2`,
		`velox_normalized_source_cache_misses_total{label="total"} 2`,
		`velox_normalized_source_encodes_total{label="total"} 2`,
	} {
		if !strings.Contains(export, series) {
			t.Fatalf("normalized-source telemetry missing %q:\n%s", series, export)
		}
	}
}
