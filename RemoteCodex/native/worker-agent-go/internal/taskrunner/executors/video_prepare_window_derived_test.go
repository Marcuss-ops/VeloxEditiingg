package executors

import (
	"context"
	"os"
	"strings"
	"testing"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

type derivedCacheTestDouble struct {
	entries map[string]workercache.Entry
	stores  int
}

func derivedCacheTestKey(source assetref.ContentHash, profile workercache.NormalizationProfile) string {
	raw, _ := profile.CanonicalJSON()
	return string(source) + "\x00" + string(raw)
}

func (c *derivedCacheTestDouble) FindDerived(_ context.Context, source assetref.ContentHash, profile workercache.NormalizationProfile) (workercache.Entry, bool, error) {
	entry, ok := c.entries[derivedCacheTestKey(source, profile)]
	return entry, ok, nil
}

func (c *derivedCacheTestDouble) StoreDerived(_ context.Context, source assetref.ContentHash, profile workercache.NormalizationProfile, entry workercache.Entry) (assetref.AssetKey, error) {
	if c.entries == nil {
		c.entries = make(map[string]workercache.Entry)
	}
	c.entries[derivedCacheTestKey(source, profile)] = entry
	c.stores++
	return assetref.AssetKey("derived:test"), nil
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
		metrics:      metrics,
		profile:      contract.CanonicalVideoProfileV1Default,
		quality:      contract.CanonicalPreparationQualityPolicyDefault,
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
