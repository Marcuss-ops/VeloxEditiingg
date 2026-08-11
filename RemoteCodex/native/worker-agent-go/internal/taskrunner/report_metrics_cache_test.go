package taskrunner

import "testing"

func TestTypedMetricsFromMap_ExplicitWarmAssetMissesOverrideGenericDelta(t *testing.T) {
	metrics := TypedMetricsFromMap(map[string]interface{}{
		"asset.cache.hit.count":  int64(169),
		"asset.cache.miss.count": int64(0),
		"cache.hits":             int64(143),
		"cache.misses":           int64(26),
		"cache.lookups":          int64(169),
	})

	if metrics == nil {
		t.Fatal("typed metrics are nil")
	}
	if metrics.AssetCacheHitCount != 169 || metrics.AssetCacheMissCount != 0 {
		t.Fatalf("asset cache counters = %d/%d, want 169/0", metrics.AssetCacheHitCount, metrics.AssetCacheMissCount)
	}
	if metrics.CacheLookups != 169 {
		t.Fatalf("cache lookups = %d, want 169", metrics.CacheLookups)
	}
}

// TestTypedMetricsFromMap_DownloadVolume verifies the Phase A1.5 seam: the
// CacheResolver's attempt-scoped download counters ride the dotted report map
// and must land on the typed envelope so the master can persist them in SQL.
func TestTypedMetricsFromMap_DownloadVolume(t *testing.T) {
	metrics := TypedMetricsFromMap(map[string]interface{}{
		"asset.cache.download.count": int64(26),
		"asset.cache.download.bytes": int64(26 * 1024),
		"unique.assets.requested":    int64(12),
	})

	if metrics == nil {
		t.Fatal("typed metrics are nil")
	}
	if metrics.CacheDownloadCount != 26 {
		t.Fatalf("cache download count = %d, want 26", metrics.CacheDownloadCount)
	}
	if metrics.CacheDownloadBytes != 26*1024 {
		t.Fatalf("cache download bytes = %d, want %d", metrics.CacheDownloadBytes, 26*1024)
	}
	if metrics.UniqueAssetsRequested != 12 {
		t.Fatalf("unique assets requested = %d, want 12", metrics.UniqueAssetsRequested)
	}

	// Round-trip through the wire envelope must survive untouched.
	proto := metrics.ToProto()
	if proto.GetCacheDownloadCount() != 26 || proto.GetCacheDownloadBytes() != 26*1024 {
		t.Fatalf("proto download volume = %d/%d, want 26/%d", proto.GetCacheDownloadCount(), proto.GetCacheDownloadBytes(), 26*1024)
	}
}

func TestFirstPresentFallsBackOnlyWhenPrimaryMissing(t *testing.T) {
	metrics := map[string]interface{}{
		"primary_zero": int64(0),
		"fallback":     int64(26),
	}
	if got := firstPresent(metrics, "primary_zero", "fallback"); got != 0 {
		t.Fatalf("firstPresent explicit zero = %d, want 0", got)
	}
	if got := firstPresent(metrics, "missing", "fallback"); got != 26 {
		t.Fatalf("firstPresent fallback = %d, want 26", got)
	}
}
