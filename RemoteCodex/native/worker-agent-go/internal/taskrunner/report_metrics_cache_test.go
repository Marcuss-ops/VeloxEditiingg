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
