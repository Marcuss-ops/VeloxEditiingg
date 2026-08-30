package telemetry

import (
	"encoding/json"
	"math"
	"testing"
)

func TestPreparedAssetRatio_AllPrefetched(t *testing.T) {
	bd := AssetPreparationBreakdown{
		AssetsRequired: 10,
		PrefetchHits:   10,
	}
	if got := bd.PreparedAssetRatio(); got != 1.0 {
		t.Fatalf("PreparedAssetRatio = %v, want 1.0", got)
	}
}

func TestPreparedAssetRatio_NonePrefetched(t *testing.T) {
	bd := AssetPreparationBreakdown{
		AssetsRequired: 10,
		PrefetchHits:   0,
	}
	if got := bd.PreparedAssetRatio(); got != 0.0 {
		t.Fatalf("PreparedAssetRatio = %v, want 0.0", got)
	}
}

func TestPreparedAssetRatio_Partial(t *testing.T) {
	bd := AssetPreparationBreakdown{
		AssetsRequired: 26,
		PrefetchHits:   25,
	}
	got := bd.PreparedAssetRatio()
	want := float64(25) / float64(26)
	if math.Abs(got-want) > 1e-10 {
		t.Fatalf("PreparedAssetRatio = %v, want %v", got, want)
	}
}

func TestPreparedAssetRatio_NoAssetsRequired(t *testing.T) {
	bd := AssetPreparationBreakdown{AssetsRequired: 0}
	if got := bd.PreparedAssetRatio(); !math.IsNaN(got) {
		t.Fatalf("PreparedAssetRatio = %v, want NaN", got)
	}
}

func TestPreparedByteRatio_AllPrefetched(t *testing.T) {
	bd := AssetPreparationBreakdown{
		RequiredAssetBytes: 677_000_000,
		PrefetchHitBytes:   677_000_000,
	}
	if got := bd.PreparedByteRatio(); got != 1.0 {
		t.Fatalf("PreparedByteRatio = %v, want 1.0", got)
	}
}

func TestPreparedByteRatio_NonePrefetched(t *testing.T) {
	bd := AssetPreparationBreakdown{
		RequiredAssetBytes: 677_000_000,
		PrefetchHitBytes:   0,
	}
	if got := bd.PreparedByteRatio(); got != 0.0 {
		t.Fatalf("PreparedByteRatio = %v, want 0.0", got)
	}
}

func TestPreparedByteRatio_MisleadingCountRatio(t *testing.T) {
	// 25 small assets prefetched (1 KB each) + 1 large video (600 MB) at runtime.
	// Count ratio = 25/26 = 96.1% (looks great)
	// Byte ratio = 25KB / ~600MB = ~0.004% (terrible)
	bd := AssetPreparationBreakdown{
		AssetsRequired:     26,
		PrefetchHits:       25,
		RequiredAssetBytes: 600_000_000 + 25*1024,
		PrefetchHitBytes:   25 * 1024,
	}
	countRatio := bd.PreparedAssetRatio()
	byteRatio := bd.PreparedByteRatio()

	if math.Abs(countRatio-0.9615) > 0.001 {
		t.Fatalf("count ratio = %v, want ~0.9615", countRatio)
	}
	if byteRatio > 0.001 {
		t.Fatalf("byte ratio = %v, want ~0 (misleading count ratio)", byteRatio)
	}
}

func TestPreparedByteRatio_NoBytesRequired(t *testing.T) {
	bd := AssetPreparationBreakdown{RequiredAssetBytes: 0}
	if got := bd.PreparedByteRatio(); !math.IsNaN(got) {
		t.Fatalf("PreparedByteRatio = %v, want NaN", got)
	}
}

func TestPrefetchReadyLeadMS_PositiveLead(t *testing.T) {
	bd := AssetPreparationBreakdown{
		LatestPreparedAtMs: 1000,
		AttemptStartedAtMs: 75000, // 75s after preparation
	}
	if got := bd.PrefetchReadyLeadMS(); got != 74000 {
		t.Fatalf("PrefetchReadyLeadMS = %d, want 74000", got)
	}
}

func TestPrefetchReadyLeadMS_NegativeLead(t *testing.T) {
	bd := AssetPreparationBreakdown{
		LatestPreparedAtMs: 75000, // prepared after attempt started
		AttemptStartedAtMs: 1000,
	}
	if got := bd.PrefetchReadyLeadMS(); got != -74000 {
		t.Fatalf("PrefetchReadyLeadMS = %d, want -74000", got)
	}
}

func TestPrefetchReadyLeadMS_ZeroWhenMissingTimestamps(t *testing.T) {
	tests := []struct {
		name string
		bd   AssetPreparationBreakdown
	}{
		{"both_zero", AssetPreparationBreakdown{}},
		{"prepared_zero", AssetPreparationBreakdown{AttemptStartedAtMs: 1000}},
		{"attempt_zero", AssetPreparationBreakdown{LatestPreparedAtMs: 1000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.bd.PrefetchReadyLeadMS(); got != 0 {
				t.Fatalf("PrefetchReadyLeadMS = %d, want 0", got)
			}
		})
	}
}

func TestPrefetchReadyLeadMS_ZeroWhenEqual(t *testing.T) {
	bd := AssetPreparationBreakdown{
		LatestPreparedAtMs: 5000,
		AttemptStartedAtMs: 5000,
	}
	if got := bd.PrefetchReadyLeadMS(); got != 0 {
		t.Fatalf("PrefetchReadyLeadMS = %d, want 0 (simultaneous)", got)
	}
}

func TestAssetPreparationBreakdown_JSONRoundTrip(t *testing.T) {
	original := AssetPreparationBreakdown{
		AssetsRequired:          26,
		AssetsUnique:            20,
		CacheHits:               25,
		CacheMisses:             1,
		ReadyBeforeAttempt:      25,
		DownloadedDuringAttempt: 1,
		CacheHitBytes:           25 * 1024,
		CacheMissBytes:          600_000_000,
		PrefetchHitBytes:        25 * 1024,
		PrefetchHits:            25,
		WarmCacheHits:           0,
		RuntimeDownloads:        1,
		WarmCacheBytes:          0,
		RuntimeDownloadBytes:    600_000_000,
		RequiredAssetBytes:      600_000_000 + 25*1024,
		LatestPreparedAtMs:      1000,
		AttemptStartedAtMs:      76000,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded AssetPreparationBreakdown
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.AssetsRequired != original.AssetsRequired {
		t.Fatalf("AssetsRequired = %d, want %d", decoded.AssetsRequired, original.AssetsRequired)
	}
	if decoded.PrefetchHits != original.PrefetchHits {
		t.Fatalf("PrefetchHits = %d, want %d", decoded.PrefetchHits, original.PrefetchHits)
	}
	if decoded.WarmCacheBytes != original.WarmCacheBytes {
		t.Fatalf("WarmCacheBytes = %d, want %d", decoded.WarmCacheBytes, original.WarmCacheBytes)
	}
	if decoded.RuntimeDownloadBytes != original.RuntimeDownloadBytes {
		t.Fatalf("RuntimeDownloadBytes = %d, want %d", decoded.RuntimeDownloadBytes, original.RuntimeDownloadBytes)
	}
	if decoded.RequiredAssetBytes != original.RequiredAssetBytes {
		t.Fatalf("RequiredAssetBytes = %d, want %d", decoded.RequiredAssetBytes, original.RequiredAssetBytes)
	}
	if decoded.LatestPreparedAtMs != original.LatestPreparedAtMs {
		t.Fatalf("LatestPreparedAtMs = %d, want %d", decoded.LatestPreparedAtMs, original.LatestPreparedAtMs)
	}
	if decoded.AttemptStartedAtMs != original.AttemptStartedAtMs {
		t.Fatalf("AttemptStartedAtMs = %d, want %d", decoded.AttemptStartedAtMs, original.AttemptStartedAtMs)
	}
}

func TestAssetPreparationBreakdown_JSONCamelCase(t *testing.T) {
	// Protojson renders int64 as quoted strings in camelCase.
	input := `{"assetsRequired":"26","prefetchHits":"25","requiredAssetBytes":"677000000","latestPreparedAtMs":"1000","attemptStartedAtMs":"76000"}`

	var bd AssetPreparationBreakdown
	if err := json.Unmarshal([]byte(input), &bd); err != nil {
		t.Fatalf("Unmarshal camelCase: %v", err)
	}
	if bd.AssetsRequired != 26 {
		t.Fatalf("AssetsRequired = %d, want 26", bd.AssetsRequired)
	}
	if bd.PrefetchHits != 25 {
		t.Fatalf("PrefetchHits = %d, want 25", bd.PrefetchHits)
	}
	if bd.RequiredAssetBytes != 677_000_000 {
		t.Fatalf("RequiredAssetBytes = %d, want 677000000", bd.RequiredAssetBytes)
	}
	if bd.LatestPreparedAtMs != 1000 {
		t.Fatalf("LatestPreparedAtMs = %d, want 1000", bd.LatestPreparedAtMs)
	}
}

func TestN1Certification_AllRatiosAndLead(t *testing.T) {
	// Simulates the N+1 certification scenario:
	// Job B was fully prefetched before its attempt.
	// All 26 assets prepared, 677 MB total, prepared 5s before attempt.
	bd := AssetPreparationBreakdown{
		AssetsRequired:       26,
		PrefetchHits:         26,
		RuntimeDownloads:     0,
		CacheHitBytes:        677_000_000,
		PrefetchHitBytes:     677_000_000,
		RuntimeDownloadBytes: 0,
		RequiredAssetBytes:   677_000_000,
		LatestPreparedAtMs:   1000,
		AttemptStartedAtMs:   6000, // 5s lead
	}

	// All assertions the certification must satisfy:
	if bd.PreparedAssetRatio() != 1.0 {
		t.Fatalf("prepared_asset_ratio = %v, want 1.0", bd.PreparedAssetRatio())
	}
	if bd.PreparedByteRatio() != 1.0 {
		t.Fatalf("prepared_byte_ratio = %v, want 1.0", bd.PreparedByteRatio())
	}
	if bd.PrefetchReadyLeadMS() != 5000 {
		t.Fatalf("prefetch_ready_lead_ms = %d, want 5000", bd.PrefetchReadyLeadMS())
	}
	if bd.RuntimeDownloads != 0 {
		t.Fatalf("runtime_downloads = %d, want 0", bd.RuntimeDownloads)
	}
	if bd.RuntimeDownloadBytes != 0 {
		t.Fatalf("runtime_download_bytes = %d, want 0", bd.RuntimeDownloadBytes)
	}
}
