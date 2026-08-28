package downloader

// cache_resolution_test.go — Phase A1: the canonical structured resolution
// surface emits telemetry exactly once per Resolve and never re-derives
// hit/miss at the caller boundary.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
)

// fakeManager implements AssetDownloadManager for resolver tests.
type fakeManager struct {
	resolve func(ctx context.Context, req DownloadRequest) (DownloadedAsset, error)
}

func (f fakeManager) Resolve(ctx context.Context, req DownloadRequest) (DownloadedAsset, error) {
	if f.resolve != nil {
		return f.resolve(ctx, req)
	}
	return DownloadedAsset{}, ErrEmptyKey
}
func (fakeManager) Snapshot(assetref.AssetKey) (DownloadSnapshot, bool) {
	return DownloadSnapshot{}, false
}
func (fakeManager) Subscribe(assetref.AssetKey) (<-chan DownloadSnapshot, func()) {
	return nil, func() {}
}
func (fakeManager) JobSnapshot(string) JobDownloadSnapshot { return JobDownloadSnapshot{} }
func (fakeManager) LatestOperational() OperationalSnapshot { return OperationalSnapshot{} }

type recordingSink struct {
	calls int32
	last  CacheResolution
}

func (s *recordingSink) RecordResolution(_ context.Context, resolution CacheResolution) {
	atomic.AddInt32(&s.calls, 1)
	s.last = resolution
}

func TestCacheResolver_EmitsResolutionExactlyOncePerResolve(t *testing.T) {
	sink := &recordingSink{}
	manager := fakeManager{resolve: func(_ context.Context, req DownloadRequest) (DownloadedAsset, error) {
		return DownloadedAsset{
			AssetID: req.AssetID, LocalPath: "/cache/a.mp4",
			SizeBytes: 0, CacheHit: true, Outcome: CacheOutcomeHitValid,
		}, nil
	}}
	resolver := NewCacheResolver(manager, sink)

	resolution, err := resolver.Resolve(context.Background(), DownloadRequest{AssetID: "a", AssetKey: "a"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolution.Outcome != CacheOutcomeHitValid || !resolution.CacheHit {
		t.Fatalf("resolution = %+v, want classified HIT_VALID", resolution)
	}
	if resolution.Source != CacheSourceLocalDisk {
		t.Fatalf("hit source = %q, want local_disk", resolution.Source)
	}
	if resolution.Downloaded || resolution.DownloadBytes != 0 {
		t.Fatalf("hit resolution carries download bytes: %+v", resolution)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want exactly 1 per Resolve", sink.calls)
	}
}

func TestCacheResolver_MissDownloadIsRecordedWithBytes(t *testing.T) {
	sink := &recordingSink{}
	manager := fakeManager{resolve: func(_ context.Context, req DownloadRequest) (DownloadedAsset, error) {
		return DownloadedAsset{
			AssetID: req.AssetID, LocalPath: "/cache/a.mp4",
			SizeBytes: 4096, CacheHit: false, Outcome: CacheOutcomeMissNotFound,
		}, nil
	}}
	resolver := NewCacheResolver(manager, sink)

	resolution, err := resolver.Resolve(context.Background(), DownloadRequest{AssetID: "a", AssetKey: "a"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolution.CacheHit || resolution.Outcome != CacheOutcomeMissNotFound {
		t.Fatalf("resolution = %+v, want classified MISS_NOT_FOUND", resolution)
	}
	if !resolution.Downloaded || resolution.DownloadBytes != 4096 {
		t.Fatalf("miss resolution download = %+v, want Downloaded=true/4096 bytes", resolution)
	}
	if resolution.Source != CacheSourceMaster {
		t.Fatalf("miss source = %q, want master_bridge", resolution.Source)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want exactly 1", sink.calls)
	}
}

func TestCacheResolver_FallbackOutcomeForLegacyTransferers(t *testing.T) {
	sink := &recordingSink{}
	// A transferer (or byte fake) that predates the Outcome classification
	// field reports only CacheHit; the resolver must still classify.
	manager := fakeManager{resolve: func(_ context.Context, req DownloadRequest) (DownloadedAsset, error) {
		return DownloadedAsset{AssetID: req.AssetID, LocalPath: "/cache/a.mp4", CacheHit: true}, nil
	}}
	resolver := NewCacheResolver(manager, sink)

	resolution, err := resolver.Resolve(context.Background(), DownloadRequest{AssetID: "a", AssetKey: "a"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolution.Outcome != CacheOutcomeHitValid || !resolution.CacheHit {
		t.Fatalf("fallback classification = %+v, want HIT_VALID", resolution)
	}
}

func TestCacheResolver_EmptyKeyIsNotAResolution(t *testing.T) {
	sink := &recordingSink{}
	resolver := NewCacheResolver(fakeManager{}, sink)

	if _, err := resolver.Resolve(context.Background(), DownloadRequest{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Resolve(empty) err = %v, want ErrEmptyKey", err)
	}
	if sink.calls != 0 {
		t.Fatalf("sink calls = %d, want 0 for a non-lookup", sink.calls)
	}
}

func TestCacheResolver_CancelledLookupIsNotRecorded(t *testing.T) {
	sink := &recordingSink{}
	manager := fakeManager{resolve: func(ctx context.Context, _ DownloadRequest) (DownloadedAsset, error) {
		return DownloadedAsset{}, context.Canceled
	}}
	resolver := NewCacheResolver(manager, sink)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(ctx, DownloadRequest{AssetID: "a", AssetKey: "a"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve err = %v, want context.Canceled", err)
	}
	if sink.calls != 0 {
		t.Fatalf("sink calls = %d, want 0 for a cancelled lookup", sink.calls)
	}
}

func TestCacheResolver_NonCancellationFailureIsAClassifiedMiss(t *testing.T) {
	sink := &recordingSink{}
	manager := fakeManager{resolve: func(context.Context, DownloadRequest) (DownloadedAsset, error) {
		return DownloadedAsset{}, errors.New("asset unavailable")
	}}
	resolver := NewCacheResolver(manager, sink)

	if _, err := resolver.Resolve(context.Background(), DownloadRequest{AssetID: "a", AssetKey: "a"}); err == nil {
		t.Fatal("Resolve unexpectedly succeeded")
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1 (the lookup missed and failed)", sink.calls)
	}
	// The accounting invariant (lookups = hits + misses) stays honest: a
	// resolution that obtained no verified local file is a miss.
	if sink.last.Outcome != CacheOutcomeMissNotFound || sink.last.CacheHit || sink.last.Downloaded {
		t.Fatalf("failure resolution = %+v, want MISS_NOT_FOUND miss with no download", sink.last)
	}
}

func TestCacheResolver_NegativeCachesPermanentFailure(t *testing.T) {
	sink := &recordingSink{}
	var calls atomic.Int32
	permanent := fmt.Errorf("%w: remote 404", ErrPermanent)
	manager := fakeManager{resolve: func(context.Context, DownloadRequest) (DownloadedAsset, error) {
		calls.Add(1)
		return DownloadedAsset{}, permanent
	}}
	resolver := NewCacheResolver(manager, sink)
	req := DownloadRequest{AssetID: "missing", AssetKey: "missing"}

	if _, err := resolver.Resolve(context.Background(), req); !errors.Is(err, ErrPermanent) {
		t.Fatalf("first Resolve err = %v, want ErrPermanent", err)
	}
	if _, err := resolver.Resolve(context.Background(), req); !errors.Is(err, ErrPermanent) {
		t.Fatalf("cached Resolve err = %v, want ErrPermanent", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("manager calls = %d, want 1 while negative entry is fresh", got)
	}
	if got := atomic.LoadInt32(&sink.calls); got != 2 {
		t.Fatalf("sink calls = %d, want one accounting event per lookup", got)
	}

	// Exercise expiry without a wall-clock sleep: an expired entry must be
	// removed and the canonical manager must be consulted again.
	resolver.negativeMu.Lock()
	resolver.negative["missing"] = negativeCacheEntry{err: permanent, expires: time.Now().Add(-time.Second)}
	resolver.negativeMu.Unlock()
	if _, err := resolver.Resolve(context.Background(), req); !errors.Is(err, ErrPermanent) {
		t.Fatalf("expired Resolve err = %v, want ErrPermanent", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("manager calls after expiry = %d, want 2", got)
	}
}

func TestCacheOutcome_IsHitIsMissVocabulary(t *testing.T) {
	if !CacheOutcomeHitValid.IsHit() || CacheOutcomeHitValid.IsMiss() {
		t.Fatal("HIT_VALID must be a hit and not a miss")
	}
	for _, miss := range []CacheOutcome{
		CacheOutcomeMissNotFound, CacheOutcomeMissInvalid,
		CacheOutcomeMissHashMismatch, CacheOutcomeMissExpired,
	} {
		if miss.IsHit() || !miss.IsMiss() {
			t.Fatalf("%s must be a miss and not a hit", miss)
		}
	}
	if CacheOutcome("").IsMiss() {
		t.Fatal("empty outcome must not count as a miss (no classification)")
	}
}
