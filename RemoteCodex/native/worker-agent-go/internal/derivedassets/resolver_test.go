package derivedassets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"velox-shared/assetref"
	"velox-worker-agent/internal/workercache"
)

func testSource() assetref.ContentHash {
	return assetref.ContentHash(strings.Repeat("a", 64))
}

func testProfile() workercache.NormalizationProfile {
	return workercache.NormalizationProfile{
		NormalizerVersion: 1,
		Codec:             "h264",
		Width:             1920,
		Height:            1080,
		FPSNum:            30,
		FPSDen:            1,
		PixelFormat:       "yuv420p",
	}
}

// newTestCacheStore opens a real workercache over a temp-dir DB and returns
// its typed DerivedAssetStore facade. Tests exercise the resolver against the
// canonical storage rather than a hand-rolled index.
func newTestCacheStore(t *testing.T) workercache.DerivedAssetStore {
	t.Helper()
	c, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c.AsCanonicalStore()
}

// writeTempFile writes content to a fresh file and returns its path, SHA-256
// and size, so tests can author "produced" assets with known identities.
func writeTempFile(t *testing.T, content []byte) (string, assetref.ContentHash, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "produced.mp4")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	sum := sha256.Sum256(content)
	return path, assetref.ContentHash(hex.EncodeToString(sum[:])), int64(len(content))
}

func TestResolver_HitReturnsCachedEntryWithoutProducing(t *testing.T) {
	store := newTestCacheStore(t)
	ctx := context.Background()
	src := testSource()
	profile := testProfile()

	path, hash, size := writeTempFile(t, []byte("already-cached-derived-bytes"))
	if _, err := store.StoreDerived(ctx, src, profile, workercache.Entry{
		LocalPath:        path,
		ContentHash:      hash,
		SizeBytes:        size,
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("StoreDerived seed: %v", err)
	}

	var produced int32
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		atomic.AddInt32(&produced, 1)
		return Produced{}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	res, err := r.Resolve(ctx, src, "unused-on-hit.mp4", profile)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !res.Hit {
		t.Fatal("expected cache hit")
	}
	if res.Entry.ContentHash != hash || res.Entry.LocalPath != path {
		t.Fatalf("entry = %+v, want hash=%q path=%q", res.Entry, hash, path)
	}
	if got := atomic.LoadInt32(&produced); got != 0 {
		t.Fatalf("producer ran %d times on a cache hit", got)
	}
}

func TestResolver_MissProducesVerifiesAndPromotes(t *testing.T) {
	store := newTestCacheStore(t)
	ctx := context.Background()
	src := testSource()
	profile := testProfile()

	path, hash, size := writeTempFile(t, []byte("freshly-produced-normalized-bytes"))
	var produced int32
	r, err := NewResolver(store, func(_ context.Context, sourcePath string, p workercache.NormalizationProfile) (Produced, error) {
		atomic.AddInt32(&produced, 1)
		if sourcePath == "" {
			t.Fatalf("producer received empty source path")
		}
		return Produced{Path: path, Size: size, Hash: hash}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	res, err := r.Resolve(ctx, src, "/source/input.mp4", profile)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if res.Hit {
		t.Fatal("expected a miss")
	}
	if res.Entry.ContentHash != hash || res.Entry.LocalPath != path || res.Entry.SizeBytes != size {
		t.Fatalf("entry = %+v, want hash=%q path=%q size=%d", res.Entry, hash, path, size)
	}
	if !res.Entry.DownloadComplete {
		t.Fatal("promoted entry must be complete")
	}
	if got := atomic.LoadInt32(&produced); got != 1 {
		t.Fatalf("producer ran %d times, want 1", got)
	}

	// A second resolve must hit and not re-run the producer.
	res2, err := r.Resolve(ctx, src, "/source/input.mp4", profile)
	if err != nil {
		t.Fatalf("second Resolve: %v", err)
	}
	if !res2.Hit {
		t.Fatal("second resolve did not hit")
	}
	if got := atomic.LoadInt32(&produced); got != 1 {
		t.Fatalf("producer ran %d times after cache population, want 1", got)
	}
}

func TestResolver_ProducerErrorIsWrapped(t *testing.T) {
	store := newTestCacheStore(t)
	ctx := context.Background()
	producerErr := errors.New("native pipeline crashed")
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{}, producerErr
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(ctx, testSource(), "/source/input.mp4", testProfile())
	if !errors.Is(err, ErrProduceFailed) {
		t.Fatalf("err = %v, want ErrProduceFailed", err)
	}
	if !errors.Is(err, producerErr) {
		t.Fatalf("err = %v, want wrapped producer error", err)
	}
}

func TestResolver_HashMismatchFailsClosedAndRemovesFile(t *testing.T) {
	store := newTestCacheStore(t)
	ctx := context.Background()
	path, _, _ := writeTempFile(t, []byte("bytes-that-do-not-match-the-claim"))
	wrongHash := assetref.ContentHash(strings.Repeat("f", 64))

	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{Path: path, Hash: wrongHash}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(ctx, testSource(), "/source/input.mp4", testProfile())
	if !errors.Is(err, ErrVerification) {
		t.Fatalf("err = %v, want ErrVerification", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("produced file survived failed verification (stat err = %v)", statErr)
	}
	// Fail closed: nothing was promoted.
	if _, found, findErr := store.FindDerived(ctx, testSource(), testProfile()); findErr != nil || found {
		t.Fatalf("failed verification still promoted an entry: found=%v err=%v", found, findErr)
	}
}

func TestResolver_EmptyProducedFileIsRejected(t *testing.T) {
	store := newTestCacheStore(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "empty.mp4")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{Path: path}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(ctx, testSource(), "/source/input.mp4", testProfile())
	if !errors.Is(err, ErrVerification) {
		t.Fatalf("err = %v, want ErrVerification", err)
	}
}

func TestResolver_MissingSourcePathOnMissIsRejected(t *testing.T) {
	store := newTestCacheStore(t)
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(context.Background(), testSource(), "", testProfile())
	if !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("err = %v, want ErrInvalidSource", err)
	}
}

func TestResolver_InvalidSourceHashIsRejected(t *testing.T) {
	store := newTestCacheStore(t)
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(context.Background(), assetref.ContentHash("not-a-sha"), "/source/input.mp4", testProfile())
	if !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("err = %v, want ErrInvalidSource", err)
	}
}

func TestResolver_InvalidProfileIsRejected(t *testing.T) {
	store := newTestCacheStore(t)
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(context.Background(), testSource(), "/source/input.mp4", workercache.NormalizationProfile{})
	if !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("err = %v, want ErrInvalidProfile", err)
	}
}

func TestNewResolverRejectsNilDependencies(t *testing.T) {
	if _, err := NewResolver(nil, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{}, nil
	}); err == nil {
		t.Fatal("nil store was accepted")
	}
	if _, err := NewResolver(newTestCacheStore(t), nil); err == nil {
		t.Fatal("nil producer was accepted")
	}
}

// scriptedStore is a DerivedAssetStore double that returns programmed
// FindDerived results in call order and a fixed StoreDerived error. It exists
// to exercise the duplicate-promotion race without threading real SQLite.
type scriptedStore struct {
	finds     []workercache.Entry
	found     []bool
	storeErr  error
	findCalls int
	storeCall int
}

func (s *scriptedStore) FindDerived(_ context.Context, _ assetref.ContentHash, _ workercache.NormalizationProfile) (workercache.Entry, bool, error) {
	i := s.findCalls
	s.findCalls++
	if i < len(s.finds) {
		return s.finds[i], s.found[i], nil
	}
	return workercache.Entry{}, false, nil
}

func (s *scriptedStore) StoreDerived(_ context.Context, _ assetref.ContentHash, _ workercache.NormalizationProfile, _ workercache.Entry) (assetref.AssetKey, error) {
	s.storeCall++
	return "", s.storeErr
}

func TestResolver_DuplicatePromotionRaceReusesWinner(t *testing.T) {
	src := testSource()
	profile := testProfile()
	key, err := workercache.DerivedAssetKey(src, profile)
	if err != nil {
		t.Fatalf("DerivedAssetKey: %v", err)
	}
	winner := workercache.Entry{
		AssetKey:         key,
		ContentHash:      assetref.ContentHash(strings.Repeat("b", 64)),
		LocalPath:        "/cache/winner.mp4",
		SizeBytes:        42,
		DownloadComplete: true,
	}
	store := &scriptedStore{
		// First FindDerived (pre-check) → miss; StoreDerived → duplicate;
		// second FindDerived → the concurrent winner's entry.
		finds:    []workercache.Entry{{}, winner},
		found:    []bool{false, true},
		storeErr: workercache.ErrDuplicate,
	}

	path, hash, size := writeTempFile(t, []byte("losing-producer-bytes"))
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{Path: path, Size: size, Hash: hash}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	res, err := r.Resolve(context.Background(), src, "/source/input.mp4", profile)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !res.Hit {
		t.Fatal("duplicate race must surface the winner as a hit")
	}
	if res.Entry.LocalPath != winner.LocalPath || res.Entry.ContentHash != winner.ContentHash {
		t.Fatalf("entry = %+v, want winner %+v", res.Entry, winner)
	}
	if store.storeCall != 1 {
		t.Fatalf("StoreDerived called %d times, want 1", store.storeCall)
	}
	if store.findCalls != 2 {
		t.Fatalf("FindDerived called %d times, want 2", store.findCalls)
	}
	// The losing producer's file must not linger.
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("losing produced file survived duplicate race (stat err = %v)", statErr)
	}
}

func TestResolver_DuplicateWithNoWinningEntryFailsClosed(t *testing.T) {
	store := &scriptedStore{
		// Pre-check miss, StoreDerived duplicate, re-check still miss (the
		// concurrent row is incomplete or was cleaned) → fail closed.
		finds:    []workercache.Entry{{}, {}},
		found:    []bool{false, false},
		storeErr: workercache.ErrDuplicate,
	}

	path, hash, size := writeTempFile(t, []byte("produced-bytes"))
	r, err := NewResolver(store, func(context.Context, string, workercache.NormalizationProfile) (Produced, error) {
		return Produced{Path: path, Size: size, Hash: hash}, nil
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	_, err = r.Resolve(context.Background(), testSource(), "/source/input.mp4", testProfile())
	if !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("err = %v, want ErrStoreFailed", err)
	}
}
