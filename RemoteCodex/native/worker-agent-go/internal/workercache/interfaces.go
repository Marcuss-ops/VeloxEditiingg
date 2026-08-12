package workercache

import (
	"context"
	"time"

	"velox-shared/assetref"
)

// AssetRegistry is the canonical metadata registry for worker input assets.
// AssetKey is the only identity accepted at this boundary; provider IDs,
// URLs, and local paths remain attributes of the Entry or transport layer.
type AssetRegistry interface {
	Find(context.Context, assetref.AssetKey) (Entry, bool, error)
	Store(context.Context, Entry) error
	List(context.Context) ([]Entry, error)
	MarkUsed(context.Context, assetref.AssetKey) error
}

// ContentAddressedCache is the verified-byte boundary. An entry becomes
// usable only through a hash-aware completion operation, and eviction is
// fenced against concurrent lease/reservation acquisition.
type ContentAddressedCache interface {
	AssetRegistry
	LeaseReservationStore
	MarkDownloadCompleteWithHash(context.Context, assetref.AssetKey, string, int64, assetref.ContentHash) error
	PreserveContentHash(context.Context, assetref.AssetKey, string, int64, assetref.ContentHash) error
	EvictIfUnleased(context.Context, assetref.AssetKey, string) error
}

// DerivedAssetStore is the normalization-cache extension of the canonical
// content store. It deliberately shares the cached_assets table, leases and
// eviction fences with source assets instead of introducing another cache.
type DerivedAssetStore interface {
	FindDerived(context.Context, assetref.ContentHash, NormalizationProfile) (Entry, bool, error)
	StoreDerived(context.Context, assetref.ContentHash, NormalizationProfile, Entry) (assetref.AssetKey, error)
}

// LeaseReservationStore is the authoritative protection barrier for cached
// input assets. Cleanup must consult this store rather than a duplicated
// active-job field or an in-memory counter.
type LeaseReservationStore interface {
	Acquire(context.Context, assetref.AssetKey, string) error
	Release(context.Context, assetref.AssetKey, string) error
	Reserve(context.Context, assetref.AssetKey, string, time.Time) error
	ReleaseReservation(context.Context, assetref.AssetKey, string) error
}

// CanonicalAssetStore is the typed facade over Cache. The SQLite Cache keeps
// its legacy string-taking methods for source compatibility with existing
// worker integrations; new pipeline components depend on these typed
// interfaces instead of reaching into that compatibility surface.
type CanonicalAssetStore struct {
	cache *Cache
}

// NewCanonicalAssetStore adapts an existing SQLite cache to the canonical
// typed asset pipeline. It does not create a second index or copy any data.
func NewCanonicalAssetStore(cache *Cache) *CanonicalAssetStore {
	if cache == nil {
		return nil
	}
	return &CanonicalAssetStore{cache: cache}
}

// AsCanonicalStore returns the typed facade for c, or nil for a nil cache.
func (c *Cache) AsCanonicalStore() *CanonicalAssetStore {
	return NewCanonicalAssetStore(c)
}

func (s *CanonicalAssetStore) Find(ctx context.Context, key assetref.AssetKey) (Entry, bool, error) {
	return s.cache.Find(ctx, string(key))
}

func (s *CanonicalAssetStore) Store(ctx context.Context, entry Entry) error {
	return s.cache.Store(ctx, entry)
}

func (s *CanonicalAssetStore) List(ctx context.Context) ([]Entry, error) {
	return s.cache.List(ctx)
}

func (s *CanonicalAssetStore) MarkUsed(ctx context.Context, key assetref.AssetKey) error {
	return s.cache.MarkUsed(ctx, string(key))
}

func (s *CanonicalAssetStore) MarkDownloadCompleteWithHash(ctx context.Context, key assetref.AssetKey, localPath string, sizeBytes int64, hash assetref.ContentHash) error {
	return s.cache.MarkDownloadCompleteWithHash(ctx, string(key), localPath, sizeBytes, hash)
}

func (s *CanonicalAssetStore) PreserveContentHash(ctx context.Context, key assetref.AssetKey, localPath string, sizeBytes int64, hash assetref.ContentHash) error {
	return s.cache.PreserveContentHash(ctx, string(key), localPath, sizeBytes, hash)
}

func (s *CanonicalAssetStore) EvictIfUnleased(ctx context.Context, key assetref.AssetKey, localPath string) error {
	return s.cache.EvictIfUnleased(ctx, string(key), localPath)
}

func (s *CanonicalAssetStore) FindDerived(ctx context.Context, sourceHash assetref.ContentHash, profile NormalizationProfile) (Entry, bool, error) {
	return s.cache.FindDerived(ctx, sourceHash, profile)
}

func (s *CanonicalAssetStore) StoreDerived(ctx context.Context, sourceHash assetref.ContentHash, profile NormalizationProfile, entry Entry) (assetref.AssetKey, error) {
	return s.cache.StoreDerived(ctx, sourceHash, profile, entry)
}

func (s *CanonicalAssetStore) Acquire(ctx context.Context, key assetref.AssetKey, jobID string) error {
	return s.cache.Acquire(ctx, string(key), jobID)
}

func (s *CanonicalAssetStore) Release(ctx context.Context, key assetref.AssetKey, jobID string) error {
	return s.cache.Release(ctx, string(key), jobID)
}

func (s *CanonicalAssetStore) Reserve(ctx context.Context, key assetref.AssetKey, reservationID string, expiresAt time.Time) error {
	return s.cache.Reserve(ctx, string(key), reservationID, expiresAt)
}

func (s *CanonicalAssetStore) ReleaseReservation(ctx context.Context, key assetref.AssetKey, reservationID string) error {
	return s.cache.ReleaseReservation(ctx, string(key), reservationID)
}

var (
	_ AssetRegistry         = (*CanonicalAssetStore)(nil)
	_ ContentAddressedCache = (*CanonicalAssetStore)(nil)
	_ DerivedAssetStore     = (*CanonicalAssetStore)(nil)
	_ LeaseReservationStore = (*CanonicalAssetStore)(nil)
)
