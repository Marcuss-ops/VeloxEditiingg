package workercache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
)

type failingCacheFileSystem struct {
	err error
}

func (f failingCacheFileSystem) Remove(string) error { return f.err }

func TestEvictIfUnleased_FileSystemFailureRollsBackIndex(t *testing.T) {
	cache, err := Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	ctx := context.Background()
	assetKey := "fs-failure"
	if err := cache.Store(ctx, Entry{
		AssetKey:         assetref.AssetKey(assetKey),
		LocalPath:        filepath.Join(t.TempDir(), "asset.mp4"),
		SizeBytes:        4,
		DownloadComplete: true,
	}); err != nil {
		t.Fatal(err)
	}
	removeErr := errors.New("injected remove failure")
	cache.fs = failingCacheFileSystem{err: removeErr}

	if err := cache.EvictIfUnleased(ctx, assetKey, "ignored-by-fake"); !errors.Is(err, removeErr) {
		t.Fatalf("EvictIfUnleased error = %v, want injected error", err)
	}
	if _, found, err := cache.Find(ctx, assetKey); err != nil || !found {
		t.Fatalf("index after failed eviction: found=%v err=%v, want retained row", found, err)
	}
}

func TestEvaluateEviction_PreservesProtectionOrder(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	base := Entry{AssetKey: "asset", DownloadComplete: true, LastUsedAt: now.Add(-10 * time.Minute)}
	protected := map[string]struct{}{"asset": {}}

	cases := []struct {
		name      string
		entry     Entry
		protected map[string]struct{}
		grace     time.Duration
		want      evictionDecision
	}{
		{name: "lease", entry: Entry{ActiveLeaseCount: 1, DownloadComplete: true}, want: evictionKeepLease},
		{name: "reservation", entry: Entry{ActiveReservationCount: 1, DownloadComplete: true}, want: evictionKeepReservation},
		{name: "in-flight", entry: Entry{DownloadComplete: false}, want: evictionKeepInFlight},
		{name: "snapshot", entry: base, protected: protected, want: evictionKeepProtected},
		{name: "grace", entry: Entry{AssetKey: "asset", DownloadComplete: true, LastUsedAt: now.Add(-time.Minute)}, grace: 3 * time.Minute, want: evictionKeepGrace},
		{name: "eligible", entry: base, want: evictionEligible},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evaluateEviction(tc.entry, tc.protected, tc.grace, now); got != tc.want {
				t.Fatalf("decision = %q, want %q", got, tc.want)
			}
		})
	}
}
