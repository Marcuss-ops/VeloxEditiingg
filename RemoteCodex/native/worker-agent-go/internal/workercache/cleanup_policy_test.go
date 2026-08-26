// Package workercache — CleanupPolicy test matrix.
//
// These tests exercise the Pass 12 additions on top of the Pass 9
// Cleanup predicates:
//   - TestCleanupPolicy_RaceCondition_T0ToT10s: user-spec scenario
//     (snapshot @ T0, new job @ T+5s, cleaner @ T+10s).
//   - TestCleanupPolicy_GraceExpiry_DeletesAfter: control — once
//     RecentUseGrace passes on a stale-but-protected-blank row, the
//     cleanup DOES delete.
//   - TestCleanupPolicy_SnapshotStale_SkipsPass: when the master
//     snapshot is older than SnapshotMaxAge, the entire cleanup is
//     a no-op (return ErrSnapshotStale + zero Removed) so workers
//     don't wipe active data during a master outage.
//
// The fixture uses an injectable `now` parameter so we don't rely on
// real-time sleeps (which would flake in CI).

package workercache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
)

// policyFixture is the deterministic test fixture: a fresh in-memory
// cache plus an injection-friendly `now`. Each subtest supplies its
// own `T0` anchor so the assertions are reproducible across runs.
type policyFixture struct {
	cache *Cache
	dir   string
}

func newPolicyFixture(t *testing.T) *policyFixture {
	t.Helper()
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return &policyFixture{cache: cache, dir: t.TempDir()}
}

// seedRow inserts a fully-downloaded row at the supplied last_used_at
// instant, mirroring what cleanupFixture / storeSeeded produce but
// with explicit timestamps so the Pass 12 grace rule can be exercised
// deterministically.
//
// The post-MarkDownloadComplete UPDATE is required because BOTH
// cache.Store (when LastUsedAt is zero) and cache.MarkDownloadComplete
// unconditionally overwrite last_used_at with time.Now().UTC(); tests
// that want a deterministic grace predicate stamp the test-controlled
// value via Cache.DB() — the exporter reserved for migrations and
// supervisor scans in cache.go. Direct SQL access for test-only
// timestamp control is documented as a legitimate use.
func seedRow(t *testing.T, c *Cache, dir, assetKey string, lastUsedAt time.Time) {
	t.Helper()
	path := filepath.Join(dir, assetKey+".mp4")
	if err := os.WriteFile(path, []byte("FAKE VIDEO BYTES "+assetKey), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := c.Store(context.Background(), Entry{
		AssetKey:         assetref.AssetKey(assetKey),
		LocalPath:        path,
		SizeBytes:        int64(len("FAKE VIDEO BYTES " + assetKey)),
		DownloadComplete: true,
		CreatedAt:        lastUsedAt,
		LastUsedAt:       lastUsedAt,
	}); err != nil {
		t.Fatalf("Store %s: %v", assetKey, err)
	}
	if err := c.MarkDownloadComplete(context.Background(), assetKey, path,
		int64(len("FAKE VIDEO BYTES "+assetKey))); err != nil {
		t.Fatalf("MarkDownloadComplete %s: %v", assetKey, err)
	}
	if _, err := c.DB().ExecContext(context.Background(),
		`UPDATE cached_assets SET last_used_at = ? WHERE asset_key = ?`,
		lastUsedAt.Format(time.RFC3339Nano), assetKey); err != nil {
		t.Fatalf("override last_used_at for %s: %v", assetKey, err)
	}
}

// TestCleanupPolicy_RaceCondition_T0ToT10s_NoDelete is the canonical
// user-spec scenario:
//   - T0:        snapshot taken with clip ALPHA included.
//   - T0+5s:     new job arrives; AcquireJobClips sets active_job_id
//     on row ALPHA (also bumps lease state).
//   - T0+10s:    cleanup runs. ALPHA MUST still be present (kept
//     by either lease OR grace OR protected-set).
//   - Side row:  BETA is seeded at T0 with no Acquire; not in the
//     T0 snapshot's protected set; grace=3m holds it
//     (now=T+10s − last_used_at=T0 = 10s < 3m).
func TestCleanupPolicy_RaceCondition_T0ToT10s_NoDelete(t *testing.T) {
	f := newPolicyFixture(t)
	ctx := context.Background()

	// Anchored clock: T0 = 2026-07-27T12:00:00Z so all assertions
	// are reproducible. The actual UTC value is irrelevant; what
	// matters is the relative offsets.
	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	T10s := T0.Add(10 * time.Second)

	// ALPHA: would be excluded by grace (last_used_at=T0, gap=10s
	// < 3m grace), so its survival is doubly assured (lease +
	// grace + protected set).
	seedRow(t, f.cache, f.dir, "ALPHA", T0)
	// BETA: not leased, not in T0 snapshot; survival rests solely
	// on the grace rule.
	seedRow(t, f.cache, f.dir, "BETA", T0)

	// T5s: new job arrives with same clip — Acquire sets the lease.
	if err := f.cache.Acquire(ctx, "ALPHA", "NEW-JOB-T+5s"); err != nil {
		t.Fatalf("Acquire ALPHA: %v", err)
	}

	// Snapshot taken at T0 contains ALPHA only (NOT BETA — BETA is
	// the recently-renamed-but-not-relisted "old" row).
	const snapshotAssetKeys = "ALPHA" // and only ALPHA
	_ = snapshotAssetKeys
	protectedIDs := []string{"ALPHA"}

	policy := CleanupPolicy{
		CleanupInterval: 5 * time.Minute,
		RecentUseGrace:  3 * time.Minute,
		SnapshotMaxAge:  2 * time.Minute,
	}

	stats, err := CleanupWithPolicy(ctx, f.cache, T0, protectedIDs, policy, T10s)
	if err != nil {
		t.Fatalf("CleanupWithPolicy: %v", err)
	}

	// ALPHA: kept via lease (Pass 9) — SkippedLeased+=1.
	if stats.SkippedLeased != 1 {
		t.Errorf("SkippedLeased=%d want 1 (ALPHA via active lease)", stats.SkippedLeased)
	}
	// BETA: kept via grace (Pass 12) — SkippedGrace+=1.
	// (Also kept hypothetically because T10s-T0 = 10s is well under
	// the 3m grace; BETA's last_used_at is unchanged.)
	if stats.SkippedGrace != 1 {
		t.Errorf("SkippedGrace=%d want 1 (BETA, last_used_at=T0, now=T+10s)", stats.SkippedGrace)
	}
	// Total kept: 2 rows. Removed: 0.
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0 (no row survived cleanup in user-spec scenario)", stats.Removed)
	}

	// Hard verification: both rows still on disk + index.
	if _, ok, _ := f.cache.Find(ctx, "ALPHA"); !ok {
		t.Errorf("ALPHA disappeared; cleanup MUST NOT delete it")
	}
	if _, ok, _ := f.cache.Find(ctx, "BETA"); !ok {
		t.Errorf("BETA disappeared; grace (3m) must have kept it at T+10s")
	}
}

// TestCleanupPolicy_GraceExpiry_DeletesAfter3Min: the inverse of the
// race-condition test. Once RecentUseGrace passes AND the row is not
// in the protected set AND no lease is held, the cleanup DELETES.
// Uses a FRESH snapshot (snapshot age < 2m max) so the staleness
// branch doesn't fire and confuse the test.
func TestCleanupPolicy_GraceExpiry_DeletesAfter3Min(t *testing.T) {
	f := newPolicyFixture(t)
	ctx := context.Background()

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	cleanupAt := T0.Add(6 * time.Minute)      // 6m > 3m grace
	snapAt := cleanupAt.Add(-5 * time.Second) // 5s before cleanup, age=5s

	seedRow(t, f.cache, f.dir, "OLD", T0)

	policy := CleanupPolicy{
		RecentUseGrace: 3 * time.Minute,
		SnapshotMaxAge: 2 * time.Minute,
	}

	stats, err := CleanupWithPolicy(ctx, f.cache, snapAt, []string{"OTHER"}, policy, cleanupAt)
	if err != nil {
		t.Fatalf("CleanupWithPolicy: %v", err)
	}
	if stats.Removed != 1 {
		t.Errorf("Removed=%d want 1 (OLD's grace 3m expired at T+6m)", stats.Removed)
	}
	if stats.SkippedGrace != 0 {
		t.Errorf("SkippedGrace=%d want 0 (grace has expired)", stats.SkippedGrace)
	}
	if _, ok, _ := f.cache.Find(ctx, "OLD"); ok {
		t.Errorf("OLD should have been removed after grace expiry")
	}
}

// TestCleanupPolicy_SnapshotStale_SkipsPass: when the master
// snapshot is older than policy.SnapshotMaxAge, the cleanup pass
// returns ErrSnapshotStale + zero Removed even when ordinary rules
// would have deleted rows. Operators MUST see this and either
// resync the worker or investigate the master snapshot loop.
func TestCleanupPolicy_SnapshotStale_SkipsPass(t *testing.T) {
	f := newPolicyFixture(t)
	ctx := context.Background()

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	snapAt := T0
	cleanupAt := T0.Add(5 * time.Minute) // 5m > 2m max_age

	// Row: would normally be deleted (not leased, complete, not in
	// protected set, last_used_at > 3m grace). The stale-snapshot
	// branch must prevent the deletion anyway.
	seedRow(t, f.cache, f.dir, "WILL_DELETE", T0)

	policy := CleanupPolicy{
		RecentUseGrace: 3 * time.Minute,
		SnapshotMaxAge: 2 * time.Minute,
	}

	stats, err := CleanupWithPolicy(ctx, f.cache, snapAt, []string{}, policy, cleanupAt)
	if !errors.Is(err, ErrSnapshotStale) {
		t.Errorf("err=%v; want ErrSnapshotStale", err)
	}
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0 (snapshot stale; cleanup must skip)", stats.Removed)
	}
	if stats.SkippedSnapshotStale != 1 {
		t.Errorf("SkippedSnapshotStale=%d want 1 (the WILL_DELETE row)", stats.SkippedSnapshotStale)
	}
	// Row must STILL exist (cleanup was skipped).
	if _, ok, _ := f.cache.Find(ctx, "WILL_DELETE"); !ok {
		t.Errorf("WILL_DELETE disappeared; stale-snapshot cleanup must be a no-op")
	}
}

// TestCleanupPolicy_NoSnapshot_GraceIsStillEffective: when
// snapshotGeneratedAt is the zero value (worker never polled),
// the staleness branch is skipped and the cleanup pass runs under
// lease + in-flight + grace rules alone.
func TestCleanupPolicy_NoSnapshot_IsFailSafe(t *testing.T) {
	f := newPolicyFixture(t)
	ctx := context.Background()

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	cleanupAt := T0.Add(5 * time.Second) // 5s < 3m grace → keep via grace

	seedRow(t, f.cache, f.dir, "RECENT", T0)

	policy := CleanupPolicy{
		RecentUseGrace: 3 * time.Minute,
		SnapshotMaxAge: 2 * time.Minute,
	}

	// snapshotGeneratedAt is the zero value: no snapshot scenario.
	stats, err := CleanupWithPolicy(ctx, f.cache, time.Time{}, []string{}, policy, cleanupAt)
	if !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("CleanupWithPolicy err=%v; want ErrSnapshotUnavailable", err)
	}
	if stats.SkippedSnapshotUnavailable != 1 {
		t.Errorf("SkippedSnapshotUnavailable=%d want 1", stats.SkippedSnapshotUnavailable)
	}
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0", stats.Removed)
	}
}

// TestLoadCleanupPolicy_DefaultsMatchSpec covers the operator-facing
// defaults requirement, including the ten-hour idle retention contract.
func TestLoadCleanupPolicy_DefaultsMatchSpec(t *testing.T) {
	// Clear any residual env.
	for _, k := range []string{
		"VELOX_CACHE_CLEANUP_INTERVAL",
		"VELOX_CACHE_RECENT_USE_GRACE",
		"VELOX_CACHE_IDLE_TTL",
		"VELOX_CACHE_SNAPSHOT_MAX_AGE",
	} {
		t.Setenv(k, "")
	}
	p := LoadCleanupPolicy()
	if p.CleanupInterval != 5*time.Minute {
		t.Errorf("CleanupInterval=%v want 5m", p.CleanupInterval)
	}
	if p.RecentUseGrace != 3*time.Minute {
		t.Errorf("RecentUseGrace=%v want 3m", p.RecentUseGrace)
	}
	if p.IdleTTL != 10*time.Hour {
		t.Errorf("IdleTTL=%v want 10h", p.IdleTTL)
	}
	if p.SnapshotMaxAge != 2*time.Minute {
		t.Errorf("SnapshotMaxAge=%v want 2m", p.SnapshotMaxAge)
	}
}

func TestCleanupWithPolicy_IdleTTLRemovesOnlyUnneededAssets(t *testing.T) {
	f := newPolicyFixture(t)
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	old := now.Add(-11 * time.Hour)
	fresh := now.Add(-30 * time.Minute)
	seedRow(t, f.cache, f.dir, "OLD", old)
	seedRow(t, f.cache, f.dir, "FUTURE", old)
	seedRow(t, f.cache, f.dir, "FRESH", fresh)

	policy := CleanupPolicy{IdleTTL: 10 * time.Hour, SnapshotMaxAge: 2 * time.Minute}
	stats, err := CleanupWithPolicy(context.Background(), f.cache, now, []string{"FUTURE"}, policy, now)
	if err != nil {
		t.Fatalf("CleanupWithPolicy: %v", err)
	}
	if stats.Removed != 1 || stats.SkippedProtected != 1 || stats.SkippedGrace != 1 {
		t.Fatalf("stats=%+v, want one removed, one protected, one fresh", stats)
	}
	for _, key := range []string{"FUTURE", "FRESH"} {
		entries, listErr := f.cache.List(context.Background())
		if listErr != nil {
			t.Fatalf("list after cleanup: %v", listErr)
		}
		found := false
		for _, entry := range entries {
			if string(entry.AssetKey) == key {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s was removed unexpectedly", key)
		}
	}
	entries, err := f.cache.List(context.Background())
	if err != nil {
		t.Fatalf("list after cleanup: %v", err)
	}
	for _, entry := range entries {
		if string(entry.AssetKey) == "OLD" {
			t.Fatal("OLD still present")
		}
	}
}
