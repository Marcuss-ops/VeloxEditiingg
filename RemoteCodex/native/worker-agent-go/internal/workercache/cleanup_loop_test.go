// Package workercache — CleanupLoop test matrix.

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

// ────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────

// cleanupLoopFixture installs a fresh in-memory cache, a temp dir,
// and a seed function per test. Cleans up on test exit via t.Cleanup.
type cleanupLoopFixture struct {
	cache   *Cache
	dir     string
	policy  CleanupPolicy
	cleanup func()
}

func newCleanupLoopFixture(t *testing.T) *cleanupLoopFixture {
	t.Helper()
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	dir := t.TempDir()
	return &cleanupLoopFixture{
		cache:   cache,
		dir:     dir,
		policy:  LoadCleanupPolicyDefaults_CleanupPolicyHelper(),
		cleanup: func() { _ = cache.Close() },
	}
}

// LoadCleanupPolicyDefaults exposes the canonical defaults used by
// LoadCleanupPolicy without parsing the OS environment (which CI
// or test rigs might mis-pollute). Implemented as a thin wrapper
// around LoadCleanupPolicy after t.Setenv-clear to provide a stable
// default policy without environment interference.
type alwaysReadyProtectionBarrier struct{}

func (alwaysReadyProtectionBarrier) WaitReady(context.Context) error { return nil }
func (alwaysReadyProtectionBarrier) IsReady() bool                   { return true }

func LoadCleanupPolicyDefaults_CleanupPolicyHelper() CleanupPolicy {
	var p CleanupPolicy
	// Match the spec defaults — used by tests that don't override
	// the env. Importing `LoadCleanupPolicy` here would re-read
	// the OS env which may be polluted by the test runner.
	p.CleanupInterval = 5 * time.Minute
	p.RecentUseGrace = 3 * time.Minute
	p.SnapshotMaxAge = 2 * time.Minute
	return p
}

// seedRow inserts a fully-downloaded row at the supplied
// last_used_at instant — same shape as cleanup_policy_test.go's
// helper, repeated locally so each package's tests can run
// independently.
func seedRowLoop(_ *testing.T, c *Cache, dir, assetKey string, lastUsedAt time.Time) {
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, assetKey+".mp4")
	_ = os.WriteFile(path, []byte("FAKE VIDEO BYTES "+assetKey), 0o644)
	_ = c.Store(context.Background(), Entry{
		AssetKey:         assetref.AssetKey(assetKey),
		LocalPath:        path,
		SizeBytes:        int64(len("FAKE VIDEO BYTES " + assetKey)),
		DownloadComplete: true,
		CreatedAt:        lastUsedAt,
		LastUsedAt:       lastUsedAt,
	})
	_ = c.MarkDownloadComplete(context.Background(), assetKey, path,
		int64(len("FAKE VIDEO BYTES "+assetKey)))
	_, _ = c.DB().ExecContext(context.Background(),
		`UPDATE cached_assets SET last_used_at = ? WHERE asset_key = ?`,
		lastUsedAt.Format(time.RFC3339Nano), assetKey)
}

// ────────────────────────────────────────────────────────────────────────
// tests
// ────────────────────────────────────────────────────────────────────────

// TestCleanupLoop_TickOnce_NoSnapshot: nil snapshot source is fail-safe.
func TestCleanupLoop_TickOnce_NoSnapshot(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	seedRowLoop(t, f.cache, f.dir, "X", T0)

	cl := &CleanupLoop{
		Cache:  f.cache,
		Policy: f.policy,
		// Snapshot is intentionally nil.
		Interval:     5 * time.Minute,
		Pressure:     PressureEvictionConfig{HighWatermarkPercent: 80, LowWatermarkPercent: 72, BatchSize: 128},
		UsagePercent: func() int { return 90 },
		Now:          func() time.Time { return T0 },
	}
	stats, err := cl.TickOnce(context.Background())
	if !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("TickOnce err=%v; want ErrSnapshotUnavailable", err)
	}
	if stats.Removed != 0 || stats.Attempted != 0 {
		t.Errorf("stats=%+v, want zero evictions without a snapshot", stats)
	}
}

// TestCleanupLoop_TickOnce_PressureEvictsLRUOnly: at/above the HIGH
// watermark the loop evicts the LRU blob but never the snapshot-protected
// blob, stopping when the usage probe reports below the LOW watermark.
func TestCleanupLoop_TickOnce_PressureEvictsLRUOnly(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	seedRowLoop(t, f.cache, f.dir, "PROTECTED", T0)
	seedRowLoop(t, f.cache, f.dir, "LRU", T0.Add(-1*time.Hour))

	// Snapshot: PROTECTED is in; LRU is not. GeneratedAt=T0 keeps
	// staleness < SnapshotMaxAge (2m) deterministically.
	cl := &CleanupLoop{
		Cache:    f.cache,
		Policy:   f.policy,
		Snapshot: &FixedSnapshotSource{GeneratedAt: T0, ProtectedIDs: []string{"PROTECTED"}},
		Interval: 5 * time.Minute,
		Pressure: PressureEvictionConfig{HighWatermarkPercent: 80, LowWatermarkPercent: 72, BatchSize: 128},
		Now:      func() time.Time { return T0 },
	}
	// One above-HIGH reading, then below-LOW so the pass stops after the
	// first LRU batch (mimicking the freed bytes of the evicted blob).
	calls := 0
	cl.UsagePercent = func() int {
		calls++
		if calls == 1 {
			return 85
		}
		return 70
	}

	stats, err := cl.TickOnce(context.Background())
	if err != nil {
		t.Fatalf("TickOnce: %v", err)
	}
	if stats.Removed != 1 {
		t.Errorf("Removed=%d want 1 (only the unleased LRU blob)", stats.Removed)
	}
	if stats.Protected != 1 {
		t.Errorf("Protected=%d want 1 (snapshot-protected blob skipped)", stats.Protected)
	}
	// LRU is physically evicted; PROTECTED stays.
	if _, err := os.Stat(filepath.Join(f.dir, "LRU.mp4")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("LRU blob should be evicted, stat err=%v", err)
	}
	if _, ok, _ := f.cache.Find(context.Background(), "PROTECTED"); !ok {
		t.Errorf("PROTECTED disappeared despite being in snapshot")
	}
}

// TestCleanupLoop_TickOnce_BelowHighWatermarkNoEviction: even with a fresh
// snapshot and unprotected warm blobs, a usage reading below the HIGH
// watermark makes the tick a no-op.
func TestCleanupLoop_TickOnce_BelowHighWatermarkNoEviction(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	seedRowLoop(t, f.cache, f.dir, "WARM", T0)

	cl := &CleanupLoop{
		Cache:        f.cache,
		Policy:       f.policy,
		Snapshot:     &FixedSnapshotSource{GeneratedAt: T0},
		Interval:     5 * time.Minute,
		Pressure:     PressureEvictionConfig{HighWatermarkPercent: 80, LowWatermarkPercent: 72, BatchSize: 128},
		UsagePercent: func() int { return 79 },
		Now:          func() time.Time { return T0 },
	}
	stats, err := cl.TickOnce(context.Background())
	if err != nil {
		t.Fatalf("TickOnce: %v", err)
	}
	if stats.Removed != 0 || stats.Attempted != 0 {
		t.Errorf("stats=%+v, want zero evictions below the HIGH watermark", stats)
	}
}

// TestCleanupLoop_TickOnce_StaleSnapshotNoEviction: a snapshot older than
// SnapshotMaxAge short-circuits the pressure pass (fail-safe) and never
// evicts, matching the ErrSnapshotStale sentinel.
func TestCleanupLoop_TickOnce_StaleSnapshotNoEviction(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	seedRowLoop(t, f.cache, f.dir, "WARM", T0.Add(-1*time.Hour))

	cl := &CleanupLoop{
		Cache:        f.cache,
		Policy:       f.policy, // SnapshotMaxAge = 2m
		Snapshot:     &FixedSnapshotSource{GeneratedAt: T0.Add(-5 * time.Minute)},
		Interval:     5 * time.Minute,
		Pressure:     PressureEvictionConfig{HighWatermarkPercent: 80, LowWatermarkPercent: 72, BatchSize: 128},
		UsagePercent: func() int { return 90 },
		Now:          func() time.Time { return T0 },
	}
	stats, err := cl.TickOnce(context.Background())
	if !errors.Is(err, ErrSnapshotStale) {
		t.Fatalf("TickOnce err=%v; want ErrSnapshotStale", err)
	}
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0 on stale snapshot", stats.Removed)
	}
}

// TestCleanupLoop_Run_RespectsContextDone: Run returns ctx.Err when
// the context is cancelled. Uses a long interval (1h) so the only
// termination path is via cancellation.
func TestCleanupLoop_Run_RespectsContextDone(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	ctx, cancel := context.WithCancel(context.Background())
	cl := &CleanupLoop{
		Cache:    f.cache,
		Policy:   f.policy,
		Snapshot: &FixedSnapshotSource{},
		Barrier:  alwaysReadyProtectionBarrier{},
		Interval: 1 * time.Hour,
	}

	done := make(chan error, 1)
	go func() {
		done <- cl.Run(ctx)
	}()

	// Cancel after a brief moment.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Errorf("Run returned nil; want ctx.Canceled or context.Canceled")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Run did not return within 500ms of ctx cancel")
	}
}

// TestCleanupLoop_Run_JobDoneTriggersTick: a signal on the JobDone
// channel triggers an immediate Tick (without waiting for the
// periodic ticker). Uses a very long ticker interval so the only
// source of the happening tick is JobDone.
func TestCleanupLoop_Run_JobDoneTriggersTick(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	seedRowLoop(t, f.cache, f.dir, "JOB-A", T0)
	seedRowLoop(t, f.cache, f.dir, "JOB-B", T0.Add(-1*time.Hour))

	// Capture each tick via OnTick. `jobDone` is bidirectional so
	// we can send; the CleanupLoop field is `<-chan struct{}` so
	// we MUST assign via a local variable (typed assertion would
	// not compile — receive-only channels are not assignable from
	// bidirectional ones at field-position time).
	var ticks []int
	jobDone := make(chan struct{}, 1)
	cl := &CleanupLoop{
		Cache:    f.cache,
		Policy:   f.policy,
		Snapshot: &FixedSnapshotSource{},
		Interval: 1 * time.Hour,
		OnTick:   func(_ PressureEvictionStats, _ error) { ticks = append(ticks, 1) },
		Barrier:  alwaysReadyProtectionBarrier{},
		JobDone:  jobDone,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- cl.Run(ctx)
	}()

	// Allow the initial tick to fire.
	time.Sleep(30 * time.Millisecond)

	// Send a JobDone signal — forces a 2nd tick before reaching
	// the 1h ticker interval.
	jobDone <- struct{}{}
	time.Sleep(50 * time.Millisecond)

	cancel()
	<-done

	if len(ticks) < 2 {
		t.Errorf("ticks observed=%d, want ≥2 (initial tick + JobDone-triggered)", len(ticks))
	}
}
