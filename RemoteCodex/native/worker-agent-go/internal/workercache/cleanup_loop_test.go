// Package workercache — CleanupLoop test matrix.

package workercache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
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
func seedRowLoop(_ *testing.T, c *Cache, dir, driveID string, lastUsedAt time.Time) {
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, driveID+".mp4")
	_ = os.WriteFile(path, []byte("FAKE VIDEO BYTES "+driveID), 0o644)
	_ = c.Store(context.Background(), Entry{
		DriveFileID:      driveID,
		LocalPath:        path,
		SizeBytes:        int64(len("FAKE VIDEO BYTES " + driveID)),
		DownloadComplete: true,
		CreatedAt:        lastUsedAt,
		LastUsedAt:       lastUsedAt,
	})
	_ = c.MarkDownloadComplete(context.Background(), driveID, path,
		int64(len("FAKE VIDEO BYTES "+driveID)))
	_, _ = c.DB().ExecContext(context.Background(),
		`UPDATE cached_assets SET last_used_at = ? WHERE drive_file_id = ?`,
		lastUsedAt.Format(time.RFC3339Nano), driveID)
}

// ────────────────────────────────────────────────────────────────────────
// tests
// ────────────────────────────────────────────────────────────────────────

// TestCleanupLoop_TickOnce_NoSnapshot: nil snapshot source means
// zero-value time → leass/in-flight/grace rules apply, no
// staleness short-circuit. TickOnce returns cleanly.
func TestCleanupLoop_TickOnce_NoSnapshot(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	seedRowLoop(t, f.cache, f.dir, "X", T0)

	cl := &CleanupLoop{
		Cache:  f.cache,
		Policy: f.policy,
		// Snapshot is intentionally nil.
		Interval: 5 * time.Minute,
	}
	stats, err := cl.TickOnce(context.Background())
	if err != nil {
		t.Fatalf("TickOnce: %v", err)
	}
	if stats.SkippedGrace != 1 {
		t.Errorf("SkippedGrace=%d want 1 (X is within grace window)", stats.SkippedGrace)
	}
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0", stats.Removed)
	}
}

// TestCleanupLoop_TickOnce_SnapshotProtected: protects rows via the
// snapshot's protected IDs.
func TestCleanupLoop_TickOnce_SnapshotProtected(t *testing.T) {
	f := newCleanupLoopFixture(t)
	t.Cleanup(f.cleanup)

	T0 := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	oldTime := T0
	seedRowLoop(t, f.cache, f.dir, "PROTECTED", T0)
	seedRowLoop(t, f.cache, f.dir, "OLD", oldTime.Add(-10*time.Minute))

	// Snapshot: PROTECTED is in; OLD is not.
	cl := &CleanupLoop{
		Cache:    f.cache,
		Policy:   f.policy,
		Snapshot: &FixedSnapshotSource{GeneratedAt: T0, ProtectedIDs: []string{"PROTECTED"}},
		Interval: 5 * time.Minute,
	}
	stats, err := cl.TickOnce(context.Background())
	if err != nil {
		t.Fatalf("TickOnce: %v", err)
	}
	if stats.SkippedProtected != 1 {
		t.Errorf("SkippedProtected=%d want 1 (PROTECTED via snapshot)", stats.SkippedProtected)
	}
	// OLD: grace expired (last_used_at=T-10m, now=T0, grace=3m → expired)
	// → must be removed.
	if stats.Removed != 1 {
		t.Errorf("Removed=%d want 1 (OLD is beyond grace, not in snapshot, not leased)", stats.Removed)
	}
	// PROTECTED stays in cache.
	if _, ok, _ := f.cache.Find(context.Background(), "PROTECTED"); !ok {
		t.Errorf("PROTECTED disappeared despite being in snapshot")
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

	// Capture each tick via OnTick.
	var ticks []int
	cl := &CleanupLoop{
		Cache:    f.cache,
		Policy:   f.policy,
		Snapshot: &FixedSnapshotSource{},
		Interval: 1 * time.Hour,
		OnTick:   func(_ CleanupStats, _ error) { ticks = append(ticks, 1) },
		JobDone:  make(chan struct{}, 1),
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
	cl.JobDone.(chan struct{}) <- struct{}{}
	time.Sleep(50 * time.Millisecond)

	cancel()
	<-done

	if len(ticks) < 2 {
		t.Errorf("ticks observed=%d, want ≥2 (initial tick + JobDone-triggered)", len(ticks))
	}
}
