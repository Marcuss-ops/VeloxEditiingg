// Package protectedasset — service tests.
//
// The 6 must-have behaviours from the design discussion:
//  1. Refresh populates Snapshot correctly (Version=1, IDs sorted,
//     GeneratedAt ≈ now, LookaheadJobs matches row count).
//  2. 3 successive refreshes → Version goes 1, 2, 3 strictly.
//  3. Empty result → ProtectedAssetKeys non-nil empty, LookaheadJobs=0,
//     Version still increments.
//  4. Snapshot() before any Refresh returns the zero Snapshot.
//  5. Concurrent Snapshot() during Refresh() does not panic
//     (race detector must be clean).
//  6. Sorting invariant — out-of-order IDs collapse to one
//     ascending slice.
//
// We use a fakeRepo (Repo interface), NOT a fake *sql.Rows, so
// tests do not require go-sqlmock or a SQLite fixture. Production
// wiring happens through RepoFunc → dispatchable.ListNextDispatchableJobs
// (see service.go package doc).
package protectedasset

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/dispatchable"
)

// Compile-time guard: ensure dispatchable.Job is the type passed
// across the boundary so a future signature drift shows up at
// build time rather than as a runtime panic in tests.
var _ dispatchable.Job

// fakeRepo implements Repo via an in-memory list of dispatched jobs.
// We capture callCount via atomic to make the concurrency test
// deterministic under -race.
type fakeRepo struct {
	mu        sync.Mutex
	jobs      [][]byte // per-job payloads (each is a json.RawMessage)
	err       error
	delay     time.Duration
	callCount atomic.Int64
}

func (f *fakeRepo) ListNextDispatchableJobs(_ context.Context, _ int) ([]dispatchable.Job, error) {
	f.callCount.Add(1)
	f.mu.Lock()
	delay, jobs, err := f.delay, f.jobs, f.err
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return nil, err
	}
	out := make([]dispatchable.Job, 0, len(jobs))
	for i, payloadStr := range jobs {
		out = append(out, dispatchable.Job{
			TaskID:  "task-" + string(rune('A'+i)),
			JobID:   "job-" + string(rune('A'+i)),
			Payload: json.RawMessage(payloadStr),
		})
	}
	return out, nil
}

func (f *fakeRepo) setJobs(payloads []string) {
	f.mu.Lock()
	f.jobs = make([][]byte, len(payloads))
	for i, p := range payloads {
		f.jobs[i] = []byte(p)
	}
	f.mu.Unlock()
}

func (f *fakeRepo) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

// fixedClock returns the same timestamp every call. Used in tests
// to pin GeneratedAt for assertionability.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 1. Refresh populates Snapshot correctly on first call.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Refresh_PopulatesSnapshot(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	repo.setJobs([]string{
		`{"scenes":[{"clip_link":"https://drive.google.com/file/d/CCC/view"}]}`,
		`{"scenes":[{"clip_links":["https://drive.google.com/uc?id=AAA"]}]}`,
		`{"scenes":[{"video_url":"https://drive.google.com/file/d/BBB/view","source_url":"https://drive.google.com/open?id=DDD"}]}`,
	})

	now := time.Date(2026, 7, 27, 14, 30, 0, 0, time.UTC)
	svc := NewService(repo, 10).SetClock(fixedClock(now))

	if err := svc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	got := svc.Snapshot()
	if got.Version != 1 {
		t.Errorf("Version = %d, want 1", got.Version)
	}
	if got.LookaheadJobs != 3 {
		t.Errorf("LookaheadJobs = %d, want 3", got.LookaheadJobs)
	}
	if !got.GeneratedAt.Equal(now) {
		t.Errorf("GeneratedAt = %v, want %v", got.GeneratedAt, now)
	}
	wantIDs := []string{"AAA", "BBB", "CCC", "DDD"}
	if !reflect.DeepEqual(got.ProtectedAssetKeys, wantIDs) {
		t.Errorf("ProtectedAssetKeys = %v, want %v (ascending sort)", got.ProtectedAssetKeys, wantIDs)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 2. 3 successive refreshes → Version strictly monotonic.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Refresh_MonotonicVersion(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	repo.setJobs([]string{`{"scenes":[{"clip_link":"https://drive.google.com/uc?id=X"}]}`})

	svc := NewService(repo, 10)
	for _, expectedVersion := range []uint64{1, 2, 3} {
		if err := svc.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
		got := svc.Snapshot().Version
		if got != expectedVersion {
			t.Errorf("Version after refresh #%d = %d, want %d", expectedVersion, got, expectedVersion)
		}
	}
	if n := repo.callCount.Load(); n != 3 {
		t.Errorf("repo callCount = %d, want 3", n)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  3. Empty repo → empty non-nil ProtectedAssetKeys, LookaheadJobs=0,
//     Version still increments (we never return the zero snapshot
//     post-Refresh).
//
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Refresh_EmptyRepo(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	// jobs slice stays empty

	svc := NewService(repo, 10)
	if err := svc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh on empty repo: %v", err)
	}

	got := svc.Snapshot()
	if got.Version != 1 {
		t.Errorf("Version on empty repo = %d, want 1 (Version increments regardless of result row count)", got.Version)
	}
	if got.LookaheadJobs != 0 {
		t.Errorf("LookaheadJobs = %d, want 0", got.LookaheadJobs)
	}
	if got.ProtectedAssetKeys == nil {
		t.Error("ProtectedAssetKeys = nil; want empty non-nil slice so HTTP handler range-loop is safe")
	}
	if len(got.ProtectedAssetKeys) != 0 {
		t.Errorf("ProtectedAssetKeys length = %d, want 0", len(got.ProtectedAssetKeys))
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  4. Snapshot() before any Refresh returns the zero Snapshot.
//     HTTP handlers MUST use Version == 0 as the "snapshot
//     unavailable" signal — this test pins it.
//
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Snapshot_ZeroBeforeFirstRefresh(t *testing.T) {
	t.Parallel()
	svc := NewService(&fakeRepo{}, 10)
	got := svc.Snapshot()
	if got.Version != 0 {
		t.Errorf("Version before first Refresh = %d, want 0", got.Version)
	}
	if !got.GeneratedAt.IsZero() {
		t.Errorf("GeneratedAt before first Refresh = %v, want zero", got.GeneratedAt)
	}
	if got.ProtectedAssetKeys != nil {
		t.Errorf("ProtectedAssetKeys before first Refresh = %v, want nil", got.ProtectedAssetKeys)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  5. Sorting invariant — out-of-order and duplicate IDs collapse
//     into a single ascending, deduplicated slice.
//
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Refresh_SortingAndDedup(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	// Deliberately: same ID across 3 slots; out-of-order IDs across jobs.
	repo.setJobs([]string{
		`{"scenes":[{"clip_links":[
			"https://drive.google.com/file/d/ZZZ/view",
			"https://drive.google.com/uc?id=AAA",
			"https://drive.google.com/uc?id=ZZZ"
		]}]}`,
		`{"scenes":[{"clip_link":"https://drive.google.com/file/d/MMM/view"}]}`,
		`{"scenes":[{"source_url":"https://drive.google.com/open?id=AAA"}]}`,
	})

	svc := NewService(repo, 10)
	if err := svc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	want := []string{"AAA", "MMM", "ZZZ"}
	got := svc.Snapshot().ProtectedAssetKeys
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ProtectedAssetKeys = %v, want %v (sorted ascending, dedup)", got, want)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  6. Concurrent Snapshot() during Refresh() — race detector must
//     stay silent. Run with -race.
//
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_ConcurrentSnapshotDuringRefresh_RaceSafe(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	repo.setJobs([]string{
		`{"scenes":[{"clip_link":"https://drive.google.com/file/d/X1/view"},{"clip_links":["https://drive.google.com/uc?id=X2"]}]}`,
	})

	svc := NewService(repo, 10)

	// Reader goroutines: spam Snapshot() while Refresh runs in the
	// main goroutine. Each reader logs what it observed; a torn /
	// mid-update read would panic (tagged field) — under -race the
	// race detector catches unsynchronised access.
	const readers = 8
	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = svc.Snapshot()
			}
		}()
	}

	// 50 refreshes from the test goroutine; readers run in parallel.
	for i := 0; i < 50; i++ {
		if err := svc.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh[%d]: %v", i, err)
		}
	}
	wg.Wait()

	if got := svc.Snapshot().Version; got < 50 {
		t.Errorf("Version after 50 refreshes = %d, want >= 50 (some may be lost to race semantics)", got)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 7. Refresh propagates repo errors. Snapshot unchanged.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Refresh_RepoErrorPropagatesAndSnapshotUnchanged(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	repo.setJobs([]string{`{"scenes":[{"clip_link":"https://drive.google.com/file/d/KEEP/view"}]}`})

	svc := NewService(repo, 10)
	if err := svc.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	beforeVersion := svc.Snapshot().Version

	wantErr := errors.New("db gone")
	repo.setErr(wantErr)

	if err := svc.Refresh(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("Refresh error = %v, want wrap of %v", err, wantErr)
	}
	if after := svc.Snapshot().Version; after != beforeVersion {
		t.Errorf("Version advanced despite repo error: before=%d after=%d (failed refresh must NOT swap snapshot)", beforeVersion, after)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 8. Run loop ticks until ctx done.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
func TestService_Run_TicksUntilContextDone(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	repo.setJobs([]string{`{"scenes":[{"clip_link":"https://drive.google.com/file/d/RUN/view"}]}`})

	svc := NewService(repo, 10)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// extremely short interval so the ticker fires fast
		done <- svc.Run(ctx, 1*time.Millisecond)
	}()

	// Let it tick a few times.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s after cancel")
	}

	if n := repo.callCount.Load(); n < 1 {
		t.Errorf("repo callCount = %d, want at least 1 tick between start and cancel", n)
	}
	if v := svc.Snapshot().Version; v < 1 {
		t.Errorf("Version after Run = %d, want at least 1 (refresh fired during tick)", v)
	}
}
