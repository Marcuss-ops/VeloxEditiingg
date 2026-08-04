// Package worker — protected-asset snapshot poller tests (Pass 7).
//
// The tests use httptest.NewServer as a stand-in for the master
// protected-asset endpoint. Each test sets a server that responds
// per its own scenario; the *api.Client is wired against that
// server's URL; TickOnce (or Run) drives the poller; assertions
// verify the user-spec invariants:
//
//   - 200 valid → Snapshot() returns the parsed snapshot (no error).
//   - 500 error → previous good snapshot is preserved; OnError fires.
//   - Timeout → no snapshot is set initially; subsequent successful
//     poll applies a snapshot (last-good retained across failures).
//   - Empty DriveFileIDs in a 200 response IS success (master
//     signals zero jobs in queue; cleanup loop's grace rule is the
//     safety net against accidental mass-eviction).
//   - Run fires the initial tick immediately on entry (no warm-up
//     delay), and terminates cleanly on ctx.Done().
//
// Each test uses time.Hour as the poller Interval so the ticker
// never fires inside the test window — TickOnce drives the
// state transitions deterministically.

package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
)

// snapshotJSON is a tiny helper to produce a canonical master
// JSON body for protected-asset snapshot responses. Centralised
// so the test fixtures match the master wire shape EXACTLY.
func snapshotJSON(version uint64, generatedAt string, lookahead int, ids []string) []byte {
	body, _ := json.Marshal(api.ProtectedAssetSnapshot{
		Version:       version,
		GeneratedAt:   generatedAt,
		LookaheadJobs: lookahead,
		DriveFileIDs:  ids,
	})
	return body
}

// newCountingServer returns an httptest.Server whose handler
// counts calls and dispatches to caller-supplied behaviour.
// Tests use the counter to verify "tick fired N times" — this
// matters for Run's initial-tick semantics (the first invocation
// MUST happen on entry, before the ticker).
func newCountingServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *int32) {
	t.Helper()
	var count int32
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		handler(w, r)
	})
	srv := httptest.NewServer(wrapped)
	t.Cleanup(srv.Close)
	return srv, &count
}

// newPollerWithClient wires a fresh Poller against a real
// *api.Client over the supplied server's URL. Uses a 1h Interval
// so the ticker never fires inside any test (TickOnce drives
// state transitions).
func newPollerWithClient(t *testing.T, srvURL string, opts ...api.ClientOption) *ProtectedAssetsPoller {
	t.Helper()
	c := api.NewClient(srvURL, opts...)
	// Run's bootstrap gate requires both registration and a bearer token;
	// TickOnce tests remain deterministic while carrying the same
	// authenticated-client contract as production.
	c.SetAuthToken("test-worker-session-token")
	p := NewProtectedAssetsPoller(c, time.Hour)
	p.SnapshotMaxAge = 0
	return p
}

// ────────────────────────────────────────────────────────────────────────────
// 200 happy path
// ────────────────────────────────────────────────────────────────────────────

// TestPoller_200_HappyPath_AppliesSnapshot: a single 200 response
// with a non-empty drive_file_ids set is parsed, stored, and
// retrievable via Snapshot(). OnSuccess fires; OnError does not.
func TestPoller_200_HappyPath_AppliesSnapshot(t *testing.T) {
	srv, _ := newCountingServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s, want GET", r.Method)
		}
		if r.URL.Path != api.ProtectedAssetsPath {
			t.Errorf("path=%s, want %s", r.URL.Path, api.ProtectedAssetsPath)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(snapshotJSON(42, "2026-07-27T12:00:00Z", 10, []string{"ABC123", "TYSON001"}))
	})
	t.Cleanup(srv.Close)

	var successCount int32
	p := newPollerWithClient(t, srv.URL)
	p.OnSuccess = func(s *api.ProtectedAssetSnapshot) {
		atomic.AddInt32(&successCount, 1)
	}

	if err := p.TickOnce(context.Background()); err != nil {
		t.Fatalf("TickOnce: %v", err)
	}

	snap := p.Snapshot()
	if snap == nil {
		t.Fatalf("Snapshot() returned nil after successful tick")
	}
	if snap.Version != 42 {
		t.Errorf("Version=%d, want 42", snap.Version)
	}
	if snap.LookaheadJobs != 10 {
		t.Errorf("LookaheadJobs=%d, want 10", snap.LookaheadJobs)
	}
	if got, want := snap.DriveFileIDs, []string{"ABC123", "TYSON001"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("DriveFileIDs=%v, want %v", got, want)
	}
	if snap.GeneratedAt != "2026-07-27T12:00:00Z" {
		t.Errorf("GeneratedAt=%q, want %q", snap.GeneratedAt, "2026-07-27T12:00:00Z")
	}

	if atomic.LoadInt32(&successCount) != 1 {
		t.Errorf("OnSuccess fired %d times, want 1", successCount)
	}
}

// TestPoller_200_EmptyDriveIDs_IsSuccess: master returns 200 with
// empty drive_file_ids — this is a legit "no jobs in queue"
// response, NOT a failure. The poller applies the snapshot (which
// is empty) and OnSuccess fires. The cleanup loop's grace rule
// (Pass 12) is the safety net — it must NOT be misinterpreted by
// the poller layer as "evict everything past grace".
func TestPoller_200_EmptyDriveIDs_IsSuccess(t *testing.T) {
	srv, _ := newCountingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(snapshotJSON(7, "2026-07-27T12:00:00Z", 0, []string{}))
	})
	t.Cleanup(srv.Close)

	var errorCount int32
	p := newPollerWithClient(t, srv.URL)
	p.OnError = func(err error) {
		atomic.AddInt32(&errorCount, 1)
	}

	if err := p.TickOnce(context.Background()); err != nil {
		t.Fatalf("TickOnce on empty-but-valid response returned err: %v", err)
	}

	snap := p.Snapshot()
	if snap == nil {
		t.Fatalf("Snapshot() nil after empty-but-valid 200")
	}
	if len(snap.DriveFileIDs) != 0 {
		t.Errorf("DriveFileIDs=%v, want empty", snap.DriveFileIDs)
	}
	if atomic.LoadInt32(&errorCount) != 0 {
		t.Errorf("OnError fired %d times on 200-with-empty-list, want 0", errorCount)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// 500 error — keeps last good
// ────────────────────────────────────────────────────────────────────────────

// TestPoller_500_KeepsLastGood: a previous successful poll set
// snapshot v=1; the next tick sees a 500. The poller must NOT
// reset p.snap to nil — the OLD snapshot pointer survives the
// failure. OnError fires with a wrapped error chain so callers
// can branch via errors.Is / errors.As.
func TestPoller_500_KeepsLastGood(t *testing.T) {
	var calls int32
	srv, _ := newCountingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// First invocation: a perfectly valid snapshot.
			w.WriteHeader(http.StatusOK)
			w.Write(snapshotJSON(1, "2026-07-27T12:00:00Z", 3, []string{"PRESERVED"}))
		} else {
			// Subsequent calls: 500.
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}
	})
	t.Cleanup(srv.Close)

	var errCount int32
	p := newPollerWithClient(t, srv.URL)
	p.OnError = func(err error) {
		atomic.AddInt32(&errCount, 1)
	}

	// First tick: success — capture the SNAPSHOT POINTER (not a
	// copy) so we can prove it survives a subsequent failure.
	if err := p.TickOnce(context.Background()); err != nil {
		t.Fatalf("first TickOnce: %v", err)
	}
	firstSnapshot := p.Snapshot()
	if firstSnapshot == nil {
		t.Fatal("Snapshot() nil after first success")
	}
	if firstSnapshot.Version != 1 || firstSnapshot.DriveFileIDs[0] != "PRESERVED" {
		t.Fatalf("unexpected first snapshot %+v", firstSnapshot)
	}

	// Second tick: 500. The poller returns the error and DOES
	// NOT touch p.snap.
	if err := p.TickOnce(context.Background()); err == nil {
		t.Fatal("second TickOnce returned nil; expected 500 error")
	}

	// Snapshot pointer identity MUST be preserved across the
	// failure. Two reads must compare equal — that's the
	// "don't overwrite on error" invariant in code form.
	secondSnapshot := p.Snapshot()
	if secondSnapshot != firstSnapshot {
		t.Errorf("Snapshot() returned a new pointer after 500; want same pointer as before")
	}
	if secondSnapshot.Version != 1 || secondSnapshot.DriveFileIDs[0] != "PRESERVED" {
		t.Errorf("Snapshot mutated by failure: %+v, want v=1 [PRESERVED]", secondSnapshot)
	}

	// Third tick: 500 again. Snapshot pointer still preserved.
	if err := p.TickOnce(context.Background()); err == nil {
		t.Fatal("third TickOnce returned nil; expected 500 error")
	}
	if p.Snapshot() != firstSnapshot {
		t.Errorf("Snapshot pointer mutated by second consecutive 500")
	}

	// OnError fired exactly twice (calls #2 and #3).
	if got := atomic.LoadInt32(&errCount); got != 2 {
		t.Errorf("OnError fired %d times, want 2", got)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Timeout — keeps last good
// ────────────────────────────────────────────────────────────────────────────

// TestPoller_Timeout_KeepsLastGoodAndRecovery: sleeper server +
// tight client timeout exercises the transport-level timeout
// path. Phase A: timeout → nil snapshot (no prior good to keep).
// Phase B: success → snapshot applied. Phase C: timeout again →
// the previous good snapshot pointer is RETAINED.
func TestPoller_Timeout_KeepsLastGoodAndRecovery(t *testing.T) {
	var calls int32
	srv, _ := newCountingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		switch n {
		case 1:
			// Phase A: hang past client timeout.
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			w.Write(snapshotJSON(99, "2026-07-27T12:00:00Z", 0, nil))
		case 2:
			// Phase B: respond quickly with a fresh snapshot.
			w.WriteHeader(http.StatusOK)
			w.Write(snapshotJSON(50, "2026-07-27T12:01:00Z", 5, []string{"RECOVERED"}))
		default:
			// Phase C: another timeout.
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			w.Write(snapshotJSON(99, "2026-07-27T12:00:00Z", 0, nil))
		}
	})
	t.Cleanup(srv.Close)

	// Tight client timeout forces the transport-level timeout
	// path inside *api.Client.doRequest.
	p := newPollerWithClient(t, srv.URL, api.WithTimeout(50*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Phase A: timeout. Snapshot stays nil because we have
	// nothing prior to keep.
	if err := p.TickOnce(ctx); err == nil {
		t.Fatal("phase A: TickOnce succeeded; expected timeout")
	}
	if p.Snapshot() != nil {
		t.Errorf("phase A: Snapshot() != nil after timeout with no prior good (got %+v)", p.Snapshot())
	}

	// Phase B: success. Snapshot applied.
	if err := p.TickOnce(ctx); err != nil {
		t.Fatalf("phase B: TickOnce: %v", err)
	}
	good := p.Snapshot()
	if good == nil {
		t.Fatal("phase B: Snapshot() nil after success")
	}
	if good.Version != 50 || good.DriveFileIDs[0] != "RECOVERED" {
		t.Errorf("phase B: Snapshot=%+v, want v=50 [RECOVERED]", good)
	}

	// Phase C: another timeout. Snapshot pointer identity
	// MUST survive.
	if err := p.TickOnce(ctx); err == nil {
		t.Fatal("phase C: TickOnce succeeded; expected timeout")
	}
	if p.Snapshot() != good {
		t.Errorf("phase C: Snapshot pointer mutated by timeout (want same as phase B)")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Run semantics
// ────────────────────────────────────────────────────────────────────────────

// TestPoller_Run_RespectsContextDone: Run returns ctx.Err() when
// the context is cancelled. Uses a long Interval so the only
// termination path is via cancellation.
func TestPoller_Run_RespectsContextDone(t *testing.T) {
	telemetry.ResetForTest()
	telemetry.MarkRegistered(true)
	t.Cleanup(telemetry.ResetForTest)
	srv, _ := newCountingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(snapshotJSON(1, "2026-07-27T12:00:00Z", 1, []string{"A"}))
	})
	t.Cleanup(srv.Close)

	p := newPollerWithClient(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	// Cancel after a brief moment. The loop MUST return.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Errorf("Run returned nil; want context.Canceled or wrapped equivalent")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Run did not return within 500ms of ctx cancel")
	}
}

// TestPoller_Run_FiresInitialTickOnEntry: Run MUST drive one tick
// synchronously on entry, before any sleep. The test verifies
// this by counting server hits: after a 20ms wait the counter
// must read 1 (initial tick) and not 0 (ticker hasn't fired yet).
func TestPoller_Run_WaitsForRegistrationAndAuthenticatedClient(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	var statuses []int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		statuses = append(statuses, func() int {
			if r.Header.Get("Authorization") == "Bearer session-token" {
				return http.StatusOK
			}
			return http.StatusUnauthorized
		}())
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer session-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(10, "2026-08-04T12:00:00Z", 1, []string{"BOOTSTRAP"}))
	}))
	defer srv.Close()

	client := api.NewClient(srv.URL)
	poller := NewProtectedAssetsPoller(client, time.Hour)
	poller.SnapshotMaxAge = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()

	// Neither prerequisite is ready initially: no request is allowed.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if len(statuses) != 0 {
		t.Fatalf("poller issued %d request(s) before registration/token; want 0", len(statuses))
	}
	mu.Unlock()

	client.SetAuthToken("session-token")
	telemetry.MarkRegistered(true)
	deadline := time.Now().Add(time.Second)
	for poller.Snapshot() == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if poller.Snapshot() == nil {
		t.Fatal("poller did not fetch the first snapshot after registration and token readiness")
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(statuses) != 1 || statuses[0] != http.StatusOK {
		t.Fatalf("bootstrap GET statuses=%v, want exactly [200] and no normal 401", statuses)
	}
}

func TestPoller_Run_ReconnectRequiresFreshToken(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	var authorization []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorization = append(authorization, r.Header.Get("Authorization"))
		requestNumber := len(authorization)
		mu.Unlock()
		if (requestNumber == 1 && r.Header.Get("Authorization") == "Bearer old-session-token") ||
			(requestNumber > 1 && r.Header.Get("Authorization") == "Bearer new-session-token") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snapshotJSON(12, "2026-08-04T12:00:00Z", 1, []string{"RECONNECTED"}))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	client := api.NewClient(srv.URL)
	client.SetAuthToken("old-session-token")
	poller := NewProtectedAssetsPoller(client, 500*time.Millisecond)
	poller.SnapshotMaxAge = 0
	telemetry.MarkRegistered(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()
	authSnapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), authorization...)
	}
	deadline := time.Now().Add(time.Second)
	for len(authSnapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	initialAuth := authSnapshot()
	if len(initialAuth) != 1 || initialAuth[0] != "Bearer old-session-token" {
		t.Fatalf("initial authorization=%v, want old token", initialAuth)
	}

	telemetry.MarkRegistered(false)
	client.ClearAuthToken()
	time.Sleep(120 * time.Millisecond)
	beforeReconnect := len(authSnapshot())
	if beforeReconnect != 1 {
		t.Fatalf("requests during disconnected session=%d, want 1 total", beforeReconnect)
	}

	client.SetAuthToken("new-session-token")
	telemetry.MarkRegistered(true)
	deadline = time.Now().Add(time.Second)
	for len(authSnapshot()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	finalAuth := authSnapshot()
	if len(finalAuth) != 2 || finalAuth[0] != "Bearer old-session-token" || finalAuth[1] != "Bearer new-session-token" {
		t.Fatalf("authorization sequence=%v, want old then new with no stale request", finalAuth)
	}
}

func TestPoller_Run_RetriesInitialPollUntil200(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(11, "2026-08-04T12:00:00Z", 1, []string{"RETRIED"}))
	}))
	defer srv.Close()

	client := api.NewClient(srv.URL)
	client.SetAuthToken("session-token")
	poller := NewProtectedAssetsPoller(client, time.Hour)
	poller.SnapshotMaxAge = 0
	telemetry.MarkRegistered(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for poller.Snapshot() == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if poller.Snapshot() == nil {
		t.Fatal("poller did not recover the initial protected-assets GET")
	}
	cancel()
	<-done
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("initial poll calls=%d, want 2 (one transient failure then 200)", got)
	}
}

func TestPoller_Run_FiresInitialTickOnEntry(t *testing.T) {
	telemetry.ResetForTest()
	telemetry.MarkRegistered(true)
	t.Cleanup(telemetry.ResetForTest)
	srv, count := newCountingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(snapshotJSON(1, "2026-07-27T12:00:00Z", 1, []string{"A"}))
	})
	t.Cleanup(srv.Close)

	// Use a 1h Interval. Only the initial tick should fire
	// before ctx cancellation.
	p := newPollerWithClient(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	// Wait long enough for the initial tick to complete but
	// shorter than the Interval. The counter must read >= 1.
	time.Sleep(50 * time.Millisecond)

	got := atomic.LoadInt32(count)
	if got < 1 {
		t.Errorf("server hit count=%d before ticker fired; want >= 1 (initial tick)", got)
	}

	// Snapshot must be valid at this point.
	if s := p.Snapshot(); s == nil {
		t.Error("Snapshot() nil shortly after Run enters; want populated from initial tick")
	}

	cancel()
	<-done
}

// TestPoller_Run_PeriodicTickerFires: confirm the poll loop
// actually re-fires on the ticker (not just the initial tick).
// Drives a 100ms interval in the test.
func TestPoller_Run_PeriodicTickerFires(t *testing.T) {
	telemetry.ResetForTest()
	telemetry.MarkRegistered(true)
	t.Cleanup(telemetry.ResetForTest)
	var mu sync.Mutex
	var counter int

	srv, _ := newCountingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		counter++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write(snapshotJSON(uint64(counter), "2026-07-27T12:00:00Z", 1, []string{"A"}))
	})
	t.Cleanup(srv.Close)

	c := api.NewClient(srv.URL)
	c.SetAuthToken("periodic-session-token")
	p := NewProtectedAssetsPoller(c, 50*time.Millisecond)
	p.SnapshotMaxAge = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.Run(ctx)

	// Wait long enough for at least initial tick + 2-3 ticks
	// on the 50ms ticker.
	time.Sleep(250 * time.Millisecond)
	cancel()

	mu.Lock()
	got := counter
	mu.Unlock()

	if got < 3 {
		t.Errorf("counter=%d after 250ms with 50ms ticker; want >= 3 (initial + ~4 ticks)", got)
	}
}
