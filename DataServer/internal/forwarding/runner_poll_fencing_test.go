package forwarding

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

func TestProcessLease_StalePollStopsWithoutRetryOrTransition(t *testing.T) {
	db := setupRunnerTestDB(t)
	insertTestForwardingRecord(t, db, "cf-runner-stale", "openai", "remote-stale", "scene.composite.v1", "PENDING")

	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-releaseResponse:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job": map[string]interface{}{
				"id":     "remote-stale",
				"status": "completed",
				"result": map[string]interface{}{"video": "done"},
			},
		})
	}))
	defer server.Close()

	oldLease, err := db.ClaimCreatorForwardings(context.Background(), "runner-old", "cf", time.Minute, 1)
	if err != nil || len(oldLease) != 1 {
		t.Fatalf("old claim: err=%v len=%d", err, len(oldLease))
	}
	// Force the first lease into an expired state so takeover is
	// deterministic and does not depend on SQLite timestamp precision.
	const sentinelNextPoll = "2030-01-01T00:00:00Z"
	if _, err := db.DB().ExecContext(context.Background(),
		`UPDATE creator_forwardings SET lease_expires_at = ?, next_poll_at = ? WHERE forwarding_id = ?`,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339), sentinelNextPoll, oldLease[0].ForwardingID); err != nil {
		t.Fatalf("expire old lease: %v", err)
	}

	cfg := DefaultRunnerConfig()
	cfg.LeaseDuration = 0 // renewal loop uses its 30s fallback; test controls expiry
	cfg.BackoffSchedule = []time.Duration{0}
	client := remoteengine.NewClient(remoteengine.Config{URL: server.URL, Retries: 0})
	r := NewCreatorForwardingRunner(cfg, db.Forwarding(), client, nil, "runner-old")

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- r.processLease(context.Background(), oldLease[0])
	}()

	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote poll did not start")
	}

	time.Sleep(1200 * time.Millisecond)
	newLease, err := db.ClaimCreatorForwardings(context.Background(), "runner-new", "cf", 5*time.Minute, 1)
	if err != nil || len(newLease) != 1 {
		t.Fatalf("takeover claim: err=%v len=%d", err, len(newLease))
	}
	close(releaseResponse)

	select {
	case err := <-resultCh:
		if !errors.Is(err, supervisor.ErrLeaseLost) {
			t.Fatalf("stale processLease error = %v, want supervisor.ErrLeaseLost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stale processLease did not stop")
	}

	if got := r.metrics.Retried.Load(); got != 0 {
		t.Fatalf("stale runner Retried metric = %d, want 0", got)
	}
	row, err := db.GetCreatorForwarding(context.Background(), oldLease[0].ForwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if row.Status != "POLLING" || row.LockedBy != newLease[0].RunnerID || row.LeaseID != newLease[0].LeaseID || row.PollAttempts != 0 || row.NextPollAt != sentinelNextPoll || row.LastRemoteStatus != "" {
		t.Fatalf("stale runner mutated takeover row: status=%q runner=%q lease=%q polls=%d next=%q remote=%q", row.Status, row.LockedBy, row.LeaseID, row.PollAttempts, row.NextPollAt, row.LastRemoteStatus)
	}
}

// Keep the store import tied to the forwarding lease contract in this test
// file; it also documents that the test exercises the typed claim result.
var _ store.CreatorForwardingLease
