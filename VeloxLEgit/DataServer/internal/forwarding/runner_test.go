package forwarding

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

func setupRunnerTestDB(t *testing.T) *store.SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cf_runner_test.sqlite")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { dbStore.Close() })
	return dbStore
}

func insertTestForwardingRecord(t *testing.T, db *store.SQLiteStore, forwardingID, provider, sourceJobID, executorID, status string) {
	t.Helper()
	cf := &store.CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   provider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: executorID,
		Status:           status,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.InsertCreatorForwarding(context.Background(), cf); err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
}

func TestNewCreatorForwardingRunner_Defaults(t *testing.T) {
	r := NewCreatorForwardingRunner(nil, nil, nil, nil, "")
	if r == nil {
		t.Fatal("NewCreatorForwardingRunner returned nil")
	}
	if r.cfg.PollInterval != 5*time.Second {
		t.Errorf("PollInterval = %v, want 5s", r.cfg.PollInterval)
	}
	if r.cfg.Concurrency != 4 {
		t.Errorf("Concurrency = %d, want 4", r.cfg.Concurrency)
	}
	if r.metrics == nil {
		t.Error("metrics should not be nil")
	}
}

func TestRunner_Stop(t *testing.T) {
	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), nil, nil, nil, "test-runner")
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx)
	}()

	// Give it a moment to start.
	time.Sleep(50 * time.Millisecond)
	r.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	cancel()
}

func TestBackoffForAttempt(t *testing.T) {
	cfg := DefaultRunnerConfig()
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 30 * time.Second},
		{2, 2 * time.Minute},
		{3, 10 * time.Minute},
		{4, 30 * time.Minute},
		{5, 30 * time.Minute}, // last entry reused
		{10, 30 * time.Minute},
		{0, 30 * time.Second}, // clamped to first
	}
	for _, tt := range tests {
		got := cfg.backoffForAttempt(tt.attempt)
		if got != tt.want {
			t.Errorf("backoffForAttempt(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffForAttempt_Empty(t *testing.T) {
	cfg := &RunnerConfig{}
	got := cfg.backoffForAttempt(1)
	if got != 30*time.Second {
		t.Errorf("empty schedule should default to 30s, got %v", got)
	}
}

func TestIsTerminalSuccess(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"completed", true},
		{"succeeded", true},
		{"done", true},
		{"failed", false},
		{"error", false},
		{"running", false},
		{"queued", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isTerminalSuccess(tt.status); got != tt.want {
			t.Errorf("isTerminalSuccess(%q) = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestIsTerminalFailure(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"failed", true},
		{"error", true},
		{"completed", false},
		{"succeeded", false},
		{"running", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isTerminalFailure(tt.status); got != tt.want {
			t.Errorf("isTerminalFailure(%q) = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestMarshalPayload(t *testing.T) {
	// Non-nil result.
	result := map[string]interface{}{"video": "test.mp4", "count": 42}
	jsonStr, sha := marshalPayload(result)
	if jsonStr == "" || jsonStr == "{}" {
		t.Errorf("expected non-empty payload, got %q", jsonStr)
	}
	if sha == "" {
		t.Error("sha256 should not be empty")
	}

	// Nil result.
	jsonStr, sha = marshalPayload(nil)
	if jsonStr != "{}" {
		t.Errorf("expected empty object for nil result, got %q", jsonStr)
	}
	if sha == "" {
		t.Error("sha256 should not be empty even for nil result")
	}
}

func TestRunner_Tick_NoClient(t *testing.T) {
	db := setupRunnerTestDB(t)
	insertTestForwardingRecord(t, db, "cf-no-client", "openai", "src-1", "scene.composite.v1", "PENDING")

	// Runner with nil client should not claim anything.
	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, nil, nil, "test")

	err := r.tick(context.Background())
	if err != nil {
		t.Errorf("tick with nil client should return nil, got %v", err)
	}

	// The record should still be PENDING (not claimed).
	cf, err := db.GetCreatorForwarding(context.Background(), "cf-no-client")
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if cf.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING (should not have been claimed without client)", cf.Status)
	}
}

func TestRunner_Tick_UnconfiguredClient(t *testing.T) {
	db := setupRunnerTestDB(t)
	insertTestForwardingRecord(t, db, "cf-unconfigured", "openai", "src-2", "scene.composite.v1", "PENDING")

	// Client that is not configured (no URL).
	client := remoteengine.NewClient(remoteengine.Config{})
	r := NewCreatorForwardingRunner(DefaultRunnerConfig(), db, client, nil, "test")

	err := r.tick(context.Background())
	if err != nil {
		t.Errorf("tick with unconfigured client: %v", err)
	}

	cf, err := db.GetCreatorForwarding(context.Background(), "cf-unconfigured")
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if cf.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING", cf.Status)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := &RunnerMetrics{}
	m.Claimed.Store(10)
	m.Forwarded.Store(8)
	m.Failed.Store(1)
	m.Retried.Store(3)
	m.QueueDepth.Store(5)
	m.OldestPending.Store(120)

	snap := m.Snapshot()
	if snap["forwarding_claimed"] != 10 {
		t.Errorf("claimed = %d, want 10", snap["forwarding_claimed"])
	}
	if snap["forwarding_queue_depth"] != 5 {
		t.Errorf("queue_depth = %d, want 5", snap["forwarding_queue_depth"])
	}
	if snap["forwarding_oldest_pending"] != 120 {
		t.Errorf("oldest_pending = %d, want 120", snap["forwarding_oldest_pending"])
	}
}
