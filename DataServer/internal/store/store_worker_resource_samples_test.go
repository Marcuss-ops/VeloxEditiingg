package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestWorkerResourceSamples_SessionTimestampAndWorkerIsolation(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-resource-samples.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetResourceRetention(0, 0)

	sampledAt := time.Date(2026, 7, 31, 10, 11, 12, 123456000, time.UTC)
	heartbeat := func(workerID, sessionID string) []byte {
		raw, err := json.Marshal(map[string]any{
			"worker_id": workerID,
			"status":    "busy",
			"resources": map[string]any{
				"sampled_at":                   sampledAt.Format(time.RFC3339Nano),
				"cpu_utilization_ratio":        0.25,
				"cpu_iowait_ratio":             0.05,
				"cpu_steal_ratio":              0.01,
				"load1":                        1.5,
				"run_queue":                    2,
				"process_rss_bytes":            1000,
				"memory_used_bytes":            2000,
				"major_page_faults_total":      3,
				"disk_read_bytes_total":        4000,
				"disk_write_bytes_total":       5000,
				"disk_free_bytes":              6000,
				"network_receive_bytes_total":  7000,
				"network_transmit_bytes_total": 8000,
				"active_tasks":                 1,
				"ffmpeg_processes":             5,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	// The same observation replayed on the same authenticated session is
	// idempotent; a new session or worker remains a separate history.
	for _, input := range []struct {
		workerID string
		session  string
	}{
		{"worker-resource-a", "session-a"},
		{"worker-resource-a", "session-a"},
		{"worker-resource-a", "session-b"},
		{"worker-resource-b", "session-a"},
	} {
		if err := s.PersistWorkerHeartbeat(context.Background(), heartbeat(input.workerID, input.session), input.session); err != nil {
			t.Fatal(err)
		}
	}

	var total int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_resource_samples`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("sample count=%d, want 3 after same-session replay", total)
	}

	rows, err := s.ListWorkerResourceSamples(context.Background(), "worker-resource-a", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("worker A sample count=%d, want 2", len(rows))
	}
	if rows[0].WorkerID != "worker-resource-a" || rows[1].WorkerID != "worker-resource-a" {
		t.Fatalf("worker A query returned another worker: %+v", rows)
	}
	if rows[0].SampledAt.IsZero() || !rows[0].SampledAt.Equal(sampledAt) {
		t.Fatalf("sampled_at=%v, want %v", rows[0].SampledAt, sampledAt)
	}
	if rows[0].IngestedAt.IsZero() {
		t.Fatal("master ingested_at must be populated")
	}
	if rows[0].FFmpegProcesses != 5 {
		t.Fatalf("ffmpeg_processes=%d, want 5", rows[0].FFmpegProcesses)
	}

	sessionRows, err := s.ListWorkerResourceSamples(context.Background(), "worker-resource-a", "session-b", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessionRows) != 1 || sessionRows[0].SessionID != "session-b" {
		t.Fatalf("session isolation failed: %+v", sessionRows)
	}

	otherWorkerRows, err := s.ListWorkerResourceSamples(context.Background(), "worker-resource-b", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(otherWorkerRows) != 1 || otherWorkerRows[0].WorkerID != "worker-resource-b" {
		t.Fatalf("worker isolation failed: %+v", otherWorkerRows)
	}
}

func TestWorkerResourceSamples_HeartbeatWithoutResourcesDoesNotInsert(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-resource-empty.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetResourceRetention(0, 0)

	payload, err := json.Marshal(map[string]any{
		"worker_id": "worker-resource-empty",
		"status":    "idle",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PersistWorkerHeartbeat(context.Background(), payload, "empty-session"); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_resource_samples`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("resource sample count=%d, want 0 without resources", count)
	}
}

func TestWorkerResourceSamples_RollupAndRetention(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-resource-retention.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetResourceRetention(90, 365)

	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(-2, 0, 0)
	payload, err := json.Marshal(map[string]any{
		"worker_id": "worker-resource-retention",
		"status":    "idle",
		"resources": map[string]any{
			"sampled_at":            old.Format(time.RFC3339Nano),
			"cpu_utilization_ratio": 0.5,
			"memory_used_bytes":     100,
			"active_tasks":          0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PersistWorkerHeartbeat(context.Background(), payload, "retention-session"); err != nil {
		t.Fatal(err)
	}
	// Retention is intentionally based on the trusted master-side clock,
	// not the worker-observed sampled_at (which may be skewed).
	if _, err := s.DB().Exec(`UPDATE worker_resource_samples SET ingested_at=?`, old.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	if err := s.MaintainWorkerResourceSamples(context.Background(), now); err != nil {
		t.Fatal(err)
	}

	var rawCount, rollupCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_resource_samples`).Scan(&rawCount); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_resource_rollups`).Scan(&rollupCount); err != nil {
		t.Fatal(err)
	}
	if rawCount != 0 {
		t.Fatalf("raw retention count=%d, want 0", rawCount)
	}
	if rollupCount != 0 {
		t.Fatalf("rollup retention count=%d, want 0 for a two-year-old rollup", rollupCount)
	}

	// A current sample is retained and produces an idempotent hourly rollup.
	currentPayload, err := json.Marshal(map[string]any{
		"worker_id": "worker-resource-retention",
		"status":    "idle",
		"resources": map[string]any{
			"sampled_at":            now.Add(-10 * time.Minute).Format(time.RFC3339Nano),
			"cpu_utilization_ratio": 0.25,
			"memory_used_bytes":     200,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PersistWorkerHeartbeat(context.Background(), currentPayload, "retention-session"); err != nil {
		t.Fatal(err)
	}
	if err := s.MaintainWorkerResourceSamples(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := s.MaintainWorkerResourceSamples(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_resource_rollups`).Scan(&rollupCount); err != nil {
		t.Fatal(err)
	}
	if rollupCount != 1 {
		t.Fatalf("idempotent hourly rollup count=%d, want 1", rollupCount)
	}
}
