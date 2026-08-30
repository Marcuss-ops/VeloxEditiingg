package store

import (
	"context"
	"encoding/json"
	"math"
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
	seedSession := func(sessionID, workerID string) {
		t.Helper()
		if _, err := s.DB().Exec(`INSERT INTO workers(worker_id, worker_name, node_role, raw_json, migrated_at)
			VALUES (?, ?, 'worker', '{}', ?)`, workerID, workerID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			// The worker may already exist when this helper is used to model a
			// reconnect to a new session.
			var count int
			if scanErr := s.DB().QueryRow(`SELECT COUNT(*) FROM workers WHERE worker_id=?`, workerID).Scan(&count); scanErr != nil || count != 1 {
				t.Fatalf("seed worker %s: insert=%v query=%v count=%d", workerID, err, scanErr, count)
			}
		}
		if err := s.InsertSession(&PersistedSession{
			SessionID: sessionID,
			WorkerID:  workerID,
			TokenHash: workerID + "-token",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}); err != nil {
			t.Fatalf("seed session %s/%s: %v", sessionID, workerID, err)
		}
	}
	seedSession("session-a", "worker-resource-a")
	seedSession("session-a-worker-b", "worker-resource-b")

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
	} {
		if err := s.PersistWorkerHeartbeat(context.Background(), heartbeat(input.workerID, input.session), input.session); err != nil {
			t.Fatal(err)
		}
	}
	// A new session replaces the prior active session for the same worker;
	// the other worker keeps its independent session-a active.
	seedSession("session-b", "worker-resource-a")
	for _, input := range []struct {
		workerID string
		session  string
	}{
		{"worker-resource-a", "session-b"},
		{"worker-resource-b", "session-a-worker-b"},
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

// TestWorkerResourceSamples_CapacityColumnsRoundTrip verifies that the
// capacity columns added in migration 165 survive the full
// insert→list round-trip. The heartbeat payload includes all new fields
// and the ListWorkerResourceSamples query returns them in the struct.
func TestWorkerResourceSamples_CapacityColumnsRoundTrip(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-capacity-roundtrip.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetResourceRetention(0, 0)

	// Seed a worker + session.
	if _, err := s.DB().Exec(`INSERT INTO workers(worker_id, worker_name, node_role, raw_json, migrated_at)
		VALUES (?, ?, 'worker', '{}', ?)`, "cap-worker-1", "cap-worker-1", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertSession(&PersistedSession{
		SessionID: "cap-session-1",
		WorkerID:  "cap-worker-1",
		TokenHash: "cap-token",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	sampledAt := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(map[string]any{
		"worker_id": "cap-worker-1",
		"status":    "busy",
		"resources": map[string]any{
			"sampled_at":                   sampledAt.Format(time.RFC3339Nano),
			"cpu_utilization_ratio":        0.65,
			"cpu_iowait_ratio":             0.08,
			"cpu_steal_ratio":              0.02,
			"load1":                        4.2,
			"run_queue":                    6,
			"process_rss_bytes":            5000000,
			"memory_used_bytes":            8000000,
			"major_page_faults_total":      12,
			"disk_read_bytes_total":        1000000,
			"disk_write_bytes_total":       2000000,
			"disk_free_bytes":              50000000000,
			"network_receive_bytes_total":  30000000,
			"network_transmit_bytes_total": 15000000,
			"active_tasks":                 4,
			"ffmpeg_processes":             8,
			// Capacity columns from migration 165.
			"effective_cpu_cores":       8,
			"process_rss_peak_bytes":    6000000,
			"memory_available_bytes":    4200000000,
			"swap_used_bytes":           100000000,
			"page_cache_bytes":          500000000,
			"temp_bytes_written":        30000000,
			"temp_files_open":           3,
			"scratch_current_bytes":     800000000,
			"scratch_peak_bytes":        1500000000,
			"disk_read_mbps":            120.5,
			"disk_write_mbps":           85.3,
			"disk_io_wait_ms":           450,
			"network_retransmits_total": 7,
			"download_mbps":             310.2,
			"upload_mbps":               420.8,
			"task_slots":                8,
			"render_jobs_active":        4,
			"prefetch_jobs_active":      2,
			"publisher_jobs_active":     1,
			"open_file_descriptors":     143,
			"max_file_descriptors":      65535,
			"fd_utilization_ratio":      0.0022,
			"resource_sample_present":   true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Persist the heartbeat.
	if err := s.PersistWorkerHeartbeat(context.Background(), payload, "cap-session-1"); err != nil {
		t.Fatal(err)
	}

	// List and verify all capacity columns round-tripped.
	rows, err := s.ListWorkerResourceSamples(context.Background(), "cap-worker-1", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("sample count=%d, want 1", len(rows))
	}
	row := rows[0]

	// Capacity columns assertions.
	assertInt64(t, "effective_cpu_cores", row.EffectiveCPUCores, 8)
	assertInt64(t, "process_rss_peak_bytes", row.ProcessRSSPeakBytes, 6000000)
	assertInt64(t, "memory_available_bytes", row.MemoryAvailableBytes, 4200000000)
	assertInt64(t, "swap_used_bytes", row.SwapUsedBytes, 100000000)
	assertInt64(t, "page_cache_bytes", row.PageCacheBytes, 500000000)
	assertInt64(t, "temp_bytes_written", row.TempBytesWritten, 30000000)
	assertInt64(t, "temp_files_open", row.TempFilesOpen, 3)
	assertInt64(t, "scratch_current_bytes", row.ScratchCurrentBytes, 800000000)
	assertInt64(t, "scratch_peak_bytes", row.ScratchPeakBytes, 1500000000)
	assertFloat64(t, "disk_read_mbps", row.DiskReadMbps, 120.5)
	assertFloat64(t, "disk_write_mbps", row.DiskWriteMbps, 85.3)
	assertInt64(t, "disk_io_wait_ms", row.DiskIOWaitMs, 450)
	assertInt64(t, "network_retransmits", row.NetworkRetransmits, 7)
	assertFloat64(t, "download_mbps", row.DownloadMbps, 310.2)
	assertFloat64(t, "upload_mbps", row.UploadMbps, 420.8)
	assertInt64(t, "task_slots", row.TaskSlots, 8)
	assertInt64(t, "render_jobs_active", row.RenderJobsActive, 4)
	assertInt64(t, "prefetch_jobs_active", row.PrefetchJobsActive, 2)
	assertInt64(t, "publisher_jobs_active", row.PublisherJobsActive, 1)
	assertInt64(t, "open_file_descriptors", row.OpenFileDescriptors, 143)
	assertInt64(t, "max_file_descriptors", row.MaxFileDescriptors, 65535)
	assertFloat64(t, "fd_utilization_ratio", row.FDUtilizationRatio, 0.0022)
}

func assertInt64(t *testing.T, field string, got, want int64) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d; want %d", field, got, want)
	}
}

func assertFloat64(t *testing.T, field string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("%s = %f; want %f", field, got, want)
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
	if err := s.PersistWorkerHeartbeat(context.Background(), payload, ""); err != nil {
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
	if err := s.PersistWorkerHeartbeat(context.Background(), payload, ""); err != nil {
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
	if err := s.PersistWorkerHeartbeat(context.Background(), currentPayload, ""); err != nil {
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
