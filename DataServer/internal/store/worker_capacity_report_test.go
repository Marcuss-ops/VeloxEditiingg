// worker_capacity_report_test.go — verifies GetWorkerCapacityReport
// correctly joins worker_resource_samples, task_attempt_metrics, and
// capacity_benchmark_runs into a single canonical capacity report.

package store

import (
	"context"
	"testing"
	"time"
)

func TestGetWorkerCapacityReport_FullAssembly(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	const workerID = "cap-report-worker-01"

	// ── 1. Seed worker_resource_samples (3 samples over the last hour) ───
	for i, minsAgo := range []int{0, 20, 40} {
		sampledAt := now.Add(-time.Duration(minsAgo) * time.Minute).Format(time.RFC3339Nano)
		ingestedAt := now.Add(-time.Duration(minsAgo) * time.Minute).Format(time.RFC3339Nano)
		cpuPct := 40.0 + float64(i)*10 // 40, 50, 60
		diskFree := int64(50000000000 - int64(i)*5000000000)
		scratchPeak := int64(1000000000 + int64(i)*500000000)
		fdPeak := int64(100 + int64(i)*20)
		fdMax := int64(65535)
		fdUtil := float64(fdPeak) / float64(fdMax)
		memAvail := int64(4000000000 - int64(i)*200000000)
		execQuery(t, s, ctx,
			`INSERT INTO worker_resource_samples (
				worker_id, session_id, sampled_at, ingested_at,
				cpu_percent, cpu_iowait_percent, cpu_steal_percent,
				load1, run_queue, rss_bytes, memory_used_bytes, major_page_faults,
				disk_read_bytes, disk_write_bytes, disk_free_bytes,
				network_rx_bytes, network_tx_bytes, active_tasks, ffmpeg_processes,
				effective_cpu_cores, process_rss_peak_bytes, memory_available_bytes,
				swap_used_bytes, page_cache_bytes, temp_bytes_written, temp_files_open,
				scratch_current_bytes, scratch_peak_bytes, disk_read_mbps,
				disk_write_mbps, disk_io_wait_ms, network_retransmits, download_mbps,
				upload_mbps, task_slots, render_jobs_active, prefetch_jobs_active,
				publisher_jobs_active, open_file_descriptors, max_file_descriptors,
				fd_utilization_ratio
			) VALUES (?, ?, ?, ?,
				?, ?, ?,
				?, ?, ?, ?, ?,
				?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?,
				?)`,
			workerID, "session-1", sampledAt, ingestedAt,
			cpuPct, 5.0, 1.0,
			2.0+float64(i), int64(i+1), int64(3000000000+int64(i)*200000000), int64(6000000000), int64(i),
			int64(1000000*i), int64(2000000*i), diskFree,
			int64(7000000+int64(i)*1000000), int64(8000000+int64(i)*500000), int64(i+1), int64(4+i),
			int64(8), int64(4000000000), memAvail,
			int64(100000000), int64(500000000), int64(1000000*i), int64(2+i),
			int64(800000000), scratchPeak, 100.0+float64(i)*20,
			80.0+float64(i)*10, int64(100+i*50), int64(i), 250.0+float64(i)*30,
			350.0+float64(i)*40, int64(8), int64(3+i), int64(2),
			int64(1), fdPeak, fdMax,
			fdUtil,
		)
	}

	// ── 2. Seed task_attempts + task_attempt_metrics (2 succeeded) ───────
	for i, scratchPeak := range []int64{1500000000, 2500000000} {
		attemptID := "cap-report-attempt-00" + string(rune('1'+i))
		jobID := "cap-report-job-00" + string(rune('1'+i))
		taskID := "cap-report-task-00" + string(rune('1'+i))

		// Seed job.
		execQuery(t, s, ctx,
			`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
			 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`, jobID, nowStr, nowStr, nowStr)
		// Seed task.
		execQuery(t, s, ctx,
			`INSERT INTO tasks
			 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
			  status, priority, revision, attempt_count, attempt_number,
			  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
			 VALUES (?, ?, '', '', 'render_batch', 3,
			         'RUNNING', 0, 0, 1, 1,
			         ?, ?, ?, ?, ?, ?)`,
			taskID, jobID, workerID, "lease-"+attemptID, attemptID, nowStr, nowStr, nowStr)
		// Seed attempt (SUCCEEDED).
		execQuery(t, s, ctx,
			`INSERT INTO task_attempts
			 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
			  started_at, completed_at, error_code, error_message, report_version,
			  created_at, updated_at)
			 VALUES (?, ?, ?, 1, ?, ?, 'SUCCEEDED',
			         ?, ?, '', '', 0,
			         ?, ?)`,
			attemptID, taskID, jobID, workerID, "lease-"+attemptID,
			nowStr, nowStr, nowStr, nowStr)
		// Seed metrics with capacity columns.
		execQuery(t, s, ctx,
			`INSERT INTO task_attempt_metrics (
				attempt_id, input_bytes, output_bytes, cpu_time_ms, gpu_time_ms,
				peak_rss_bytes, wall_clock_seconds, output_sha256,
				job_publish_bytes, job_page_faults, job_scratch_peak_bytes,
				job_peak_rss_delta_bytes
			) VALUES (?, 1024, 512, 1000, 500, 200000000, 10.5, 'abc',
			          ?, ?, ?, ?)`,
			attemptID,
			int64(1000000*(i+1)),  // job_publish_bytes: 1000000, 2000000
			int64(20*(i+1)),       // job_page_faults: 20, 40
			scratchPeak,           // job_scratch_peak_bytes: 1500000000, 2500000000
			int64(300000000*(i+1)), // job_peak_rss_delta_bytes
		)
	}

	// ── 3. Seed capacity_benchmark_runs (1 row) ──────────────────────────
	execQuery(t, s, ctx,
		`INSERT INTO capacity_benchmark_runs (
			worker_id, benchmark_run_id, fixture_id, max_concurrency,
			runs_per_level, cache_mode, sweet_spot, limiting_factor, summary,
			snapshot_ram_bytes, snapshot_cpu_cores, snapshot_disk_read_mbps,
			snapshot_upload_mbps, started_at, completed_at
		) VALUES (?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?)`,
		workerID, "bench-run-001", "fixture-canonical", 8,
		3, "cold_cache", 6, "RAM", "Benchmark complete: sweet spot=6",
		int64(32000000000), 8, 250.0,
		400.0, nowStr, nowStr,
	)

	// ── 4. Call GetWorkerCapacityReport ──────────────────────────────────
	report, err := s.GetWorkerCapacityReport(ctx, workerID)
	if err != nil {
		t.Fatalf("GetWorkerCapacityReport: %v", err)
	}
	if report == nil {
		t.Fatal("GetWorkerCapacityReport returned nil")
	}

	// ── 5. Verify worker_resource_samples aggregation ────────────────────
	if report.WorkerID != workerID {
		t.Errorf("WorkerID = %q; want %q", report.WorkerID, workerID)
	}
	if report.DataSource != "master_sql" {
		t.Errorf("DataSource = %q; want master_sql", report.DataSource)
	}
	if report.SampleCount != 3 {
		t.Errorf("SampleCount = %d; want 3", report.SampleCount)
	}
	// CPU peak = MAX(40, 50, 60) = 60 → normalized to 0.60
	if report.CPUPeakRatio < 0.59 || report.CPUPeakRatio > 0.61 {
		t.Errorf("CPUPeakRatio = %f; want ~0.60", report.CPUPeakRatio)
	}
	// Disk free min = MIN(50G, 45G, 40G) = 40G
	if report.DiskFreeMinBytes < 39000000000 || report.DiskFreeMinBytes > 41000000000 {
		t.Errorf("DiskFreeMinBytes = %d; want ~40000000000", report.DiskFreeMinBytes)
	}
	// Scratch peak = MAX(1G, 1.5G, 2G) = 2G
	if report.ScratchPeakBytes < 1900000000 || report.ScratchPeakBytes > 2100000000 {
		t.Errorf("ScratchPeakBytes = %d; want ~2000000000", report.ScratchPeakBytes)
	}
	// FD peak = MAX(100, 120, 140) = 140
	if report.FDPeak != 140 {
		t.Errorf("FDPeak = %d; want 140", report.FDPeak)
	}
	// FD limit = 65535
	if report.FDLimit != 65535 {
		t.Errorf("FDLimit = %d; want 65535", report.FDLimit)
	}
	// Memory available min = MIN(4G, 3.8G, 3.6G) = 3.6G → converted to MB
	if report.MemoryAvailableMinMB < 3400 || report.MemoryAvailableMinMB > 3800 {
		t.Errorf("MemoryAvailableMinMB = %f; want ~3600", report.MemoryAvailableMinMB)
	}
	// Render jobs active avg = AVG(3, 4, 5) = 4
	if report.RenderJobsActiveAvg < 3.9 || report.RenderJobsActiveAvg > 4.1 {
		t.Errorf("RenderJobsActiveAvg = %f; want ~4.0", report.RenderJobsActiveAvg)
	}

	// ── 6. Verify task_attempt_metrics aggregation ────────────────────────
	if report.AttemptCount != 2 {
		t.Errorf("AttemptCount = %d; want 2", report.AttemptCount)
	}
	// Avg job scratch peak = (1500000000 + 2500000000) / 2 = 2000000000
	if report.AvgJobScratchPeakBytes != 2000000000 {
		t.Errorf("AvgJobScratchPeakBytes = %d; want 2000000000", report.AvgJobScratchPeakBytes)
	}
	// Max job scratch peak = 2500000000
	if report.MaxJobScratchPeakBytes != 2500000000 {
		t.Errorf("MaxJobScratchPeakBytes = %d; want 2500000000", report.MaxJobScratchPeakBytes)
	}
	// Avg job publish bytes = (1000000 + 2000000) / 2 = 1500000
	if report.AvgJobPublishBytes != 1500000 {
		t.Errorf("AvgJobPublishBytes = %d; want 1500000", report.AvgJobPublishBytes)
	}
	// Avg job page faults = (20 + 40) / 2 = 30
	if report.AvgJobPageFaults != 30 {
		t.Errorf("AvgJobPageFaults = %d; want 30", report.AvgJobPageFaults)
	}

	// ── 7. Verify capacity_benchmark_runs ────────────────────────────────
	if report.BenchmarkRunID != "bench-run-001" {
		t.Errorf("BenchmarkRunID = %q; want bench-run-001", report.BenchmarkRunID)
	}
	if report.SweetSpot != 6 {
		t.Errorf("SweetSpot = %d; want 6", report.SweetSpot)
	}
	if report.LimitingFactor != "RAM" {
		t.Errorf("LimitingFactor = %q; want RAM", report.LimitingFactor)
	}
	if report.BenchmarkRunAt == "" {
		t.Error("BenchmarkRunAt should not be empty")
	}

	// ── 8. Verify derived recommendation ─────────────────────────────────
	// Sweet spot = 6, no resource pressure → MaxRecommendedJobs = 6
	if report.MaxRecommendedJobs != 6 {
		t.Errorf("MaxRecommendedJobs = %d; want 6 (from benchmark sweet spot)", report.MaxRecommendedJobs)
	}
}

func TestGetWorkerCapacityReport_NoData(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	ctx := context.Background()

	report, err := s.GetWorkerCapacityReport(ctx, "unknown-worker")
	if err != nil {
		t.Fatalf("GetWorkerCapacityReport: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report for unknown worker")
	}
	if report.SampleCount != 0 {
		t.Errorf("SampleCount = %d; want 0", report.SampleCount)
	}
	if report.AttemptCount != 0 {
		t.Errorf("AttemptCount = %d; want 0", report.AttemptCount)
	}
	if report.MaxRecommendedJobs != 1 {
		t.Errorf("MaxRecommendedJobs = %d; want 1 (safest default)", report.MaxRecommendedJobs)
	}
}

func TestGetWorkerCapacityReport_CPUPressureCapsRecommendation(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	const workerID = "cap-pressure-worker-01"

	// Seed resource sample with high CPU (90%) and low memory available.
	sampledAt := now.Format(time.RFC3339Nano)
	execQuery(t, s, ctx,
		`INSERT INTO worker_resource_samples (
			worker_id, session_id, sampled_at, ingested_at,
			cpu_percent, cpu_iowait_percent, cpu_steal_percent,
			load1, run_queue, rss_bytes, memory_used_bytes, major_page_faults,
			disk_read_bytes, disk_write_bytes, disk_free_bytes,
			network_rx_bytes, network_tx_bytes, active_tasks, ffmpeg_processes,
			effective_cpu_cores, process_rss_peak_bytes, memory_available_bytes,
			swap_used_bytes, page_cache_bytes, temp_bytes_written, temp_files_open,
			scratch_current_bytes, scratch_peak_bytes, disk_read_mbps,
			disk_write_mbps, disk_io_wait_ms, network_retransmits, download_mbps,
			upload_mbps, task_slots, render_jobs_active, prefetch_jobs_active,
			publisher_jobs_active, open_file_descriptors, max_file_descriptors,
			fd_utilization_ratio
		) VALUES (?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?,
			?)`,
		workerID, "session-1", sampledAt, sampledAt,
		90.0, 10.0, 2.0,
		8.0, 10, int64(7000000000), int64(8000000000), 5,
		int64(5000000), int64(8000000), int64(30000000000),
		int64(30000000), int64(15000000), int64(6), int64(12),
		int64(8), int64(6000000000), int64(800000000),
		int64(200000000), int64(600000000), int64(5000000), int64(300),
		int64(1500000000), int64(2000000000), 150.0,
		100.0, int64(800), int64(10), 300.0,
		400.0, int64(8), int64(6), int64(2),
		int64(1), int64(500), int64(65535),
		0.008,
	)

	// Seed benchmark with sweet_spot = 6.
	execQuery(t, s, ctx,
		`INSERT INTO capacity_benchmark_runs (
			worker_id, benchmark_run_id, fixture_id, max_concurrency,
			runs_per_level, cache_mode, sweet_spot, limiting_factor, summary,
			snapshot_ram_bytes, snapshot_cpu_cores, snapshot_disk_read_mbps,
			snapshot_upload_mbps, started_at, completed_at
		) VALUES (?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?)`,
		workerID, "bench-pressure-001", "fixture-canonical", 8,
		3, "cold_cache", 6, "CPU", "CPU bound",
		int64(32000000000), 8, 250.0,
		400.0, nowStr, nowStr,
	)

	report, err := s.GetWorkerCapacityReport(ctx, workerID)
	if err != nil {
		t.Fatalf("GetWorkerCapacityReport: %v", err)
	}

	// CPU peak = 90% → 0.90 > 0.85 → CPU cap = int(6)+1 = 7
	// But sweet spot = 6, and 7 > 6, so candidate stays at 6.
	if report.MaxRecommendedJobs != 6 {
		t.Errorf("MaxRecommendedJobs = %d; want 6 (sweet spot wins over CPU cap)", report.MaxRecommendedJobs)
	}

	// Verify the CPU peak was correctly normalized.
	if report.CPUPeakRatio < 0.89 || report.CPUPeakRatio > 0.91 {
		t.Errorf("CPUPeakRatio = %f; want ~0.90", report.CPUPeakRatio)
	}
}

func TestGetWorkerCapacityReport_EmptyWorkerID(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	_, err := s.GetWorkerCapacityReport(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty worker_id")
	}
}

func TestGetWorkerCapacityReport_NilStore(t *testing.T) {
	var s *SQLiteStore
	_, err := s.GetWorkerCapacityReport(context.Background(), "any-worker")
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}
