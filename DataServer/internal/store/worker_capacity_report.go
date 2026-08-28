// worker_capacity_report.go — canonical capacity read model.
//
// GetWorkerCapacityReport joins three SQL read models into a single
// capacity report for one worker:
//
//   - worker_resource_samples  → host resource peaks/floors over a configurable window
//   - task_attempt_metrics     → per-job capacity facts (scratch, publish, page faults)
//   - capacity_benchmark_runs  → latest benchmark sweet spot + validation
//
// The report answers "does this worker handle 3, 5, or 8 jobs?" with
// historical evidence from the database, not live Prometheus queries.

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// WorkerCapacityReport is the single canonical capacity document for one worker.
type WorkerCapacityReport struct {
	WorkerID string `json:"worker_id"`

	// Host resource peaks/floors from worker_resource_samples over the
	// configurable lookback window (default: 24h).
	CPUPeakRatio           float64 `json:"cpu_peak_ratio"`
	CPUIOWaitPeakRatio     float64 `json:"cpu_iowait_peak_ratio"`
	MemoryPeakRatio        float64 `json:"memory_peak_ratio"`
	MemoryAvailableMinMB   float64 `json:"memory_available_min_mb"`
	DiskFreeMinBytes       int64   `json:"disk_free_min_bytes"`
	ScratchPeakBytes       int64   `json:"scratch_peak_bytes"`
	FDPeak                 int64   `json:"fd_peak"`
	FDLimit                int64   `json:"fd_limit"`
	FDUtillizationPeak     float64 `json:"fd_utilization_peak"`
	NetworkRxMBPS          float64 `json:"network_rx_mbps"`
	NetworkTxMBPS          float64 `json:"network_tx_mbps"`
	Load1Peak              float64 `json:"load_1_peak"`
	RunQueuePeak           int64   `json:"run_queue_peak"`
	DiskIOWaitMSMax        int64   `json:"disk_io_wait_ms_max"`
	NetworkRetransmitsMax  int64   `json:"network_retransmits_max"`
	RenderJobsActiveAvg    float64 `json:"render_jobs_active_avg"`
	PrefetchJobsActiveAvg  float64 `json:"prefetch_jobs_active_avg"`
	PublisherJobsActiveAvg float64 `json:"publisher_jobs_active_avg"`
	TaskSlotsAvg           float64 `json:"task_slots_avg"`
	SampleCount            int     `json:"sample_count"`
	WindowStart            string  `json:"window_start"`
	WindowEnd              string  `json:"window_end"`

	// Per-job capacity facts from task_attempt_metrics (most recent N succeeded attempts).
	AvgJobScratchPeakBytes int64 `json:"avg_job_scratch_peak_bytes"`
	MaxJobScratchPeakBytes int64 `json:"max_job_scratch_peak_bytes"`
	AvgJobPublishBytes     int64 `json:"avg_job_publish_bytes"`
	AvgJobPageFaults       int64 `json:"avg_job_page_faults"`
	AvgJobPeakRSSDelta     int64 `json:"avg_job_peak_rss_delta_bytes"`
	AttemptCount           int   `json:"attempt_count"`

	// Latest benchmark result from capacity_benchmark_runs.
	BenchmarkRunID string  `json:"benchmark_run_id,omitempty"`
	SweetSpot      int     `json:"sweet_spot,omitempty"`
	LimitingFactor string  `json:"limiting_factor,omitempty"`
	PredictedSlots *int    `json:"predicted_render_slots,omitempty"`
	PredictionAcc  *string `json:"prediction_accuracy,omitempty"`
	BenchmarkRunAt string  `json:"benchmark_run_at,omitempty"`

	// Derived recommendation.
	MaxRecommendedJobs int    `json:"max_recommended_jobs"`
	DataSource         string `json:"data_source"`
}

// GetWorkerCapacityReport builds the canonical capacity report for one worker
// by joining the three SQL read models.
func (s *SQLiteStore) GetWorkerCapacityReport(ctx context.Context, workerID string) (*WorkerCapacityReport, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("capacity report: store not initialized")
	}
	if workerID == "" {
		return nil, fmt.Errorf("capacity report: worker_id is required")
	}

	report := &WorkerCapacityReport{
		WorkerID:   workerID,
		DataSource: "master_sql",
	}

	// 1. Host resource peaks/floors from worker_resource_samples (last 24h).
	if err := s.queryResourceSamples(ctx, workerID, report); err != nil {
		return nil, fmt.Errorf("capacity report resource samples: %w", err)
	}

	// 2. Per-job capacity facts from task_attempt_metrics (last 100 succeeded attempts).
	if err := s.queryAttemptMetrics(ctx, workerID, report); err != nil {
		return nil, fmt.Errorf("capacity report attempt metrics: %w", err)
	}

	// 3. Latest benchmark result from capacity_benchmark_runs.
	if err := s.queryBenchmarkResult(ctx, workerID, report); err != nil {
		return nil, fmt.Errorf("capacity report benchmark: %w", err)
	}

	// 4. Derive max recommended jobs.
	report.MaxRecommendedJobs = deriveMaxRecommendedJobs(report)

	return report, nil
}

// queryResourceSamples computes peaks/floors over the last 24h window.
func (s *SQLiteStore) queryResourceSamples(ctx context.Context, workerID string, report *WorkerCapacityReport) error {
	windowHours := 24
	windowStart := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour).Format(time.RFC3339Nano)
	windowEnd := time.Now().UTC().Format(time.RFC3339Nano)

	row := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(MAX(cpu_percent), 0),
			COALESCE(MAX(cpu_iowait_percent), 0),
			COALESCE(MAX(rss_bytes * 100.0 / NULLIF(memory_used_bytes + rss_bytes, 0)), 0),
			COALESCE(MIN(memory_available_bytes), 0),
			COALESCE(MIN(disk_free_bytes), 0),
			COALESCE(MAX(scratch_peak_bytes), 0),
			COALESCE(MAX(open_file_descriptors), 0),
			COALESCE(MAX(max_file_descriptors), 0),
			COALESCE(MAX(fd_utilization_ratio), 0),
			COALESCE(AVG(download_mbps), 0),
			COALESCE(AVG(upload_mbps), 0),
			COALESCE(MAX(load1), 0),
			COALESCE(MAX(run_queue), 0),
			COALESCE(MAX(disk_io_wait_ms), 0),
			COALESCE(MAX(network_retransmits), 0),
			COALESCE(AVG(render_jobs_active), 0),
			COALESCE(AVG(prefetch_jobs_active), 0),
			COALESCE(AVG(publisher_jobs_active), 0),
			COALESCE(AVG(task_slots), 0),
			COUNT(*)
		FROM worker_resource_samples
		WHERE worker_id = ? AND ingested_at >= ? AND ingested_at <= ?
	`, workerID, windowStart, windowEnd)

	err := row.Scan(
		&report.CPUPeakRatio,
		&report.CPUIOWaitPeakRatio,
		&report.MemoryPeakRatio,
		&report.MemoryAvailableMinMB,
		&report.DiskFreeMinBytes,
		&report.ScratchPeakBytes,
		&report.FDPeak,
		&report.FDLimit,
		&report.FDUtillizationPeak,
		&report.NetworkRxMBPS,
		&report.NetworkTxMBPS,
		&report.Load1Peak,
		&report.RunQueuePeak,
		&report.DiskIOWaitMSMax,
		&report.NetworkRetransmitsMax,
		&report.RenderJobsActiveAvg,
		&report.PrefetchJobsActiveAvg,
		&report.PublisherJobsActiveAvg,
		&report.TaskSlotsAvg,
		&report.SampleCount,
	)
	if err == sql.ErrNoRows {
		// No samples yet — not an error, just empty.
		report.WindowStart = windowStart
		report.WindowEnd = windowEnd
		return nil
	}
	if err != nil {
		return err
	}
	report.WindowStart = windowStart
	report.WindowEnd = windowEnd

	// Convert memory_available_min from bytes to MB for readability.
	report.MemoryAvailableMinMB = float64(report.MemoryAvailableMinMB) / (1024 * 1024)

	// CPU percent is stored as 0-100; normalize to 0-1 ratio for the report.
	report.CPUPeakRatio = report.CPUPeakRatio / 100.0
	report.CPUIOWaitPeakRatio = report.CPUIOWaitPeakRatio / 100.0

	return nil
}

// queryAttemptMetrics computes per-job capacity facts from recent succeeded attempts.
func (s *SQLiteStore) queryAttemptMetrics(ctx context.Context, workerID string, report *WorkerCapacityReport) error {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			ROUND(COALESCE(AVG(m.job_scratch_peak_bytes), 0)),
			COALESCE(MAX(m.job_scratch_peak_bytes), 0),
			ROUND(COALESCE(AVG(m.job_publish_bytes), 0)),
			ROUND(COALESCE(AVG(m.job_page_faults), 0)),
			ROUND(COALESCE(AVG(m.job_peak_rss_delta_bytes), 0)),
			COUNT(*)
		FROM task_attempt_metrics m
		JOIN task_attempts a ON a.id = m.attempt_id
		WHERE a.worker_id = ? AND a.status = 'SUCCEEDED'
		ORDER BY a.updated_at DESC
		LIMIT 100
	`, workerID)

	// SQLite AVG()/ROUND() return float64 to the Go driver even after
	// CAST(... AS INTEGER). Scan into float64 intermediates and convert
	// in Go to avoid the "converting driver.Value type float64 to int64"
	// scan error that appears under aggregate load.
	var avgScratch, maxScratch, avgPublish, avgPageFaults, avgPeakRSS float64
	if err := row.Scan(
		&avgScratch,
		&maxScratch,
		&avgPublish,
		&avgPageFaults,
		&avgPeakRSS,
		&report.AttemptCount,
	); err != nil {
		return fmt.Errorf("capacity report attempt metrics: %w", err)
	}
	report.AvgJobScratchPeakBytes = int64(avgScratch)
	report.MaxJobScratchPeakBytes = int64(maxScratch)
	report.AvgJobPublishBytes = int64(avgPublish)
	report.AvgJobPageFaults = int64(avgPageFaults)
	report.AvgJobPeakRSSDelta = int64(avgPeakRSS)
	return nil
}

// queryBenchmarkResult reads the most recent benchmark result for the worker.
func (s *SQLiteStore) queryBenchmarkResult(ctx context.Context, workerID string, report *WorkerCapacityReport) error {
	var predictedSlots sql.NullInt64
	var predictionAcc sql.NullString

	row := s.db.QueryRowContext(ctx, `
		SELECT
			benchmark_run_id, sweet_spot, limiting_factor,
			predicted_render_slots, prediction_accuracy, completed_at
		FROM capacity_benchmark_runs
		WHERE worker_id = ?
		ORDER BY completed_at DESC
		LIMIT 1
	`, workerID)

	err := row.Scan(
		&report.BenchmarkRunID,
		&report.SweetSpot,
		&report.LimitingFactor,
		&predictedSlots,
		&predictionAcc,
		&report.BenchmarkRunAt,
	)
	if err == sql.ErrNoRows {
		// No benchmark yet — not an error.
		return nil
	}
	if err != nil {
		return err
	}
	if predictedSlots.Valid {
		v := int(predictedSlots.Int64)
		report.PredictedSlots = &v
	}
	if predictionAcc.Valid {
		report.PredictionAcc = &predictionAcc.String
	}
	return nil
}

// deriveMaxRecommendedJobs picks the most conservative limit from the
// available evidence sources. When no data is available, returns 1 (safest).
func deriveMaxRecommendedJobs(r *WorkerCapacityReport) int {
	// Start with a conservative default.
	candidate := 1

	// If we have benchmark data, use the sweet spot as the primary signal.
	if r.SweetSpot > 0 {
		candidate = r.SweetSpot
	}

	// Apply resource-based caps.
	// CPU ceiling: if peak CPU ratio > 0.85, cap at current active + 1.
	if r.CPUPeakRatio > 0.85 && r.SampleCount > 0 {
		cpuCap := int(r.RenderJobsActiveAvg) + 1
		if cpuCap < candidate {
			candidate = cpuCap
		}
	}

	// Memory ceiling: if memory available < 1 GB, don't add jobs.
	if r.MemoryAvailableMinMB > 0 && r.MemoryAvailableMinMB < 1024 {
		memCap := int(r.RenderJobsActiveAvg)
		if memCap < candidate {
			candidate = memCap
		}
	}

	// FD ceiling: if FD utilization > 0.8, don't add jobs.
	if r.FDUtillizationPeak > 0.8 && r.FDLimit > 0 {
		fdCap := int(r.RenderJobsActiveAvg)
		if fdCap < candidate {
			candidate = fdCap
		}
	}

	// Scratch ceiling: if scratch_peak > 2 GB per job, be conservative.
	if r.AvgJobScratchPeakBytes > 2*1024*1024*1024 && r.MemoryAvailableMinMB > 0 {
		scratchCap := int(r.MemoryAvailableMinMB*1024*1024) / int(r.AvgJobScratchPeakBytes)
		if scratchCap > 0 && scratchCap < candidate {
			candidate = scratchCap
		}
	}

	if candidate < 1 {
		candidate = 1
	}
	return candidate
}
