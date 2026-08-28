package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// WorkerResourceSample is one master-persisted resource observation. SampledAt
// is the worker-observed timestamp; IngestedAt is assigned by the master and
// is the trusted clock used for retention.
type WorkerResourceSample struct {
	ID                   int64
	WorkerID             string
	SessionID            string
	SampledAt            time.Time
	IngestedAt           time.Time
	CPUPercent           float64
	CPUIOWaitPercent     float64
	CPUStealPercent      float64
	Load1                float64
	RunQueue             int64
	RSSBytes             int64
	MemoryUsedBytes      int64
	MajorPageFaults      int64
	DiskReadBytes        int64
	DiskWriteBytes       int64
	DiskFreeBytes        int64
	NetworkRxBytes       int64
	NetworkTxBytes       int64
	ActiveTasks          int64
	FFmpegProcesses      int64
	EffectiveCPUCores    int64
	ProcessRSSPeakBytes  int64
	MemoryAvailableBytes int64
	SwapUsedBytes        int64
	PageCacheBytes       int64
	TempBytesWritten     int64
	TempFilesOpen        int64
	ScratchCurrentBytes  int64
	ScratchPeakBytes     int64
	DiskReadMbps         float64
	DiskWriteMbps        float64
	DiskIOWaitMs         int64
	NetworkRetransmits   int64
	DownloadMbps         float64
	UploadMbps           float64
	TaskSlots            int64
	RenderJobsActive     int64
	PrefetchJobsActive   int64
	PublisherJobsActive  int64
	OpenFileDescriptors  int64
	MaxFileDescriptors   int64
	FDUtilizationRatio   float64
}

// ListWorkerResourceSamples returns samples for one worker, newest observed
// timestamp first. The worker_id predicate is intentionally mandatory so a
// caller cannot accidentally mix resource histories from different workers.
func (s *SQLiteStore) ListWorkerResourceSamples(ctx context.Context, workerID, sessionID string, limit int) ([]WorkerResourceSample, error) {
	if workerID == "" {
		return []WorkerResourceSample{}, nil
	}
	if limit < 1 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	query := `SELECT id, worker_id, session_id, sampled_at, ingested_at,
		cpu_percent, cpu_iowait_percent, cpu_steal_percent, load1, run_queue,
		rss_bytes, memory_used_bytes, major_page_faults,
		disk_read_bytes, disk_write_bytes, disk_free_bytes,
		network_rx_bytes, network_tx_bytes, active_tasks, ffmpeg_processes
		, effective_cpu_cores, process_rss_peak_bytes, memory_available_bytes,
		swap_used_bytes, page_cache_bytes, temp_bytes_written, temp_files_open,
		scratch_current_bytes, scratch_peak_bytes, disk_read_mbps,
		disk_write_mbps, disk_io_wait_ms, network_retransmits, download_mbps,
		upload_mbps, task_slots, render_jobs_active, prefetch_jobs_active,
		publisher_jobs_active, open_file_descriptors, max_file_descriptors,
		fd_utilization_ratio
		FROM worker_resource_samples WHERE worker_id=?`
	args := []any{workerID}
	if sessionID != "" {
		query += ` AND session_id=?`
		args = append(args, sessionID)
	}
	query += ` ORDER BY sampled_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list worker resource samples: %w", err)
	}
	defer rows.Close()
	out := make([]WorkerResourceSample, 0, limit)
	for rows.Next() {
		row, err := scanWorkerResourceSample(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list worker resource samples: rows: %w", err)
	}
	return out, nil
}

// maybeInsertWorkerResourceSample persists the typed heartbeat resource
// projection inside the caller's heartbeat transaction. A heartbeat replay
// with the same worker/session/sample timestamp is deliberately ignored.
func maybeInsertWorkerResourceSample(ctx context.Context, tx *sql.Tx, m map[string]any, workerID, sessionID, ingestedAt string) error {
	if workerID == "" {
		return nil
	}
	sampledAt, ok := resourceSampleTimestamp(m)
	if !ok || !resourceSamplePresent(m) {
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO worker_resource_samples (
		worker_id, session_id, sampled_at, ingested_at,
		cpu_percent, cpu_iowait_percent, cpu_steal_percent, load1, run_queue,
		rss_bytes, memory_used_bytes, major_page_faults,
		disk_read_bytes, disk_write_bytes, disk_free_bytes,
		network_rx_bytes, network_tx_bytes, active_tasks, ffmpeg_processes,
		effective_cpu_cores, process_rss_peak_bytes, memory_available_bytes,
		swap_used_bytes, page_cache_bytes, temp_bytes_written, temp_files_open,
		scratch_current_bytes, scratch_peak_bytes, disk_read_mbps,
		disk_write_mbps, disk_io_wait_ms, network_retransmits, download_mbps,
		upload_mbps, task_slots, render_jobs_active, prefetch_jobs_active,
		publisher_jobs_active, open_file_descriptors, max_file_descriptors,
		fd_utilization_ratio
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?)
	ON CONFLICT(worker_id, session_id, sampled_at) DO NOTHING`,
		workerID, sessionID, sampledAt, ingestedAt,
		floatValue(metricValue(m, "cpu_utilization_ratio"))*100,
		floatValue(metricValue(m, "cpu_iowait_ratio"))*100,
		floatValue(metricValue(m, "cpu_steal_ratio"))*100,
		floatValue(metricValue(m, "load1")),
		int64Value(metricValue(m, "run_queue")),
		int64Value(metricValue(m, "process_rss_bytes")),
		int64Value(metricValue(m, "memory_used_bytes")),
		int64Value(metricValue(m, "major_page_faults_total")),
		int64Value(metricValue(m, "disk_read_bytes_total")),
		int64Value(metricValue(m, "disk_write_bytes_total")),
		int64Value(metricValue(m, "disk_free_bytes")),
		int64Value(metricValue(m, "network_receive_bytes_total", "network_rx_bytes")),
		int64Value(metricValue(m, "network_transmit_bytes_total", "network_tx_bytes")),
		int64Value(metricValue(m, "active_task_count", "active_tasks")),
		int64Value(metricValue(m, "ffmpeg_processes")),
		int64Value(metricValue(m, "effective_cpu_cores")),
		int64Value(metricValue(m, "process_rss_peak_bytes")),
		int64Value(metricValue(m, "memory_available_bytes")),
		int64Value(metricValue(m, "swap_used_bytes")),
		int64Value(metricValue(m, "page_cache_bytes")),
		int64Value(metricValue(m, "temp_bytes_written")),
		int64Value(metricValue(m, "temp_files_open")),
		int64Value(metricValue(m, "scratch_current_bytes")),
		int64Value(metricValue(m, "scratch_peak_bytes")),
		floatValue(metricValue(m, "disk_read_mbps")),
		floatValue(metricValue(m, "disk_write_mbps")),
		int64Value(metricValue(m, "disk_io_wait_ms")),
		int64Value(metricValue(m, "network_retransmits_total")),
		floatValue(metricValue(m, "download_mbps")),
		floatValue(metricValue(m, "upload_mbps")),
		int64Value(metricValue(m, "task_slots")),
		int64Value(metricValue(m, "render_jobs_active")),
		int64Value(metricValue(m, "prefetch_jobs_active")),
		int64Value(metricValue(m, "publisher_jobs_active")),
		int64Value(metricValue(m, "open_file_descriptors")),
		int64Value(metricValue(m, "max_file_descriptors")),
		floatValue(metricValue(m, "fd_utilization_ratio")),
	)
	if err != nil {
		return fmt.Errorf("insert worker resource sample: %w", err)
	}
	return nil
}

func boolValue(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		return err == nil && parsed
	case float64:
		return value != 0
	case int:
		return value != 0
	case int64:
		return value != 0
	default:
		return false
	}
}

func resourceSamplePresent(m map[string]any) bool {
	if value, ok := m["resource_sample_present"]; ok {
		return boolValue(value)
	}
	if value := metricValue(m, "resource_sample_present"); value != nil {
		return boolValue(value)
	}
	if resources, ok := m["resources"].(map[string]any); ok {
		return len(resources) > 0
	}
	return len(nestedMetricMap(m, "metrics")) > 0 && metricValue(m, "sampled_at") != nil
}

func resourceSampleTimestamp(m map[string]any) (string, bool) {
	value := asString(metricValue(m, "sampled_at"))
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() {
		return "", false
	}
	return parsed.UTC().Format(time.RFC3339Nano), true
}

func scanWorkerResourceSample(scanner interface{ Scan(...any) error }) (WorkerResourceSample, error) {
	var row WorkerResourceSample
	var sampledAt, ingestedAt string
	err := scanner.Scan(
		&row.ID, &row.WorkerID, &row.SessionID, &sampledAt, &ingestedAt,
		&row.CPUPercent, &row.CPUIOWaitPercent, &row.CPUStealPercent,
		&row.Load1, &row.RunQueue, &row.RSSBytes, &row.MemoryUsedBytes,
		&row.MajorPageFaults, &row.DiskReadBytes, &row.DiskWriteBytes,
		&row.DiskFreeBytes, &row.NetworkRxBytes, &row.NetworkTxBytes,
		&row.ActiveTasks, &row.FFmpegProcesses,
		&row.EffectiveCPUCores, &row.ProcessRSSPeakBytes, &row.MemoryAvailableBytes,
		&row.SwapUsedBytes, &row.PageCacheBytes, &row.TempBytesWritten,
		&row.TempFilesOpen, &row.ScratchCurrentBytes, &row.ScratchPeakBytes,
		&row.DiskReadMbps, &row.DiskWriteMbps, &row.DiskIOWaitMs,
		&row.NetworkRetransmits, &row.DownloadMbps, &row.UploadMbps,
		&row.TaskSlots, &row.RenderJobsActive, &row.PrefetchJobsActive,
		&row.PublisherJobsActive, &row.OpenFileDescriptors, &row.MaxFileDescriptors,
		&row.FDUtilizationRatio,
	)
	if err != nil {
		return row, fmt.Errorf("scan worker resource sample: %w", err)
	}
	row.SampledAt, err = parsePersistedWorkerTimestamp(sampledAt, "worker_resource_samples.sampled_at")
	if err != nil {
		return row, err
	}
	row.IngestedAt, err = parsePersistedWorkerTimestamp(ingestedAt, "worker_resource_samples.ingested_at")
	if err != nil {
		return row, err
	}
	return row, nil
}

// MaintainWorkerResourceSamples recalculates hourly rollups and prunes raw
// and rolled-up data. It is idempotent: the same hour is upserted and can be
// recalculated after late heartbeats. Retention is based on master ingested_at
// for raw samples and hour_bucket for rollups.
func (s *SQLiteStore) MaintainWorkerResourceSamples(ctx context.Context, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker resource maintenance: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_resource_rollups (
		worker_id, session_id, hour_bucket, sample_count,
		cpu_percent_avg, cpu_iowait_percent_avg, cpu_steal_percent_avg,
		load1_avg, run_queue_avg, rss_bytes_avg, memory_used_bytes_avg,
		disk_read_bytes_avg, disk_write_bytes_avg, network_rx_bytes_avg,
		network_tx_bytes_avg, active_tasks_avg, ffmpeg_processes_avg,
		effective_cpu_cores_avg, process_rss_peak_bytes_max,
		memory_available_bytes_avg, swap_used_bytes_avg, page_cache_bytes_avg,
		temp_bytes_written_max, temp_files_open_max,
		scratch_current_bytes_avg, scratch_peak_bytes_max,
		disk_read_mbps_avg, disk_write_mbps_avg, disk_io_wait_ms_max,
		disk_free_bytes_min, network_retransmits_max,
		download_mbps_avg, upload_mbps_avg,
		task_slots_avg, render_jobs_active_avg,
		prefetch_jobs_active_avg, publisher_jobs_active_avg,
		open_file_descriptors_max, max_file_descriptors_max,
		fd_utilization_ratio_avg,
		calculated_at
	)
	SELECT worker_id, session_id,
		strftime('%Y-%m-%dT%H:00:00Z', sampled_at), COUNT(*),
		AVG(cpu_percent), AVG(cpu_iowait_percent), AVG(cpu_steal_percent),
		AVG(load1), AVG(run_queue), AVG(rss_bytes), AVG(memory_used_bytes),
		MAX(disk_read_bytes), MAX(disk_write_bytes), MAX(network_rx_bytes),
		MAX(network_tx_bytes), AVG(active_tasks), AVG(ffmpeg_processes),
		AVG(effective_cpu_cores), MAX(process_rss_peak_bytes),
		AVG(memory_available_bytes), AVG(swap_used_bytes), AVG(page_cache_bytes),
		MAX(temp_bytes_written), MAX(temp_files_open),
		AVG(scratch_current_bytes), MAX(scratch_peak_bytes),
		AVG(disk_read_mbps), AVG(disk_write_mbps), MAX(disk_io_wait_ms),
		MIN(disk_free_bytes), MAX(network_retransmits),
		AVG(download_mbps), AVG(upload_mbps),
		AVG(task_slots), AVG(render_jobs_active),
		AVG(prefetch_jobs_active), AVG(publisher_jobs_active),
		MAX(open_file_descriptors), MAX(max_file_descriptors),
		AVG(fd_utilization_ratio),
		?
	FROM worker_resource_samples
	GROUP BY worker_id, session_id, strftime('%Y-%m-%dT%H:00:00Z', sampled_at)
	ON CONFLICT(worker_id, session_id, hour_bucket) DO UPDATE SET
		sample_count=excluded.sample_count,
		cpu_percent_avg=excluded.cpu_percent_avg,
		cpu_iowait_percent_avg=excluded.cpu_iowait_percent_avg,
		cpu_steal_percent_avg=excluded.cpu_steal_percent_avg,
		load1_avg=excluded.load1_avg, run_queue_avg=excluded.run_queue_avg,
		rss_bytes_avg=excluded.rss_bytes_avg,
		memory_used_bytes_avg=excluded.memory_used_bytes_avg,
		disk_read_bytes_avg=excluded.disk_read_bytes_avg,
		disk_write_bytes_avg=excluded.disk_write_bytes_avg,
		network_rx_bytes_avg=excluded.network_rx_bytes_avg,
		network_tx_bytes_avg=excluded.network_tx_bytes_avg,
		active_tasks_avg=excluded.active_tasks_avg,
		ffmpeg_processes_avg=excluded.ffmpeg_processes_avg,
		effective_cpu_cores_avg=excluded.effective_cpu_cores_avg,
		process_rss_peak_bytes_max=excluded.process_rss_peak_bytes_max,
		memory_available_bytes_avg=excluded.memory_available_bytes_avg,
		swap_used_bytes_avg=excluded.swap_used_bytes_avg,
		page_cache_bytes_avg=excluded.page_cache_bytes_avg,
		temp_bytes_written_max=excluded.temp_bytes_written_max,
		temp_files_open_max=excluded.temp_files_open_max,
		scratch_current_bytes_avg=excluded.scratch_current_bytes_avg,
		scratch_peak_bytes_max=excluded.scratch_peak_bytes_max,
		disk_read_mbps_avg=excluded.disk_read_mbps_avg,
		disk_write_mbps_avg=excluded.disk_write_mbps_avg,
		disk_io_wait_ms_max=excluded.disk_io_wait_ms_max,
		disk_free_bytes_min=excluded.disk_free_bytes_min,
		network_retransmits_max=excluded.network_retransmits_max,
		download_mbps_avg=excluded.download_mbps_avg,
		upload_mbps_avg=excluded.upload_mbps_avg,
		task_slots_avg=excluded.task_slots_avg,
		render_jobs_active_avg=excluded.render_jobs_active_avg,
		prefetch_jobs_active_avg=excluded.prefetch_jobs_active_avg,
		publisher_jobs_active_avg=excluded.publisher_jobs_active_avg,
		open_file_descriptors_max=excluded.open_file_descriptors_max,
		max_file_descriptors_max=excluded.max_file_descriptors_max,
		fd_utilization_ratio_avg=excluded.fd_utilization_ratio_avg,
		calculated_at=excluded.calculated_at`, now.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("roll up worker resources: %w", err)
	}

	if s.resourceRetention.RawDays > 0 {
		cutoff := now.UTC().AddDate(0, 0, -s.resourceRetention.RawDays).Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `DELETE FROM worker_resource_samples WHERE ingested_at < ?`, cutoff); err != nil {
			return fmt.Errorf("prune worker resource samples: %w", err)
		}
	}
	if s.resourceRetention.RollupDays > 0 {
		cutoff := now.UTC().AddDate(0, 0, -s.resourceRetention.RollupDays).Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `DELETE FROM worker_resource_rollups WHERE hour_bucket < ?`, cutoff); err != nil {
			return fmt.Errorf("prune worker resource rollups: %w", err)
		}
	}
	return tx.Commit()
}
