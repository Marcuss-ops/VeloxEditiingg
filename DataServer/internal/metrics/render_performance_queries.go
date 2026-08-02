package metrics

import (
	"context"
	"fmt"
	"strings"
)

// ListRenderPerformanceDaily returns rows filtered by optional day, cohort and phase.
func (r *SQLiteLabelResolver) ListRenderPerformanceDaily(ctx context.Context, day, cohortBaseKey, phase string) ([]RenderPerformanceDaily, error) {
	query := `SELECT day, cohort_key, cohort_base_key, phase, executor_id, executor_version,
		worker_id, worker_class, git_sha, engine_version, ffmpeg_version, docker_image_digest,
		config_hash, attempts, succeeded, failed, phase_ms_total, phase_ms_avg, phase_ms_p25,
		phase_ms_p50, phase_ms_p95, phase_ms_p99, baseline_p25_ms, recoverable_ms_total,
		output_seconds, wall_ms_total, cpu_ms_total,
		download_ms_total, decode_ms_total, composite_ms_total, encode_ms_total, upload_ms_total,
		output_bytes_total, temp_bytes_total, wasted_cpu_ms_total, wasted_download_bytes_total,
		render_factor_avg, calculated_at
		FROM render_performance_daily WHERE 1=1`
	args := make([]any, 0, 3)
	if day != "" {
		query += " AND day=?"
		args = append(args, day)
	}
	if cohortBaseKey != "" {
		query += " AND cohort_base_key=?"
		args = append(args, cohortBaseKey)
	}
	if phase != "" {
		query += " AND phase=?"
		args = append(args, phase)
	}
	query += " ORDER BY cohort_base_key, phase, executor_version, day"
	return r.queryPerformanceRows(ctx, query, args...)
}

// CompareRenderPerformanceVersions returns all daily rows for one equivalent cohort and phase.
func (r *SQLiteLabelResolver) CompareRenderPerformanceVersions(ctx context.Context, cohortBaseKey, phase string) ([]RenderPerformanceVersionRegression, error) {
	rows, err := r.ListRenderPerformanceDaily(ctx, "", cohortBaseKey, phase)
	if err != nil {
		return nil, err
	}
	out := make([]RenderPerformanceVersionRegression, 0, len(rows))
	for _, row := range rows {
		out = append(out, RenderPerformanceVersionRegression{
			CohortBaseKey: row.CohortBaseKey, Phase: row.Phase,
			WorkerID: row.WorkerID, WorkerClass: row.WorkerClass,
			ExecutorVersion: row.ExecutorVersion, Day: row.Day,
			PhaseMSP25: row.PhaseMSP25, BaselineP25MS: row.BaselineP25MS,
			RecoverableMS: row.RecoverableMSTotal, Attempts: row.Attempts, Succeeded: row.Succeeded,
		})
	}
	return out, nil
}

func (r *SQLiteLabelResolver) queryPerformanceRows(ctx context.Context, query string, args ...any) ([]RenderPerformanceDaily, error) {
	dbRows, err := r.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query render performance daily: %w", err)
	}
	defer dbRows.Close()
	var out []RenderPerformanceDaily
	for dbRows.Next() {
		var row RenderPerformanceDaily
		if err := dbRows.Scan(
			&row.Day, &row.CohortKey, &row.CohortBaseKey, &row.Phase,
			&row.ExecutorID, &row.ExecutorVersion, &row.WorkerID, &row.WorkerClass,
			&row.GitSHA, &row.EngineVersion, &row.FFmpegVersion, &row.DockerImageDigest,
			&row.ConfigHash, &row.Attempts, &row.Succeeded, &row.Failed,
			&row.PhaseMSTotal, &row.PhaseMSAvg, &row.PhaseMSP25, &row.PhaseMSP50,
			&row.PhaseMSP95, &row.PhaseMSP99, &row.BaselineP25MS, &row.RecoverableMSTotal,
			&row.OutputSeconds, &row.WallMSTotal, &row.CPUMSTotal,
			&row.DownloadMSTotal, &row.DecodeMSTotal, &row.CompositeMSTotal, &row.EncodeMSTotal, &row.UploadMSTotal,
			&row.OutputBytesTotal, &row.TempBytesTotal, &row.WastedCPUMSTotal, &row.WastedDownloadBytesTotal,
			&row.RenderFactorAvg, &row.CalculatedAt); err != nil {
			return nil, fmt.Errorf("scan render performance daily: %w", err)
		}
		out = append(out, row)
	}
	return out, dbRows.Err()
}

func phaseBucketTotals(phase string, duration float64) (download, decode, composite, encode, upload float64) {
	switch {
	case strings.Contains(phase, "download") || strings.Contains(phase, "asset"):
		download = duration
	case strings.Contains(phase, "decode"):
		decode = duration
	case strings.Contains(phase, "composite") || strings.Contains(phase, "scale") || strings.Contains(phase, "transform"):
		composite = duration
	case strings.Contains(phase, "encode") || strings.Contains(phase, "mux"):
		encode = duration
	case strings.Contains(phase, "upload"):
		upload = duration
	}
	return
}
