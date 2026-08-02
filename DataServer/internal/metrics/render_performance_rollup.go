package metrics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ComputeRenderPerformanceDailyRollup computes one idempotent UTC day. The
// baseline is p25 of earlier successful observations in the same base cohort
// and phase. No history means baseline/recoverable values remain zero.
func (r *SQLiteLabelResolver) ComputeRenderPerformanceDailyRollup(ctx context.Context, day string) error {
	if _, err := time.Parse("2006-01-02", day); err != nil {
		return fmt.Errorf("render performance rollup: invalid day %q: %w", day, err)
	}
	current, err := r.fetchPerformanceObservations(ctx, day, false)
	if err != nil {
		return err
	}
	prior, err := r.fetchPerformanceObservations(ctx, day, true)
	if err != nil {
		return err
	}

	baselines := make(map[string][]float64)
	for _, obs := range prior {
		if obs.status != "SUCCEEDED" {
			continue
		}
		_, base := BuildRenderCohortKeys(obs.input)
		key := base + "\x00" + obs.phase
		baselines[key] = append(baselines[key], obs.durationMS)
	}
	for key := range baselines {
		sort.Float64s(baselines[key])
	}

	groups := make(map[string]*performanceGroup)
	for _, obs := range current {
		cohortKey, baseKey := BuildRenderCohortKeys(obs.input)
		key := cohortKey + "\x00" + obs.phase + "\x00" + obs.workerID
		group := groups[key]
		if group == nil {
			group = &performanceGroup{
				day: obs.day, cohortKey: cohortKey, cohortBaseKey: baseKey,
				phase: obs.phase, workerID: obs.workerID, input: obs.input,
				gitSHA: obs.gitSHA, engineVersion: obs.engineVersion,
				ffmpegVersion: obs.ffmpegVersion, dockerDigest: obs.dockerDigest,
				baselineValues: baselines[baseKey+"\x00"+obs.phase],
				statuses:       make(map[string]string), attemptTotals: make(map[string]performanceAttemptTotals),
			}
			groups[key] = group
		}
		group.values = append(group.values, obs.durationMS)
		group.statuses[obs.attemptID] = obs.status
		group.attemptTotals[obs.attemptID] = performanceAttemptTotals{
			wallMS: obs.wallMS, cpuMS: obs.cpuMS, outputSeconds: obs.outputSeconds,
			outputBytes: obs.outputBytes, tempBytes: obs.tempBytes,
			wastedCPU: obs.wastedCPU, wastedDownload: obs.wastedDownload,
		}
	}

	calculatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, group := range groups {
		if err := r.persistPerformanceGroup(ctx, group, calculatedAt); err != nil {
			return err
		}
	}
	return nil
}

func (r *SQLiteLabelResolver) fetchPerformanceObservations(ctx context.Context, day string, prior bool) ([]performanceObservation, error) {
	operator := "="
	if prior {
		operator = "<"
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT substr(COALESCE(NULLIF(a.completed_at, ''), a.updated_at), 1, 10), a.id, a.status, a.worker_id,
		       a.git_sha, a.engine_version, a.ffmpeg_version,
		       a.docker_image_digest, a.config_hash,
		       COALESCE(t.executor_id, ''), COALESCE(t.executor_version, 0),
		       COALESCE(w.worker_class, ''),
		       COALESCE(m.resolution_width, 0), COALESCE(m.resolution_height, 0),
		       COALESCE(m.fps, 0), COALESCE(m.media_duration_seconds, 0),
		       COALESCE(m.scene_count, 0), COALESCE(m.segment_count, 0),
		       COALESCE(m.audio_track_count, 0), COALESCE(m.subtitle_count, 0),
		       COALESCE(m.template_id, ''),
		       COALESCE(m.asset_cache_hit_count, 0), COALESCE(m.asset_cache_miss_count, 0),
		       COALESCE(m.concat_mode, ''),
		       COALESCE((SELECT s.codec FROM task_attempt_segment_timings s
		                 WHERE s.attempt_id = a.id ORDER BY s.segment_index LIMIT 1), ''),
		       COALESCE((SELECT s.preset FROM task_attempt_segment_timings s
		                 WHERE s.attempt_id = a.id ORDER BY s.segment_index LIMIT 1), ''),
		       p.component, p.action, p.phase, COALESCE(p.duration_ms, 0),
		       COALESCE(m.wall_clock_seconds, 0) * 1000,
		       COALESCE(m.cpu_time_ms, 0), COALESCE(m.output_bytes, 0),
		       COALESCE(m.temp_bytes_written, 0), COALESCE(m.wasted_cpu_ms, 0),
		       COALESCE(m.wasted_download_bytes, 0)
		FROM task_attempts a
		JOIN task_attempt_metrics m ON m.attempt_id = a.id
		JOIN task_phase_timings p ON p.attempt_id = a.id
		LEFT JOIN tasks t ON t.task_id = a.task_id
		LEFT JOIN workers w ON w.worker_id = a.worker_id
		WHERE substr(COALESCE(NULLIF(a.completed_at, ''), a.updated_at), 1, 10) `+operator+` ?
		ORDER BY COALESCE(NULLIF(a.completed_at, ''), a.updated_at) ASC, a.id ASC, p.phase_order ASC`, day)
	if err != nil {
		return nil, fmt.Errorf("render performance observations: %w", err)
	}
	defer rows.Close()

	var observations []performanceObservation
	for rows.Next() {
		var obs performanceObservation
		var phase, component, action, concatMode, codec, preset string
		var width, height, scenes, segments, audio, subtitles, executorVersion int
		var fps, duration, wall, cpu float64
		var cacheHits, cacheMisses int64
		if err := rows.Scan(&obs.day, &obs.attemptID, &obs.status, &obs.workerID,
			&obs.gitSHA, &obs.engineVersion, &obs.ffmpegVersion, &obs.dockerDigest, &obs.input.ConfigHash,
			&obs.input.ExecutorID, &executorVersion, &obs.input.WorkerClass,
			&width, &height, &fps, &duration, &scenes, &segments, &audio, &subtitles,
			&obs.input.TemplateID, &cacheHits, &cacheMisses, &concatMode,
			&codec, &preset, &component, &action, &phase, &obs.durationMS,
			&wall, &cpu, &obs.outputBytes, &obs.tempBytes, &obs.wastedCPU, &obs.wastedDownload); err != nil {
			return nil, fmt.Errorf("scan render performance observation: %w", err)
		}
		obs.input.ExecutorVersion = executorVersion
		obs.input.ResolutionWidth, obs.input.ResolutionHeight = width, height
		obs.input.FPS, obs.input.OutputDuration = fps, duration
		obs.input.SceneCount, obs.input.SegmentCount = scenes, segments
		obs.input.AudioTracks, obs.input.SubtitleCount = audio, subtitles
		obs.input.Codec, obs.input.Preset = codec, preset
		obs.input.CacheMode = cacheMode(cacheHits, cacheMisses, concatMode)
		if component != "" && action != "" {
			obs.phase = component + "." + action
		} else if obs.phase == "" {
			obs.phase = "unknown"
		}
		obs.wallMS, obs.cpuMS, obs.outputSeconds = wall, cpu, duration
		observations = append(observations, obs)
	}
	return observations, rows.Err()
}

func cacheMode(hits, misses int64, concatMode string) string {
	switch {
	case hits > 0 && misses == 0:
		return "warm"
	case misses > 0:
		return "cold"
	}
	mode := strings.ToLower(strings.TrimSpace(concatMode))
	if mode == "" || mode == "n/a" || mode == "na" {
		return "unknown"
	}
	return mode
}

func (r *SQLiteLabelResolver) persistPerformanceGroup(ctx context.Context, group *performanceGroup, calculatedAt string) error {
	if len(group.values) == 0 {
		return nil
	}
	sort.Float64s(group.values)
	baseline := percentileFloat64(group.baselineValues, 0.25)
	attempts := len(group.statuses)
	succeeded := 0
	for _, status := range group.statuses {
		if status == "SUCCEEDED" {
			succeeded++
		}
	}
	totals := performanceAttemptTotals{}
	for _, value := range group.attemptTotals {
		totals.wallMS += value.wallMS
		totals.cpuMS += value.cpuMS
		totals.outputSeconds += value.outputSeconds
		totals.outputBytes += value.outputBytes
		totals.tempBytes += value.tempBytes
		totals.wastedCPU += value.wastedCPU
		totals.wastedDownload += value.wastedDownload
	}
	phaseTotal := 0.0
	recoverable := 0.0
	for _, value := range group.values {
		phaseTotal += value
		if baseline > 0 && value > baseline {
			recoverable += value - baseline
		}
	}
	renderFactor := 0.0
	if totals.outputSeconds > 0 {
		renderFactor = totals.wallMS / (totals.outputSeconds * 1000) / float64(maxInt(attempts, 1))
	}
	downloadMS, decodeMS, compositeMS, encodeMS, uploadMS := phaseBucketTotals(group.phase, phaseTotal)
	_, err := r.DB.ExecContext(ctx, `
		INSERT INTO render_performance_daily (
			day, cohort_key, cohort_base_key, phase,
			executor_id, executor_version, worker_id, worker_class,
			git_sha, engine_version, ffmpeg_version, docker_image_digest, config_hash,
			attempts, succeeded, failed,
			phase_ms_total, phase_ms_avg, phase_ms_p25, phase_ms_p50, phase_ms_p95, phase_ms_p99,
			baseline_p25_ms, recoverable_ms_total, output_seconds, wall_ms_total, cpu_ms_total,
			download_ms_total, decode_ms_total, composite_ms_total, encode_ms_total, upload_ms_total,
			output_bytes_total, temp_bytes_total, wasted_cpu_ms_total, wasted_download_bytes_total,
			render_factor_avg, calculated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(day, cohort_key, phase, worker_id, config_hash) DO UPDATE SET
			attempts=excluded.attempts, succeeded=excluded.succeeded, failed=excluded.failed,
			phase_ms_total=excluded.phase_ms_total, phase_ms_avg=excluded.phase_ms_avg,
			phase_ms_p25=excluded.phase_ms_p25, phase_ms_p50=excluded.phase_ms_p50,
			phase_ms_p95=excluded.phase_ms_p95, phase_ms_p99=excluded.phase_ms_p99,
			baseline_p25_ms=excluded.baseline_p25_ms, recoverable_ms_total=excluded.recoverable_ms_total,
			output_seconds=excluded.output_seconds, wall_ms_total=excluded.wall_ms_total,
			cpu_ms_total=excluded.cpu_ms_total,
			download_ms_total=excluded.download_ms_total, decode_ms_total=excluded.decode_ms_total,
			composite_ms_total=excluded.composite_ms_total, encode_ms_total=excluded.encode_ms_total,
			upload_ms_total=excluded.upload_ms_total, output_bytes_total=excluded.output_bytes_total,
			temp_bytes_total=excluded.temp_bytes_total, wasted_cpu_ms_total=excluded.wasted_cpu_ms_total,
			wasted_download_bytes_total=excluded.wasted_download_bytes_total,
			render_factor_avg=excluded.render_factor_avg, calculated_at=excluded.calculated_at`,
		group.day, group.cohortKey, group.cohortBaseKey, group.phase,
		group.input.ExecutorID, group.input.ExecutorVersion, group.workerID, group.input.WorkerClass,
		group.gitSHA, group.engineVersion, group.ffmpegVersion, group.dockerDigest, group.input.ConfigHash,
		attempts, succeeded, attempts-succeeded,
		phaseTotal, phaseTotal/float64(len(group.values)), percentileFloat64(group.values, .25),
		percentileFloat64(group.values, .50), percentileFloat64(group.values, .95), percentileFloat64(group.values, .99),
		baseline, recoverable, totals.outputSeconds, totals.wallMS, totals.cpuMS,
		downloadMS, decodeMS, compositeMS, encodeMS, uploadMS,
		totals.outputBytes, totals.tempBytes, totals.wastedCPU, totals.wastedDownload, renderFactor, calculatedAt)
	if err != nil {
		return fmt.Errorf("persist render performance group: %w", err)
	}
	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
