package metrics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// RenderCohortInput contains normalized workload dimensions. ExecutorVersion
// is deliberately excluded from the base key so versions can be compared.
type RenderCohortInput struct {
	ExecutorID       string
	ExecutorVersion  int
	WorkerClass      string
	ResolutionWidth  int
	ResolutionHeight int
	FPS              float64
	OutputDuration   float64
	SceneCount       int
	SegmentCount     int
	AudioTracks      int
	SubtitleCount    int
	Codec            string
	Preset           string
	CacheMode        string
	TemplateID       string
	ConfigHash       string
}

// BuildRenderCohortKeys returns a versioned key and a version-neutral key.
func BuildRenderCohortKeys(in RenderCohortInput) (string, string) {
	base := strings.Join([]string{
		"executor=" + safeCohortPart(in.ExecutorID),
		"worker_class=" + safeCohortPart(in.WorkerClass),
		"resolution=" + resolutionBucket(in.ResolutionWidth, in.ResolutionHeight),
		"fps=" + fpsBucket(in.FPS),
		"duration=" + durationBucket(in.OutputDuration),
		"scenes=" + countBucket(in.SceneCount),
		"segments=" + countBucket(in.SegmentCount),
		"audio=" + countBucket(in.AudioTracks),
		"subtitles=" + countBucket(in.SubtitleCount),
		"codec=" + safeCohortPart(in.Codec),
		"preset=" + safeCohortPart(in.Preset),
		"cache=" + safeCohortPart(in.CacheMode),
		"template=" + safeCohortPart(in.TemplateID),
		"config=" + safeCohortPart(in.ConfigHash),
	}, "|")
	return fmt.Sprintf("%s|executor_version=%d", base, in.ExecutorVersion), base
}

func safeCohortPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("|", "%7c", "=", "%3d", " ", "_").Replace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func resolutionBucket(width, height int) string {
	if width <= 0 || height <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%dx%d", width, height)
}

func fpsBucket(fps float64) string {
	switch {
	case fps <= 0:
		return "unknown"
	case fps < 24:
		return "lt24"
	case fps <= 30:
		return "24_30"
	case fps <= 60:
		return "30_60"
	default:
		return "gt60"
	}
}

func durationBucket(seconds float64) string {
	switch {
	case seconds <= 0:
		return "unknown"
	case seconds < 15:
		return "lt15s"
	case seconds < 60:
		return "15_60s"
	case seconds < 300:
		return "1_5m"
	case seconds < 900:
		return "5_15m"
	default:
		return "gt15m"
	}
}

func countBucket(value int) string {
	switch {
	case value <= 0:
		return "0"
	case value <= 5:
		return "1_5"
	case value <= 20:
		return "6_20"
	case value <= 50:
		return "21_50"
	default:
		return "gt50"
	}
}

// RenderPerformanceDaily is one compact day/cohort/phase observation.
type RenderPerformanceDaily struct {
	Day                      string
	CohortKey                string
	CohortBaseKey            string
	Phase                    string
	ExecutorID               string
	ExecutorVersion          int
	WorkerID                 string
	WorkerClass              string
	GitSHA                   string
	EngineVersion            string
	FFmpegVersion            string
	DockerImageDigest        string
	ConfigHash               string
	Attempts                 int
	Succeeded                int
	Failed                   int
	PhaseMSTotal             float64
	PhaseMSAvg               float64
	PhaseMSP25               float64
	PhaseMSP50               float64
	PhaseMSP95               float64
	PhaseMSP99               float64
	BaselineP25MS            float64
	RecoverableMSTotal       float64
	OutputSeconds            float64
	WallMSTotal              float64
	CPUMSTotal               float64
	DownloadMSTotal          float64
	DecodeMSTotal            float64
	CompositeMSTotal         float64
	EncodeMSTotal            float64
	UploadMSTotal            float64
	OutputBytesTotal         int64
	TempBytesTotal           int64
	WastedCPUMSTotal         int64
	WastedDownloadBytesTotal int64
	RenderFactorAvg          float64
	CalculatedAt             string
}

type RenderPerformanceVersionRegression struct {
	CohortBaseKey   string
	Phase           string
	WorkerID        string
	WorkerClass     string
	ExecutorVersion int
	Day             string
	PhaseMSP25      float64
	BaselineP25MS   float64
	RecoverableMS   float64
	Attempts        int
	Succeeded       int
}

type performanceObservation struct {
	day, attemptID, status, workerID string
	input                            RenderCohortInput
	gitSHA, engineVersion            string
	ffmpegVersion, dockerDigest      string
	phase                            string
	durationMS                       float64
	wallMS, cpuMS, outputSeconds     float64
	outputBytes, tempBytes           int64
	wastedCPU, wastedDownload        int64
}

type performanceGroup struct {
	day, cohortKey, cohortBaseKey, phase, workerID string
	input                                          RenderCohortInput
	gitSHA, engineVersion, ffmpegVersion           string
	dockerDigest                                   string
	values, baselineValues                         []float64
	statuses                                       map[string]string
	attemptTotals                                  map[string]performanceAttemptTotals
}

type performanceAttemptTotals struct {
	wallMS, cpuMS, outputSeconds float64
	outputBytes, tempBytes       int64
	wastedCPU, wastedDownload    int64
}

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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

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
