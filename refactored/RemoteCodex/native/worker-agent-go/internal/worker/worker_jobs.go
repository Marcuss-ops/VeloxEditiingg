// Package worker provides job processing logic for the worker agent.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/api/renderplan"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video"
)

// convertToStringSlice safely converts an interface{} to []string.
// Handles both []string and []interface{} (from JSON decoding).
func convertToStringSlice(input interface{}) []string {
	if input == nil {
		return []string{}
	}
	switch v := input.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			if str, ok := item.(string); ok {
				result[i] = str
			}
		}
		return result
	default:
		return []string{}
	}
}

// getStringParam safely extracts a string parameter, returning fallback if missing or wrong type.
func getStringParam(params map[string]interface{}, key, fallback string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return fallback
}

// getMapParam safely extracts a map[string]interface{} parameter.
func getMapParam(params map[string]interface{}, key string) map[string]interface{} {
	if v, ok := params[key].(map[string]interface{}); ok {
		return v
	}
	return map[string]interface{}{}
}

// getSliceParam safely extracts a []interface{} parameter.
func getSliceParam(params map[string]interface{}, key string) []interface{} {
	if v, ok := params[key].([]interface{}); ok {
		return v
	}
	return []interface{}{}
}

// renderJobParams holds the extracted parameters common to render/video/audio jobs.
type renderJobParams struct {
	audioPath                         string
	outputPath                        string
	scenesJSON                        string
	scriptText                        string
	startClipPaths                    []string
	middleClipPaths                   []string
	stockClipSources                  []string
	endClipPaths                      []string
	backgroundMusicPaths              []string
	backgroundVideoForImgOverlaysPath string
	associazioniFinaliConTimestamp    map[string]interface{}
	formattedImgEntities              map[string]interface{}
	preAssociatedEntities             map[string]interface{}
	rawEntities                       map[string]interface{}
	audioLanguageForSRT               string
	segmentsForSRTGeneration          []interface{}
	videoMode                         string
	introClipPaths                    []string
	stockClipPaths                    []string
	clipSegments                      []interface{}
	sceneImagePaths                   []string
	driveOutputFolder                 string
}

// extractRenderJobParams safely extracts all render/video/audio job parameters.
func extractRenderJobParams(params map[string]interface{}) renderJobParams {
	introClipPaths := convertToStringSlice(params["intro_clip_paths"])
	if len(introClipPaths) == 0 {
		introClipPaths = convertToStringSlice(params["start_clip_paths"])
	}
	stockClipPaths := convertToStringSlice(params["stock_clip_paths"])
	if len(stockClipPaths) == 0 {
		stockClipPaths = convertToStringSlice(params["stock_clip_sources"])
	}

	return renderJobParams{
		audioPath:                         getStringParam(params, "audio_path", ""),
		outputPath:                        getStringParam(params, "output_path", ""),
		scenesJSON:                        getStringParam(params, "scenes_json", ""),
		scriptText:                        getStringParam(params, "script_text", ""),
		startClipPaths:                    convertToStringSlice(params["start_clip_paths"]),
		middleClipPaths:                   convertToStringSlice(params["middle_clip_paths"]),
		stockClipSources:                  convertToStringSlice(params["stock_clip_sources"]),
		endClipPaths:                      convertToStringSlice(params["end_clip_paths"]),
		backgroundMusicPaths:              convertToStringSlice(params["background_music_paths"]),
		backgroundVideoForImgOverlaysPath: getStringParam(params, "background_video_for_img_overlays_path", ""),
		associazioniFinaliConTimestamp:    getMapParam(params, "associazioni_finali_con_timestamp"),
		formattedImgEntities:              getMapParam(params, "formatted_img_entities"),
		preAssociatedEntities:             getMapParam(params, "pre_associated_entities"),
		rawEntities:                       getMapParam(params, "raw_entities"),
		audioLanguageForSRT:               getStringParam(params, "audio_language_for_srt", ""),
		segmentsForSRTGeneration:          getSliceParam(params, "segments_for_srt_generation"),
		videoMode:                         getStringParam(params, "video_mode", ""),
		introClipPaths:                    introClipPaths,
		stockClipPaths:                    stockClipPaths,
	clipSegments:                      getSliceParam(params, "clip_segments"),
	sceneImagePaths:                   convertToStringSlice(params["scene_image_paths"]),
	driveOutputFolder:                 getStringParam(params, "drive_output_folder", getStringParam(params, "output_directory", "")),
	}
}

// jobLoop polls for jobs and executes them.
func (w *Worker) jobLoop(ctx context.Context) {
	defer w.wg.Done()

	pollInterval := 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Job loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Job loop exiting (stop signal)")
			return
		case <-ticker.C:
			// Only poll if idle and not stopped and not in drain mode
			if w.Status() != StatusIdle || w.IsStopped() || w.drainMode.Load() {
				continue
			}

			job, err := w.pollJob(ctx)
			if err != nil {
				w.logger.Warn("Failed to poll for job: %v", err)
				continue
			}

			if job != nil {
				// Execute job in same goroutine to ensure single job at a time
				w.executeJob(ctx, job)
			}
		}
	}
}

// pollJob checks for an available job from the master.
func (w *Worker) pollJob(ctx context.Context) (*api.Job, error) {
	w.logger.Debug("Polling for job...")
	job, err := w.apiClient.GetJob(ctx, w.config.WorkerID)
	if err != nil {
		return nil, err
	}

	if job != nil {
		w.logger.Info("Received job: %s (type: %s, priority: %d)", job.JobID, job.JobType, job.Priority)

		// Phase 2: Build render plan with canonical version
		rp := renderplan.FromMap(map[string]interface{}{
			"version":   renderplan.RenderPlanVersion,
			"job_id":              job.JobID,
			"job_type":            job.JobType,
			"created_at":          job.CreatedAt,
			"priority":            job.Priority,
			"parameters":          job.Parameters,
		})

		// Phase 2: Validate render plan with centralized entrypoint
		if err := renderplan.ValidateRenderPlan(rp); err != nil {
			w.logger.Error("[RENDERPLAN] Job validation failed: %v", err)
			// Log error code if it's a PlanError
			if planErrs, ok := err.(renderplan.PlanErrors); ok {
				for _, planErr := range planErrs {
					w.logger.Error("[RENDERPLAN] error_code=%s field=%s message=%s", planErr.Code, planErr.Field, planErr.Message)
				}
			}
			telemetry.GetPrometheusMetrics().RecordIdempotencyConflict("validation_failed")
			return nil, fmt.Errorf("job validation failed: %w", err)
		}

		// Apply defaults after validation
		rp.SetDefaults()

		// Apply validated defaults back to job
		if rp.Priority != job.Priority {
			w.logger.Debug("[RENDERPLAN] Applied default priority: %d -> %d", job.Priority, rp.Priority)
			job.Priority = rp.Priority
		}

		// Log render_plan_version for every job
		w.logger.Info("[RENDERPLAN] Job %s validated: render_plan_version=%s", job.JobID, rp.Version)

		// Phase 1: Check if we can accept the job based on concurrency policy
		if !w.concurrencyLimiter.CanAcceptJob(job.Priority) {
			w.logger.Warn("[CONCURRENCY] Cannot accept job %s: concurrency limit reached", job.JobID)
			telemetry.GetPrometheusMetrics().RecordIdempotencyConflict("concurrency_limit")
			return nil, fmt.Errorf("concurrency limit reached for job %s", job.JobID)
		}

		// Record job received metric
		telemetry.RecordJobReceived()
	}

	return job, nil
}

// executeJob executes a job and reports the result.
func (w *Worker) executeJob(ctx context.Context, job *api.Job) {
	// Phase 1: Acquire concurrency slot
	if err := w.concurrencyLimiter.Acquire(ctx, job.JobID, job.Priority); err != nil {
		w.logger.Warn("[CONCURRENCY] Failed to acquire slot for job %s: %v", job.JobID, err)
		return
	}
	defer w.concurrencyLimiter.Release()

	// Transition to busy
	if !w.canTransitionTo(StatusBusy) {
		w.logger.Warn("Cannot accept job: invalid state transition from %s to busy", w.Status())
		return
	}

	w.mu.Lock()
	w.currentJob = job
	w.status = StatusBusy
	w.mu.Unlock()

	// Update worker status metric
	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 2) // 2 = busy
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	// Log structured job start event
	logger.LogJobStart(w.config.WorkerID, job.JobID, job.JobType, job.Priority)

	startTime := time.Now()
	result := &api.JobResult{
		JobID:     job.JobID,
		JobRunID:  resolveJobRunID(job),
		WorkerID:  w.config.WorkerID,
		StartTime: startTime.Format(time.RFC3339),
		Output:    make(map[string]interface{}),
	}

	var output map[string]interface{}
	var execErr error

	w.logger.Info("[JOB] Executing job %s via runJobTask", job.JobID)
	output, execErr = w.runJobTask(ctx, job)

	if execErr == nil {
		// Ensure the master receives the actual rendered file, not a container-local path.
		updatedOutput, upErr := w.uploadCompletedVideo(ctx, job, output)
		if upErr != nil {
			execErr = fmt.Errorf("upload completed video failed: %w", upErr)
		} else {
			output = updatedOutput
		}
	}

	// Determine final status and transition
	w.mu.Lock()
	w.currentJob = nil
	duration := time.Since(startTime)

	if execErr != nil {
		// Log structured job failure event
		logger.LogJobFailedWithType(w.config.WorkerID, job.JobID, job.JobType, execErr, duration)
		result.Status = "failed"
		result.Error = execErr.Error()
		w.status = StatusError

		// Phase 1: Record KPI metrics for failure
		telemetry.RecordJobFailure(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	} else {
		// Log structured job success event
		logger.LogJobSuccess(w.config.WorkerID, job.JobID, job.JobType, duration)
		result.Status = "success"
		result.Output = output
		w.status = StatusIdle

		// Phase 1: Record KPI metrics for success
		telemetry.RecordJobSuccess(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	}
	w.mu.Unlock()

	// Update worker status metric
	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 1) // 1 = idle
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	result.EndTime = time.Now().Format(time.RFC3339)
	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	if result.Output == nil {
		result.Output = make(map[string]interface{})
	}
	result.Output["worker_id"] = w.config.WorkerID
	result.Output["worker_name"] = w.config.WorkerName
	result.Output["worker_status"] = string(w.Status())
	result.Output["worker_recent_logs"] = recentLogs
	result.Output["worker_recent_errors"] = recentErrors
	result.Output["worker_recent_logs_count"] = len(recentLogs)
	result.Output["worker_recent_errors_count"] = len(recentErrors)
	if job != nil {
		result.Output["job_type"] = job.JobType
		result.Output["job_priority"] = job.Priority
		result.Output["job_run_id"] = resolveJobRunID(job)
	}

	// Submit result with timeout
	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Phase 1: Record job complete ack time
	ackStartTime := time.Now()
	if err := w.apiClient.SubmitJobResult(submitCtx, result); err != nil {
		w.logger.Error("Failed to submit job result for %s: %v", job.JobID, err)
	} else {
		w.logger.Debug("Job result submitted: %s (status: %s)", job.JobID, result.Status)
		telemetry.GetPrometheusMetrics().RecordJobCompleteAck(job.JobType, float64(time.Since(ackStartTime).Milliseconds()))
	}

	// If we were in error state, transition back to idle after reporting
	if execErr != nil {
		// Brief pause before accepting new jobs after error
		time.Sleep(2 * time.Second)
		if w.canTransitionTo(StatusIdle) {
			w.setStatus(StatusIdle)
		}
	}
}

func resolveJobRunID(job *api.Job) string {
	if job == nil {
		return ""
	}
	if trimmed := strings.TrimSpace(job.JobRunID); trimmed != "" {
		return trimmed
	}
	if trimmed, ok := job.Parameters["job_run_id"].(string); ok && strings.TrimSpace(trimmed) != "" {
		return strings.TrimSpace(trimmed)
	}
	if trimmed, ok := job.Parameters["run_id"].(string); ok && strings.TrimSpace(trimmed) != "" {
		return strings.TrimSpace(trimmed)
	}
	return ""
}

func extractOutputVideoPath(output map[string]interface{}) string {
	if len(output) == 0 {
		return ""
	}
	for _, k := range []string{"master_video_path", "output_path", "result_path", "video_path", "output"} {
		if s, ok := output[k].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	// Limit recursion depth to prevent stack overflow on malformed payloads
	if nested, ok := output["result"].(map[string]interface{}); ok {
		return extractOutputVideoPathWithDepth(nested, 5)
	}
	return ""
}

// extractOutputVideoPathWithDepth recurses into nested result with a depth limit.
func extractOutputVideoPathWithDepth(output map[string]interface{}, depth int) string {
	if depth <= 0 || len(output) == 0 {
		return ""
	}
	for _, k := range []string{"master_video_path", "output_path", "result_path", "video_path", "output"} {
		if s, ok := output[k].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if nested, ok := output["result"].(map[string]interface{}); ok {
		return extractOutputVideoPathWithDepth(nested, depth-1)
	}
	return ""
}

func (w *Worker) uploadCompletedVideo(ctx context.Context, job *api.Job, output map[string]interface{}) (map[string]interface{}, error) {
	if len(output) == 0 {
		return output, fmt.Errorf("empty job output")
	}
	localVideoPath := extractOutputVideoPath(output)
	if strings.TrimSpace(localVideoPath) == "" {
		return output, fmt.Errorf("missing output video path in result payload")
	}
	st, err := os.Stat(localVideoPath)
	if err != nil || st.IsDir() {
		return output, fmt.Errorf("output video not found on worker filesystem: %s", localVideoPath)
	}

	uploadURL := strings.TrimRight(w.config.MasterURL, "/") + "/api/v1/video/upload-completed"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("job_id", job.JobID); err != nil {
		return output, fmt.Errorf("write job_id field: %w", err)
	}
	if err := writer.WriteField("worker_id", w.config.WorkerID); err != nil {
		return output, fmt.Errorf("write worker_id field: %w", err)
	}

	if runID := resolveJobRunID(job); strings.TrimSpace(runID) != "" {
		if err := writer.WriteField("job_run_id", strings.TrimSpace(runID)); err != nil {
			return output, fmt.Errorf("write job_run_id field: %w", err)
		}
	}
	if len(job.Parameters) > 0 {
		if raw, err := json.Marshal(job.Parameters); err == nil {
			if err := writer.WriteField("upload_info", string(raw)); err != nil {
				return output, fmt.Errorf("write upload_info field: %w", err)
			}
		}
	}

	part, err := writer.CreateFormFile("video_file", filepath.Base(localVideoPath))
	if err != nil {
		return output, err
	}
	f, err := os.Open(localVideoPath)
	if err != nil {
		return output, err
	}
	defer f.Close()
	if _, err := io.Copy(part, f); err != nil {
		return output, err
	}
	if err := writer.Close(); err != nil {
		return output, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, body)
	if err != nil {
		return output, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Use a shared HTTP client with a configurable timeout for uploads.
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return output, err
	}
	defer resp.Body.Close()

	// Limit response body to 10 MB to prevent OOM from buggy/malicious servers.
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return output, fmt.Errorf("read upload response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return output, fmt.Errorf("upload endpoint http %d: %s", resp.StatusCode, string(respBytes))
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(respBytes, &parsed); err == nil {
		if vp, ok := parsed["video_path"].(string); ok && strings.TrimSpace(vp) != "" {
			output["master_video_path"] = strings.TrimSpace(vp)
			output["video_uploaded"] = true
		}
	}
	return output, nil
}

// runJobTask executes the actual job task (single-job workflow).
func (w *Worker) runJobTask(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	w.logger.Info("[JOB] Starting execution: id=%s type=%s", job.JobID, job.JobType)
	// Check for job timeout
	jobTimeout := 30 * time.Minute // default
	if job.TimeoutSecs > 0 {
		jobTimeout = time.Duration(job.TimeoutSecs) * time.Second
	}

	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	switch job.JobType {
	case "render":
		w.logger.Info("[JOB] Phase: render pipeline")
		return w.runRenderJob(jobCtx, job)
	case "process_video":
		w.logger.Info("[JOB] Phase: video pipeline")
		return w.runVideoJob(jobCtx, job)
	case "process_audio":
		w.logger.Info("[JOB] Phase: audio pipeline")
		return w.runAudioJob(jobCtx, job)
	case "health_check":
		w.logger.Info("[JOB] Phase: health_check")
		return map[string]interface{}{"status": "healthy", "worker_id": w.config.WorkerID}, nil
	default:
		return nil, fmt.Errorf("unknown job type: %s", job.JobType)
	}
}





// executeWorkflowJob is a shared implementation for render/video/audio jobs.
// It extracts parameters, creates the workflow, and executes it.
func (w *Worker) executeWorkflowJob(ctx context.Context, job *api.Job, jobLabel string, defaultExt string) (map[string]interface{}, error) {

	// Extract parameters safely
	p := extractRenderJobParams(job.Parameters)

	// Create workflow instance
	wfLogger := logger.New(logger.DebugLevel, os.Stdout)
	wfLogger.SetPrefix("[WORKFLOW]")

	workflow := video.NewVideoGenerationWorkflow(&config.WorkerConfig{
		WorkerID:   w.config.WorkerID,
		WorkerName: w.config.WorkerName,
		MasterURL:  w.config.MasterURL,
		LogLevel:   w.config.LogLevel,
	}, wfLogger)

	// Set default output path if not provided
	outputPath := p.outputPath
	if outputPath == "" {
		outputPath = fmt.Sprintf("/tmp/velox/output/%s.%s", job.JobID, defaultExt)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory %s: %w", filepath.Dir(outputPath), err)
	}

	statusCallback := func(msg string, isError bool) {
		if isError {
			w.logger.Error("%s", msg)
		} else {
			w.logger.Info("%s", msg)
		}
	}

	// Execute the workflow
	resultPath, err := workflow.ProcessSingleVideo(ctx,
		video.VideoGenerationInput{
			AudioPath:                         p.audioPath,
			OutputPath:                        outputPath,
			ScenesJSON:                        p.scenesJSON,
			ScriptText:                        p.scriptText,
			StartClipPaths:                    p.startClipPaths,
			MiddleClipPaths:                   p.middleClipPaths,
			StockClipSources:                  p.stockClipSources,
			EndClipPaths:                      p.endClipPaths,
			BackgroundMusicPaths:              p.backgroundMusicPaths,
			BackgroundVideoForImgOverlaysPath: p.backgroundVideoForImgOverlaysPath,
			AssociazioniFinaliConTimestamp:    p.associazioniFinaliConTimestamp,
			FormattedImgEntities:              p.formattedImgEntities,
			PreAssociatedEntities:             p.preAssociatedEntities,
			RawEntities:                       p.rawEntities,
			AudioLanguageForSRT:               p.audioLanguageForSRT,
			SegmentsForSRTGeneration:          p.segmentsForSRTGeneration,
			VideoMode:                         p.videoMode,
			IntroClipPaths:                    p.introClipPaths,
			StockClipPaths:                    p.stockClipPaths,
			ClipSegments:                      p.clipSegments,
			SceneImagePaths:                   p.sceneImagePaths,
			DriveOutputFolder:                 p.driveOutputFolder,
		},
		statusCallback)

	if err != nil {
		return nil, fmt.Errorf("%s job failed: %w", jobLabel, err)
	}

	return map[string]interface{}{
		"status":      "completed",
		"job_id":      job.JobID,
		"output_path": resultPath,
	}, nil
}

func (w *Worker) runRenderJob(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	return w.executeWorkflowJob(ctx, job, "render", "mp4")
}

func (w *Worker) runVideoJob(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	return w.executeWorkflowJob(ctx, job, "video", "mp4")
}

func (w *Worker) runAudioJob(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	return w.executeWorkflowJob(ctx, job, "audio", "mp3")
}
