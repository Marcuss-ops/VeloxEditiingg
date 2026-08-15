package remoteengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
)

// StartPipeline starts a new pipeline job.
//
// The Idempotency-Key header is set to idempotencyKey when non-empty, so
// a timeout after the remote service has already created the job does not
// produce a duplicate on retry. The remote service must return the same
// remote_job_id for the same key.
//
// idempotencyKey should be the pipeline_run_id (e.g. "run_...").
func (c *Client) StartPipeline(ctx context.Context, payload map[string]interface{}, idempotencyKey string) (map[string]interface{}, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	ctx, span := telemetry.StartSpan(ctx, "remoteengine_start_pipeline",
		attribute.String("velox.idempotency_key", idempotencyKey),
	)
	defer span.End()

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "MARSHAL",
			Message: fmt.Sprintf("failed to marshal request: %v", err),
			Cause:   err,
		}
	}

	path := "/api/script/generate-with-images"
	c.recordRequest("start_pipeline")
	c.logInfo(ctx, logging.CodeRemoteEngineRequestStarted, logging.F("method", "POST", "path", path, "body_bytes", len(body), "idempotency_key", idempotencyKey))

	startTime := time.Now()
	httpReq, err := c.newRequest(ctx, http.MethodPost, path, body, idempotencyKey)
	if err != nil {
		c.logError(ctx, logging.CodeRemoteEngineRequestFailed, logging.F("method", "POST", "path", path, "elapsed_ms", time.Since(startTime).Milliseconds(), "err", err))
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	elapsedMs := time.Since(startTime).Milliseconds()
	if err != nil {
		c.logError(ctx, logging.CodeRemoteEngineRequestFailed, logging.F("method", "POST", "path", path, "elapsed_ms", elapsedMs, "err", err))
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	// Area 2: Validate the initial response — the remote engine must
	// return at least a job_id (with fallback to trace_id/id) and a
	// known status (queued, running, completed, failed, cancelled).
	// A contract violation is a PERMANENT error — no retry.
	initial, valErr := ValidateInitialResponse(result)
	if valErr != nil {
		c.logWarn(ctx, logging.CodeRemoteEngineContractViolation, logging.F("method", "POST", "path", path, "elapsed_ms", elapsedMs, "err", valErr))
		return nil, valErr
	}

	span.SetAttributes(
		attribute.String("velox.remote_job_id", initial.JobID),
		attribute.String("velox.remote_status", initial.Status),
	)
	c.logInfo(ctx, logging.CodeRemoteEngineRequestCompleted, logging.F("method", "POST", "path", path, "job_id", initial.JobID, "status", initial.Status, "elapsed_ms", elapsedMs))

	return result, nil
}

// GetPipelineStatus gets the status of a pipeline job.
func (c *Client) GetPipelineStatus(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	ctx, span := telemetry.StartSpan(ctx, "remoteengine_get_pipeline_status",
		attribute.String("velox.remote_job_id", traceID),
	)
	defer span.End()

	var resp *PipelineStatusResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doPipelineStatusRequest(ctx, traceID)
		if e != nil {
			return e
		}
		resp = r
		return nil
	})

	if retryErr != nil {
		return nil, retryErr
	}
	if resp != nil {
		span.SetAttributes(
			attribute.String("velox.remote_status", resp.Status),
			attribute.Float64("velox.remote_progress", resp.Progress),
		)
	}
	return resp, nil
}

func (c *Client) doPipelineStatusRequest(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	path := fmt.Sprintf("/api/jobs/%s", traceID)
	c.recordRequest("get_pipeline_status")
	c.logInfo(ctx, logging.CodeRemoteEngineRequestStarted, logging.F("method", "GET", "path", path, "job_id", traceID))

	startTime := time.Now()
	httpReq, err := c.newRequest(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		c.logError(ctx, logging.CodeRemoteEngineRequestFailed, logging.F("method", "GET", "path", path, "elapsed_ms", time.Since(startTime).Milliseconds(), "err", err))
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	elapsedMs := time.Since(startTime).Milliseconds()
	if err != nil {
		c.logError(ctx, logging.CodeRemoteEngineRequestFailed, logging.F("method", "GET", "path", path, "elapsed_ms", elapsedMs, "err", err))
		return nil, err
	}

	result, err := parseRemoteJobResponse(respBody)
	if err != nil {
		return nil, err
	}

	c.logInfo(ctx, logging.CodeRemoteEngineRequestCompleted, logging.F("method", "GET", "path", path, "job_id", result.TraceID, "status", result.Status, "progress", int(result.Progress), "elapsed_ms", elapsedMs))
	return result, nil
}

// CancelPipeline cancels/deletes a running pipeline job.
func (c *Client) CancelPipeline(ctx context.Context, traceID string) error {
	if !c.IsConfigured() {
		return ErrNotConfigured
	}

	ctx, span := telemetry.StartSpan(ctx, "remoteengine_cancel_pipeline",
		attribute.String("velox.remote_job_id", traceID),
	)
	defer span.End()

	path := fmt.Sprintf("/api/jobs/%s", traceID)
	c.recordRequest("cancel_pipeline")
	c.logInfo(ctx, logging.CodeRemoteEngineRequestStarted, logging.F("method", "DELETE", "path", path, "job_id", traceID))

	startTime := time.Now()
	httpReq, err := c.newRequest(ctx, http.MethodDelete, path, nil, "")
	if err != nil {
		c.logError(ctx, logging.CodeRemoteEngineRequestFailed, logging.F("method", "DELETE", "path", path, "elapsed_ms", time.Since(startTime).Milliseconds(), "err", err))
		return err
	}

	_, err = c.doRequest(httpReq)
	elapsedMs := time.Since(startTime).Milliseconds()
	if err != nil {
		c.logError(ctx, logging.CodeRemoteEngineRequestFailed, logging.F("method", "DELETE", "path", path, "elapsed_ms", elapsedMs, "err", err))
		return err
	}

	c.logInfo(ctx, logging.CodeRemoteEngineRequestCompleted, logging.F("method", "DELETE", "path", path, "job_id", traceID, "elapsed_ms", elapsedMs))
	return nil
}
