package remoteengine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
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
	log.Printf("[CLIENT] StartPipeline POST %s body=%d bytes idempotency_key=%s", strings.TrimSuffix(c.config.URL, "/")+path, len(body), idempotencyKey)

	startTime := time.Now()
	httpReq, err := c.newRequest(ctx, http.MethodPost, path, body, idempotencyKey)
	if err != nil {
		log.Printf("[CLIENT] StartPipeline FAILED after %s: %v", time.Since(startTime).Round(time.Millisecond), err)
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] StartPipeline FAILED after %s: %v", elapsed, err)
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
		log.Printf("[CLIENT] StartPipeline CONTRACT VIOLATION after %s: %s", elapsed, valErr)
		return nil, valErr
	}

	log.Printf("[CLIENT] StartPipeline OK job_id=%s status=%s elapsed=%s", initial.JobID, initial.Status, elapsed)

	return result, nil
}

// GetPipelineStatus gets the status of a pipeline job.
func (c *Client) GetPipelineStatus(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

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
	return resp, nil
}

func (c *Client) doPipelineStatusRequest(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	path := fmt.Sprintf("/api/jobs/%s", traceID)
	log.Printf("[CLIENT] GetPipelineStatus GET %s", strings.TrimSuffix(c.config.URL, "/")+path)

	startTime := time.Now()
	httpReq, err := c.newRequest(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		log.Printf("[CLIENT] GetPipelineStatus FAILED after %s: %v", time.Since(startTime).Round(time.Millisecond), err)
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] GetPipelineStatus FAILED after %s: %v", elapsed, err)
		return nil, err
	}

	result, err := parseRemoteJobResponse(respBody)
	if err != nil {
		return nil, err
	}

	log.Printf("[CLIENT] GetPipelineStatus OK job_id=%s status=%s progress=%d elapsed=%s", result.TraceID, result.Status, int(result.Progress), elapsed)
	return result, nil
}

// CancelPipeline cancels/deletes a running pipeline job.
func (c *Client) CancelPipeline(ctx context.Context, traceID string) error {
	if !c.IsConfigured() {
		return ErrNotConfigured
	}

	path := fmt.Sprintf("/api/jobs/%s", traceID)
	log.Printf("[CLIENT] CancelPipeline DELETE %s", strings.TrimSuffix(c.config.URL, "/")+path)

	startTime := time.Now()
	httpReq, err := c.newRequest(ctx, http.MethodDelete, path, nil, "")
	if err != nil {
		log.Printf("[CLIENT] CancelPipeline FAILED after %s: %v", time.Since(startTime).Round(time.Millisecond), err)
		return err
	}

	_, err = c.doRequest(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] CancelPipeline FAILED after %s: %v", elapsed, err)
		return err
	}

	log.Printf("[CLIENT] CancelPipeline OK job_id=%s elapsed=%s", traceID, elapsed)
	return nil
}
