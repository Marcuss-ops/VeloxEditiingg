// Package api — endpoint methods extracted from client.go.
//
// All control-plane traffic between worker and master flows over the gRPC
// `WorkerControl` bidi stream (see RemoteCodex/.../internal/transport). The
// HTTP endpoints that used to live here (RegisterWorker, Heartbeat, Commands,
// legacy /api/jobs/* CRUD) were decommissioned with the cutover to gRPC.
//
// We keep only:
//   - V2 job methods used by the upload/asset data-plane bridges
//     (`GetJobV2`, `SubmitJobResultV2`, `CompleteJobV2`, `RenewJobLeaseV2`)
//     which still serve their HTTP routes on the master (and which the worker
//     can fall back to via 4xx/5xx propagation rather than client-side fallback).
//   - `HealthCheck` for transport readiness probes during bootstrap.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"velox-worker-agent/pkg/logger"
)

// GetJobV2 fetches the next available job via the canonical v2 endpoint.
// On a 4xx/5xx the error is returned verbatim; the previous client-side
// fallback to /api/jobs/get is removed together with the legacy HTTP control
// plane (see package doc).
func (c *Client) GetJobV2(ctx context.Context, workerID string) (*Job, error) {
	path := endpointV2GetJob + "?worker_id=" + url.QueryEscape(workerID)
	respBody, err := c.doRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	var apiResp struct {
		Job    *Job   `json:"job"`
		Reason string `json:"reason,omitempty"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return apiResp.Job, nil
}

// SubmitJobResultV2 submits the result of a completed job via v2.
func (c *Client) SubmitJobResultV2(ctx context.Context, jobID string, result *JobResult) error {
	path := fmt.Sprintf(endpointV2SubmitResult, url.PathEscape(jobID))
	_, err := c.doRequest(ctx, "POST", path, result)
	return err
}

// CompleteJobV2 notifies completion via the canonical v2 endpoint.
func (c *Client) CompleteJobV2(ctx context.Context, jobID, workerID, leaseID string, attempt int) error {
	path := fmt.Sprintf(endpointV2CompleteJob, url.PathEscape(jobID))
	body := map[string]interface{}{
		"worker_id": workerID,
	}
	if trimmed := strings.TrimSpace(leaseID); trimmed != "" {
		body["lease_id"] = trimmed
	}
	if attempt > 0 {
		body["attempt"] = attempt
	}
	_, err := c.doRequest(ctx, "POST", path, body)
	return err
}

// RenewJobLeaseV2 renews a job lease via the canonical v2 endpoint.
func (c *Client) RenewJobLeaseV2(ctx context.Context, jobID, workerID, leaseID string, attempt int, leaseExpiresAt string) error {
	path := fmt.Sprintf(endpointV2RenewLease, url.PathEscape(jobID))
	body := map[string]interface{}{
		"worker_id": workerID,
	}
	if trimmed := strings.TrimSpace(leaseID); trimmed != "" {
		body["lease_id"] = trimmed
	}
	if attempt > 0 {
		body["attempt"] = attempt
	}
	if trimmed := strings.TrimSpace(leaseExpiresAt); trimmed != "" {
		body["lease_expires_at"] = trimmed
	}
	body["contract_version"] = ContractVersionV2
	if _, err := c.doRequest(ctx, "POST", path, body); err != nil {
		if c.circuitBreaker != nil {
			logger.Debug("[API] RenewJobLeaseV2 error (circuit: %s): %v", c.circuitBreaker.GetState(), err)
		}
		return err
	}
	return nil
}

// HealthCheck probes master readiness on /health. Used by the
// `--validate-config` path and by data-plane bridges before issuing requests.
func (c *Client) HealthCheck(ctx context.Context) error {
	_, err := c.doRequest(ctx, "GET", endpointHealthCheck, nil)
	return err
}
