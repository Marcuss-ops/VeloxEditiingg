// Package api — endpoint methods extracted from client.go.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"velox-worker-agent/pkg/logger"
)

func shouldFallbackToLegacyEndpoint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "status 404") ||
		strings.Contains(msg, "status 405") ||
		strings.Contains(msg, "api route not found") ||
		strings.Contains(msg, "endpoint does not exist") ||
		strings.Contains(msg, "not found")
}

// registerResponse is used to parse token and other fields from registration response.
type registerResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Token   string `json:"token,omitempty"`
	Error   string `json:"error,omitempty"`
}

// RegisterWorker registers this worker with the master server.
func (c *Client) RegisterWorker(ctx context.Context, info *WorkerInfo) error {
	respBody, err := c.doRequest(ctx, "POST", endpointRegisterWorker, info)
	if err != nil {
		return err
	}
	var resp registerResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		logger.Debug("[%s] Could not parse registration response for token: %v", EventAPIRequest, err)
		return nil
	}
	if resp.Token != "" {
		c.authToken = resp.Token
		logger.Debug("[%s] Auth token received and stored (length: %d)", EventAPIRequest, len(resp.Token))
	}
	return nil
}

// UnregisterWorker unregisters this worker from the master server.
func (c *Client) UnregisterWorker(ctx context.Context, workerID string) error {
	_, err := c.doRequest(ctx, "POST", endpointUnregisterWorker, map[string]string{"worker_id": workerID})
	return err
}

// GetJob fetches the next available job from the master (legacy endpoint).
func (c *Client) GetJob(ctx context.Context, workerID string) (*Job, error) {
	respBody, err := c.doRequest(ctx, "POST", endpointGetJob, &JobRequest{WorkerID: workerID})
	if err != nil {
		return nil, err
	}
	var apiResp APIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	if !apiResp.Success {
		return nil, nil
	}
	jobData, err := json.Marshal(apiResp.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job data: %w", err)
	}
	var job Job
	if err := json.Unmarshal(jobData, &job); err != nil {
		return nil, fmt.Errorf("failed to parse job: %w", err)
	}
	return &job, nil
}

// GetJobV2 fetches the next available job via the canonical v2 endpoint.
func (c *Client) GetJobV2(ctx context.Context, workerID string) (*Job, error) {
	path := endpointV2GetJob + "?worker_id=" + url.QueryEscape(workerID)
	respBody, err := c.doRequest(ctx, "GET", path, nil)
	if err != nil {
		if shouldFallbackToLegacyEndpoint(err) {
			logger.Warn("[API_FALLBACK] Falling back to legacy job poll endpoint after v2 failure: %v", err)
			return c.GetJob(ctx, workerID)
		}
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

// SubmitJobResult submits the result of a completed job (legacy endpoint).
func (c *Client) SubmitJobResult(ctx context.Context, result *JobResult) error {
	_, err := c.doRequest(ctx, "POST", endpointSubmitResult, result)
	return err
}

// SubmitJobResultV2 submits the result via the canonical v2 endpoint.
func (c *Client) SubmitJobResultV2(ctx context.Context, jobID string, result *JobResult) error {
	path := fmt.Sprintf(endpointV2SubmitResult, url.PathEscape(jobID))
	_, err := c.doRequest(ctx, "POST", path, result)
	if shouldFallbackToLegacyEndpoint(err) {
		logger.Warn("[API_FALLBACK] Falling back to legacy result submit after v2 failure for job %s: %v", jobID, err)
		return c.SubmitJobResult(ctx, result)
	}
	return err
}

// CompleteJob notifies the master that a job has completed successfully (legacy).
func (c *Client) CompleteJob(ctx context.Context, jobID, workerID, leaseID string, attempt int) error {
	body := map[string]interface{}{
		"job_id":    jobID,
		"worker_id": workerID,
	}
	if strings.TrimSpace(leaseID) != "" {
		body["lease_id"] = strings.TrimSpace(leaseID)
	}
	if attempt > 0 {
		body["attempt"] = attempt
	}
	_, err := c.doRequest(ctx, "POST", endpointCompleteJob, body)
	return err
}

// CompleteJobV2 notifies completion via the canonical v2 endpoint.
func (c *Client) CompleteJobV2(ctx context.Context, jobID, workerID, leaseID string, attempt int) error {
	path := fmt.Sprintf(endpointV2CompleteJob, url.PathEscape(jobID))
	body := map[string]interface{}{
		"worker_id": workerID,
	}
	if strings.TrimSpace(leaseID) != "" {
		body["lease_id"] = strings.TrimSpace(leaseID)
	}
	if attempt > 0 {
		body["attempt"] = attempt
	}
	_, err := c.doRequest(ctx, "POST", path, body)
	if shouldFallbackToLegacyEndpoint(err) {
		logger.Warn("[API_FALLBACK] Falling back to legacy complete endpoint after v2 failure for job %s: %v", jobID, err)
		return c.CompleteJob(ctx, jobID, workerID, leaseID, attempt)
	}
	return err
}

// RenewJobLease tells the master the worker is still processing the job (legacy).
func (c *Client) RenewJobLease(ctx context.Context, jobID, workerID, leaseID string, attempt int, leaseExpiresAt string) error {
	body := map[string]interface{}{
		"job_id":    jobID,
		"worker_id": workerID,
	}
	if strings.TrimSpace(leaseID) != "" {
		body["lease_id"] = strings.TrimSpace(leaseID)
	}
	if attempt > 0 {
		body["attempt"] = attempt
	}
	if strings.TrimSpace(leaseExpiresAt) != "" {
		body["lease_expires_at"] = strings.TrimSpace(leaseExpiresAt)
	}
	body["contract_version"] = ContractVersionV2
	_, err := c.doRequest(ctx, "POST", endpointRenewLease, body)
	return err
}

// RenewJobLeaseV2 renews lease via the canonical v2 endpoint.
func (c *Client) RenewJobLeaseV2(ctx context.Context, jobID, workerID, leaseID string, attempt int, leaseExpiresAt string) error {
	path := fmt.Sprintf(endpointV2RenewLease, url.PathEscape(jobID))
	body := map[string]interface{}{
		"worker_id": workerID,
	}
	if strings.TrimSpace(leaseID) != "" {
		body["lease_id"] = strings.TrimSpace(leaseID)
	}
	if attempt > 0 {
		body["attempt"] = attempt
	}
	if strings.TrimSpace(leaseExpiresAt) != "" {
		body["lease_expires_at"] = strings.TrimSpace(leaseExpiresAt)
	}
	body["contract_version"] = ContractVersionV2
	_, err := c.doRequest(ctx, "POST", path, body)
	if shouldFallbackToLegacyEndpoint(err) {
		logger.Warn("[API_FALLBACK] Falling back to legacy lease renewal after v2 failure for job %s: %v", jobID, err)
		return c.RenewJobLease(ctx, jobID, workerID, leaseID, attempt, leaseExpiresAt)
	}
	return err
}

// SendHeartbeat sends a heartbeat to the master server.
func (c *Client) SendHeartbeat(ctx context.Context, payload *HeartbeatPayload) error {
	_, err := c.doRequest(ctx, "POST", endpointHeartbeat, payload)
	return err
}

// HealthCheck checks if the master server is healthy.
func (c *Client) HealthCheck(ctx context.Context) error {
	_, err := c.doRequest(ctx, "GET", endpointHealthCheck, nil)
	return err
}

// GetCommands fetches pending commands for this worker from the master.
func (c *Client) GetCommands(ctx context.Context, workerID string) ([]WorkerCommand, error) {
	respBody, err := c.doRequest(ctx, "GET", endpointGetCommands+"?worker_id="+url.QueryEscape(workerID), nil)
	if err != nil {
		return nil, err
	}
	var apiResp APIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	if !apiResp.Success {
		return nil, nil
	}
	commandsData, err := json.Marshal(apiResp.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal commands data: %w", err)
	}
	var commands []WorkerCommand
	if err := json.Unmarshal(commandsData, &commands); err != nil {
		var singleCmd WorkerCommand
		if err := json.Unmarshal(commandsData, &singleCmd); err != nil {
			return nil, fmt.Errorf("failed to parse commands: %w", err)
		}
		commands = []WorkerCommand{singleCmd}
	}
	return commands, nil
}

// AckCommand acknowledges a command has been processed.
func (c *Client) AckCommand(ctx context.Context, workerID, command string) error {
	_, err := c.doRequest(ctx, "POST", endpointAckCommand, map[string]string{
		"worker_id": workerID, "command": command,
	})
	return err
}

// UpdateStatus sends a status update to the master (for command responses).
func (c *Client) UpdateStatus(ctx context.Context, workerID, status string, details map[string]interface{}) error {
	_, err := c.doRequest(ctx, "POST", endpointUpdateStatus, map[string]interface{}{
		"worker_id": workerID, "status": status, "details": details,
	})
	return err
}
