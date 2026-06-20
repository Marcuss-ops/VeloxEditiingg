// Package transport — HTTP polling transport for worker↔master communication.
// polling_transport.go provides PollingHTTPTransport that satisfies
// controltransport.ControlTransport using the remaining V2 HTTP endpoints
// (GET /api/v1/queue/job, POST /api/v1/jobs/:id/result, POST /api/v1/jobs/:id/complete,
// POST /api/v1/jobs/:id/lease).
//
// This transport exists for backward compatibility and shadow-mode validation
// (PR11). The primary transport is GRPCStreamTransport. Heartbeats and command
// ACKs have no HTTP endpoints (decommissioned in favour of gRPC) and are
// best-effort no-ops here.
package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// PollingHTTPTransport implements controltransport.ControlTransport using
// HTTP polling against the master's remaining V2 endpoints.
//
// Connect is a health-check no-op. Receive polls GET /api/v1/queue/job
// and pushes JobOffer messages. Send dispatches ControlMessage types to
// the corresponding HTTP endpoints where available (lease renewal, job
// result); message types without HTTP endpoints (heartbeat, command ack,
// goodbye) are logged and dropped.
type PollingHTTPTransport struct {
	client   *api.Client
	workerID string

	mu         sync.Mutex
	recvCh     chan controltransport.ControlMessage
	errCh      chan error
	errCloseOnce sync.Once
	recvOnce   sync.Once
	recvDone   chan struct{}
	closeCh    chan struct{}
	closeOnce  sync.Once
	closed     bool

	jobPollInterval   time.Duration
	jobPollTicker     *time.Ticker
}

// PollingTransportConfig holds configuration for PollingHTTPTransport.
type PollingTransportConfig struct {
	// JobPollInterval is how often to poll GET /api/v1/queue/job.
	// Default: 5s.
	JobPollInterval time.Duration
}

// DefaultPollingTransportConfig returns sensible defaults.
func DefaultPollingTransportConfig() PollingTransportConfig {
	return PollingTransportConfig{
		JobPollInterval: 5 * time.Second,
	}
}

// NewPollingHTTPTransport creates a new HTTP polling transport.
func NewPollingHTTPTransport(client *api.Client, workerID string, cfg PollingTransportConfig) *PollingHTTPTransport {
	if cfg.JobPollInterval <= 0 {
		cfg.JobPollInterval = 5 * time.Second
	}
	return &PollingHTTPTransport{
		client:         client,
		workerID:       workerID,
		recvCh:         make(chan controltransport.ControlMessage, 64),
		errCh:          make(chan error, 1),
		recvDone:       make(chan struct{}),
		closeCh:        make(chan struct{}),
		jobPollInterval: cfg.JobPollInterval,
	}
}

// Connect performs a health check against the master. HTTP polling does not
// maintain a persistent connection — this is a best-effort liveness probe.
func (t *PollingHTTPTransport) Connect(ctx context.Context, hello controltransport.WorkerHello) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return controltransport.ErrTransportClosed
	}
	t.mu.Unlock()

	// Best-effort health check to verify the master is reachable.
	if err := t.client.HealthCheck(ctx); err != nil {
		return fmt.Errorf("polling transport: health check failed: %w", err)
	}

	logger.Info("[POLLING] Connected (worker=%s)", t.workerID)
	return nil
}

// Receive starts the job polling goroutine and returns the message channel
// (master→worker) and an error channel. The message channel is closed when
// the transport is closed or an unrecoverable error occurs.
func (t *PollingHTTPTransport) Receive(ctx context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, controltransport.ErrTransportClosed
	}
	t.mu.Unlock()

	t.recvOnce.Do(func() {
		go t.pollLoop(ctx)
	})

	return t.recvCh, t.errCh, nil
}

// pollLoop periodically polls GET /api/v1/queue/job and pushes JobOffer
// messages to the receive channel. Also sends periodic Ping messages to
// keep the channel alive.
func (t *PollingHTTPTransport) pollLoop(ctx context.Context) {
	t.mu.Lock()
	t.jobPollTicker = time.NewTicker(t.jobPollInterval)
	ticker := t.jobPollTicker
	t.mu.Unlock()

	defer func() {
		t.errCloseOnce.Do(func() {
			close(t.errCh)
		})
		if t.recvCh != nil {
			close(t.recvCh)
		}
		close(t.recvDone)
	}()

	// Track last-seen job IDs to avoid duplicate offers.
	seenJobs := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.closeCh:
			return
		case <-ticker.C:
			job, err := t.client.GetJobV2(ctx, t.workerID)
			if err != nil {
				logger.Warn("[POLLING] GetJobV2 failed: %v", err)
				// Send a Ping to keep the channel alive and signal the
				// worker that the transport is still operational.
				t.sendRecvMsg(controltransport.NewMessage(
					controltransport.MsgPing, t.workerID, controltransport.ProtocolVersionCurrent,
				))
				continue
			}
			if job == nil || job.JobID == "" {
				// No job available — send periodic ping.
				t.sendRecvMsg(controltransport.NewMessage(
					controltransport.MsgPing, t.workerID, controltransport.ProtocolVersionCurrent,
				))
				continue
			}

			// Skip duplicate offers.
			if seenJobs[job.JobID] {
				continue
			}
			seenJobs[job.JobID] = true

			// Build a JobOffer message from the polled job.
			msg := t.jobToOffer(job)
			if !t.sendRecvMsg(msg) {
				return
			}

			// Prune the seenJobs map periodically to avoid unbounded growth.
			if len(seenJobs) > 1000 {
				seenJobs = make(map[string]bool)
			}
		}
	}
}

// sendRecvMsg attempts to send a message to the receive channel.
// Returns false if the transport is closing.
func (t *PollingHTTPTransport) sendRecvMsg(msg controltransport.ControlMessage) bool {
	select {
	case t.recvCh <- msg:
		return true
	case <-t.closeCh:
		return false
	}
}

// jobToOffer converts an api.Job (from GetJobV2) into a ControlMessage with
// type JobOffer and payload containing the job fields.
func (t *PollingHTTPTransport) jobToOffer(job *api.Job) controltransport.ControlMessage {
	payload := map[string]interface{}{
		"job_id":      job.JobID,
		"run_id":      job.JobRunID,
		"job_type":    job.JobType,
		"lease_id":    job.LeaseID,
		"attempt":     job.Attempt,
		"lease_expiry": job.LeaseExpiry,
	}

	// Include parameters as top-level payload fields.
	if job.Parameters != nil {
		for k, v := range job.Parameters {
			if _, exists := payload[k]; !exists {
				payload[k] = v
			}
		}
	}

	// Include created_at if present.
	if job.CreatedAt != nil {
		payload["created_at"] = job.CreatedAt
	}

	msg := controltransport.NewMessageWithPayload(
		controltransport.MsgJobOffer,
		t.workerID,
		controltransport.ProtocolVersionCurrent,
		payload,
	)

	return msg
}

// Send transmits a ControlMessage to the master via the appropriate HTTP endpoint.
// Message types without HTTP endpoints (heartbeat, command ack, goodbye) are
// best-effort no-ops.
func (t *PollingHTTPTransport) Send(ctx context.Context, msg controltransport.ControlMessage) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return controltransport.ErrTransportClosed
	}
	t.mu.Unlock()

	switch msg.Type {
	case controltransport.MsgLeaseRenewal:
		return t.sendLeaseRenewal(ctx, msg)

	case controltransport.MsgJobResult:
		return t.sendJobResult(ctx, msg)

	case controltransport.MsgHeartbeat:
		// Heartbeat HTTP endpoint is decommissioned (replaced by gRPC stream).
		// Best-effort no-op: the worker's heartbeat data is not sent via HTTP.
		logger.Debug("[POLLING] Heartbeat dropped (no HTTP endpoint)")
		return nil

	case controltransport.MsgCommandAck:
		// Command ACK HTTP endpoint is decommissioned (replaced by gRPC stream).
		logger.Debug("[POLLING] CommandAck dropped (no HTTP endpoint)")
		return nil

	case controltransport.MsgGoodbye:
		// Unregister HTTP endpoint is decommissioned.
		logger.Debug("[POLLING] Goodbye dropped (no HTTP endpoint)")
		return nil

	case controltransport.MsgJobAccepted:
		// JobAccepted is implicit in the HTTP claim (GetJobV2 returns the lease).
		logger.Debug("[POLLING] JobAccepted implicit (HTTP claim already holds lease)")
		return nil

	case controltransport.MsgJobRejected:
		logger.Warn("[POLLING] JobRejected: job_id=%s reason=%s",
			getPayloadStr(msg.Payload, "job_id"),
			getPayloadStr(msg.Payload, "reason"))
		return nil

	case controltransport.MsgJobProgress:
		// Progress is bundled into heartbeats, which have no HTTP endpoint.
		return nil

	case controltransport.MsgArtifactUploaded:
		// Artifact upload is handled by the data-plane HTTP pipeline directly.
		logger.Debug("[POLLING] ArtifactUploaded handled by upload pipeline")
		return nil

	default:
		logger.Warn("[POLLING] Unsupported message type: %s", msg.Type)
		return controltransport.ErrUnsupportedMessage
	}
}

// sendLeaseRenewal calls POST /api/v1/jobs/:id/lease via the API client.
func (t *PollingHTTPTransport) sendLeaseRenewal(ctx context.Context, msg controltransport.ControlMessage) error {
	jobID := getPayloadStr(msg.Payload, "job_id")
	leaseID := getPayloadStr(msg.Payload, "lease_id")
	attempt := int(getPayloadInt64(msg.Payload, "attempt"))
	expiresAt := getPayloadStr(msg.Payload, "lease_expires_at")

	if jobID == "" {
		return fmt.Errorf("polling transport: lease renewal missing job_id")
	}

	return t.client.RenewJobLeaseV2(ctx, jobID, t.workerID, leaseID, attempt, expiresAt)
}

// sendJobResult calls POST /api/v1/jobs/:id/result or /complete via the API client.
func (t *PollingHTTPTransport) sendJobResult(ctx context.Context, msg controltransport.ControlMessage) error {
	jobID := getPayloadStr(msg.Payload, "job_id")
	status := getPayloadStr(msg.Payload, "status")
	leaseID := getPayloadStr(msg.Payload, "lease_id")
	attempt := int(getPayloadInt64(msg.Payload, "attempt"))

	if jobID == "" {
		return fmt.Errorf("polling transport: job result missing job_id")
	}

	switch status {
	case "succeeded", "SUCCEEDED", "completed", "COMPLETED":
		return t.client.CompleteJobV2(ctx, jobID, t.workerID, leaseID, attempt)

	case "failed", "FAILED", "error", "ERROR":
		result := &api.JobResult{
			JobID:   jobID,
			Status:  status,
			LeaseID: leaseID,
			Attempt: attempt,
		}
		if errMsg := getPayloadStr(msg.Payload, "error"); errMsg != "" {
			result.Error = errMsg
		}
		return t.client.SubmitJobResultV2(ctx, jobID, result)

	default:
		// Fallback: submit result with the declared status.
		result := &api.JobResult{
			JobID:   jobID,
			Status:  status,
			LeaseID: leaseID,
			Attempt: attempt,
		}
		return t.client.SubmitJobResultV2(ctx, jobID, result)
	}
}

// Close gracefully terminates the polling transport.
// Stops the job poll ticker and closes all channels.
func (t *PollingHTTPTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()

	t.closeOnce.Do(func() {
		close(t.closeCh)
	})

	// Wait for pollLoop to exit.
	select {
	case <-t.recvDone:
	case <-time.After(5 * time.Second):
	}

	t.errCloseOnce.Do(func() {
		close(t.errCh)
	})

	logger.Info("[POLLING] Transport closed (worker=%s)", t.workerID)
	return nil
}


