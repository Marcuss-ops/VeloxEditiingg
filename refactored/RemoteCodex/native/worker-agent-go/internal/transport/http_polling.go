// Package transport implements ControlTransport for worker↔master communication.
// http_polling.go provides a polling-based HTTP transport that satisfies the
// ControlTransport interface by wrapping the existing api.Client.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/pkg/api"
)

// transportState tracks the internal connection state of the transport.
type transportState int

const (
	stateDisconnected transportState = iota
	stateConnecting
	stateReady
)

// PollingHTTPTransport implements ControlTransport via HTTP polling.
// It wraps the existing api.Client and maps ControlMessage types to the
// corresponding HTTP API calls. In Phase 2-5, job and command polling
// loops run inside Receive(), pushing messages onto the returned channel.
// In Phase 6, when DisablePolling is true, no polling goroutines start.
type PollingHTTPTransport struct {
	client   *api.Client
	workerID string

	mu         sync.Mutex
	sessionID  string
	state      transportState
	closed     bool
	closeCh    chan struct{}
	recvCh     chan controltransport.ControlMessage
	errCh      chan error
	msgSeq     int64
	pollCtx    context.Context
	pollCancel context.CancelFunc
	recvOnce   sync.Once

	// DisablePolling: when true, Receive() returns a channel but starts
	// no background polling goroutines. All master→worker messages arrive
	// via gRPC stream instead (Phase 6).
	DisablePolling bool
}

// NewPollingHTTPTransport creates a new polling-based HTTP transport.
func NewPollingHTTPTransport(client *api.Client, workerID string) *PollingHTTPTransport {
	return &PollingHTTPTransport{
		client:  client,
		workerID: workerID,
		state:   stateDisconnected,
		closeCh: make(chan struct{}),
	}
}

// Connect registers the worker with the master via HTTP.
// The hello message is converted to an api.WorkerInfo struct and sent
// to the /api/workers/register endpoint. The auth token is extracted
// and stored in the API client.
func (t *PollingHTTPTransport) Connect(ctx context.Context, hello controltransport.WorkerHello) error {
	t.mu.Lock()
	t.state = stateConnecting
	t.mu.Unlock()

	info := &api.WorkerInfo{
		WorkerID:        hello.WorkerID,
		WorkerName:      hello.WorkerName,
		Hostname:        hello.Hostname,
		Version:         hello.Version,
		BundleVersion:   hello.BundleVersion,
		BundleHash:      hello.BundleHash,
		ProtocolVersion: hello.ProtocolVersion,
		EngineVersion:   hello.EngineVersion,
		Capabilities:    hello.Capabilities,
		Credential:      hello.CredentialHash,
	}

	if err := t.client.RegisterWorker(ctx, info); err != nil {
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("polling transport connect: %w", err)
	}

	t.mu.Lock()
	t.sessionID = fmt.Sprintf("poll-%s-%d", hello.WorkerID, time.Now().UnixNano())
	t.state = stateReady
	t.mu.Unlock()

	return nil
}

// Receive returns the message channel and an error channel (HTTP poll fatal errors).
// The message channel is closed when the transport is closed.
func (t *PollingHTTPTransport) Receive(ctx context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, controltransport.ErrTransportClosed
	}
	t.mu.Unlock()

	t.recvOnce.Do(func() {
		t.recvCh = make(chan controltransport.ControlMessage, 64)
		t.errCh = make(chan error, 1)
		t.pollCtx, t.pollCancel = context.WithCancel(ctx)
		if !t.DisablePolling {
			go t.pollJobsLoop(t.pollCtx)
			go t.pollCommandsLoop(t.pollCtx)
		}
	})

	return t.recvCh, t.errCh, nil
}

// pollJobsLoop polls the master for available jobs every 5 seconds.
// When a job is received, it is wrapped in a MsgJobOffer and pushed to recvCh.
func (t *PollingHTTPTransport) pollJobsLoop(ctx context.Context) {
	const pollInterval = 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.closeCh:
			return
		case <-ticker.C:
			t.mu.Lock()
			if t.closed || t.state != stateReady {
				t.mu.Unlock()
				continue
			}
			t.mu.Unlock()

			job, err := t.client.GetJobV2(ctx, t.workerID)
			if err != nil {
				continue
			}
			if job == nil {
				continue
			}

			// Wrap job data as payload
			payload := map[string]interface{}{
				"job_id":           job.JobID,
				"job_run_id":       job.JobRunID,
				"job_type":         job.JobType,
				"priority":         job.Priority,
				"parameters":       job.Parameters,
				"created_at":       job.CreatedAt,
				"timeout_secs":     job.TimeoutSecs,
				"contract_version": job.ContractVersion,
				"lease_id":         job.LeaseID,
				"lease_expiry":     job.LeaseExpiry,
				"attempt":          job.Attempt,
			}

			msg := t.newMessage(controltransport.MsgJobOffer, payload)
			select {
			case t.recvCh <- msg:
			case <-ctx.Done():
				return
			case <-t.closeCh:
				return
			}
		}
	}
}

// pollCommandsLoop polls the master for pending commands every 30 seconds.
// When commands are received, each is wrapped in a MsgCommand and pushed to recvCh.
func (t *PollingHTTPTransport) pollCommandsLoop(ctx context.Context) {
	const pollInterval = 30 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.closeCh:
			return
		case <-ticker.C:
			t.mu.Lock()
			if t.closed {
				t.mu.Unlock()
				continue
			}
			t.mu.Unlock()

			commands, err := t.client.GetCommands(ctx, t.workerID)
			if err != nil {
				continue
			}

			for _, cmd := range commands {
				payload := map[string]interface{}{
					"command_id": cmd.CommandID,
					"command":    cmd.Command,
					"timestamp":  cmd.Timestamp,
				}
				if cmd.Payload != nil {
					payload["params"] = cmd.Payload
				}

				msg := t.newMessage(controltransport.MsgCommand, payload)
				select {
				case t.recvCh <- msg:
				case <-ctx.Done():
					return
				case <-t.closeCh:
					return
				}
			}
		}
	}
}

// Send transmits a ControlMessage to the master by mapping its type to the
// appropriate HTTP API call.
func (t *PollingHTTPTransport) Send(ctx context.Context, msg controltransport.ControlMessage) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return controltransport.ErrTransportClosed
	}
	t.mu.Unlock()

	switch msg.Type {
	case controltransport.MsgHeartbeat:
		return t.sendHeartbeat(ctx, msg)
	case controltransport.MsgLeaseRenewal:
		return t.sendLeaseRenewal(ctx, msg)
	case controltransport.MsgJobResult:
		return t.sendJobResult(ctx, msg)
	case controltransport.MsgCommandAck:
		return t.sendCommandAck(ctx, msg)
	case controltransport.MsgJobProgress:
		return t.sendJobProgress(ctx, msg)
	case controltransport.MsgGoodbye:
		return t.sendGoodbye(ctx, msg)
	case controltransport.MsgArtifactUploaded:
		// Artifact upload is handled via separate HTTP multipart upload;
		// this message is informational only.
		return nil
	default:
		return fmt.Errorf("polling transport: unsupported send type %s", msg.Type)
	}
}

// Close gracefully terminates the transport. Sends a Goodbye/unregister
// and closes internal channels.
func (t *PollingHTTPTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.state = stateDisconnected
	t.mu.Unlock()

	// Signal polling goroutines to stop
	close(t.closeCh)
	if t.pollCancel != nil {
		t.pollCancel()
	}

	// Unregister from master
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = t.client.UnregisterWorker(ctx, t.workerID)

	// Close receive channel safely: all polling goroutines are signaled
	// via closeCh and pollCancel, so no one will send on recvCh anymore.
	if t.recvCh != nil {
		close(t.recvCh)
	}
	// Close error channel
	if t.errCh != nil {
		close(t.errCh)
	}

	return nil
}

// --- Internal helpers ---

func (t *PollingHTTPTransport) newMessage(msgType controltransport.ControlMessageType, payload map[string]interface{}) controltransport.ControlMessage {
	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	sid := t.sessionID
	t.mu.Unlock()

	m := controltransport.NewMessageWithPayload(msgType, t.workerID, controltransport.ProtocolVersionCurrent, payload)
	m = m.WithSession(sid)
	m = m.WithSequence(seq)
	return m
}

func (t *PollingHTTPTransport) sendHeartbeat(ctx context.Context, msg controltransport.ControlMessage) error {
	payload := &api.HeartbeatPayload{
		WorkerID:        t.workerID,
		WorkerName:      getString(msg.Payload, "worker_name"),
		Status:          getString(msg.Payload, "status"),
		JobID:           getString(msg.Payload, "current_job"),
		CurrentJob:      getString(msg.Payload, "current_job"),
		CodeVersion:     getString(msg.Payload, "code_version"),
		BundleVersion:   getString(msg.Payload, "bundle_version"),
		BundleHash:      getString(msg.Payload, "bundle_hash"),
		ProtocolVersion: getString(msg.Payload, "protocol_version"),
		EngineVersion:   getString(msg.Payload, "engine_version"),
		Extra:           msg.Payload,
	}
	return t.client.SendHeartbeat(ctx, payload)
}

func (t *PollingHTTPTransport) sendLeaseRenewal(ctx context.Context, msg controltransport.ControlMessage) error {
	jobID := getString(msg.Payload, "job_id")
	leaseID := getString(msg.Payload, "lease_id")
	attempt := getInt(msg.Payload, "attempt")
	leaseExpiry := getString(msg.Payload, "lease_expires_at")

	return t.client.RenewJobLeaseV2(ctx, jobID, t.workerID, leaseID, attempt, leaseExpiry)
}

func (t *PollingHTTPTransport) sendJobResult(ctx context.Context, msg controltransport.ControlMessage) error {
	result := &api.JobResult{
		JobID:           getString(msg.Payload, "job_id"),
		JobRunID:        getString(msg.Payload, "job_run_id"),
		WorkerID:        t.workerID,
		Status:          getString(msg.Payload, "status"),
		Error:           getString(msg.Payload, "error"),
		StartTime:       getString(msg.Payload, "start_time"),
		EndTime:         getString(msg.Payload, "end_time"),
		ContractVersion: api.ContractVersionV2,
		LeaseID:         getString(msg.Payload, "lease_id"),
		Attempt:         getInt(msg.Payload, "attempt"),
	}

	// Submit the result
	if err := t.client.SubmitJobResultV2(ctx, result.JobID, result); err != nil {
		return err
	}

	// Send completion notification
	return t.client.CompleteJobV2(ctx, result.JobID, t.workerID, result.LeaseID, result.Attempt)
}

func (t *PollingHTTPTransport) sendCommandAck(ctx context.Context, msg controltransport.ControlMessage) error {
	commandID := getString(msg.Payload, "command_id")
	command := getString(msg.Payload, "command")

	if commandID != "" {
		return t.client.AckCommandByID(ctx, t.workerID, commandID)
	}
	// Legacy fallback
	return t.client.AckCommand(ctx, t.workerID, command)
}

func (t *PollingHTTPTransport) sendJobProgress(ctx context.Context, msg controltransport.ControlMessage) error {
	// Job progress is reported within heartbeat; this is a no-op for HTTP transport.
	// In gRPC mode, this would send a dedicated progress update.
	return nil
}

func (t *PollingHTTPTransport) sendGoodbye(ctx context.Context, msg controltransport.ControlMessage) error {
	return t.client.UnregisterWorker(ctx, t.workerID)
}

// --- JSON payload extraction helpers ---

func getString(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func getInt(m map[string]interface{}, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}
