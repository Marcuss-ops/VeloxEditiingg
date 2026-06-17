// Package grpcserver provides the master-side gRPC handler for the
// WorkerControl bidirectional stream service using generated protobuf types.
//
// The handler manages persistent worker streams, forwarding heartbeats,
// lease renewals, job claims, and commands between the gRPC stream and
// the existing HTTP-based control plane components (Registry, CommandManager,
// TransitionService).
package grpcserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handler implements pb.WorkerControlServer. It manages persistent worker
// streams and bridges gRPC messages to the existing control plane.
type Handler struct {
	pb.UnimplementedWorkerControlServer

	registry      *workersreg.Registry
	cmdMgr        *workersreg.CommandManager
	tokenMgr      *workersreg.TokenManager
	transitionSvc *queue.TransitionService
	dbStore       *store.SQLiteStore
	config        *HandlerConfig

	mu              sync.Mutex
	sessions        map[string]*workerSession // sessionID → active stream session
	workerSessions  map[string]string         // workerID → sessionID (for lookup)
}

// HandlerConfig holds configuration for the gRPC handler.
type HandlerConfig struct {
	ShadowMode  bool // Phase 4: notify workers, still claim via HTTP
	PushMode    bool // Phase 5+: send JobOffer directly, workers respond JobAccepted
	AllowInsecure bool // Dev-only: allow insecure gRPC connections (VELOX_GRPC_ALLOW_INSECURE_DEV)
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID     string
	sessionID    string
	stream       grpc.BidiStreamingServer[pb.TransportMessage, pb.TransportMessage]
	done         chan struct{}
	pendingOffer *queue.Job // JobOffer sent, awaiting JobAccepted/JobRejected
}

// NewHandler creates a new gRPC WorkerControl handler.
func NewHandler(
	registry *workersreg.Registry,
	cmdMgr *workersreg.CommandManager,
	tokenMgr *workersreg.TokenManager,
	transitionSvc *queue.TransitionService,
	dbStore *store.SQLiteStore,
	config *HandlerConfig,
) *Handler {
	if config == nil {
		config = &HandlerConfig{ShadowMode: true}
	}
	return &Handler{
		registry:       registry,
		cmdMgr:         cmdMgr,
		tokenMgr:       tokenMgr,
		transitionSvc:  transitionSvc,
		dbStore:        dbStore,
		config:         config,
		sessions:       make(map[string]*workerSession),
		workerSessions: make(map[string]string),
	}
}

// Stream handles a bidirectional gRPC stream from a single worker.
func (h *Handler) Stream(stream grpc.BidiStreamingServer[pb.TransportMessage, pb.TransportMessage]) error {
	// P0 security: extract worker identity from client certificate (mTLS).
	// The worker_id declared in the Hello message is validated against the cert.
	certWorkerID := h.extractWorkerIDFromStream(stream)

	// Wait for Hello message to identify the worker
	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("stream: recv hello: %w", err)
	}
	if msg.Type != string(controltransport.MsgHello) {
		return fmt.Errorf("stream: expected hello, got %s", msg.Type)
	}

	// P0 security: validate the declared worker_id against the client certificate.
	// In production (mTLS), the CN/SAN must match. In insecure dev mode, we trust the declared ID.
	declaredWorkerID := msg.WorkerId
	if certWorkerID != "" {
		// mTLS mode: validate that the declared ID matches the certificate
		if certWorkerID != declaredWorkerID {
			return fmt.Errorf("stream: worker_id mismatch: cert=%s, declared=%s", certWorkerID, declaredWorkerID)
		}
		log.Printf("[GRPC] Worker authenticated via mTLS: %s", certWorkerID)
	} else if !h.config.AllowInsecure {
		return fmt.Errorf("stream: insecure connections not allowed (set VELOX_GRPC_ALLOW_INSECURE_DEV=true for dev)")
	}

	// P0 security: validate credential_hash against stored worker credentials
	if err := h.validateCredentialHash(declaredWorkerID, msg); err != nil {
		return fmt.Errorf("stream: credential validation failed: %w", err)
	}

	workerID := declaredWorkerID
	sessionID := fmt.Sprintf("grpc-%s-%d", workerID, time.Now().UnixNano())

	// Register session — keyed by sessionID to prevent defer from deleting a newer session.
	// Close any existing session for this workerID before registering the new one.
	h.mu.Lock()
	h.closeOldSessionLocked(workerID)

	sess := &workerSession{
		workerID:  workerID,
		sessionID: sessionID,
		stream:    stream,
		done:      make(chan struct{}),
	}
	h.sessions[sessionID] = sess
	h.workerSessions[workerID] = sessionID
	h.mu.Unlock()

	log.Printf("[GRPC] Worker %s connected (session: %s)", workerID, sessionID)

	defer func() {
		h.mu.Lock()
		// Only delete this session if it's still the current one for this worker
		if currentSID, ok := h.workerSessions[workerID]; ok && currentSID == sessionID {
			delete(h.workerSessions, workerID)
		}
		delete(h.sessions, sessionID)
		h.mu.Unlock()
		close(sess.done)
		log.Printf("[GRPC] Worker %s disconnected (session: %s)", workerID, sessionID)
	}()

	// Send HelloAck
	ack := &pb.TransportMessage{
		MessageId:       fmt.Sprintf("ack-%s-%d", workerID, time.Now().UnixNano()),
		Type:            string(controltransport.MsgHelloAck),
		WorkerId:        workerID,
		SessionId:       sessionID,
		SequenceNumber:  1,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: msg.ProtocolVersion,
	}
	if err := stream.Send(ack); err != nil {
		return fmt.Errorf("stream: send hello_ack: %w", err)
	}

	// Dispatch any pending commands that arrived while worker was disconnected
	h.dispatchCommands(workerID, sess)

	// Start shadow/push mode job notifier
	var notifyCh chan struct{}
	var notifyStop context.CancelFunc
	if h.config.ShadowMode || h.config.PushMode {
		notifyCtx, cancel := context.WithCancel(context.Background())
		notifyStop = cancel
		notifyCh = make(chan struct{}, 1)
		go h.notifyJobsAvailable(notifyCtx, workerID, notifyCh, sess.done)
	}

	if notifyStop != nil {
		defer notifyStop()
	}

	// Main message loop
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream: recv: %w", err)
		}

		switch controltransport.ControlMessageType(msg.Type) {
		case controltransport.MsgHeartbeat:
			h.handleHeartbeat(workerID, msg)
			// Dispatch any pending commands on each heartbeat
			h.dispatchCommands(workerID, sess)
			if notifyCh != nil {
				select {
				case notifyCh <- struct{}{}:
				default:
				}
			}

		case controltransport.MsgLeaseRenewal:
			h.handleLeaseRenewal(workerID, msg)

		case controltransport.MsgJobAccepted:
			h.handleJobAccepted(workerID, msg)

		case controltransport.MsgJobRejected:
			h.handleJobRejected(workerID, msg)

		case controltransport.MsgJobProgress:
			h.handleJobProgress(workerID, msg)

		case controltransport.MsgCommandAck:
			h.handleCommandAck(workerID, msg)

		case controltransport.MsgJobResult:
			h.handleJobResult(workerID, msg)

		case controltransport.MsgGoodbye:
			return nil

		default:
			log.Printf("[GRPC] Unknown message type from worker %s: %s", workerID, msg.Type)
		}
	}
}

// notifyJobsAvailable checks for pending jobs and sends appropriate notifications.
// In ShadowMode: sends JobAvailable (worker claims via HTTP).
// In PushMode: does SQLite CAS claim and sends full JobOffer with lease_id.
func (h *Handler) notifyJobsAvailable(ctx context.Context, workerID string, trigger <-chan struct{}, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-trigger:
		case <-ticker.C:
		}

		if h.config.PushMode {
			h.sendPushJobOffer(ctx, workerID)
		} else if h.config.ShadowMode {
			h.sendJobAvailable(ctx, workerID)
		}
	}
}

// sendJobAvailable sends a lightweight JobAvailable notification (Shadow mode).
// The worker will claim the job via HTTP after receiving this signal.
func (h *Handler) sendJobAvailable(ctx context.Context, workerID string) {
	jobID, err := h.transitionSvc.GetNextJobID(ctx)
	if err != nil || jobID == "" {
		return
	}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"compatible_job_exists": true,
		"message":              "Job available for claim",
	})

	msg := &pb.TransportMessage{
		MessageId:       fmt.Sprintf("javail-%s-%d", workerID, time.Now().UnixNano()),
		Type:            string(controltransport.MsgJobAvailable),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Payload:         payload,
	}

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	_ = sess.stream.Send(msg)
}

// sendPushJobOffer does a real SQLite CAS claim and sends a full JobOffer
// with lease_id, job_id, job_type, and parameters (Push mode).
func (h *Handler) sendPushJobOffer(ctx context.Context, workerID string) {
	// P0 fix: real SQLite CAS claim — the master atomically claims the job
	job, err := h.transitionSvc.ClaimNextJob(ctx, workerID, nil)
	if err != nil {
		log.Printf("[GRPC] ClaimNextJob failed for worker %s: %v", workerID, err)
		return
	}
	if job == nil {
		return
	}

	payloadMap := map[string]interface{}{
		"job_id":       job.JobID,
		"run_id":       job.RunID,
		"video_name":   job.VideoName,
		"created_at":   job.CreatedAt,
		"lease_id":     job.LeaseID,
		"lease_expiry": job.LeaseExpiry,
		"attempt":      job.Attempt,
		"max_retries":  job.MaxRetries,
	}
	// Include the full job payload (scenes, clips, parameters) from the SQLite job
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := payloadMap[k]; !exists {
				payloadMap[k] = v
			}
		}
	}

	payload, _ := structpb.NewStruct(payloadMap)

	msg := &pb.TransportMessage{
		MessageId:       fmt.Sprintf("joboffer-%s-%s", workerID, job.JobID),
		Type:            string(controltransport.MsgJobOffer),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Payload:         payload,
	}

	sess := h.getSession(workerID)
	if sess == nil {
		log.Printf("[GRPC] Worker %s disconnected before JobOffer sent for %s", workerID, job.JobID)
		return
	}

	if err := sess.stream.Send(msg); err != nil {
		log.Printf("[GRPC] Failed to send JobOffer to worker %s for job %s: %v", workerID, job.JobID, err)
		return
	}

	// Store the offer so we can match JobAccepted
	h.mu.Lock()
	sess.pendingOffer = job
	h.mu.Unlock()
}

// handleHeartbeat processes a worker heartbeat received via gRPC stream.
func (h *Handler) handleHeartbeat(workerID string, msg *pb.TransportMessage) {
	payload := msg.Payload.AsMap()
	workerName := getPayloadString(payload, "worker_name")
	status := getPayloadString(payload, "status")
	currentJob := getPayloadString(payload, "current_job")

	extra := make(map[string]interface{})
	for k, v := range payload {
		extra[k] = v
	}

	if err := h.registry.Heartbeat(context.Background(), workerID, workerName, status, currentJob, extra); err != nil {
		log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
	}
}

// handleLeaseRenewal processes a lease renewal via gRPC stream.
func (h *Handler) handleLeaseRenewal(workerID string, msg *pb.TransportMessage) {
	payload := msg.Payload.AsMap()
	jobID := getPayloadString(payload, "job_id")
	leaseID := getPayloadString(payload, "lease_id")

	leaseExpiry := time.Now().UTC().Add(30 * time.Minute)
	if expiryStr := getPayloadString(payload, "lease_expires_at"); expiryStr != "" {
		if parsed, err := time.Parse(time.RFC3339, expiryStr); err == nil {
			leaseExpiry = parsed
		}
	}
	if err := h.transitionSvc.RenewLease(context.Background(), jobID, workerID, leaseID, leaseExpiry); err != nil {
		log.Printf("[GRPC] Lease renewal failed for job %s worker %s: %v", jobID, workerID, err)
	}
}

// handleJobAccepted processes JobAccepted — Phase 5+ real push mode.
// Confirms the lease in SQLite and sends JobLeaseGranted so the worker can begin.
func (h *Handler) handleJobAccepted(workerID string, msg *pb.TransportMessage) {
	if !h.config.PushMode {
		return
	}
	payload := msg.Payload.AsMap()
	jobID := getPayloadString(payload, "job_id")

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Confirm the lease in SQLite
	if jobID != "" {
		if err := h.transitionSvc.LeaseJob(context.Background(), jobID, workerID); err != nil {
			log.Printf("[GRPC] Job accepted lease failed for %s: %v", jobID, err)
			return
		}
	}

	// Send JobLeaseGranted — the worker must wait for this before executing
	grantedPayload, _ := structpb.NewStruct(map[string]interface{}{
		"job_id":   jobID,
		"worker_id": workerID,
		"status":   "granted",
	})

	grantedMsg := &pb.TransportMessage{
		MessageId:       fmt.Sprintf("leasegrant-%s-%s", workerID, jobID),
		Type:            string(controltransport.MsgJobLeaseGranted),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Payload:         grantedPayload,
	}

	if err := sess.stream.Send(grantedMsg); err != nil {
		log.Printf("[GRPC] Failed to send JobLeaseGranted to worker %s: %v", workerID, err)
		return
	}

	// Clear pending offer
	h.mu.Lock()
	if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	h.mu.Unlock()
}

// handleJobRejected processes JobRejected — releases the claimed job for requeue.
func (h *Handler) handleJobRejected(workerID string, msg *pb.TransportMessage) {
	payload := msg.Payload.AsMap()
	jobID := getPayloadString(payload, "job_id")
	reason := getPayloadString(payload, "reason")
	log.Printf("[GRPC] Worker %s rejected job %s: %s", workerID, jobID, reason)

	// Release the claimed job — fail it so it can be retried or requeued
	if jobID != "" {
		if err := h.transitionSvc.FailJob(context.Background(), jobID, reason, workerID, true, 3); err != nil {
			log.Printf("[GRPC] Failed to release rejected job %s: %v", jobID, err)
		}
	}

	// Clear pending offer (inline lookup to avoid double-lock with getSession)
	h.mu.Lock()
	sid, ok := h.workerSessions[workerID]
	var sess *workerSession
	if ok {
		sess = h.sessions[sid]
	}
	if sess != nil && sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	h.mu.Unlock()
}

// handleJobProgress tracks per-job progress (forwarded via heartbeat for now).
func (h *Handler) handleJobProgress(workerID string, msg *pb.TransportMessage) {
	// Progress updates are informational; the registry already tracks them
	// via the heartbeat's active_jobs payload.
}

// handleCommandAck processes command acknowledgments via gRPC stream.
func (h *Handler) handleCommandAck(workerID string, msg *pb.TransportMessage) {
	payload := msg.Payload.AsMap()
	commandID := getPayloadString(payload, "command_id")
	command := getPayloadString(payload, "command")

	if commandID != "" {
		if err := h.cmdMgr.AckCommandByID(commandID); err != nil {
			log.Printf("[GRPC] Command ACK failed for %s: %v", commandID, err)
		}
	} else if command != "" {
		h.cmdMgr.AckCommand(workerID, command)
	}
}

// handleJobResult processes job results via gRPC stream.
func (h *Handler) handleJobResult(workerID string, msg *pb.TransportMessage) {
	payload := msg.Payload.AsMap()
	jobID := getPayloadString(payload, "job_id")
	status := getPayloadString(payload, "status")
	errMsg := getPayloadString(payload, "error")

	if status == "success" {
		if err := h.transitionSvc.CompleteJob(context.Background(), jobID); err != nil {
			log.Printf("[GRPC] Job completion failed for %s: %v", jobID, err)
		}
	} else if status == "failed" {
		if err := h.transitionSvc.FailJob(context.Background(), jobID, errMsg, workerID, true, 3); err != nil {
			log.Printf("[GRPC] Job failure transition failed for %s: %v", jobID, err)
		}
	}
}

// StartGRPCServer starts a gRPC server on the configured port and registers
// the WorkerControl handler. Supports mTLS when certFile/keyFile/caFile are provided.
func StartGRPCServer(port int, handler *Handler, certFile, keyFile, caFile string) (*grpc.Server, net.Listener, error) {
	if port <= 0 {
		return nil, nil, nil // gRPC disabled
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc: listen on :%d: %w", port, err)
	}

	var grpcOpts []grpc.ServerOption

	if certFile != "" && keyFile != "" {
		// Load server certificate
		serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("grpc: load server cert/key: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			MinVersion:   tls.VersionTLS12,
		}

		// If CA file provided, enable mTLS (require and verify client certs)
		if caFile != "" {
			caPEM, err := os.ReadFile(caFile)
			if err != nil {
				return nil, nil, fmt.Errorf("grpc: read ca file: %w", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caPEM) {
				return nil, nil, fmt.Errorf("grpc: failed to parse CA certificate")
			}
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			tlsConfig.ClientCAs = certPool
			log.Printf("[GRPC] mTLS enabled — requiring client certificates signed by CA")
		}

		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		// No TLS files provided — check if insecure mode is allowed
		allowInsecure := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV") == "true"
		if !allowInsecure {
			return nil, nil, fmt.Errorf("grpc: TLS cert/key required in production (set VELOX_GRPC_ALLOW_INSECURE_DEV=true for dev)")
		}
		handler.config.AllowInsecure = true
		log.Printf("[GRPC] WARNING: insecure gRPC server — dev mode only")
		grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))
	}

	srv := grpc.NewServer(grpcOpts...)
	pb.RegisterWorkerControlServer(srv, handler)

	go func() {
		log.Printf("[GRPC] Velox master gRPC server listening on :%d", port)
		if err := srv.Serve(lis); err != nil {
			log.Printf("[GRPC] Server error: %v", err)
		}
	}()

	return srv, lis, nil
}

// dispatchCommands reads pending commands from SQLite for the worker,
// sends each on the gRPC stream, and marks them as delivered.
// Called on initial connect and on each heartbeat — the Stream() message
// loop goroutine is single-threaded so stream.Send() is safe.
func (h *Handler) dispatchCommands(workerID string, sess *workerSession) {
	cmds := h.cmdMgr.GetPendingCommandsAndMarkDelivered(workerID)
	if len(cmds) == 0 {
		return
	}

	log.Printf("[GRPC] Dispatching %d pending commands to worker %s", len(cmds), workerID)

	for _, cmd := range cmds {
		payload, _ := structpb.NewStruct(map[string]interface{}{
			"command_id": cmd.CommandID,
			"command":    cmd.Command,
			"timestamp":  cmd.Timestamp,
			"params":     cmd.Params,
		})

		msg := &pb.TransportMessage{
			MessageId:       fmt.Sprintf("cmd-%s-%s", workerID, cmd.CommandID),
			Type:            string(controltransport.MsgCommand),
			WorkerId:        workerID,
			SessionId:       sess.sessionID,
			SentAt:          timestamppb.Now(),
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			Payload:         payload,
		}

		if err := sess.stream.Send(msg); err != nil {
			log.Printf("[GRPC] Failed to send command %s to worker %s: %v", cmd.CommandID, workerID, err)
			return
		}
	}
}

// closeOldSessionLocked removes any existing session for the given workerID.
// The old Stream() goroutine will return naturally when it detects the session
// was removed (next Recv/operation will fail or the client disconnects).
// Must be called with h.mu held.
func (h *Handler) closeOldSessionLocked(workerID string) {
	oldSID, ok := h.workerSessions[workerID]
	if !ok {
		return
	}
	if _, exists := h.sessions[oldSID]; exists {
		log.Printf("[GRPC] Worker %s reconnecting — removing old session %s", workerID, oldSID)
	}
	delete(h.sessions, oldSID)
	delete(h.workerSessions, workerID)
	// Don't close oldSess.done — the old defer handles it when Stream() returns.
	// Don't call stream methods — BidiStreamingServer has no CloseSend on server side.
}

// getSession returns the active session for a workerID, or nil if none.
// Thread-safe: acquires h.mu.
func (h *Handler) getSession(workerID string) *workerSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	sid, ok := h.workerSessions[workerID]
	if !ok {
		return nil
	}
	return h.sessions[sid]
}

// ---- Security Helpers ----

// extractWorkerIDFromStream extracts the worker identity from the client TLS certificate.
// Returns the Common Name (CN) from the client cert, or empty string if no cert is present
// (insecure mode). In production mTLS mode, this must return a non-empty worker ID.
func (h *Handler) extractWorkerIDFromStream(stream grpc.ServerStream) string {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return ""
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return ""
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	cn := clientCert.Subject.CommonName
	if cn == "" {
		// Fallback: try the first DNS SAN
		if len(clientCert.DNSNames) > 0 {
			cn = clientCert.DNSNames[0]
		}
	}

	return strings.TrimSpace(cn)
}

// validateCredentialHash checks the worker's credential_hash against the
// stored persistent credential in SQLite (worker_credentials table).
// Missing credentials result in rejection in production mode.
func (h *Handler) validateCredentialHash(workerID string, msg *pb.TransportMessage) error {
	payload := msg.Payload.AsMap()
	declaredHash, _ := payload["credential_hash"].(string)

	// Check if this worker has a stored credential
	hasCred, err := h.dbStore.HasWorkerCredential(workerID)
	if err != nil {
		log.Printf("[GRPC] Credential lookup failed for worker %s: %v", workerID, err)
		// Don't fail — allow fallback to insecure dev mode
		if h.config.AllowInsecure {
			return nil
		}
		return fmt.Errorf("credential lookup failed for %s", workerID)
	}

	if !hasCred {
		// First registration: store the credential if one is provided
		if declaredHash != "" {
			if err := h.dbStore.SetWorkerCredential(workerID, declaredHash); err != nil {
				return fmt.Errorf("store initial credential: %w", err)
			}
			log.Printf("[GRPC] Worker %s: initial credential stored", workerID)
			return nil
		}
		// No credential provided — allow in dev mode, reject in production
		if h.config.AllowInsecure {
			log.Printf("[GRPC] Worker %s: no credential — allowing in insecure dev mode", workerID)
			return nil
		}
		return fmt.Errorf("worker %s: credential required", workerID)
	}

	// Stored credential exists — validate the declared hash
	if declaredHash == "" {
		return fmt.Errorf("worker %s: credential required (existing credential stored)", workerID)
	}

	valid, err := h.dbStore.ValidateWorkerCredential(workerID, declaredHash)
	if err != nil {
		return fmt.Errorf("validate credential: %w", err)
	}
	if !valid {
		return fmt.Errorf("worker %s: credential hash mismatch", workerID)
	}

	return nil
}

// ---- Helpers ----

func getPayloadString(payload map[string]interface{}, key string) string {
	if v, ok := payload[key].(string); ok {
		return v
	}
	return ""
}
