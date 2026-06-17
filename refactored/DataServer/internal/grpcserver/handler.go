// Package grpcserver provides the master-side gRPC handler for the
// WorkerControl bidirectional stream service using typed protobuf envelopes.
//
// The handler manages persistent worker streams, forwarding heartbeats,
// lease renewals, job claims, and commands between the gRPC stream and
// the existing HTTP-based control plane components (Registry, CommandManager,
// TransitionService).
//
// Phase 2 (typed protobuf): uses WorkerToMasterEnvelope / MasterToWorkerEnvelope
// with typed oneof messages instead of TransportMessage { string type; Struct payload }.
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

	mu             sync.Mutex
	sessions       map[string]*workerSession // sessionID → active stream session
	workerSessions map[string]string         // workerID → sessionID (for lookup)
}

// HandlerConfig holds configuration for the gRPC handler.
type HandlerConfig struct {
	ShadowMode    bool // Phase 4: notify workers, still claim via HTTP
	PushMode      bool // Phase 5+: send JobOffer directly, workers respond JobAccepted
	AllowInsecure bool // Dev-only: allow insecure gRPC connections (VELOX_GRPC_ALLOW_INSECURE_DEV)
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID     string
	sessionID    string
	stream       grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	done         chan struct{}
	pendingOffer *queue.Job    // JobOffer sent, awaiting JobAccepted/JobRejected
	sendMu       sync.Mutex    // serializes stream.Send() across goroutines (notifier + main loop)
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
// Receives WorkerToMasterEnvelope, sends MasterToWorkerEnvelope.
func (h *Handler) Stream(stream grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]) error {
	// P0 security: extract worker identity from client certificate (mTLS).
	certWorkerID := h.extractWorkerIDFromStream(stream)

	// Wait for Hello message to identify the worker
	env, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("stream: recv hello: %w", err)
	}

	hello := env.GetHello()
	if hello == nil {
		return fmt.Errorf("stream: expected hello, got %T", env.Msg)
	}

	// P0 security: validate the declared worker_id against the client certificate.
	declaredWorkerID := env.WorkerId
	if certWorkerID != "" {
		if certWorkerID != declaredWorkerID {
			return fmt.Errorf("stream: worker_id mismatch: cert=%s, declared=%s", certWorkerID, declaredWorkerID)
		}
		log.Printf("[GRPC] Worker authenticated via mTLS: %s", certWorkerID)
	} else if !h.config.AllowInsecure {
		return fmt.Errorf("stream: insecure connections not allowed (set VELOX_GRPC_ALLOW_INSECURE_DEV=true for dev)")
	}

	// P0 security: validate credential_hash against stored worker credentials
	if err := h.validateCredentialHash(declaredWorkerID, hello.GetCredentialHash()); err != nil {
		return fmt.Errorf("stream: credential validation failed: %w", err)
	}

	workerID := declaredWorkerID
	sessionID := fmt.Sprintf("grpc-%s-%d", workerID, time.Now().UnixNano())

	// Register session — keyed by sessionID to prevent defer from deleting a newer session.
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

	log.Printf("[GRPC] Worker %s connected (session: %s, name: %s)", workerID, sessionID, hello.GetWorkerName())

	defer func() {
		h.mu.Lock()
		if currentSID, ok := h.workerSessions[workerID]; ok && currentSID == sessionID {
			delete(h.workerSessions, workerID)
		}
		delete(h.sessions, sessionID)
		h.mu.Unlock()
		close(sess.done)
		log.Printf("[GRPC] Worker %s disconnected (session: %s)", workerID, sessionID)
	}()

	// Send typed HelloAck
	ack := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("ack-%s-%d", workerID, time.Now().UnixNano()),
		WorkerId:        workerID,
		SessionId:       sessionID,
		SequenceNumber:  1,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: env.ProtocolVersion,
		Msg:             &pb.MasterToWorkerEnvelope_HelloAck{HelloAck: &pb.HelloAck{}},
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

	// Main message loop — type-switch on the oneof Msg field
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream: recv: %w", err)
		}

		// P0: drop messages from stale sessions (zombie connections after reconnect).
		if !h.isCurrentSession(workerID, sessionID) {
			continue
		}

		switch m := env.Msg.(type) {
		case *pb.WorkerToMasterEnvelope_Heartbeat:
			h.handleHeartbeat(workerID, m.Heartbeat)
			h.dispatchCommands(workerID, sess)
			if notifyCh != nil {
				select {
				case notifyCh <- struct{}{}:
				default:
				}
			}

		case *pb.WorkerToMasterEnvelope_LeaseRenewal:
			h.handleLeaseRenewal(workerID, m.LeaseRenewal)

		case *pb.WorkerToMasterEnvelope_JobAccepted:
			h.handleJobAccepted(workerID, m.JobAccepted)

		case *pb.WorkerToMasterEnvelope_JobRejected:
			h.handleJobRejected(workerID, m.JobRejected)

		case *pb.WorkerToMasterEnvelope_JobProgress:
			h.handleJobProgress(workerID, m.JobProgress)

		case *pb.WorkerToMasterEnvelope_CommandAck:
			h.handleCommandAck(workerID, m.CommandAck)

		case *pb.WorkerToMasterEnvelope_JobResult:
			h.handleJobResult(workerID, m.JobResult)

		case *pb.WorkerToMasterEnvelope_ArtifactUploaded:
			h.handleArtifactUploaded(workerID, m.ArtifactUploaded)

		case *pb.WorkerToMasterEnvelope_Goodbye:
			return nil

		default:
			log.Printf("[GRPC] Unknown message type from worker %s: %T", workerID, env.Msg)
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

// sendJobAvailable sends a typed JobAvailable notification (Shadow mode).
func (h *Handler) sendJobAvailable(ctx context.Context, workerID string) {
	jobID, err := h.transitionSvc.GetNextJobID(ctx)
	if err != nil || jobID == "" {
		return
	}

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("javail-%s-%d", workerID, time.Now().UnixNano()),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_JobAvailable{
			JobAvailable: &pb.JobAvailable{
				CompatibleJobExists: true,
				Message:             "Job available for claim",
			},
		},
	}

	sess.sendMu.Lock()
	_ = sess.stream.Send(env)
	sess.sendMu.Unlock()
}

// sendPushJobOffer does a real SQLite CAS claim and sends a typed JobOffer (Push mode).
func (h *Handler) sendPushJobOffer(ctx context.Context, workerID string) {
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// P0: prevent multiple concurrent offers — only one claim at a time.
	// If a previous offer is still pending (not yet accepted/rejected), wait for it.
	h.mu.Lock()
	hasPending := sess.pendingOffer != nil
	h.mu.Unlock()
	if hasPending {
		return
	}

	job, err := h.transitionSvc.ClaimNextJob(ctx, workerID, nil)
	if err != nil {
		log.Printf("[GRPC] ClaimNextJob failed for worker %s: %v", workerID, err)
		return
	}
	if job == nil {
		return
	}

	// Build job payload struct from job.Payload map
	var jobPayload *structpb.Struct
	if job.Payload != nil {
		jobPayload, _ = structpb.NewStruct(job.Payload)
	}

	// Parse CreatedAt timestamp if present
	var createdAt *timestamppb.Timestamp
	if job.CreatedAt != nil {
		switch t := job.CreatedAt.(type) {
		case time.Time:
			createdAt = timestamppb.New(t)
		case string:
			if parsed, err := time.Parse(time.RFC3339, t); err == nil {
				createdAt = timestamppb.New(parsed)
			}
		}
	}

	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("joboffer-%s-%s", workerID, job.JobID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_JobOffer{
			JobOffer: &pb.JobOffer{
				JobId:      job.JobID,
				RunId:      job.RunID,
				VideoName:  job.VideoName,
				CreatedAt:  createdAt,
				LeaseId:    job.LeaseID,
				Attempt:    int32(job.Attempt),
				MaxRetries: int32(job.MaxRetries),
				JobPayload: jobPayload,
			},
		},
	}

	// P0: serialize stream.Send() — notifier goroutine shares the stream with the main loop.
	sess.sendMu.Lock()
	sendErr := sess.stream.Send(env)
	sess.sendMu.Unlock()

	if sendErr != nil {
		log.Printf("[GRPC] Failed to send JobOffer to worker %s for job %s: %v", workerID, job.JobID, sendErr)
		// P0: rollback the claim — release the job so it can be retried by another worker.
		// Don't increment retry count since the worker never received the job.
		if releaseErr := h.transitionSvc.ReleaseClaim(ctx, job.JobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for job %s after send failure: %v", job.JobID, releaseErr)
		}
		return
	}

	// Store the offer so we can match JobAccepted
	h.mu.Lock()
	sess.pendingOffer = job
	h.mu.Unlock()
}

// handleHeartbeat processes a typed Heartbeat received via gRPC stream.
func (h *Handler) handleHeartbeat(workerID string, hb *pb.Heartbeat) {
	extra := make(map[string]interface{})
	// Populate extra from typed fields for backward compat with registry
	extra["worker_name"] = hb.GetWorkerName()
	extra["worker_status"] = hb.GetWorkerStatus()
	extra["status"] = hb.GetStatus()
	extra["current_job"] = hb.GetCurrentJob()
	extra["code_version"] = hb.GetCodeVersion()
	extra["bundle_version"] = hb.GetBundleVersion()
	extra["bundle_hash"] = hb.GetBundleHash()
	extra["protocol_version"] = hb.GetProtocolVersion()
	extra["engine_version"] = hb.GetEngineVersion()
	extra["jobs_completed"] = hb.GetJobsCompleted()
	extra["jobs_failed"] = hb.GetJobsFailed()
	extra["active_jobs_count"] = hb.GetActiveJobsCount()

	if hb.GetExtra() != nil {
		for k, v := range hb.GetExtra().AsMap() {
			extra[k] = v
		}
	}

	if err := h.registry.Heartbeat(context.Background(), workerID, hb.GetWorkerName(), hb.GetStatus(), hb.GetCurrentJob(), extra); err != nil {
		log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
	}
}

// handleLeaseRenewal processes a typed LeaseRenewal via gRPC stream.
func (h *Handler) handleLeaseRenewal(workerID string, lr *pb.LeaseRenewal) {
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute)
	if lr.GetLeaseExpiresAt() != nil {
		leaseExpiry = lr.GetLeaseExpiresAt().AsTime()
	}
	if err := h.transitionSvc.RenewLease(context.Background(), lr.GetJobId(), workerID, lr.GetLeaseId(), leaseExpiry); err != nil {
		log.Printf("[GRPC] Lease renewal failed for job %s worker %s: %v", lr.GetJobId(), workerID, err)
	}
}

// handleJobAccepted processes typed JobAccepted — Phase 5+ real push mode.
// The lease was already created by ClaimNextJob; we just verify and grant.
func (h *Handler) handleJobAccepted(workerID string, ja *pb.JobAccepted) {
	if !h.config.PushMode {
		return
	}
	jobID := ja.GetJobId()

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// P0 fix: don't call LeaseJob() — ClaimNextJob already created the lease.
	// Calling LeaseJob again would generate a NEW lease_id and increment retry count.
	// The worker's JobAccepted includes its lease_id from the JobOffer, which must match.
	h.mu.Lock()
	offer := sess.pendingOffer
	h.mu.Unlock()

	if offer == nil || offer.JobID != jobID {
		log.Printf("[GRPC] Worker %s accepted job %s but no matching pending offer", workerID, jobID)
		return
	}

	// Verify the lease_id the worker is accepting matches what we offered.
	// If they don't match, the worker may be responding to a stale offer.
	declaredLeaseID := ja.GetLeaseId()
	if declaredLeaseID != "" && declaredLeaseID != offer.LeaseID {
		log.Printf("[GRPC] Worker %s accepted job %s with mismatched lease_id: got %s, want %s",
			workerID, jobID, declaredLeaseID, offer.LeaseID)
		return
	}

	// Send typed JobLeaseGranted — confirms the lease created by ClaimNextJob.
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("leasegrant-%s-%s", workerID, jobID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_JobLeaseGranted{
			JobLeaseGranted: &pb.JobLeaseGranted{
				JobId:    jobID,
				WorkerId: workerID,
				Status:   "granted",
			},
		},
	}

	sess.sendMu.Lock()
	sendErr := sess.stream.Send(env)
	sess.sendMu.Unlock()

	if sendErr != nil {
		log.Printf("[GRPC] Failed to send JobLeaseGranted to worker %s: %v", workerID, sendErr)
		// Don't clear pendingOffer — let the notifier retry or the lease expire.
		return
	}

	// Clear pending offer — job is now running on this worker.
	h.mu.Lock()
	if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	h.mu.Unlock()
}

// handleJobRejected processes typed JobRejected — releases the claimed job for requeue.
func (h *Handler) handleJobRejected(workerID string, jr *pb.JobRejected) {
	jobID := jr.GetJobId()
	reason := jr.GetReason()
	log.Printf("[GRPC] Worker %s rejected job %s: %s", workerID, jobID, reason)

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

// handleJobProgress tracks per-job progress (typed, forwarded via heartbeat for now).
func (h *Handler) handleJobProgress(workerID string, jp *pb.JobProgress) {
	// Progress updates are informational; the registry already tracks them
	// via the heartbeat's active_jobs payload.
	log.Printf("[GRPC] Worker %s progress on job %s: stage=%s %d%% (scene %d/%d)",
		workerID, jp.GetJobId(), jp.GetStage(), jp.GetProgressPercent(), jp.GetScene(), jp.GetTotalScenes())
}

// handleCommandAck processes typed CommandAck via gRPC stream.
func (h *Handler) handleCommandAck(workerID string, ca *pb.CommandAck) {
	if ca.GetCommandId() != "" {
		if err := h.cmdMgr.AckCommandByID(workerID, ca.GetCommandId()); err != nil {
			log.Printf("[GRPC] Command ACK failed for %s (worker %s): %v", ca.GetCommandId(), workerID, err)
		}
	} else if ca.GetCommand() != "" {
		h.cmdMgr.AckCommand(workerID, ca.GetCommand())
	}
}

// handleJobResult processes typed JobResult via gRPC stream.
func (h *Handler) handleJobResult(workerID string, jr *pb.JobResult) {
	jobID := jr.GetJobId()
	status := jr.GetStatus()
	errMsg := jr.GetError()

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

// handleArtifactUploaded processes typed ArtifactUploaded via gRPC stream.
func (h *Handler) handleArtifactUploaded(workerID string, a *pb.ArtifactUploaded) {
	log.Printf("[GRPC] Worker %s uploaded artifact %s (type: %s, size: %d bytes, status: %s)",
		workerID, a.GetArtifactId(), a.GetArtifactType(), a.GetArtifactSize(), a.GetUploadStatus())

	if a.GetJobId() == "" || a.GetArtifactId() == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s missing job_id or artifact_id — skipping DB update", workerID)
		return
	}

	if err := h.dbStore.UpdateJobSupplementary(a.GetJobId(), map[string]interface{}{
		"artifact_id": a.GetArtifactId(),
	}); err != nil {
		log.Printf("[GRPC] Failed to update job %s with artifact %s: %v", a.GetJobId(), a.GetArtifactId(), err)
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
		serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("grpc: load server cert/key: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			MinVersion:   tls.VersionTLS12,
		}

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
// sends each as a typed Command on the gRPC stream, and marks them as delivered.
func (h *Handler) dispatchCommands(workerID string, sess *workerSession) {
	cmds := h.cmdMgr.GetPendingCommandsAndMarkDelivered(workerID)
	if len(cmds) == 0 {
		return
	}

	log.Printf("[GRPC] Dispatching %d pending commands to worker %s", len(cmds), workerID)

	// P0: lock once for the batch — single goroutine (Stream() main loop) sends commands.
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()

	for _, cmd := range cmds {
		var params *structpb.Struct
		if cmd.Params != nil {
			params, _ = structpb.NewStruct(cmd.Params)
		}

		ts, err := time.Parse(time.RFC3339, cmd.Timestamp)
		if err != nil {
			ts = time.Now().UTC()
		}

		env := &pb.MasterToWorkerEnvelope{
			MessageId:       fmt.Sprintf("cmd-%s-%s", workerID, cmd.CommandID),
			WorkerId:        workerID,
			SessionId:       sess.sessionID,
			SentAt:          timestamppb.Now(),
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			Msg: &pb.MasterToWorkerEnvelope_Command{
				Command: &pb.Command{
					CommandId: cmd.CommandID,
					Command:   cmd.Command,
					Timestamp: timestamppb.New(ts),
					Params:    params,
				},
			},
		}

		if err := sess.stream.Send(env); err != nil {
			log.Printf("[GRPC] Failed to send command %s to worker %s: %v", cmd.CommandID, workerID, err)
			return
		}
	}
}

// closeOldSessionLocked removes any existing session for the given workerID.
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
	// Messages from the old session are dropped by isCurrentSession() check in the main loop.
}

// isCurrentSession returns true if the given sessionID is still the active
// session for workerID. Used to drop messages from stale/zombie connections.
func (h *Handler) isCurrentSession(workerID, sessionID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	sid, ok := h.workerSessions[workerID]
	return ok && sid == sessionID
}

// getSession returns the active session for a workerID, or nil if none.
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
		if len(clientCert.DNSNames) > 0 {
			cn = clientCert.DNSNames[0]
		}
	}

	return strings.TrimSpace(cn)
}

// validateCredentialHash checks the worker's credential_hash against the
// stored persistent credential in SQLite (worker_credentials table).
// Accepts the credential hash string directly from typed Hello message.
func (h *Handler) validateCredentialHash(workerID string, declaredHash string) error {
	// Check if this worker has a stored credential
	hasCred, err := h.dbStore.HasWorkerCredential(workerID)
	if err != nil {
		log.Printf("[GRPC] Credential lookup failed for worker %s: %v", workerID, err)
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
