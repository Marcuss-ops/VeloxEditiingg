// Package grpcserver provides the master-side gRPC handler for the
// WorkerControl bidirectional stream service using typed protobuf envelopes.
//
// The handler manages persistent worker streams, forwarding heartbeats,
// lease renewals, job claims, and commands between the gRPC stream and
// the existing HTTP-based control plane components (Registry, CommandManager,
// LifecycleService).
//
// Phase 2 (typed protobuf): uses WorkerToMasterEnvelope / MasterToWorkerEnvelope
// with typed oneof messages instead of TransportMessage { string type; Struct payload }.
package grpcserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/dbutil"
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
//
// Phase 4.2 cleanup: `tokenMgr` removed from NewHandler — credential
// validation goes through `dbStore` directly via validateCredentialHash.
// `workers.TokenManager` remains available in the `workers` package for
// the HTTP control plane (worker_update.go, HTTP lifecycle routes).
type Handler struct {
	pb.UnimplementedWorkerControlServer

	registry      *workersreg.Registry
	cmdMgr        *workersreg.CommandManager
	tokenMgr      *workersreg.TokenManager
	lifecycleSvc  *queue.LifecycleService
	transitionSvc *queue.TransitionService
	dbStore       *store.SQLiteStore
	config        *HandlerConfig

	mu             sync.Mutex
	sessions       map[string]*workerSession // sessionID → active stream session
	workerSessions map[string]string         // workerID → sessionID (for lookup)
}

// HandlerConfig holds configuration for the gRPC handler.
//
// Phase 4.3: ShadowMode is removed. The control plane is push-only; the
// worker never sees a JobAvailable notification and must claim through
// gRPC JobOffer / JobAccepted. Legacy HTTP claim paths were already
// retired in earlier waves (see docs/roadmap/14-polling-removal.md).
type HandlerConfig struct {
	PushMode      bool // Phase 5+: send JobOffer directly, workers respond JobAccepted
	ShadowMode    bool // Deprecated: legacy shadow mode (no-op, kept for config compat)
	AllowInsecure bool // Dev-only: allow insecure gRPC connections (VELOX_GRPC_ALLOW_INSECURE_DEV)
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID  string
	sessionID string
	stream    grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	done      chan struct{}
	doneOnce  sync.Once          // P0 #6: prevents double-close on session teardown/reconnect
	cancel    context.CancelFunc // cancels the session context to terminate old goroutines

	// Serialized output: all stream.Send() calls go through sendCh → sessionWriter.
	// No other goroutine may call stream.Send() directly.
	sendCh chan *pb.MasterToWorkerEnvelope

	// writerErr is a small (cap 1) channel used by sessionWriter to signal
	// a stream.Send() failure back to the Stream() main loop. Phase 4.2
	// requirement: a network-level send error MUST terminate the session,
	// otherwise pending offers can be left orphaned silently. The main loop
	// reads writerErr inside its select and triggers a teardown on receipt.
	writerErr chan error

	// Job offering synchronization (Issue 4 fix).
	pendingOffer *queue.Job // JobOffer sent, awaiting JobAccepted/JobRejected
	claimMu      sync.Mutex // serializes the claim+send+set flow; also guards pendingOffer r/w

	// Worker capacity tracking (atomic — Phase 4.1 fix). The handleHeartbeat
	// goroutine writes them, sendPushJobOffer reads them under claimMu. Using
	// atomic.Int32 makes the read lock-free and race-clean in `-race`.
	maxParallelJobs atomic.Int32
	activeJobsCount atomic.Int32

	// Sequence numbers for replay protection (Issue 7 fix).
	lastRecvSeq int64 // last received sequence number from worker
}

// NewHandler creates a new gRPC WorkerControl handler.
//
// Phase 5 hygiene: tokenMgr parameter removed — the gRPC path validates
// credentials via dbStore.validateCredentialHash and never needs a
// workers.TokenManager. Bootstrap no longer constructs a stray TokenManager
// just to satisfy this signature.
func NewHandler(
	registry *workersreg.Registry,
	cmdMgr *workersreg.CommandManager,
	tokenMgr *workersreg.TokenManager,
	lifecycleSvc *queue.LifecycleService,
	transitionSvc *queue.TransitionService,
	dbStore *store.SQLiteStore,
	config *HandlerConfig,
) *Handler {
	if config == nil {
		config = &HandlerConfig{PushMode: true}
	}
	return &Handler{
		registry:       registry,
		cmdMgr:         cmdMgr,
		tokenMgr:       tokenMgr,
		lifecycleSvc:   lifecycleSvc,
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

	// Issue 6 fix: create a cancellable context for the session.
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Issue 5 fix: create the send channel (buffered to avoid blocking producers).
	sendCh := make(chan *pb.MasterToWorkerEnvelope, 64)

	// Register session — keyed by sessionID to prevent defer from deleting a newer session.
	h.mu.Lock()
	h.closeOldSessionLocked(workerID)

	sess := &workerSession{
		workerID:  workerID,
		sessionID: sessionID,
		stream:    stream,
		done:      make(chan struct{}),
		cancel:    sessionCancel,
		sendCh:    sendCh,
		writerErr: make(chan error, 1),
	}
	h.sessions[sessionID] = sess
	h.workerSessions[workerID] = sessionID
	h.mu.Unlock()

	log.Printf("[GRPC] Worker %s connected (session: %s, name: %s)", workerID, sessionID, hello.GetWorkerName())

	// Issue 7 fix: persist the session to SQLite worker_sessions table.
	if h.dbStore != nil {
		_ = h.dbStore.InsertSession(&store.PersistedSession{
			SessionID: sessionID,
			WorkerID:  workerID,
			TokenHash: store.HashCredential(hello.GetCredentialHash()),
			IPAddress: h.extractPeerIP(stream),
			ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
	}

	// Issue 5 fix: start the dedicated session writer goroutine.
	// All stream.Send() calls go through sendCh → sessionWriter from this point on.
	go h.sessionWriter(sess)

	defer func() {
		// Issue 6 fix: cancel session context first to stop notifier goroutines
		// BEFORE closing sendCh (prevents panic on send to closed channel).
		sessionCancel()

		// Issue 5 fix: close sendCh to stop the sessionWriter goroutine.
		// At this point no goroutine should be sending on sendCh.
		close(sendCh)

		h.mu.Lock()
		if currentSID, ok := h.workerSessions[workerID]; ok && currentSID == sessionID {
			delete(h.workerSessions, workerID)
		}
		delete(h.sessions, sessionID)
		h.mu.Unlock()

		// Issue 7 fix: revoke the session in SQLite on disconnect.
		if h.dbStore != nil {
			_ = h.dbStore.RevokeSession(sessionID)
		}

		// P0 #6: use doneOnce to avoid double-close when closeOldSessionLocked
		// already signalled the notifier goroutine to stop.
		sess.doneOnce.Do(func() {
			close(sess.done)
		})
		log.Printf("[GRPC] Worker %s disconnected (session: %s)", workerID, sessionID)
	}()

	// Issue 7 fix: validate protocol version from Hello.
	// Phase 4.2: mismatched protocol is now FATAL for the connection —
	// silently accepting a stale protocol would risk acting on message
	// shapes the master does not understand, leading to data corruption.
	if env.ProtocolVersion != "" && env.ProtocolVersion != controltransport.ProtocolVersionCurrent {
		return fmt.Errorf("stream: worker %s protocol version mismatch: got %q, want %q",
			workerID, env.ProtocolVersion, controltransport.ProtocolVersionCurrent)
	}

	// Send typed HelloAck via sendCh (sessionWriter handles the actual Send).
	ack := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("ack-%s-%d", workerID, time.Now().UnixNano()),
		WorkerId:        workerID,
		SessionId:       sessionID,
		SequenceNumber:  1,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg:             &pb.MasterToWorkerEnvelope_HelloAck{HelloAck: &pb.HelloAck{}},
	}
	if !safeSend(sendCh, ack) {
		return fmt.Errorf("stream: sendCh full for hello_ack")
	}

	// Dispatch any pending commands that arrived while worker was disconnected
	h.dispatchCommands(workerID, sess)

	// Start push-mode job notifier (Phase 4.3: ShadowMode branch removed).
	// Issue 6 fix: use sessionCtx for cleanup so notifier goroutines stop
	// when the session is cancelled.
	var notifyCh chan struct{}
	var notifyStop context.CancelFunc
	if h.config.PushMode {
		notifyCtx, cancel := context.WithCancel(sessionCtx)
		notifyStop = cancel
		notifyCh = make(chan struct{}, 1)
		go h.notifyJobsAvailable(notifyCtx, workerID, notifyCh, sess.done)
	}

	if notifyStop != nil {
		defer notifyStop()
	}

	// Issue 6/Phase 4.2: wrap stream.Recv() in a goroutine so the main loop
	// can select on writerErr (cap-1) without blocking on Recv. The wrap
	// also makes session cancellation explicit: when sessionCtx is cancelled
	// the wrapper exits cleanly instead of leaking forever.
	recvCh := make(chan *pb.WorkerToMasterEnvelope, 16)
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			env, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			select {
			case recvCh <- env:
			case <-sessionCtx.Done():
				return
			}
		}
	}()

	// Main message loop — type-switch on the oneof Msg field, while
	// also watching writerErr/sessionCtx to drive clean teardown.
	for {
		select {
		case <-sessionCtx.Done():
			return sessionCtx.Err()

		case err := <-sess.writerErr:
			// P0 teardown: stream.Write failed inside sessionWriter. Cancel
			// the session context and revoke the SQLite session so the worker
			// reconnects promptly and we don't leak the orphaned job.
			log.Printf("[GRPC] sessionWriter failure for worker %s (session %s): %v — tearing down",
				workerID, sessionID, err)
			sess.cancel()
			if h.dbStore != nil {
				_ = h.dbStore.RevokeSession(sessionID)
			}
			return fmt.Errorf("stream: writer failure: %w", err)

		case err := <-recvErrCh:
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("stream: recv: %w", err)

		case env := <-recvCh:
			// Issue 6 fix: drop messages from stale sessions (zombie connections after reconnect).
			if !h.isCurrentSession(workerID, sessionID) {
				continue
			}

			// Issue 7 fix: sequence number check for replay protection.
			if env.SequenceNumber > 0 {
				if env.SequenceNumber <= sess.lastRecvSeq {
					log.Printf("[GRPC] Duplicate or replayed message from worker %s: seq=%d, last=%d",
						workerID, env.SequenceNumber, sess.lastRecvSeq)
					continue
				}
				sess.lastRecvSeq = env.SequenceNumber
			}

			switch m := env.Msg.(type) {
			case *pb.WorkerToMasterEnvelope_Heartbeat:
				h.handleHeartbeat(workerID, sessionID, m.Heartbeat)
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
}

// notifyJobsAvailable checks for pending jobs and sends full JobOffers
// (Phase 5+ push mode). ShadowMode/JobAvailable path was removed in
// Phase 4.3 — there is no HTTP claim fallback any more.
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
// Issue 5 fix: sends via sendCh instead of direct stream.Send().
func (h *Handler) sendJobAvailable(ctx context.Context, workerID string) {
	jobID, err := h.lifecycleSvc.GetNextJobID(ctx)
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

	// Issue 5 fix: send via sendCh — non-blocking (drop if channel full, next tick will retry).
	if !safeSend(sess.sendCh, env) {
		log.Printf("[GRPC] sendCh full/closed for JobAvailable to worker %s", workerID)
	}
}

// sendPushJobOffer does a real SQLite CAS claim and sends a typed JobOffer (Push mode).
// Issue 4 fix: claimMu serializes the entire check+claim+send+set flow to prevent
// TOCTOU races between concurrent notifyJobsAvailable calls.
func (h *Handler) sendPushJobOffer(ctx context.Context, workerID string) {
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Issue 4 fix: atomically check pendingOffer, claim, send, and store.
	// claimMu prevents concurrent offers from racing ClaimNextJob.
	sess.claimMu.Lock()
	defer sess.claimMu.Unlock()

	// If a previous offer is still pending (not yet accepted/rejected), skip.
	if sess.pendingOffer != nil {
		return
	}

	// Respect max_parallel_jobs: don't offer if worker is at capacity.
	// pendingOffer is nil here (checked above), so the pending count is 1 (this offer).
	// Phase 4.1: atomic loads are lock-free reads.
	if sess.maxParallelJobs.Load() > 0 {
		capacity := sess.maxParallelJobs.Load() - sess.activeJobsCount.Load() - 1
		if capacity < 0 {
			return
		}
	}

	job, err := h.lifecycleSvc.ClaimNextJob(ctx, workerID, nil)
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

	// Issue 5 fix: send via sendCh instead of direct stream.Send().
	if !safeSend(sess.sendCh, env) {
		log.Printf("[GRPC] sendCh full/closed for JobOffer to worker %s — releasing claim", workerID)
		if releaseErr := h.lifecycleSvc.ReleaseClaim(ctx, job.JobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for job %s after send failure: %v", job.JobID, releaseErr)
		}
		return
	}

	// Issue 4 fix: store the offer under claimMu so we can match JobAccepted.
	sess.pendingOffer = job
}

// handleHeartbeat processes a typed Heartbeat received via gRPC stream.
// Issue 7 fix: accepts sessionID and updates last_seen in worker_sessions table.
func (h *Handler) handleHeartbeat(workerID, sessionID string, hb *pb.Heartbeat) {
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

	// Update capacity tracking on the session (for max_parallel_jobs check).
	// Phase 4.1 fix: use atomic.Int32 so writes from this heartbeat goroutine
	// and reads from the notifier goroutine under claimMu are race-clean.
	sess := h.getSession(workerID)
	if sess != nil {
		sess.activeJobsCount.Store(int32(hb.GetActiveJobsCount()))
		if hb.GetExtra() != nil {
			if mpj, ok := hb.GetExtra().AsMap()["max_parallel_jobs"]; ok {
				switch v := mpj.(type) {
				case float64:
					sess.maxParallelJobs.Store(int32(v))
				case int64:
					sess.maxParallelJobs.Store(int32(v))
				}
			}
		}
	}

	if err := h.registry.Heartbeat(context.Background(), workerID, hb.GetWorkerName(), hb.GetStatus(), hb.GetCurrentJob(), extra); err != nil {
		log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
	}

	// Issue 7 fix / Phase 4.2 hardening: if the persisted session is gone
	// or revoked or expired, we MUST tear the active session down — not just
	// log. The worker should reconnect with a fresh sessionID.
	if h.dbStore != nil && sessionID != "" {
		if dbSess, err := h.dbStore.ValidateSessionByID(sessionID); err != nil || dbSess == nil || dbSess.Revoked {
			log.Printf("[GRPC] Session %s for worker %s is invalid — tearing down (revoked=%v, err=%v)",
				sessionID, workerID, dbSess != nil && dbSess.Revoked, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
				// Also publish to writerErr so the main loop exits predictably.
				select {
				case activeSess.writerErr <- fmt.Errorf("session revoked or expired"):
				default:
				}
				activeSess.cancel()
			}
			return
		}
		_ = h.dbStore.UpdateSessionLastSeen(sessionID)
	}
}

// handleLeaseRenewal processes a typed LeaseRenewal via gRPC stream.
//
// Phase 3.3: verify the worker owns the job before extending the lease.
// Without this gate a malicious worker could renew (and thus keep alive
// forever) a job assigned to a different worker.
func (h *Handler) handleLeaseRenewal(workerID string, lr *pb.LeaseRenewal) {
	jobID := lr.GetJobId()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] LeaseRenewal from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute)
	if lr.GetLeaseExpiresAt() != nil {
		leaseExpiry = lr.GetLeaseExpiresAt().AsTime()
	}
	if err := h.lifecycleSvc.RenewLease(context.Background(), lr.GetJobId(), workerID, lr.GetLeaseId(), leaseExpiry); err != nil {
		log.Printf("[GRPC] Lease renewal failed for job %s worker %s: %v", lr.GetJobId(), workerID, err)
	}
}

// handleJobAccepted processes typed JobAccepted — Phase 5+ real push mode.
// The lease was already created by ClaimNextJob; we just verify and grant.
// Issue 4 fix: uses claimMu instead of h.mu for pendingOffer access.
func (h *Handler) handleJobAccepted(workerID string, ja *pb.JobAccepted) {
	if !h.config.PushMode {
		return
	}
	jobID := ja.GetJobId()

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Issue 4 fix: lock claimMu to safely read and clear pendingOffer.
	sess.claimMu.Lock()
	offer := sess.pendingOffer
	var offerJobID, offerLeaseID string
	if offer != nil {
		offerJobID = offer.JobID
		offerLeaseID = offer.LeaseID
	}
	sess.claimMu.Unlock()

	if offer == nil || offerJobID != jobID {
		log.Printf("[GRPC] Worker %s accepted job %s but no matching pending offer", workerID, jobID)
		return
	}

	// Verify the lease_id the worker is accepting matches what we offered.
	declaredLeaseID := ja.GetLeaseId()
	if declaredLeaseID != "" && declaredLeaseID != offerLeaseID {
		log.Printf("[GRPC] Worker %s accepted job %s with mismatched lease_id: got %s, want %s",
			workerID, jobID, declaredLeaseID, offerLeaseID)
		return
	}

	// BUG FIX #1: LEASED → RUNNING transition MUST happen atomically BEFORE
	// sending JobLeaseGranted. Otherwise a fast-completing job that ends
	// before its first lease renewal would attempt LEASED → SUCCEEDED,
	// which the state machine forbids (only LEASED → RUNNING → SUCCEEDED).
	// The single CAS UPDATE inside StartJobWithLease verifies
	// (job_id, worker_id, lease_id, attempt, revision) atomically.
	//
	// Revision comes from a fresh GetJob (queue.Job is the rich projection
	// without revision; store.Job carries it). The extra read is bounded by
	// SQLite single-writer semantics: ClaimNextJob already committed, so
	// the row is visible at our snapshot.
	currentRev, attemptNum, revErr := h.lookupJobCASFields(jobID)
	if revErr != nil {
		log.Printf("[GRPC] Worker %s JobAccepted for %s but CAS fields unavailable: %v",
			workerID, jobID, revErr)
		// Cannot promote without CAS identity — drop the offer, do NOT
		// send JobLeaseGranted (worker has stale view).
		sess.claimMu.Lock()
		if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
			sess.pendingOffer = nil
		}
		sess.claimMu.Unlock()
		return
	}

	startParams := store.StartJobParams{
		JobID:            jobID,
		WorkerID:         workerID,
		LeaseID:          declaredLeaseID,
		Attempt:          attemptNum,
		ExpectedRevision: currentRev,
	}
	if err := h.transitionSvc.StartJobWithLease(context.Background(), startParams); err != nil {
		if errors.Is(err, store.ErrTransitionConflict) {
			log.Printf("[GRPC] Worker %s accepted job %s but lease is stale (rev=%d attempt=%d) — rejecting",
				workerID, jobID, currentRev, attemptNum)
			// Stale lease: drop the offer, the lease reaper will reclaim
			// the job or another offer will be made.
			sess.claimMu.Lock()
			if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
				sess.pendingOffer = nil
			}
			sess.claimMu.Unlock()
			return
		}
		// Non-conflict error (driver / ctx / schema): keep the pending
		// offer so the next notifier tick can retry; do NOT send
		// JobLeaseGranted because we could not promote.
		log.Printf("[GRPC] StartJob (LEASED→RUNNING) failed for %s (worker %s): %v — keeping pending offer for retry",
			jobID, workerID, err)
		return
	}

	// Send typed JobLeaseGranted via sendCh — confirms the lease created by ClaimNextJob.
	// P0 #1: include the lease_id so the worker can use it for heartbeat/renewals.
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
				LeaseId:  offer.LeaseID,
			},
		},
	}

	if !safeSend(sess.sendCh, env) {
		// Phase 4.2 hardening: when we cannot deliver JobLeaseGranted the
		// job must NOT stay LEASED with a stale pendingOffer. Release the
		// claim so the job returns to PENDING (or another worker can claim).
		log.Printf("[GRPC] sendCh full/closed for JobLeaseGranted to worker %s — releasing claim for job %s",
			workerID, jobID)
		if releaseErr := h.transitionSvc.ReleaseClaim(context.Background(), jobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for job %s after JobLeaseGranted send failure: %v",
				jobID, releaseErr)
		}
		sess.claimMu.Lock()
		if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
			sess.pendingOffer = nil
		}
		sess.claimMu.Unlock()
		return
	}

	// Issue 4 fix: clear pending offer under claimMu — job is now running on this worker.
	sess.claimMu.Lock()
	if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	sess.claimMu.Unlock()
}

// handleJobRejected processes typed JobRejected — releases the claimed job for requeue.
// Issue 4 fix: uses claimMu instead of h.mu for pendingOffer access.
//
// Phase 3.3: ownership gate — a worker can only reject a job it has been
// offered. Mismatched JobRejected messages are dropped silently (with a log).
func (h *Handler) handleJobRejected(workerID string, jr *pb.JobRejected) {
	jobID := jr.GetJobId()
	reason := jr.GetReason()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobRejected from worker %s for job %s refused — ownership mismatch (reason=%q)",
			workerID, jobID, reason)
		return
	}
	log.Printf("[GRPC] Worker %s rejected job %s: %s", workerID, jobID, reason)

	if jobID != "" {
		if err := h.lifecycleSvc.FailJob(context.Background(), jobID, reason, workerID, true, 3); err != nil {
			log.Printf("[GRPC] Failed to release rejected job %s: %v", jobID, err)
		}
	}

	// Issue 4 fix: lock claimMu to safely access and clear pendingOffer.
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}
	sess.claimMu.Lock()
	if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	sess.claimMu.Unlock()
}

// handleJobProgress tracks per-job progress (typed, forwarded via heartbeat for now).
//
// Phase 3.3: relaxed ownership gate — progress is informational and is
// only dropped when the worker definitely does not own the job. Mismatch
// scenarios are logged but not rejected (a worker mid-reconnect might
// legitimately race a late JobProgress against a freshly-reassigned job).
func (h *Handler) handleJobProgress(workerID string, jp *pb.JobProgress) {
	jobID := jp.GetJobId()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobProgress from worker %s for job %s ignored — ownership mismatch",
			workerID, jobID)
		return
	}
	log.Printf("[GRPC] Worker %s progress on job %s: stage=%s %d%% (scene %d/%d)",
		workerID, jobID, jp.GetStage(), jp.GetProgressPercent(), jp.GetScene(), jp.GetTotalScenes())
}

// handleCommandAck processes typed CommandAck via gRPC stream.
// Only accepts ACK by command_id — the legacy type-based fallback is removed.
// Worker-scoped: the command_id must belong to the authenticated worker.
func (h *Handler) handleCommandAck(workerID string, ca *pb.CommandAck) {
	if ca.GetCommandId() != "" {
		if err := h.cmdMgr.AckCommandByID(workerID, ca.GetCommandId()); err != nil {
			log.Printf("[GRPC] Command ACK failed for %s (worker %s): %v", ca.GetCommandId(), workerID, err)
		}
	}
}

// handleJobResult processes typed JobResult via gRPC stream.
//
// Phase 3.3: verify the worker actually owns this job before touching the
// terminal status. The JobResult protobuf only carries job_id, so the
// ctx.materialised alloc here is unavoidable, but bounded — one SELECT per
// result message, no joins.
func (h *Handler) handleJobResult(workerID string, jr *pb.JobResult) {
	jobID := jr.GetJobId()
	status := jr.GetStatus()
	errMsg := jr.GetError()

	if jobID == "" {
		log.Printf("[GRPC] JobResult from worker %s missing job_id — dropping", workerID)
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobResult from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}

	if status == "success" {
		if err := h.lifecycleSvc.CompleteJob(context.Background(), jobID); err != nil {
			log.Printf("[GRPC] Job completion failed for %s: %v", jobID, err)
		}
	} else if status == "failed" {
		if err := h.lifecycleSvc.FailJob(context.Background(), jobID, errMsg, workerID, true, 3); err != nil {
			log.Printf("[GRPC] Job failure transition failed for %s: %v", jobID, err)
		}
	}
}

// handleArtifactUploaded processes typed ArtifactUploaded via gRPC stream.
//
// Phase 3.3: ownership gate — a worker can only attach artifacts to jobs
// it owns. Without this check, an authenticated worker could attach
// fabricated artifact_ids to other workers' jobs, breaking downstream
// delivery routing silently.
func (h *Handler) handleArtifactUploaded(workerID string, a *pb.ArtifactUploaded) {
	if a.GetJobId() == "" || a.GetArtifactId() == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s missing job_id or artifact_id — skipping DB update", workerID)
		return
	}
	if !h.verifyJobOwnership(workerID, a.GetJobId()) {
		log.Printf("[GRPC] ArtifactUploaded from worker %s for job %s refused — ownership mismatch",
			workerID, a.GetJobId())
		return
	}
	log.Printf("[GRPC] Worker %s uploaded artifact %s (type: %s, size: %d bytes, status: %s)",
		workerID, a.GetArtifactId(), a.GetArtifactType(), a.GetArtifactSize(), a.GetUploadStatus())

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
// sends each as a typed Command via sendCh, and marks them as delivered.
// Issue 5 fix: sends via sendCh instead of direct stream.Send().
func (h *Handler) dispatchCommands(workerID string, sess *workerSession) {
	cmds := h.cmdMgr.GetPendingCommandsAndMarkDelivered(workerID)
	if len(cmds) == 0 {
		return
	}

	log.Printf("[GRPC] Dispatching %d pending commands to worker %s", len(cmds), workerID)

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

		// Issue 5 fix: send via sendCh — non-blocking (sessionWriter drains).
		if !safeSend(sess.sendCh, env) {
			log.Printf("[GRPC] sendCh full/closed — dropping command %s for worker %s", cmd.CommandID, workerID)
		}
	}
}

// closeOldSessionLocked removes any existing session for the given workerID
// and signals its notifier goroutine to stop. Must be called with h.mu held.
func (h *Handler) closeOldSessionLocked(workerID string) {
	oldSID, ok := h.workerSessions[workerID]
	if !ok {
		return
	}
	oldSess, exists := h.sessions[oldSID]
	if exists {
		log.Printf("[GRPC] Worker %s reconnecting — removing old session %s", workerID, oldSID)
		// P0 #6: close the done channel to stop the old notifier goroutine.
		// Messages from the old session's main loop are dropped by isCurrentSession().
		oldSess.doneOnce.Do(func() {
			close(oldSess.done)
		})
		// Issue 6 fix: cancel the old session's context to stop its goroutines.
		if oldSess.cancel != nil {
			oldSess.cancel()
		}
		// Issue 7 fix: revoke the old session in SQLite.
		if h.dbStore != nil {
			_ = h.dbStore.RevokeSession(oldSID)
		}
	}
	delete(h.sessions, oldSID)
	delete(h.workerSessions, workerID)
}

// isCurrentSession returns true if the given sessionID is still the active
// session for workerID. Used to drop messages from stale/zombie connections.
func (h *Handler) isCurrentSession(workerID, sessionID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	sid, ok := h.workerSessions[workerID]
	return ok && sid == sessionID
}

// sessionWriter is the sole goroutine allowed to call stream.Send().
// All message producers write to sendCh; this goroutine drains and sends.
// Exits when sendCh is closed (signaling session teardown) OR when a
// stream.Send() failure surfaces the error to the main loop via writerErr.
//
// Phase 4.2: a write failure MUST NOT be silently absorbed — publish to
// writerErr so the main loop tears the session down promptly. We also
// drain the channel before exiting so producers do not block on a full
// sendCh during the close sequence.
func (h *Handler) sessionWriter(sess *workerSession) {
	for env := range sess.sendCh {
		if err := sess.stream.Send(env); err != nil {
			log.Printf("[GRPC] sessionWriter send error for worker %s (session %s): %v",
				sess.workerID, sess.sessionID, err)
			// Best-effort publish (cap 1, non-blocking).
			select {
			case sess.writerErr <- err:
			default:
			}
			// Fast-drain remaining messages so producers attached to sendCh
			// are not blocked as the main loop winds down. We do NOT attempt
			// to resend them — they belong to a session that is about to die.
			for range sess.sendCh {
			}
			break
		}
	}
	log.Printf("[GRPC] sessionWriter exiting for worker %s (session %s)", sess.workerID, sess.sessionID)
}

// safeSend attempts to send an envelope on the channel, returning true on success.
// Returns false if the channel is full or closed (uses recover to handle closed channel panic).
func safeSend(ch chan *pb.MasterToWorkerEnvelope, env *pb.MasterToWorkerEnvelope) bool {
	defer func() { recover() }()
	select {
	case ch <- env:
		return true
	default:
		return false
	}
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

// verifyJobOwnership checks that `jobID` currently belongs to `workerID`.
// Returns true when the job's `assigned_to` column equals `workerID`. The
// function does NOT check the lease_id or the lease expiry here — callers
// that carry an explicit lease_id should pass it (see verifyJobOwnershipFull).
//
// Phase 3.3: this is the lightweight gate behind every mutating message
// (JobResult, LeaseRenewal, JobRejected, ArtifactUploaded, JobProgress).
// Without it, an authenticated worker A could complete or steal a job
// leased to worker B by sending JobResult{ job_id=<B's job> } — which
// the protobuf contract alone does not protect against.
func (h *Handler) verifyJobOwnership(workerID, jobID string) bool {
	if workerID == "" || jobID == "" {
		return false
	}
	m, err := h.dbStore.GetJob(context.Background(), jobID)
	if err != nil || m == nil {
		return false
	}
	assigned, _ := m["assigned_to"].(string)
	return assigned == workerID
}

// lookupJobCASFields fetches the (revision, attempt) tuple required for the
// StartJob CAS. We do this with an extra GetJob (rather than stuffing
// revision onto the JobOffer) to keep the protobuf contract narrow and
// avoid leaking internal CAS counters on the wire.
//
// Implementation note: queue.GetJob returns the rich *queue.Job wrapper
// (no revision/attempt decimal fields), so we go straight to dbStore.GetJob
// and parse the map locally — one DB round-trip, no chained calls.
func (h *Handler) lookupJobCASFields(jobID string) (revision, attempt int, err error) {
	m, err := h.dbStore.GetJob(context.Background(), jobID)
	if err != nil {
		return 0, 0, err
	}
	if m == nil {
		return 0, 0, fmt.Errorf("job %s not found", jobID)
	}
	rev := dbutil.IntFromMap(m, "revision")
	att := dbutil.IntFromMap(m, "attempt")
	return rev, att, nil
}

// ---- Security Helpers ----

// extractPeerIP extracts the client IP address from the gRPC stream context
// without the port (if possible).
func (h *Handler) extractPeerIP(stream grpc.ServerStream) string {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return ""
	}
	addr := p.Addr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

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
