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

	mu       sync.Mutex
	sessions map[string]*workerSession // workerID → active stream session
}

// HandlerConfig holds configuration for the gRPC handler.
type HandlerConfig struct {
	ShadowMode bool // Phase 4: notify workers, still claim via HTTP
	PushMode   bool // Phase 5+: send JobOffer directly, workers respond JobAccepted
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID  string
	sessionID string
	stream    grpc.BidiStreamingServer[pb.TransportMessage, pb.TransportMessage]
	done      chan struct{}
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
		registry:      registry,
		cmdMgr:        cmdMgr,
		tokenMgr:      tokenMgr,
		transitionSvc: transitionSvc,
		dbStore:       dbStore,
		config:        config,
		sessions:      make(map[string]*workerSession),
	}
}

// Stream handles a bidirectional gRPC stream from a single worker.
func (h *Handler) Stream(stream grpc.BidiStreamingServer[pb.TransportMessage, pb.TransportMessage]) error {
	// Wait for Hello message to identify the worker
	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("stream: recv hello: %w", err)
	}
	if msg.Type != string(controltransport.MsgHello) {
		return fmt.Errorf("stream: expected hello, got %s", msg.Type)
	}

	workerID := msg.WorkerId
	sessionID := fmt.Sprintf("grpc-%s-%d", workerID, time.Now().UnixNano())

	// Register session
	h.mu.Lock()
	sess := &workerSession{
		workerID:  workerID,
		sessionID: sessionID,
		stream:    stream,
		done:      make(chan struct{}),
	}
	h.sessions[workerID] = sess
	h.mu.Unlock()

	log.Printf("[GRPC] Worker %s connected (session: %s)", workerID, sessionID)

	defer func() {
		h.mu.Lock()
		delete(h.sessions, workerID)
		h.mu.Unlock()
		close(sess.done)
		log.Printf("[GRPC] Worker %s disconnected", workerID)
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

// notifyJobsAvailable checks for pending jobs and sends JobOffer notifications.
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

		jobID, err := h.transitionSvc.GetNextJobID(ctx)
		if err != nil || jobID == "" {
			continue
		}

		payload, _ := structpb.NewStruct(map[string]interface{}{
			"available": true,
			"message":   "Job available for claim",
		})

		msg := &pb.TransportMessage{
			MessageId:       fmt.Sprintf("job-%s-%d", workerID, time.Now().UnixNano()),
			Type:            string(controltransport.MsgJobOffer),
			WorkerId:        workerID,
			SentAt:          timestamppb.Now(),
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			Payload:         payload,
		}

		h.mu.Lock()
		sess, ok := h.sessions[workerID]
		h.mu.Unlock()
		if !ok {
			return
		}

		if err := sess.stream.Send(msg); err != nil {
			log.Printf("[GRPC] Failed to send job notification to worker %s: %v", workerID, err)
			return
		}
	}
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
func (h *Handler) handleJobAccepted(workerID string, msg *pb.TransportMessage) {
	if !h.config.PushMode {
		return
	}
	payload := msg.Payload.AsMap()
	jobID := getPayloadString(payload, "job_id")
	if jobID != "" {
		if err := h.transitionSvc.LeaseJob(context.Background(), jobID, workerID); err != nil {
			log.Printf("[GRPC] Job accepted lease failed for %s: %v", jobID, err)
		}
	}
}

// handleJobRejected processes JobRejected — Phase 5+ real push mode.
func (h *Handler) handleJobRejected(workerID string, msg *pb.TransportMessage) {
	if !h.config.PushMode {
		return
	}
	payload := msg.Payload.AsMap()
	jobID := getPayloadString(payload, "job_id")
	reason := getPayloadString(payload, "reason")
	log.Printf("[GRPC] Worker %s rejected job %s: %s", workerID, jobID, reason)
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

// ---- Helpers ----

func getPayloadString(payload map[string]interface{}, key string) string {
	if v, ok := payload[key].(string); ok {
		return v
	}
	return ""
}
