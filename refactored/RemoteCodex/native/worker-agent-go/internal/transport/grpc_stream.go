// Package transport — gRPC bidirectional stream transport for worker↔master.
// grpc_stream.go provides GRPCStreamTransport that satisfies ControlTransport
// via a single bidirectional gRPC stream using typed protobuf envelopes.
// Phase 2 (typed protobuf): eliminates TransportMessage { string type; Struct payload }
// in favor of WorkerToMasterEnvelope / MasterToWorkerEnvelope with typed oneof messages.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCStreamTransport implements ControlTransport via a gRPC bidirectional stream.
// The worker opens a single gRPC connection, authenticates with Hello/HelloAck
// handshake, then exchanges typed messages over the persistent stream.
type GRPCStreamTransport struct {
	grpcURL  string
	workerID string

	// mTLS configuration (Phase 7). If nil, insecure transport is used.
	tlsConfig *tls.Config

	mu          sync.Mutex
	conn        *grpc.ClientConn
	stream      grpc.BidiStreamingClient[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	sessionID   string
	state       transportState
	closed      bool
	closeCh     chan struct{}
	closeOnce   sync.Once
	recvCh      chan controltransport.ControlMessage
	errCh       chan error
	errCloseOnce sync.Once
	recvOnce    sync.Once
	msgSeq      int64
	sendMu      sync.Mutex
}

// NewGRPCStreamTransport creates a new gRPC stream transport.
func NewGRPCStreamTransport(grpcURL, workerID string) *GRPCStreamTransport {
	return &GRPCStreamTransport{
		grpcURL:  grpcURL,
		workerID: workerID,
		state:    stateDisconnected,
		closeCh:  make(chan struct{}),
		recvCh:   make(chan controltransport.ControlMessage, 64),
		errCh:    make(chan error, 1),
	}
}

// WithTLS configures mTLS credentials from cert, key, and CA file paths.
// The client presents its certificate and verifies the server against the CA.
func (t *GRPCStreamTransport) WithTLS(certFile, keyFile, caFile string) error {
	if certFile == "" || keyFile == "" || caFile == "" {
		return fmt.Errorf("tls cert, key, and ca files are required")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load client cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("read ca file: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("failed to parse CA certificate")
	}

	t.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS12,
	}
	return nil
}

// Connect establishes a gRPC connection, opens a bidirectional stream,
// and completes the Hello/HelloAck handshake using typed envelopes.
func (t *GRPCStreamTransport) Connect(ctx context.Context, hello controltransport.WorkerHello) error {
	t.mu.Lock()
	t.state = stateConnecting
	t.mu.Unlock()

	// Establish gRPC connection with TLS or insecure credentials
	var transportCreds grpc.DialOption
	if t.tlsConfig != nil {
		transportCreds = grpc.WithTransportCredentials(credentials.NewTLS(t.tlsConfig))
	} else {
		transportCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	conn, err := grpc.DialContext(ctx, t.grpcURL, transportCreds)
	if err != nil {
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: dial %s: %w", t.grpcURL, err)
	}

	t.conn = conn
	client := pb.NewWorkerControlClient(conn)

	stream, err := client.Stream(ctx)
	if err != nil {
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: open stream: %w", err)
	}

	t.stream = stream

	// Build typed Hello envelope
	helloEnv := t.helloToEnvelope(hello)
	if err := stream.Send(helloEnv); err != nil {
		stream.CloseSend()
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: send hello: %w", err)
	}

	// Wait for HelloAck
	resp, err := stream.Recv()
	if err != nil {
		stream.CloseSend()
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: recv hello_ack: %w", err)
	}

	// Verify response is HelloAck
	if resp.GetHelloAck() == nil {
		stream.CloseSend()
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: expected hello_ack, got %T", resp.Msg)
	}

	t.mu.Lock()
	t.sessionID = resp.SessionId
	t.state = stateReady
	t.mu.Unlock()

	return nil
}

// Receive returns the message channel (master→worker) and an error channel.
// The message channel is closed when the transport is closed or the stream fails.
// The error channel receives the terminal error (if any) and is then closed.
func (t *GRPCStreamTransport) Receive(ctx context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, controltransport.ErrTransportClosed
	}
	if t.stream == nil {
		t.mu.Unlock()
		return nil, nil, controltransport.ErrNotConnected
	}
	t.mu.Unlock()

	t.recvOnce.Do(func() {
		go t.recvLoop()
	})

	return t.recvCh, t.errCh, nil
}

// recvLoop reads typed MasterToWorkerEnvelope messages from the gRPC stream
// and converts them to ControlMessage for the worker's receiveLoop.
func (t *GRPCStreamTransport) recvLoop() {
	defer t.errCloseOnce.Do(func() {
		close(t.errCh)
	})

	for {
		env, err := t.stream.Recv()
		if err != nil {
			return
		}

		msg := t.envelopeToMessage(env)

		select {
		case t.recvCh <- msg:
		case <-t.closeCh:
			return
		}
	}
}

// Send transmits a ControlMessage over the gRPC stream as a typed envelope.
func (t *GRPCStreamTransport) Send(ctx context.Context, msg controltransport.ControlMessage) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return controltransport.ErrTransportClosed
	}
	stream := t.stream
	t.mu.Unlock()

	if stream == nil {
		return controltransport.ErrNotConnected
	}

	t.sendMu.Lock()
	defer t.sendMu.Unlock()

	env := t.messageToEnvelope(msg)
	return stream.Send(env)
}

// Close gracefully terminates the gRPC stream and connection.
func (t *GRPCStreamTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.state = stateDisconnected
	stream := t.stream
	conn := t.conn
	t.mu.Unlock()

	// Signal recvLoop to stop
	t.closeOnce.Do(func() {
		close(t.closeCh)
	})

	if stream != nil {
		// Send typed Goodbye before closing
		goodbye := &pb.WorkerToMasterEnvelope{
			MessageId:       fmt.Sprintf("goodbye-%s-%d", t.workerID, time.Now().UnixNano()),
			WorkerId:        t.workerID,
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			SentAt:          timestamppb.Now(),
			Msg:             &pb.WorkerToMasterEnvelope_Goodbye{Goodbye: &pb.Goodbye{}},
		}

		_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = stream.Send(goodbye) // Best effort
		_ = stream.CloseSend()
	}

	// Close receive channel and error channel safely
	if t.recvCh != nil {
		close(t.recvCh)
	}
	t.errCloseOnce.Do(func() {
		close(t.errCh)
	})

	if conn != nil {
		_ = conn.Close()
	}

	return nil
}

// ---- Typed Proto ↔ ControlMessage conversion ----

// helloToEnvelope builds a typed WorkerToMasterEnvelope with a Hello message.
func (t *GRPCStreamTransport) helloToEnvelope(hello controltransport.WorkerHello) *pb.WorkerToMasterEnvelope {
	var caps *structpb.Struct
	if hello.Capabilities != nil {
		caps, _ = structpb.NewStruct(hello.Capabilities)
	}

	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	t.mu.Unlock()

	return &pb.WorkerToMasterEnvelope{
		MessageId:       fmt.Sprintf("grpc-%s-%d", t.workerID, time.Now().UnixNano()),
		WorkerId:        hello.WorkerID,
		SequenceNumber:  seq,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: hello.ProtocolVersion,
		Msg: &pb.WorkerToMasterEnvelope_Hello{
			Hello: &pb.Hello{
				WorkerName:     hello.WorkerName,
				Hostname:       hello.Hostname,
				Version:        hello.Version,
				BundleVersion:  hello.BundleVersion,
				BundleHash:     hello.BundleHash,
				EngineVersion:  hello.EngineVersion,
				CredentialHash: hello.CredentialHash,
				Capabilities:   caps,
			},
		},
	}
}

// messageToEnvelope converts a ControlMessage to a typed WorkerToMasterEnvelope.
func (t *GRPCStreamTransport) messageToEnvelope(msg controltransport.ControlMessage) *pb.WorkerToMasterEnvelope {
	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	sid := t.sessionID
	t.mu.Unlock()

	env := &pb.WorkerToMasterEnvelope{
		MessageId:       msg.MessageID,
		WorkerId:        msg.WorkerID,
		SessionId:       sid,
		SequenceNumber:  seq,
		SentAt:          timestamppb.New(msg.SentAt),
		ProtocolVersion: msg.ProtocolVersion,
	}

	switch msg.Type {
	case controltransport.MsgHeartbeat:
		hb := &pb.Heartbeat{
			WorkerName:      getPayloadStr(msg.Payload, "worker_name"),
			WorkerStatus:    getPayloadStr(msg.Payload, "worker_status"),
			Status:          getPayloadStr(msg.Payload, "status"),
			CurrentJob:      getPayloadStr(msg.Payload, "current_job"),
			CodeVersion:     getPayloadStr(msg.Payload, "code_version"),
			BundleVersion:   getPayloadStr(msg.Payload, "bundle_version"),
			BundleHash:      getPayloadStr(msg.Payload, "bundle_hash"),
			ProtocolVersion: getPayloadStr(msg.Payload, "protocol_version"),
			EngineVersion:   getPayloadStr(msg.Payload, "engine_version"),
			JobsCompleted:   getPayloadInt64(msg.Payload, "jobs_completed"),
			JobsFailed:      getPayloadInt64(msg.Payload, "jobs_failed"),
			ActiveJobsCount: int32(getPayloadInt64(msg.Payload, "active_jobs_count")),
		}
		// Collect remaining dynamic fields into Extra (recent_logs, capabilities, active_jobs, etc.)
		hb.Extra = collectPayloadExtra(msg.Payload,
			"worker_name", "worker_status", "status", "current_job", "code_version",
			"bundle_version", "bundle_hash", "protocol_version", "engine_version",
			"jobs_completed", "jobs_failed", "active_jobs_count")
		env.Msg = &pb.WorkerToMasterEnvelope_Heartbeat{Heartbeat: hb}

	case controltransport.MsgLeaseRenewal:
		lr := &pb.LeaseRenewal{
			JobId:   getPayloadStr(msg.Payload, "job_id"),
			LeaseId: getPayloadStr(msg.Payload, "lease_id"),
			Attempt: int32(getPayloadInt64(msg.Payload, "attempt")),
		}
		if ts := getPayloadStr(msg.Payload, "lease_expires_at"); ts != "" {
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				lr.LeaseExpiresAt = timestamppb.New(t)
			}
		}
		env.Msg = &pb.WorkerToMasterEnvelope_LeaseRenewal{LeaseRenewal: lr}

	case controltransport.MsgJobAccepted:
		env.Msg = &pb.WorkerToMasterEnvelope_JobAccepted{
			JobAccepted: &pb.JobAccepted{
				JobId:    getPayloadStr(msg.Payload, "job_id"),
				JobRunId: getPayloadStr(msg.Payload, "job_run_id"),
				LeaseId:  getPayloadStr(msg.Payload, "lease_id"),
			},
		}

	case controltransport.MsgJobRejected:
		env.Msg = &pb.WorkerToMasterEnvelope_JobRejected{
			JobRejected: &pb.JobRejected{
				JobId:  getPayloadStr(msg.Payload, "job_id"),
				Reason: getPayloadStr(msg.Payload, "reason"),
			},
		}

	case controltransport.MsgJobProgress:
		env.Msg = &pb.WorkerToMasterEnvelope_JobProgress{
			JobProgress: &pb.JobProgress{
				JobId:           getPayloadStr(msg.Payload, "job_id"),
				Stage:           getPayloadStr(msg.Payload, "stage"),
				ProgressPercent: int32(getPayloadInt64(msg.Payload, "progress_percent")),
				Scene:           int32(getPayloadInt64(msg.Payload, "scene")),
				TotalScenes:     int32(getPayloadInt64(msg.Payload, "total_scenes")),
			},
		}

	case controltransport.MsgCommandAck:
		env.Msg = &pb.WorkerToMasterEnvelope_CommandAck{
			CommandAck: &pb.CommandAck{
				CommandId: getPayloadStr(msg.Payload, "command_id"),
				Command:   getPayloadStr(msg.Payload, "command"),
				Error:     getPayloadStr(msg.Payload, "error"),
			},
		}

	case controltransport.MsgJobResult:
		env.Msg = &pb.WorkerToMasterEnvelope_JobResult{
			JobResult: &pb.JobResult{
				JobId:     getPayloadStr(msg.Payload, "job_id"),
				JobRunId:  getPayloadStr(msg.Payload, "job_run_id"),
				Status:    getPayloadStr(msg.Payload, "status"),
				Error:     getPayloadStr(msg.Payload, "error"),
				StartTime: getPayloadStr(msg.Payload, "start_time"),
				EndTime:   getPayloadStr(msg.Payload, "end_time"),
				LeaseId:   getPayloadStr(msg.Payload, "lease_id"),
				Attempt:   int32(getPayloadInt64(msg.Payload, "attempt")),
			},
		}

	case controltransport.MsgArtifactUploaded:
		env.Msg = &pb.WorkerToMasterEnvelope_ArtifactUploaded{
			ArtifactUploaded: &pb.ArtifactUploaded{
				JobId:        getPayloadStr(msg.Payload, "job_id"),
				ArtifactId:   getPayloadStr(msg.Payload, "artifact_id"),
				ArtifactType: getPayloadStr(msg.Payload, "artifact_type"),
				ArtifactPath: getPayloadStr(msg.Payload, "artifact_path"),
				ArtifactSize: getPayloadInt64(msg.Payload, "artifact_size"),
				UploadStatus: getPayloadStr(msg.Payload, "upload_status"),
				Error:        getPayloadStr(msg.Payload, "error"),
			},
		}
	}

	return env
}

// envelopeToMessage converts a typed MasterToWorkerEnvelope to a ControlMessage.
// Populates the Payload map for backward compatibility with the worker's
// receiveLoop (msgToJob, msgToCommand, etc.).
func (t *GRPCStreamTransport) envelopeToMessage(env *pb.MasterToWorkerEnvelope) controltransport.ControlMessage {
	sentAt := time.Now().UTC()
	if env.SentAt != nil {
		sentAt = env.SentAt.AsTime()
	}

	msg := controltransport.ControlMessage{
		MessageID:       env.MessageId,
		WorkerID:        env.WorkerId,
		SessionID:       env.SessionId,
		SequenceNumber:  env.SequenceNumber,
		SentAt:          sentAt,
		ProtocolVersion: env.ProtocolVersion,
	}

	switch m := env.Msg.(type) {
	case *pb.MasterToWorkerEnvelope_HelloAck:
		msg.Type = controltransport.MsgHelloAck

	case *pb.MasterToWorkerEnvelope_JobAvailable:
		msg.Type = controltransport.MsgJobAvailable
		msg.TypedPayload = m.JobAvailable

	case *pb.MasterToWorkerEnvelope_JobOffer:
		msg.Type = controltransport.MsgJobOffer
		msg.TypedPayload = m.JobOffer

	case *pb.MasterToWorkerEnvelope_JobLeaseGranted:
		msg.Type = controltransport.MsgJobLeaseGranted
		msg.TypedPayload = m.JobLeaseGranted

	case *pb.MasterToWorkerEnvelope_Command:
		msg.Type = controltransport.MsgCommand
		msg.TypedPayload = m.Command

	case *pb.MasterToWorkerEnvelope_CancelJob:
		msg.Type = controltransport.MsgCancelJob
		msg.TypedPayload = m.CancelJob

	case *pb.MasterToWorkerEnvelope_Drain:
		msg.Type = controltransport.MsgDrain
		msg.TypedPayload = m.Drain

	case *pb.MasterToWorkerEnvelope_ConfigurationUpdate:
		msg.Type = controltransport.MsgConfigurationUpdate
		msg.TypedPayload = m.ConfigurationUpdate

	case *pb.MasterToWorkerEnvelope_LeaseRevoked:
		msg.Type = controltransport.MsgLeaseRevoked
		msg.TypedPayload = m.LeaseRevoked

	case *pb.MasterToWorkerEnvelope_Ping:
		msg.Type = controltransport.MsgPing
	}

	return msg
}

// ---- Payload helpers (used by messageToEnvelope for ControlMessage.Payload access) ----

func getPayloadStr(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getPayloadInt64(m map[string]interface{}, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case int32:
		return int64(v)
	}
	return 0
}

// collectPayloadExtra builds a *structpb.Struct from payload fields that are
// NOT in the namedKeys set. Used to forward dynamic telemetry fields (e.g.
// recent_logs, capabilities, active_jobs) that don't map to proto typed fields.
// Returns nil if no extra fields exist.
func collectPayloadExtra(payload map[string]interface{}, namedKeys ...string) *structpb.Struct {
	known := make(map[string]bool, len(namedKeys))
	for _, k := range namedKeys {
		known[k] = true
	}

	extraMap := make(map[string]interface{})
	for k, v := range payload {
		if !known[k] {
			extraMap[k] = v
		}
	}

	if len(extraMap) == 0 {
		return nil
	}

	extra, err := structpb.NewStruct(extraMap)
	if err != nil {
		return nil
	}
	return extra
}
