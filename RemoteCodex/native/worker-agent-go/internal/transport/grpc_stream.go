// Package transport — gRPC bidirectional stream transport for worker↔master.
// grpc_stream.go provides GRPCStreamTransport that satisfies ControlTransport
// via a single bidirectional gRPC stream using typed protobuf envelopes.
// Uses WorkerToMasterEnvelope / MasterToWorkerEnvelope with typed oneof messages
// (TaskAccepted, TaskRejected, Heartbeat, etc.) instead of generic Struct payloads.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
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

// transportState represents the gRPC stream connection state.
type transportState int

const (
	stateDisconnected transportState = iota
	stateConnecting
	stateReady
)

// GRPCStreamTransport implements ControlTransport via a gRPC bidirectional stream.
// The worker opens a single gRPC connection, authenticates with Hello/HelloAck
// handshake, then exchanges typed messages over the persistent stream.
type GRPCStreamTransport struct {
	grpcURL  string
	workerID string

	// mTLS configuration (Phase 7). If nil, insecure transport is used.
	tlsConfig *tls.Config

	mu           sync.Mutex
	conn         *grpc.ClientConn
	stream       grpc.BidiStreamingClient[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	sessionID    string
	state        transportState
	closed       bool
	closeCh      chan struct{}
	closeOnce    sync.Once
	recvDone     chan struct{} // closed by recvLoop on exit; Close() waits before closing recvCh
	recvCh       chan controltransport.ControlMessage
	errCh        chan error
	errCloseOnce sync.Once
	recvOnce     sync.Once
	msgSeq       int64
	sendMu       sync.Mutex
}

// NewGRPCStreamTransport creates a new gRPC stream transport.
func NewGRPCStreamTransport(grpcURL, workerID string) *GRPCStreamTransport {
	return &GRPCStreamTransport{
		grpcURL:  grpcURL,
		workerID: workerID,
		state:    stateDisconnected,
		closeCh:  make(chan struct{}),
		recvDone: make(chan struct{}),
		recvCh:   make(chan controltransport.ControlMessage, 64),
		errCh:    make(chan error, 1),
	}
}

// WithTLS configures mTLS credentials from cert, key, and CA file paths.
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

// Connect establishes a gRPC connection and completes the Hello/HelloAck handshake.
func (t *GRPCStreamTransport) Connect(ctx context.Context, hello controltransport.WorkerHello) error {
	t.mu.Lock()
	t.state = stateConnecting
	t.mu.Unlock()

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

	helloEnv := t.helloToEnvelope(hello)
	if err := stream.Send(helloEnv); err != nil {
		stream.CloseSend()
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: send hello: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		stream.CloseSend()
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: recv hello_ack: %w", err)
	}

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

// recvLoop reads typed MasterToWorkerEnvelope messages and converts to ControlMessage.
func (t *GRPCStreamTransport) recvLoop() {
	defer func() {
		t.errCloseOnce.Do(func() {
			close(t.errCh)
		})
		if t.recvCh != nil {
			close(t.recvCh)
		}
		close(t.recvDone)
	}()

	for {
		env, err := t.stream.Recv()
		if err != nil {
			log.Printf("[GRPC-TRANSPORT] recvLoop error for worker %s: %v", t.workerID, err)
			select {
			case t.errCh <- err:
			default:
			}
			return
		}
		switch env.Msg.(type) {
		case *pb.MasterToWorkerEnvelope_TaskOffer:
			log.Printf("[GRPC-TRANSPORT] recvLoop received TaskOffer for worker %s: msg=%s session=%s",
				t.workerID, env.MessageId, env.SessionId)
		case *pb.MasterToWorkerEnvelope_TaskLeaseGranted:
			log.Printf("[GRPC-TRANSPORT] recvLoop received TaskLeaseGranted for worker %s: msg=%s session=%s",
				t.workerID, env.MessageId, env.SessionId)
		}

		msg := t.envelopeToMessage(env)

		select {
		case t.recvCh <- msg:
		case <-t.closeCh:
			return
		}
	}
}

// Send transmits a ControlMessage over the gRPC stream.
// The ControlMessage.TypedPayload contains a typed proto message (e.g. *pb.TaskAccepted)
// that messageToEnvelope wraps in a WorkerToMasterEnvelope.
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

	t.closeOnce.Do(func() {
		close(t.closeCh)
	})

	if stream != nil {
		t.sendMu.Lock()
		goodbye := &pb.WorkerToMasterEnvelope{
			MessageId:       fmt.Sprintf("goodbye-%s-%d", t.workerID, time.Now().UnixNano()),
			WorkerId:        t.workerID,
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			SentAt:          timestamppb.Now(),
			Msg:             &pb.WorkerToMasterEnvelope_Goodbye{Goodbye: &pb.Goodbye{}},
		}
		_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = stream.Send(goodbye)
		t.sendMu.Unlock()
		_ = stream.CloseSend()
	}

	select {
	case <-t.recvDone:
	case <-time.After(5 * time.Second):
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

	switch tp := msg.TypedPayload.(type) {
	case *pb.Heartbeat:
		env.Msg = &pb.WorkerToMasterEnvelope_Heartbeat{Heartbeat: tp}

	case *pb.TaskLeaseRenewal:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskLeaseRenewal{TaskLeaseRenewal: tp}

	case *pb.TaskAccepted:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskAccepted{TaskAccepted: tp}

	case *pb.TaskRejected:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskRejected{TaskRejected: tp}

	case *pb.TaskResult:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskResult{TaskResult: tp}

	case *pb.CommandAck:
		env.Msg = &pb.WorkerToMasterEnvelope_CommandAck{CommandAck: tp}

	case *pb.ArtifactUploaded:
		env.Msg = &pb.WorkerToMasterEnvelope_ArtifactUploaded{ArtifactUploaded: tp}

	case *pb.TaskOutputDeclared:
		env.Msg = &pb.WorkerToMasterEnvelope_TaskOutputDeclared{TaskOutputDeclared: tp}

	case *pb.ArtifactUploadCompleted:
		env.Msg = &pb.WorkerToMasterEnvelope_ArtifactUploadCompleted{ArtifactUploadCompleted: tp}
	}

	return env
}

// envelopeToMessage converts a typed MasterToWorkerEnvelope to a ControlMessage.
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

	case *pb.MasterToWorkerEnvelope_TaskOffer:
		msg.Type = controltransport.MsgTaskOffer
		msg.TypedPayload = m.TaskOffer

	case *pb.MasterToWorkerEnvelope_TaskLeaseGranted:
		msg.Type = controltransport.MsgTaskLeaseGranted
		msg.TypedPayload = m.TaskLeaseGranted

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

	case *pb.MasterToWorkerEnvelope_ArtifactUploadPlan:
		msg.Type = controltransport.MsgArtifactUploadPlan
		msg.TypedPayload = m.ArtifactUploadPlan

	case *pb.MasterToWorkerEnvelope_TaskCommitAck:
		msg.Type = controltransport.MsgTaskCommitAck
		msg.TypedPayload = m.TaskCommitAck
	}

	return msg
}
