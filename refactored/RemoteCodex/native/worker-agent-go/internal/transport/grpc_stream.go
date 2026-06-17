// Package transport — gRPC bidirectional stream transport for worker↔master.
// grpc_stream.go provides GRPCStreamTransport that satisfies ControlTransport
// via a single bidirectional gRPC stream using generated protobuf types.
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
	stream      grpc.BidiStreamingClient[pb.TransportMessage, pb.TransportMessage]
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
// and completes the Hello/HelloAck handshake.
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

	// Send Hello on the stream
	helloMsg := t.helloToProto(hello)
	if err := stream.Send(helloMsg); err != nil {
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

	if resp.Type != string(controltransport.MsgHelloAck) {
		stream.CloseSend()
		conn.Close()
		t.mu.Lock()
		t.state = stateDisconnected
		t.mu.Unlock()
		return fmt.Errorf("grpc transport: expected hello_ack, got %s", resp.Type)
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

// recvLoop reads messages from the gRPC stream and pushes them to recvCh.
// When the stream fails, it closes errCh to signal the worker's runSession(),
// which then cancels the session context, causing receiveLoop to exit via ctx.Done().
// Close() handles closing recvCh safely after all goroutines have stopped.
func (t *GRPCStreamTransport) recvLoop() {
	defer t.errCloseOnce.Do(func() {
		close(t.errCh)
	})

	for {
		pb, err := t.stream.Recv()
		if err != nil {
			return
		}

		msg := t.protoToMessage(pb)

		select {
		case t.recvCh <- msg:
		case <-t.closeCh:
			return
		}
	}
}

// Send transmits a ControlMessage over the gRPC stream.
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

	pbMsg := t.messageToProto(msg)
	return stream.Send(pbMsg)
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
		// Send Goodbye before closing
		goodbye := &pb.TransportMessage{
			Type:            string(controltransport.MsgGoodbye),
			WorkerId:        t.workerID,
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			SentAt:          timestamppb.Now(),
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

// ---- Proto ↔ ControlMessage conversion ----

func (t *GRPCStreamTransport) helloToProto(hello controltransport.WorkerHello) *pb.TransportMessage {
	payload, _ := structpb.NewStruct(map[string]interface{}{
		"worker_name":     hello.WorkerName,
		"hostname":        hello.Hostname,
		"version":         hello.Version,
		"bundle_version":  hello.BundleVersion,
		"bundle_hash":     hello.BundleHash,
		"engine_version":  hello.EngineVersion,
		"credential_hash": hello.CredentialHash,
		"capabilities":    hello.Capabilities,
	})

	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	t.mu.Unlock()

	return &pb.TransportMessage{
		MessageId:       fmt.Sprintf("grpc-%s-%d", t.workerID, time.Now().UnixNano()),
		Type:            string(controltransport.MsgHello),
		WorkerId:        hello.WorkerID,
		SequenceNumber:  seq,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: hello.ProtocolVersion,
		Payload:         payload,
	}
}

func (t *GRPCStreamTransport) messageToProto(msg controltransport.ControlMessage) *pb.TransportMessage {
	payload, _ := structpb.NewStruct(msg.Payload)

	t.mu.Lock()
	t.msgSeq++
	seq := t.msgSeq
	sid := t.sessionID
	t.mu.Unlock()

	return &pb.TransportMessage{
		MessageId:       msg.MessageID,
		Type:            string(msg.Type),
		WorkerId:        msg.WorkerID,
		SessionId:       sid,
		SequenceNumber:  seq,
		SentAt:          timestamppb.New(msg.SentAt),
		ProtocolVersion: msg.ProtocolVersion,
		Payload:         payload,
	}
}

func (t *GRPCStreamTransport) protoToMessage(pbMsg *pb.TransportMessage) controltransport.ControlMessage {
	var payload map[string]interface{}
	if pbMsg.Payload != nil {
		payload = pbMsg.Payload.AsMap()
	}

	sentAt := time.Now().UTC()
	if pbMsg.SentAt != nil {
		sentAt = pbMsg.SentAt.AsTime()
	}

	return controltransport.ControlMessage{
		MessageID:       pbMsg.MessageId,
		Type:            controltransport.ControlMessageType(pbMsg.Type),
		WorkerID:        pbMsg.WorkerId,
		SessionID:       pbMsg.SessionId,
		SequenceNumber:  pbMsg.SequenceNumber,
		SentAt:          sentAt,
		ProtocolVersion: pbMsg.ProtocolVersion,
		Payload:         payload,
	}
}
