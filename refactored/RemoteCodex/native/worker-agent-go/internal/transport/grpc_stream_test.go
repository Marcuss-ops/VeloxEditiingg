package transport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testStreamServer implements a minimal pb.WorkerControlServer for integration testing.
// It handles Hello → HelloAck, tracks received heartbeats, and sends JobOffer on demand.
type testStreamServer struct {
	pb.UnimplementedWorkerControlServer

	mu           sync.Mutex
	lastHello    *pb.TransportMessage
	heartbeats   []*pb.TransportMessage
	jobOfferCh   chan struct{} // signals when to send a JobOffer
	sendJobOffer bool
	gotGoodbye   bool
	heartbeatCh  chan struct{} // closed after first heartbeat
	goodbyeCh    chan struct{} // closed on Goodbye
}

func newTestStreamServer() *testStreamServer {
	return &testStreamServer{
		jobOfferCh:  make(chan struct{}, 1),
		heartbeatCh: make(chan struct{}),
		goodbyeCh:   make(chan struct{}),
	}
}

func (s *testStreamServer) Stream(stream grpc.BidiStreamingServer[pb.TransportMessage, pb.TransportMessage]) error {
	// Wait for Hello
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	if msg.Type != string(controltransport.MsgHello) {
		return fmt.Errorf("expected hello, got %s", msg.Type)
	}

	s.mu.Lock()
	s.lastHello = msg
	s.mu.Unlock()

	// Send HelloAck
	ack := &pb.TransportMessage{
		MessageId:       "ack-test-001",
		Type:            string(controltransport.MsgHelloAck),
		WorkerId:        msg.WorkerId,
		SessionId:       "test-session-001",
		SequenceNumber:  1,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: msg.ProtocolVersion,
	}
	if err := stream.Send(ack); err != nil {
		return err
	}

	// If configured to send a JobOffer, do it after a short delay
	if s.sendJobOffer {
		go func() {
			select {
			case <-s.jobOfferCh:
				payload, _ := structpb.NewStruct(map[string]interface{}{
					"job_id":     "test-job-001",
					"job_type":   "render",
					"priority":   1,
					"parameters": map[string]interface{}{"video_name": "test-video"},
				})
				jobOffer := &pb.TransportMessage{
					MessageId:       "job-offer-001",
					Type:            string(controltransport.MsgJobOffer),
					WorkerId:        msg.WorkerId,
					SessionId:       "test-session-001",
					SentAt:          timestamppb.Now(),
					ProtocolVersion: msg.ProtocolVersion,
					Payload:         payload,
				}
				_ = stream.Send(jobOffer)
			case <-time.After(5 * time.Second):
			}
		}()
	}

	// Main loop: receive messages until stream closes
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream recv: %w", err)
		}

		switch controltransport.ControlMessageType(msg.Type) {
		case controltransport.MsgHeartbeat:
			s.mu.Lock()
			s.heartbeats = append(s.heartbeats, msg)
			n := len(s.heartbeats)
			s.mu.Unlock()

			// Signal first heartbeat (for test synchronization)
			if n == 1 {
				close(s.heartbeatCh)
			}

			// Signal to send JobOffer after first heartbeat
			if s.sendJobOffer && n == 1 {
				select {
				case s.jobOfferCh <- struct{}{}:
				default:
				}
			}

		case controltransport.MsgGoodbye:
			s.mu.Lock()
			s.gotGoodbye = true
			s.mu.Unlock()
			close(s.goodbyeCh)
			return nil

		case controltransport.MsgJobAccepted:
			// Track it; no action needed for test

		case controltransport.MsgJobRejected:
			// Track it; no action needed for test
		}
	}
}

// ---- Integration Tests ----

func startTestGRPCServer(t *testing.T, srv pb.WorkerControlServer) (*grpc.Server, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	gsrv := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)
	pb.RegisterWorkerControlServer(gsrv, srv)

	go func() {
		_ = gsrv.Serve(lis)
	}()

	return gsrv, lis.Addr().String()
}

func TestGRPCStreamTransport_HelloHandshake(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestGRPCServer(t, ts)
	defer srv.Stop()

	transport := NewGRPCStreamTransport(addr, "test-worker-001")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-001",
		WorkerName:      "test-worker",
		Hostname:        "test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Capabilities: map[string]interface{}{
			"max_parallel_jobs": 2,
		},
	}

	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Verify server received Hello with correct fields
	ts.mu.Lock()
	lastHello := ts.lastHello
	ts.mu.Unlock()

	if lastHello == nil {
		t.Fatal("Server did not receive Hello message")
	}
	if lastHello.WorkerId != "test-worker-001" {
		t.Errorf("Hello WorkerId = %q, want %q", lastHello.WorkerId, "test-worker-001")
	}
	if lastHello.Type != string(controltransport.MsgHello) {
		t.Errorf("Hello Type = %q, want %q", lastHello.Type, controltransport.MsgHello)
	}

	// Verify transport state is ready
	if transport.state != stateReady {
		t.Errorf("Transport state = %v, want stateReady", transport.state)
	}
	if transport.sessionID == "" {
		t.Error("Transport sessionID is empty")
	}
}

func TestGRPCStreamTransport_SendHeartbeat(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestGRPCServer(t, ts)
	defer srv.Stop()

	transport := NewGRPCStreamTransport(addr, "test-worker-002")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect first
	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-002",
		WorkerName:      "test-worker",
		Hostname:        "test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}
	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Send heartbeat
	heartbeatMsg := controltransport.ControlMessage{
		MessageID:       "hb-test-001",
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        "test-worker-002",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Payload: map[string]interface{}{
			"status":            "idle",
			"worker_name":       "test-worker",
			"active_jobs_count": 0,
		},
	}

	if err := transport.Send(ctx, heartbeatMsg); err != nil {
		t.Fatalf("Send heartbeat failed: %v", err)
	}

	// Wait for server to receive heartbeat via channel notification
	select {
	case <-ts.heartbeatCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for server to receive heartbeat")
	}

	ts.mu.Lock()
	numHeartbeats := len(ts.heartbeats)
	ts.mu.Unlock()

	if numHeartbeats != 1 {
		t.Fatalf("Expected 1 heartbeat, got %d", numHeartbeats)
	}

	ts.mu.Lock()
	hb := ts.heartbeats[0]
	ts.mu.Unlock()

	if hb.Type != string(controltransport.MsgHeartbeat) {
		t.Errorf("Heartbeat Type = %q, want %q", hb.Type, controltransport.MsgHeartbeat)
	}
	if hb.WorkerId != "test-worker-002" {
		t.Errorf("Heartbeat WorkerId = %q, want %q", hb.WorkerId, "test-worker-002")
	}
}

func TestGRPCStreamTransport_ReceiveJobOffer(t *testing.T) {
	ts := newTestStreamServer()
	ts.sendJobOffer = true // Enable JobOffer after first heartbeat
	srv, addr := startTestGRPCServer(t, ts)
	defer srv.Stop()

	transport := NewGRPCStreamTransport(addr, "test-worker-003")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect
	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-003",
		WorkerName:      "test-worker",
		Hostname:        "test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}
	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Start receiving
	recvCh, _, err := transport.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Send a heartbeat to trigger the JobOffer on the server side
	heartbeatMsg := controltransport.ControlMessage{
		MessageID:       "hb-test-001",
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        "test-worker-003",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Payload: map[string]interface{}{
			"status": "idle",
		},
	}
	if err := transport.Send(ctx, heartbeatMsg); err != nil {
		t.Fatalf("Send heartbeat failed: %v", err)
	}

	// Wait for JobOffer message on receive channel
	var receivedJobOffer *controltransport.ControlMessage
	select {
	case msg, ok := <-recvCh:
		if !ok {
			t.Fatal("Receive channel closed unexpectedly")
		}
		receivedJobOffer = &msg
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for JobOffer message")
	}

	if receivedJobOffer == nil {
		t.Fatal("Received nil message")
	}
	if receivedJobOffer.Type != controltransport.MsgJobOffer {
		t.Errorf("Received message type = %q, want %q", receivedJobOffer.Type, controltransport.MsgJobOffer)
	}

	// Verify JobOffer payload contains job details
	jobID, ok := receivedJobOffer.Payload["job_id"].(string)
	if !ok || jobID != "test-job-001" {
		t.Errorf("JobOffer payload job_id = %q, want %q", jobID, "test-job-001")
	}
	jobType, ok := receivedJobOffer.Payload["job_type"].(string)
	if !ok || jobType != "render" {
		t.Errorf("JobOffer payload job_type = %q, want %q", jobType, "render")
	}
}

func TestGRPCStreamTransport_CloseSendsGoodbye(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestGRPCServer(t, ts)
	defer srv.Stop()

	transport := NewGRPCStreamTransport(addr, "test-worker-004")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-004",
		WorkerName:      "test-worker",
		Hostname:        "test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}
	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Close the transport
	if err := transport.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Wait for Goodbye to reach the server
	select {
	case <-ts.goodbyeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for server to receive Goodbye")
	}

	ts.mu.Lock()
	gotGoodbye := ts.gotGoodbye
	ts.mu.Unlock()

	if !gotGoodbye {
		t.Error("Server did not receive Goodbye message on transport Close")
	}

	// Verify transport state is disconnected
	if transport.state != stateDisconnected {
		t.Errorf("Transport state after close = %v, want stateDisconnected", transport.state)
	}
}

func TestGRPCStreamTransport_ConnectInvalidServer(t *testing.T) {
	// Connect to a non-existent gRPC endpoint
	transport := NewGRPCStreamTransport("127.0.0.1:19999", "test-worker-fail")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-fail",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	err := transport.Connect(ctx, hello)
	if err == nil {
		t.Error("Expected error connecting to non-existent server, got nil")
	}
}

func TestGRPCStreamTransport_SendBeforeConnect(t *testing.T) {
	transport := NewGRPCStreamTransport("127.0.0.1:0", "test-worker")

	ctx := context.Background()
	msg := controltransport.ControlMessage{
		Type: controltransport.MsgHeartbeat,
	}

	err := transport.Send(ctx, msg)
	if err != controltransport.ErrNotConnected {
		t.Errorf("Send before connect error = %v, want ErrNotConnected", err)
	}
}

func TestGRPCStreamTransport_ReceiveBeforeConnect(t *testing.T) {
	transport := NewGRPCStreamTransport("127.0.0.1:0", "test-worker")

	ctx := context.Background()
	_, _, err := transport.Receive(ctx)
	if err != controltransport.ErrNotConnected {
		t.Errorf("Receive before connect error = %v, want ErrNotConnected", err)
	}
}

// ---- mTLS Integration Tests (Phase 7) ----

// certsBaseDir resolves the path to the refactored/certs/ directory relative
// to the test package directory.
func certsBaseDir(t *testing.T) string {
	t.Helper()
	// Test runs from: refactored/RemoteCodex/native/worker-agent-go/internal/transport/
	// Target:         refactored/certs/
	// Path: 5 levels up → certs/
	abs, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "..", "certs"))
	if err != nil {
		t.Fatalf("cannot resolve certs directory: %v", err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("certs directory not found at %s: %v", abs, err)
	}
	return abs
}

// startTestMTLSServer creates a gRPC server with mTLS requiring client
// certificates signed by the test CA. Returns server + address.
func startTestMTLSServer(t *testing.T, srv pb.WorkerControlServer) (*grpc.Server, string) {
	t.Helper()

	certsDir := certsBaseDir(t)

	serverCert, err := tls.LoadX509KeyPair(
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
	)
	if err != nil {
		t.Fatalf("Load server cert: %v", err)
	}

	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatalf("Read CA cert: %v", err)
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("Failed to parse CA cert")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS12,
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	gsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	pb.RegisterWorkerControlServer(gsrv, srv)

	go func() {
		_ = gsrv.Serve(lis)
	}()

	return gsrv, lis.Addr().String()
}

// TestGRPCStreamTransport_mTLS_Handshake verifies the full mTLS handshake:
// client presents its certificate, server verifies it against the CA,
// and the Hello/HelloAck exchange completes successfully.
func TestGRPCStreamTransport_mTLS_Handshake(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestMTLSServer(t, ts)
	defer srv.Stop()

	certsDir := certsBaseDir(t)

	transport := NewGRPCStreamTransport(addr, "test-worker-mtls-001")
	if err := transport.WithTLS(
		filepath.Join(certsDir, "client.crt"),
		filepath.Join(certsDir, "client.key"),
		filepath.Join(certsDir, "ca.crt"),
	); err != nil {
		t.Fatalf("WithTLS failed: %v", err)
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-mtls-001",
		WorkerName:      "test-worker-mtls",
		Hostname:        "mtls-test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("mTLS Connect failed: %v", err)
	}

	// Verify server received Hello
	ts.mu.Lock()
	lastHello := ts.lastHello
	ts.mu.Unlock()

	if lastHello == nil {
		t.Fatal("Server did not receive Hello over mTLS")
	}
	if lastHello.WorkerId != "test-worker-mtls-001" {
		t.Errorf("Hello WorkerId = %q, want %q", lastHello.WorkerId, "test-worker-mtls-001")
	}

	// Verify transport is ready
	if transport.state != stateReady {
		t.Errorf("Transport state = %v, want stateReady", transport.state)
	}
}

// TestGRPCStreamTransport_mTLS_NoClientCert verifies that a client without a
// certificate is rejected by the mTLS server.
func TestGRPCStreamTransport_mTLS_NoClientCert(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestMTLSServer(t, ts)
	defer srv.Stop()

	// Transport WITHOUT TLS — uses insecure credentials
	transport := NewGRPCStreamTransport(addr, "test-worker-nocert")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-nocert",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	err := transport.Connect(ctx, hello)
	if err == nil {
		t.Error("Expected connection rejection for client without cert, got nil error")
	}
}

// TestGRPCStreamTransport_mTLS_WrongCA verifies that a client with a
// certificate signed by a different CA (self-signed, not by the test CA)
// is rejected by the mTLS server.
func TestGRPCStreamTransport_mTLS_WrongCA(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestMTLSServer(t, ts)
	defer srv.Stop()

	// Generate a self-signed cert NOT signed by the test CA
	wrongCert := generateSelfSignedCert(t)

	transport := NewGRPCStreamTransport(addr, "test-worker-wrongca")

	// Trust the server's CA (needed to verify the server during handshake)
	certsDir := certsBaseDir(t)
	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatalf("Read CA cert: %v", err)
	}
	serverCAPool := x509.NewCertPool()
	if !serverCAPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("Failed to parse CA cert for server trust")
	}

	// Present a self-signed client cert (NOT signed by test CA)
	transport.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{wrongCert},
		RootCAs:      serverCAPool,
		MinVersion:   tls.VersionTLS12,
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-wrongca",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	err = transport.Connect(ctx, hello)
	if err == nil {
		t.Error("Expected connection rejection for client with self-signed cert (wrong CA), got nil error")
	}
}

// generateSelfSignedCert creates a real self-signed TLS certificate that
// is NOT signed by the test CA. Used for negative mTLS tests.
func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "wrong-ca-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("Create certificate: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
}

// TestGRPCStreamTransport_mTLS_HeartbeatSend verifies heartbeat over mTLS.
func TestGRPCStreamTransport_mTLS_HeartbeatSend(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr := startTestMTLSServer(t, ts)
	defer srv.Stop()

	certsDir := certsBaseDir(t)

	transport := NewGRPCStreamTransport(addr, "test-worker-mtls-hb")
	if err := transport.WithTLS(
		filepath.Join(certsDir, "client.crt"),
		filepath.Join(certsDir, "client.key"),
		filepath.Join(certsDir, "ca.crt"),
	); err != nil {
		t.Fatalf("WithTLS failed: %v", err)
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-mtls-hb",
		WorkerName:      "test-worker-mtls",
		Hostname:        "mtls-test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}
	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	heartbeatMsg := controltransport.ControlMessage{
		MessageID:       "hb-mtls-001",
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        "test-worker-mtls-hb",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Payload: map[string]interface{}{
			"status": "idle",
		},
	}

	if err := transport.Send(ctx, heartbeatMsg); err != nil {
		t.Fatalf("Send heartbeat over mTLS failed: %v", err)
	}

	// Wait for server to process the heartbeat
	select {
	case <-ts.heartbeatCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for heartbeat over mTLS")
	}

	ts.mu.Lock()
	numHeartbeats := len(ts.heartbeats)
	ts.mu.Unlock()
	if numHeartbeats != 1 {
		t.Fatalf("Expected 1 heartbeat over mTLS, got %d", numHeartbeats)
	}
}
