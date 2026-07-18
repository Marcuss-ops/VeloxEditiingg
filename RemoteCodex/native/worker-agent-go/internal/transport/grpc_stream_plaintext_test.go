package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testStreamServer implements a minimal pb.WorkerControlServer for integration testing.
// Uses typed envelopes (WorkerToMasterEnvelope / MasterToWorkerEnvelope).
type testStreamServer struct {
	pb.UnimplementedWorkerControlServer

	mu           sync.Mutex
	lastHello    *pb.Hello
	lastWorkerID string
	heartbeats   []*pb.Heartbeat
	jobOfferCh   chan struct{} // signals when to send a JobOffer
	sendJobOffer bool
	gotGoodbye   bool
	heartbeatCh  chan struct{} // closed after first heartbeat
	goodbyeCh    chan struct{} // closed on Goodbye

	// disconnect-reconnect testing: track how many connections were made
	hbCount   int
	connCount int
}

func newTestStreamServer() *testStreamServer {
	return &testStreamServer{}
}

func (s *testStreamServer) Stream(stream grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]) error {
	// Reset per-connection state so reconnections don't panic on double-close
	// and heartbeat counting starts fresh for each connection.
	s.mu.Lock()
	s.heartbeatCh = make(chan struct{})
	s.goodbyeCh = make(chan struct{})
	s.jobOfferCh = make(chan struct{}, 1)
	s.hbCount = 0
	s.mu.Unlock()

	// Wait for Hello
	env, err := stream.Recv()
	if err != nil {
		return err
	}

	hello := env.GetHello()
	if hello == nil {
		return fmt.Errorf("expected hello, got %T", env.Msg)
	}

	s.mu.Lock()
	s.lastHello = hello
	s.lastWorkerID = env.WorkerId
	s.connCount++
	connNum := s.connCount
	s.mu.Unlock()

	// Send typed HelloAck
	ack := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("ack-conn-%d", connNum),
		WorkerId:        env.WorkerId,
		SessionId:       fmt.Sprintf("test-session-%d", connNum),
		SequenceNumber:  1,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: env.ProtocolVersion,
		Msg:             &pb.MasterToWorkerEnvelope_HelloAck{HelloAck: &pb.HelloAck{}},
	}
	if err := stream.Send(ack); err != nil {
		return err
	}

	// If configured to send a JobOffer, do it after a short delay
	if s.sendJobOffer {
		workerID := env.WorkerId
		jobOfferID := fmt.Sprintf("test-job-%03d", connNum)
		sessID := fmt.Sprintf("test-session-%d", connNum)
		jobCh := s.jobOfferCh // capture per-connection channel to avoid racing with new connections
		go func() {
			select {
			case <-jobCh:
				jobOfferEnv := &pb.MasterToWorkerEnvelope{
					MessageId:       fmt.Sprintf("job-offer-%03d", connNum),
					WorkerId:        workerID,
					SessionId:       sessID,
					SentAt:          timestamppb.Now(),
					ProtocolVersion: env.ProtocolVersion,
					Msg: &pb.MasterToWorkerEnvelope_TaskOffer{
						TaskOffer: &pb.TaskOffer{
							TaskId:   "test-task-001",
							JobId:    jobOfferID,
							LeaseId:  "lease-001",
							TaskSpec: nil,
						},
					},
				}
				_ = stream.Send(jobOfferEnv)
			case <-time.After(5 * time.Second):
			}
		}()
	}

	// Main loop: receive typed messages until stream closes
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream recv: %w", err)
		}

		switch m := env.Msg.(type) {
		case *pb.WorkerToMasterEnvelope_Heartbeat:
			s.mu.Lock()
			s.heartbeats = append(s.heartbeats, m.Heartbeat)
			s.hbCount++
			n := s.hbCount
			s.mu.Unlock()

			if n == 1 {
				close(s.heartbeatCh)
			}

			if s.sendJobOffer && n == 1 {
				select {
				case s.jobOfferCh <- struct{}{}:
				default:
				}
			}

		case *pb.WorkerToMasterEnvelope_Goodbye:
			s.mu.Lock()
			s.gotGoodbye = true
			s.mu.Unlock()
			close(s.goodbyeCh)
			return nil

		case *pb.WorkerToMasterEnvelope_TaskAccepted:
			// Track it; no action needed for test

		case *pb.WorkerToMasterEnvelope_TaskRejected:
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
	lastWorkerID := ts.lastWorkerID
	ts.mu.Unlock()

	if lastHello == nil {
		t.Fatal("Server did not receive Hello message")
	}
	if lastWorkerID != "test-worker-001" {
		t.Errorf("Hello WorkerId = %q, want %q", lastWorkerID, "test-worker-001")
	}
	if lastHello.GetWorkerName() != "test-worker" {
		t.Errorf("Hello WorkerName = %q, want %q", lastHello.GetWorkerName(), "test-worker")
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
		TypedPayload: &pb.Heartbeat{
			Status:          "idle",
			WorkerName:      "test-worker",
			ActiveJobsCount: 0,
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

	if hb.GetStatus() != "idle" {
		t.Errorf("Heartbeat Status = %q, want %q", hb.GetStatus(), "idle")
	}
	if hb.GetWorkerName() != "test-worker" {
		t.Errorf("Heartbeat WorkerName = %q, want %q", hb.GetWorkerName(), "test-worker")
	}
}

func TestGRPCStreamTransport_ReceiveTaskOffer(t *testing.T) {
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
		TypedPayload: &pb.Heartbeat{
			Status: "idle",
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
	if receivedJobOffer.Type != controltransport.MsgTaskOffer {
		t.Errorf("Received message type = %q, want %q", receivedJobOffer.Type, controltransport.MsgTaskOffer)
	}

	// Verify TaskOffer typed payload contains task details
	offer, ok := receivedJobOffer.TypedPayload.(*pb.TaskOffer)
	if !ok || offer == nil {
		t.Fatalf("TaskOffer TypedPayload is not *pb.TaskOffer: %T", receivedJobOffer.TypedPayload)
	}
	if offer.GetJobId() != "test-job-001" {
		t.Errorf("TaskOffer JobId = %q, want %q", offer.GetJobId(), "test-job-001")
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
