package transport

import (
	"context"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
)

// ---- End-to-End: Disconnect + Reconnect ---- //

// TestGRPCStreamTransport_DisconnectReconnect verifies that a worker can
// disconnect from the master and reconnect cleanly with no data loss.
//
// Flow:
//  1. Connect transport A, complete Hello handshake, start Receive
//  2. Send heartbeat → receive JobOffer → verify it
//  3. Close transport A (simulates network failure)
//  4. Connect transport B with a fresh instance
//  5. Send heartbeat → receive a new JobOffer → verify it
//  6. Verify both connections were tracked by the server
func TestGRPCStreamTransport_DisconnectReconnect(t *testing.T) {
	ts := newTestStreamServer()
	ts.sendJobOffer = true
	srv, addr := startTestGRPCServer(t, ts)
	defer srv.Stop()

	// ---- Connection A ----
	transportA := NewGRPCStreamTransport(addr, "test-worker-e2e")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-e2e",
		WorkerName:      "e2e-worker",
		Hostname:        "e2e-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}
	if err := transportA.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect A failed: %v", err)
	}

	// Start receiving on transport A
	recvChA, errChA, err := transportA.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive A failed: %v", err)
	}

	// Send heartbeat A to trigger JobOffer A
	hbA := controltransport.ControlMessage{
		MessageID:       "hb-a-001",
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        "test-worker-e2e",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		TypedPayload: &pb.Heartbeat{
			Status: "idle",
		},
	}
	if err := transportA.Send(ctx, hbA); err != nil {
		t.Fatalf("Send heartbeat A failed: %v", err)
	}

	// Receive JobOffer A
	var offerA *controltransport.ControlMessage
	select {
	case msg, ok := <-recvChA:
		if !ok {
			t.Fatal("recvCh A closed unexpectedly")
		}
		offerA = &msg
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for JobOffer A")
	}
	if offerA.Type != controltransport.MsgTaskOffer {
		t.Fatalf("Offer A: want MsgTaskOffer, got %s", offerA.Type)
	}
	offerAPB, _ := offerA.TypedPayload.(*pb.TaskOffer)
	if offerAPB == nil || offerAPB.GetJobId() != "test-job-001" {
		t.Fatalf("Offer A: want test-job-001, got %v", offerAPB)
	}

	// ---- Disconnect A ----
	if err := transportA.Close(); err != nil {
		t.Fatalf("Close A failed: %v", err)
	}

	// Verify the error channel gets a signal (stream terminated)
	select {
	case <-errChA:
		// Expected: errCh closed after disconnect
	case <-time.After(2 * time.Second):
		t.Fatal("errCh A did not close after transport close")
	}

	// Short pause to ensure server-side cleanup
	time.Sleep(100 * time.Millisecond)

	// ---- Reconnect: transport B ----
	transportB := NewGRPCStreamTransport(addr, "test-worker-e2e")

	if err := transportB.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect B failed: %v", err)
	}
	defer transportB.Close()

	// Start receiving on transport B
	recvChB, _, err := transportB.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive B failed: %v", err)
	}

	// Send heartbeat B to trigger JobOffer B
	hbB := controltransport.ControlMessage{
		MessageID:       "hb-b-001",
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        "test-worker-e2e",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		TypedPayload: &pb.Heartbeat{
			Status: "idle",
		},
	}
	if err := transportB.Send(ctx, hbB); err != nil {
		t.Fatalf("Send heartbeat B failed: %v", err)
	}

	// Receive JobOffer B
	var offerB *controltransport.ControlMessage
	select {
	case msg, ok := <-recvChB:
		if !ok {
			t.Fatal("recvCh B closed unexpectedly")
		}
		offerB = &msg
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for JobOffer B")
	}
	if offerB.Type != controltransport.MsgTaskOffer {
		t.Fatalf("Offer B: want MsgTaskOffer, got %s", offerB.Type)
	}
	offerBPB, _ := offerB.TypedPayload.(*pb.TaskOffer)
	if offerBPB == nil || offerBPB.GetJobId() != "test-job-002" {
		t.Fatalf("Offer B: want test-job-002 (2nd connection), got %v", offerBPB)
	}

	// ---- Verify server saw both connections ----
	ts.mu.Lock()
	connCount := ts.connCount
	ts.mu.Unlock()

	if connCount != 2 {
		t.Errorf("Server connCount = %d, want 2 (both connections A and B)", connCount)
	}
}
