package grpcserver

import (
	"context"
	"net"
	"strings"
	"testing"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// minimalProtocolHandler builds a Handler suitable for protocol-version
// rejection tests. Insecure mode is allowed; no credential or allowlist
// restriction — the protocol-version check is the first gate that fails.
func minimalProtocolHandler(t *testing.T) *Handler {
	t.Helper()
	h := NewHandler(
		nil, // registry
		nil, // cmdMgr
		nil, // jobsRepo
		nil, // taskRepo
		nil, // taskAttemptRepo
		nil, // artifactSvc
		nil, // dbStore
		&HandlerConfig{
			PushMode:      true,
			AllowInsecure: true,
		},
	)
	return h
}

// startProtocolTestServer creates an insecure gRPC server + client stream
// and returns the stream and a cleanup function. Reduces boilerplate across
// the 4 protocol-version tests.
func startProtocolTestServer(t *testing.T, h *Handler) (pb.WorkerControl_StreamClient, func()) {
	t.Helper()

	srv := grpc.NewServer()
	pb.RegisterWorkerControlServer(srv, h)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("grpc.NewClient: %v", err)
	}

	client := pb.NewWorkerControlClient(conn)
	// Use background context — the per-test timeouts are handled by the
	// test binary (-timeout flag). Deferring cancel here would cancel
	// the context before the caller can use the returned stream.
	stream, err := client.Stream(context.Background())
	if err != nil {
		conn.Close()
		srv.Stop()
		t.Fatalf("Stream: %v", err)
	}

	return stream, func() {
		stream.CloseSend()
		conn.Close()
		srv.Stop()
		lis.Close()
	}
}

func sendHello(t *testing.T, stream pb.WorkerControl_StreamClient, workerID, protocolVersion string) {
	t.Helper()
	sendHelloWithCapabilities(t, stream, workerID, protocolVersion, map[string]interface{}{
		controltransport.CapabilityCanonicalPayloadV2: true,
	})
}

func sendHelloWithCapabilities(t *testing.T, stream pb.WorkerControl_StreamClient, workerID, protocolVersion string, capabilities map[string]interface{}) {
	t.Helper()
	canonical := make(map[string]interface{}, len(capabilities)+2)
	canonical["schema_version"] = controltransport.CapabilitySchemaVersion
	if _, present := capabilities["executors"]; !present {
		canonical["executors"] = []interface{}{}
	}
	for key, value := range capabilities {
		canonical[key] = value
	}
	caps, err := structpb.NewStruct(canonical)
	if err != nil {
		t.Fatalf("NewStruct capabilities: %v", err)
	}
	env := &pb.WorkerToMasterEnvelope{
		MessageId:       "test-hello-" + workerID,
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: protocolVersion,
		Msg: &pb.WorkerToMasterEnvelope_Hello{
			Hello: &pb.Hello{
				WorkerName:   workerID,
				Version:      "1.0.0",
				Capabilities: caps,
			},
		},
	}
	if err := stream.Send(env); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
}

func assertRejectedWithFailedPrecondition(t *testing.T, err error, label string) {
	t.Helper()
	if err == nil {
		t.Fatalf("[%s] expected error, got nil", label)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("[%s] expected gRPC status error, got %T: %v", label, err, err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("[%s] expected FailedPrecondition, got %v (%q)", label, st.Code(), st.Message())
	}
	if !strings.Contains(st.Message(), "protocol_version") {
		t.Errorf("[%s] error message should mention protocol_version: %q", label, st.Message())
	}
}

// TestStream_RejectsEmptyProtocolVersion verifies that a Hello with an
// empty protocol_version is rejected with FailedPrecondition.
func TestStream_RejectsEmptyProtocolVersion(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHello(t, stream, "test-worker-empty", "")
	_, err := stream.Recv()
	assertRejectedWithFailedPrecondition(t, err, "empty")
}

// TestStream_RejectsLegacyProtocolVersion verifies that a Hello with a
// legacy protocol_version (e.g. "2026-06-worker-v1") is rejected.
func TestStream_RejectsLegacyProtocolVersion(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHello(t, stream, "test-worker-legacy", "2026-06-worker-v1")
	_, err := stream.Recv()
	assertRejectedWithFailedPrecondition(t, err, "legacy")
}

// TestStream_RejectsUnknownProtocolVersion verifies that arbitrary
// unknown protocol versions are rejected.
func TestStream_RejectsUnknownProtocolVersion(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHello(t, stream, "test-worker-unknown", "v9-future-experimental")
	_, err := stream.Recv()
	assertRejectedWithFailedPrecondition(t, err, "unknown")
}

// TestStream_AcceptsCurrentProtocolVersion verifies that a Hello with
// ProtocolVersionCurrent ("v3") is accepted and receives a HelloAck.
func TestStream_RejectsWorkerWithoutCanonicalPayloadCapability(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHelloWithCapabilities(t, stream, "test-worker-noncanonical", controltransport.ProtocolVersionCurrent, map[string]interface{}{})
	_, err := stream.Recv()
	if err == nil {
		t.Fatal("expected non-canonical worker to be rejected")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if !strings.Contains(st.Message(), controltransport.CapabilityCanonicalPayloadV2) {
		t.Fatalf("error should mention %s, got %q", controltransport.CapabilityCanonicalPayloadV2, st.Message())
	}
}

func TestStream_RejectsFalseCanonicalPayloadCapability(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHelloWithCapabilities(t, stream, "test-worker-false-canonical", controltransport.ProtocolVersionCurrent, map[string]interface{}{
		controltransport.CapabilityCanonicalPayloadV2: false,
	})
	_, err := stream.Recv()
	if err == nil {
		t.Fatal("expected worker with false canonical payload capability to be rejected")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if !strings.Contains(st.Message(), controltransport.CapabilityCanonicalPayloadV2) {
		t.Fatalf("error should mention %s, got %q", controltransport.CapabilityCanonicalPayloadV2, st.Message())
	}
}

func TestStream_RejectsExecutorVersionOneWithoutCanonicalPayloadCapability(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHelloWithCapabilities(t, stream, "test-worker-executor-v1-legacy", controltransport.ProtocolVersionCurrent, map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{"id": "scene.composite.v1", "version": 1},
		},
	})
	_, err := stream.Recv()
	if err == nil {
		t.Fatal("expected executorVersion=1 worker without canonical payload capability to be rejected")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if !strings.Contains(st.Message(), controltransport.CapabilityCanonicalPayloadV2) {
		t.Fatalf("error should mention %s, got %q", controltransport.CapabilityCanonicalPayloadV2, st.Message())
	}
}

func TestStream_AcceptsExecutorVersionOneWithCanonicalPayloadCapability(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHelloWithCapabilities(t, stream, "test-worker-executor-v1-canonical", controltransport.ProtocolVersionCurrent, map[string]interface{}{
		"executors": []interface{}{
			map[string]interface{}{"id": "scene.composite.v1", "version": 1},
		},
		controltransport.CapabilityCanonicalPayloadV2: true,
	})
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("executorVersion=1 worker with canonical payload capability was rejected: %v", err)
	}
	if resp.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %T", resp.Msg)
	}
}

func TestStream_AcceptsCurrentProtocolVersion(t *testing.T) {
	h := minimalProtocolHandler(t)
	stream, cleanup := startProtocolTestServer(t, h)
	defer cleanup()

	sendHello(t, stream, "test-worker-current", controltransport.ProtocolVersionCurrent)

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected HelloAck for current protocol version, got error: %v", err)
	}
	if resp.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %T", resp.Msg)
	}
}
