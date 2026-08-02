// Command dev-hello-client / client_stream.go
//
// Transport + stream helpers for the synthetic Hello/HelloAck client:
// dial options (plaintext vs mTLS), capability struct building, the
// heartbeat loop, Goodbye send, and the PR 2 shutdown-state machine
// (recvResult, shutdownState, isExpectedLocalClose, drainStream).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "velox-shared/controltransport/pb"
)

// buildDialOptions returns the gRPC dial options for either plaintext
// (inline `insecure.NewCredentials()`) or mTLS. Pulled into a helper so
// the main flow stays linear and the helper is independently testable.
func buildDialOptions(certPath, keyPath, caPath string) ([]grpc.DialOption, error) {
	if certPath == "" && keyPath == "" && caPath == "" {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load client keypair (%s, %s): %w", certPath, keyPath, err)
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse CA PEM at %s", caPath)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
		// ServerName intentionally left empty so the dialer's
		// target host is used for SAN matching — matches the worker's
		// transport_factory.go behavior.
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))}, nil
}

// buildCapabilities constructs the typed `google.protobuf.Struct` that
// the master reads for executor-discovery / ClaimNext filtering. Kept
// intentionally small: one synthetic executor entry so the master's
// heartbeat capabilities-shape validation accepts the payload even
// when no real executor registry is wired.
func buildCapabilities(executorID string) *structpb.Struct {
	item, err := structpb.NewStruct(map[string]any{
		"id":      executorID,
		"version": 1,
	})
	if err != nil {
		// structpb.NewStruct on a primitive map cannot fail; the
		// error is purely a runtime diagnostic if the map contains
		// unsupported types. Falling back to an empty struct keeps
		// the wire payload valid.
		return &structpb.Struct{}
	}
	list, err := structpb.NewList([]any{structpb.NewStructValue(item)})
	if err != nil {
		return &structpb.Struct{}
	}
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"executors": structpb.NewListValue(list),
		},
	}
}

// runHeartbeatLoop ticks at HeartbeatInterval and sends typed Heartbeats
// until ctx is done. Sequence numbers are monotonic per flow; master
// seq-tracking is best-effort and short-circuits on duplicates, so
// being out-of-order is harmless for a dev probe.
func runHeartbeatLoop(
	ctx context.Context,
	stream grpc.BidiStreamingClient[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope],
	done chan<- struct{},
	p runParams,
	seq *int64,
	logger *log.Logger,
) {
	defer close(done)
	ticker := time.NewTicker(p.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			*seq++
			hb := &pb.WorkerToMasterEnvelope{
				MessageId:       fmt.Sprintf("hb-%d", t.UnixNano()),
				WorkerId:        p.WorkerID,
				SequenceNumber:  *seq,
				SentAt:          timestamppb.New(t),
				ProtocolVersion: p.ProtocolVersion,
				Msg: &pb.WorkerToMasterEnvelope_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						WorkerName:      p.WorkerName,
						WorkerStatus:    "idle",
						Status:          "idle",
						ProtocolVersion: p.ProtocolVersion,
					},
				},
			}
			if err := stream.Send(hb); err != nil {
				logger.Printf("heartbeat send failed (likely master closed stream): %v", err)
				return
			}
			logger.Printf("heartbeat sent (seq=%d, ts=%s)", *seq, t.Format(time.RFC3339))
		}
	}
}

// sendGoodbye is best-effort: a send failure during shutdown is logged
// but does not block exit (the master would treat stream close as
// implicit Goodbye anyway).
func sendGoodbye(stream grpc.BidiStreamingClient[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope], seq int64, workerID string, logger *log.Logger) {
	msg := &pb.WorkerToMasterEnvelope{
		WorkerId:       workerID,
		SequenceNumber: seq,
		SentAt:         timestamppb.Now(),
		Msg:            &pb.WorkerToMasterEnvelope_Goodbye{Goodbye: &pb.Goodbye{}},
	}
	if err := stream.Send(msg); err != nil {
		logger.Printf("goodbye send failed (non-fatal): %v", err)
	}
}

// recvResult is the (env, err) tuple a streaming Recv produces. Lifted
// to package scope so drainStream's signature can declare it without
// relying on Go's anonymous-struct-type identity (structurally it works
// but reviewers + future contributors trip over it).
type recvResult struct {
	env *pb.MasterToWorkerEnvelope
	err error
}

// shutdownState is the PR 2 flag board. All three bits must be true
// for drainStream to classify a terminal recv err as a normal local
// close (exit 0). Each bit flips in exactly one location:
//
//   - helloAckReceived: in run()'s await-and-match loop, the moment
//     the master emits a typed HelloAck.
//   - goodbyeSent:      in drainStream, immediately after
//     stream.CloseSend() — CloseSend IS the wire-level goodbye.
//   - localCancelSent:  in run(), immediately before each drainStream
//     call site (fast-mode exit / SIGINT / window-end / hb-loop-self-exit).
type shutdownState struct {
	helloAckReceived bool
	goodbyeSent      bool
	localCancelSent  bool
}

// isExpectedLocalClose is the single source of truth for classifying
// drainStream's terminal recv err. Returns true iff ALL THREE:
//
//  1. state is non-nil AND helloAckReceived AND goodbyeSent AND
//     localCancelSent (the worker registered, said goodbye, and
//     initiated its own teardown — NOT a server-driven kick)
//  2. err is either nil OR sits in the canonical normal-exit
//     taxonomy (context.Canceled, io.EOF, gRPC codes.Canceled,
//     gRPC codes.DeadlineExceeded).
//
// Anything else (e.g. PermissionDenied mid-session, Unauthenticated
// from credential rotation, Unavailable from a flaky master) falls
// through to false and drainStream propagates the error with a WARN
// so the operator sees the real cause.
func isExpectedLocalClose(err error, state *shutdownState) bool {
	if state == nil {
		return false
	}
	if !state.helloAckReceived || !state.goodbyeSent || !state.localCancelSent {
		return false
	}
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Canceled, codes.DeadlineExceeded:
			return true
		}
	}
	return false
}

// drainStream closes the bidi stream and lets the recv goroutine drain.
// Classification of the terminal recv err is delegated to
// isExpectedLocalClose (see above). If the err is unexpected, it is
// returned to run() and surfaced via main's Fatalf path.
//
// PR 2 invariants:
//
//   - state.goodbyeSent = true is flipped unconditionally right after
//     stream.CloseSend(). The wire-level CloseSend IS the gRPC goodbye
//     the master mirrors, so flipping this in one centralized place
//     keeps both fast-mode and heartbeat-mode teardowns on the same
//     flag board.
//   - A nil stream is allowed: drainStream becomes a no-op drain of
//     recvCh. This is benign — it just means the connection failed
//     before the bidi stream was established, which is the same path
//     run()'s pre-stream error returns already take.
func drainStream(
	stream grpc.BidiStreamingClient[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope],
	recvDone <-chan struct{},
	recvCh <-chan recvResult,
	state *shutdownState,
	logger *log.Logger,
) error {
	if stream != nil {
		_ = stream.CloseSend()
	}
	// CloseSend IS the wire-level goodbye. Flipping goodbyeSent here
	// unifies the fast-mode and heartbeat-mode teardowns: both call
	// drainStream after their respective cleanup, and both result in
	// a CloseSend hitting the master. drainStream is called exactly
	// once per run() invocation, so no double-flip guard is needed.
	state.goodbyeSent = true

	// Wait briefly for recv goroutine to drain any last server frame.
	// Tradeoff: a hard cancel() on timeout would close the gRPC
	// transport immediately but might lose the last recvResult. The
	// 2s window is short enough that an operator inspecting logs gets
	// a fast exit; long enough that a graceful master-side close
	// completes naturally before we abandon it.
	select {
	case <-recvDone:
	case <-time.After(2 * time.Second):
		logger.Printf("WARN: recv goroutine did not exit within 2s after CloseSend (likely master is wedged on the auth path)")
	}

	// Drain any pending events so we don't leak the channel buffer.
	for {
		select {
		case r := <-recvCh:
			if r.err == nil {
				continue
			}
			if isExpectedLocalClose(r.err, state) {
				logger.Printf("recv: normal exit (%v)", r.err)
				return nil
			}
			logger.Printf("WARN: recv terminal error after HelloAck: %v", r.err)
			return r.err
		default:
			return nil
		}
	}
}
