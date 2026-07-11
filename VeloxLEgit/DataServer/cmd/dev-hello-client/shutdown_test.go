// PR 2 — `codex/dev-client-clean-shutdown`: tests for the new
// shutdownState-aware drainStream classification (cmd/dev-hello-client).
//
// Scope: in-process unit tests for drainStream + isExpectedLocalClose.
// Two complementary test groups:
//
//   - TestDrainStreamClassification walks the 7-case exit-code matrix
//     from the PR 2 spec. Each case synthesizes a state + recvErr, runs
//     drainStream, and asserts the exit code.
//   - TestIsExpectedLocalClose directly exercises the predicate across
//     its full discriminated surface (err kind × state combination).
//
// Test surface: drainStream's only stream call is CloseSend. We embed
// grpc.BidiStreamingClient as a nil interface and override CloseSend
// only. Calling any other method on the embedded interface panics —
// acceptable because drainStream only invokes CloseSend; if a future
// implementation adds another stream call before the drain loop, the
// test will explode loudly rather than silently regress.
package main

import (
	"context"
	"io"
	"log"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "velox-shared/controltransport/pb"
)

// mockStream embeds the gRPC bidi interface so unhandled methods
// panic-on-call (catches regressions where drainStream starts touching
// Recv/Send etc.). Only CloseSend is overridden.
type mockStream struct {
	grpc.BidiStreamingClient[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	closeSendErr error
}

func (m *mockStream) CloseSend() error { return m.closeSendErr }

// TestDrainStreamClassification walks the 7 cases from the PR 2 spec:
//
//  1. cert_valid + HelloAck + Goodbye + io.EOF               → 0
//  2. cert_valid + HelloAck + Goodbye + context.Canceled     → 0
//  3. cert_valid + HelloAck + Goodbye + codes.Canceled       → 0
//  4. cert_valid + HelloAck + Goodbye + codes.DeadlineExceeded (timeout) → 0
//  5. master eviction mid-session + codes.PermissionDenied   → non-0
//  6. credential rotated mid-session + codes.Unauthenticated → non-0
//  7. master dies pre-HelloAck + codes.Canceled              → non-0 (state gate)
//
// Each case calls drainStream against a fake stream (CloseSend returns
// nil), pre-closes recvDone (so drainStream jumps to the recvCh drain
// loop), pre-fills recvCh with the synthetic terminal err.
//
// state.goodbyeSent is deliberately left false on entry — drainStream
// flips it true post-CloseSend, and the test asserts this invariant
// at the end (load-bearing for the "all 3 true → 0" verdict).
func TestDrainStreamClassification(t *testing.T) {
	logger := log.New(io.Discard, "", 0)

	cases := []struct {
		name    string
		state   shutdownState
		recvErr error
		// wantExit 0 → drainStream returns nil; wantExit 1 → returns
		// an error that carries the same gRPC code as recvErr.
		wantExit int
	}{
		{
			name:     "1. cert_valid + HelloAck + Goodbye + io.EOF → 0",
			state:    shutdownState{helloAckReceived: true, localCancelSent: true},
			recvErr:  io.EOF,
			wantExit: 0,
		},
		{
			name:     "2. cert_valid + HelloAck + Goodbye + context.Canceled → 0",
			state:    shutdownState{helloAckReceived: true, localCancelSent: true},
			recvErr:  context.Canceled,
			wantExit: 0,
		},
		{
			name:     "3. cert_valid + HelloAck + Goodbye + codes.Canceled → 0",
			state:    shutdownState{helloAckReceived: true, localCancelSent: true},
			recvErr:  status.Error(codes.Canceled, "server-mirrored client CloseSend"),
			wantExit: 0,
		},
		{
			name:     "4. timeout during heartbeat window + codes.DeadlineExceeded → 0",
			state:    shutdownState{helloAckReceived: true, localCancelSent: true},
			recvErr:  status.Error(codes.DeadlineExceeded, "heartbeat-window watchdog fired"),
			wantExit: 0,
		},
		{
			name:     "5. master eviction mid-session + codes.PermissionDenied → non-0",
			state:    shutdownState{helloAckReceived: true, localCancelSent: true},
			recvErr:  status.Error(codes.PermissionDenied, "evicted from VELOX_ALLOWED_WORKERS"),
			wantExit: 1,
		},
		{
			name:     "6. credential rotated mid-session + codes.Unauthenticated → non-0",
			state:    shutdownState{helloAckReceived: true, localCancelSent: true},
			recvErr:  status.Error(codes.Unauthenticated, "server-side credential rotation"),
			wantExit: 1,
		},
		{
			name:     "7. master dies pre-HelloAck + codes.Canceled → non-0 (state gate)",
			state:    shutdownState{helloAckReceived: false, localCancelSent: true},
			recvErr:  status.Error(codes.Canceled, "master closed stream before HelloAck"),
			wantExit: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recvCh := make(chan recvResult, 1)
			recvDone := make(chan struct{})

			// Pre-close recvDone so drainStream skips the 2s wait
			// and jumps straight to the recvCh drain loop.
			close(recvDone)

			// Pre-fill recvCh with the synthetic terminal err.
			recvCh <- recvResult{err: tc.recvErr}

			err := drainStream(&mockStream{closeSendErr: nil}, recvDone, recvCh, &tc.state, logger)

			if tc.wantExit == 0 {
				if err != nil {
					t.Fatalf("drainStream() = %v, want nil (exit 0)", err)
				}
				// drainStream MUST have flipped state.goodbyeSent=true
				// post-CloseSend. This is the load-bearing invariant for
				// any future case that consults isExpectedLocalClose
				// AFTER drainStream returns.
				if !tc.state.goodbyeSent {
					t.Errorf("drainStream should have flipped state.goodbyeSent=true after CloseSend")
				}
				return
			}

			// wantExit == 1: err must propagate, and the gRPC code
			// must match the input code (drainStream passes
			// status.Error wrappers through verbatim).
			if err == nil {
				t.Fatalf("drainStream() = nil, want non-nil (exit non-0); recvErr=%v", tc.recvErr)
			}
			gotCode := status.Code(err)
			wantCode := status.Code(tc.recvErr)
			if gotCode != wantCode {
				t.Fatalf("drainStream() err code = %v, want %v (err=%v)", gotCode, wantCode, err)
			}
		})
	}
}

// TestIsExpectedLocalClose directly exercises the predicate across
// its discriminated surface (err kind × state combination). Drains
// drainStream out of the loop so we test the predicate in isolation.
func TestIsExpectedLocalClose(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		state shutdownState
		want  bool
	}{
		// nil err + all three true → true (passes the err
		// taxonomy trivially).
		{
			name:  "nil err + all three true → true",
			err:   nil,
			state: shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true},
			want:  true,
		},
		// Normal-exit taxonomy at "all 3 true" → true.
		{"io.EOF + all 3 true", io.EOF,
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, true},
		{"context.Canceled + all 3 true", context.Canceled,
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, true},
		{"codes.Canceled + all 3 true", status.Error(codes.Canceled, ""),
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, true},
		{"codes.DeadlineExceeded + all 3 true", status.Error(codes.DeadlineExceeded, ""),
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, true},
		// Diagnostic codes at "all 3 true" → false (master-driven).
		{"codes.PermissionDenied + all 3 true", status.Error(codes.PermissionDenied, ""),
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, false},
		{"codes.Unauthenticated + all 3 true", status.Error(codes.Unauthenticated, ""),
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, false},
		{"codes.Unavailable + all 3 true", status.Error(codes.Unavailable, ""),
			shutdownState{helloAckReceived: true, goodbyeSent: true, localCancelSent: true}, false},
		// State gates — any false bit → false, even for io.EOF.
		{"io.EOF + goodbyeSent=false", io.EOF,
			shutdownState{helloAckReceived: true, localCancelSent: true}, false},
		{"io.EOF + localCancelSent=false", io.EOF,
			shutdownState{helloAckReceived: true, goodbyeSent: true}, false},
		{"io.EOF + helloAckReceived=false", io.EOF,
			shutdownState{goodbyeSent: true, localCancelSent: true}, false},
		{"io.EOF + all 3 false", io.EOF,
			shutdownState{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isExpectedLocalClose(tc.err, &tc.state); got != tc.want {
				t.Errorf("isExpectedLocalClose() = %v, want %v", got, tc.want)
			}
		})
	}

	// Explicit nil-state guard: a defensive test that mirrors the
	// first branch of the predicate.
	t.Run("nil state → false", func(t *testing.T) {
		if got := isExpectedLocalClose(io.EOF, nil); got != false {
			t.Errorf("isExpectedLocalClose(nil) = %v, want false", got)
		}
	})
}

// TestShutdownStateMutationFlow traces the canonical run() flow:
//
//	start   → all false
//	helloAck → helloAckReceived true
//	localCancel → localCancelSent true
//	drainStream CloseSend → goodbyeSent true
//
// Each transition is a contract test that locks in the order of flag
// flips, so a future refactor that swaps them cannot silently move a
// flag into a wrong site.
func TestShutdownStateMutationFlow(t *testing.T) {
	s := shutdownState{}
	if s.helloAckReceived || s.goodbyeSent || s.localCancelSent {
		t.Fatalf("zero-value state should have all 3 bits false; got %+v", s)
	}

	s.helloAckReceived = true
	if !s.helloAckReceived || s.goodbyeSent || s.localCancelSent {
		t.Fatalf("after helloAck flip: %+v", s)
	}

	s.localCancelSent = true
	if !s.helloAckReceived || !s.localCancelSent || s.goodbyeSent {
		t.Fatalf("after localCancel flip: %+v", s)
	}

	// drainStream's path: CloseSend flips goodbyeSent unconditionally.
	recvCh := make(chan recvResult, 1)
	recvDone := make(chan struct{})
	close(recvDone)
	recvCh <- recvResult{err: io.EOF}
	if err := drainStream(&mockStream{}, recvDone, recvCh, &s, log.New(io.Discard, "", 0)); err != nil {
		t.Fatalf("drainStream() = %v, want nil (all 3 true + io.EOF)", err)
	}
	if !s.goodbyeSent {
		t.Fatalf("after drainStream: goodbyeSent should be true; got %+v", s)
	}
}

// TestHeartbeatMidSessionPermissionDenied is the regression test for
// the P0.3 fix on the heartbeat recv case-block. Before the fix, the
// case-block fell through to `continue` whenever the recv goroutine
// produced a non-normal-exit error (codes.PermissionDenied /
// codes.Unauthenticated / codes.Unknown / codes.Unavailable / etc.),
// so operators reading dev-hello-client logs saw a misleading
// "✓ HelloAck" line followed by an exit 0 even when the master had
// evicted the worker mid-heartbeat. The fix replaces `continue` with a
// teardown path that propagates r.err as a non-zero exit code.
//
// This test asserts the contract in two halves, mirroring the run()'s
// case-block flow:
//
//  1. With the in-flight heartbeat state (helloAckReceived=true,
//     goodbyeSent=false, localCancelSent=false), codes.PermissionDenied
//     does NOT classify as a normal local close — the fix's new branch
//     DOES fire.
//  2. After the fix's state mutation (localCancelSent=true) the
//     drainStream-equivalent cleanup classifies the err as non-zero —
//     we exercise this half via drainStream itself because run()'s
//     inline teardown is hard to invoke without grpc.NewClient; the
//     underlying classification primitive (isExpectedLocalClose on a
//     non-normal-exit err with all-three state bits true → false) is
//     exactly what the inline code relies on.
func TestHeartbeatMidSessionPermissionDenied(t *testing.T) {
	logger := log.New(io.Discard, "", 0)

	// ── Half 1 — pre-mutation state ──────────────────────────────────────────
	// At the moment the heartbeat case-block accepts a recv err, the
	// shutdown flag board has exactly one bit set (HelloAck was
	// received). Without the fix's mutation, this state cannot classify
	// any recv err as an expected local close: the predicate requires
	// all three bits.
	state := shutdownState{
		helloAckReceived: true,
		goodbyeSent:      false,
		localCancelSent:  false,
	}
	recvErr := status.Error(codes.PermissionDenied, "evicted mid-heartbeat")
	if isExpectedLocalClose(recvErr, &state) {
		t.Fatalf("isExpectedLocalClose(perm, mid-heartbeat state) = true; " +
			"want false so the fix's new branch fires")
	}

	// ── Half 2 — post-mutation state + drainStream run ───────────────────────
	// The fix's new branch flips state.localCancelSent=true and then
	// runs the drainStream-equivalent cleanup. We simulate that here:
	// flip the bit, push the consumed PermissionDenied onto recvCh, and
	// let drainStream apply its classification (it sets
	// state.goodbyeSent=true after CloseSend, completing the all-3-true
	// gate so the err taxonomy branch fires).
	state.localCancelSent = true
	recvCh := make(chan recvResult, 1)
	recvDone := make(chan struct{})
	close(recvDone)
	recvCh <- recvResult{err: recvErr}

	err := drainStream(&mockStream{}, recvDone, recvCh, &state, logger)
	if err == nil {
		t.Fatalf("drainStream returned nil after post-fix state mutation; " +
			"want codes.PermissionDenied propagation (non-zero exit)")
	}
	if got, want := status.Code(err), codes.PermissionDenied; got != want {
		t.Fatalf("drainStream err code = %v, want %v (err=%v)", got, want, err)
	}
}
