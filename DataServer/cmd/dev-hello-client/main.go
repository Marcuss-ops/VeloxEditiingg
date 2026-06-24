// Command dev-hello-client is a synthetic Hello/HelloAck client for the
// Velox master gRPC control plane.
//
// Purpose
// ────────
// Operators want to validate the v3 worker→master handshake end-to-end
// against a *real* dev master (DataServer on :50051) without standing
// up the full worker-agent. This binary:
//
//  1. opens a bidi gRPC stream to the master;
//  2. sends a typed `Hello` envelope (worker_id, name, credential_hash,
//     capabilities);
//  3. waits up to 15 seconds for a typed `HelloAck`;
//  4. (optional) sends synthetic Heartbeats every N seconds for a
//     window so the operator can probe `/api/v1/workers` and watch the
//     registry state from another shell.
//
// Two transport modes
// ───────────────────
//   - Plaintext — default. Aligns with `VELOX_GRPC_ALLOW_INSECURE_DEV=true`
//     on the master side. The master will refuse unless that flag AND
//     an empty (or sentinel) `VELOX_ALLOWED_WORKERS` allow it through.
//   - mTLS — pass `--tls-cert/--tls-key/--tls-ca` together. Self-signed
//     triples are produced by `scripts/gen-worker-certs.sh`; the master
//     uses `tls.RequireAndVerifyClientCert` (handler.go) so a missing or
//     unknown-CA client cert is rejected, exercising the same path the
//     production worker-agent walks.
//
// Why this lives outside cmd/server/
// ──────────────────────────────────
// Adding it as a subcommand of `velox-server` would couple the dev tool
// to the master binary — every CI build of the master would pull this
// in even though it's irrelevant to the running server. Splitting it
// into `cmd/dev-hello-client/` keeps the blast radius tiny: dev-only,
// two-target build (`go build ./cmd/server ./cmd/dev-hello-client`).
//
// PR 2 (`codex/dev-client-clean-shutdown`)
// ────────────────────────────────────────
// Pre-PR-2 drainStream silently treated every codes.Canceled /
// codes.DeadlineExceeded / io.EOF / context.Canceled as a normal
// exit, masking server-driven kicks (eviction mid-session, mTLS auth
// fail mid-stream) as a clean exit-0. PR 2 introduces shutdownState and
// tightens the predicate to ALL THREE of {helloAckReceived,
// goodbyeSent, localCancelSent} true AND err is in the normal-exit
// taxonomy. Anything else surfaces with a non-zero exit code so
// operators see the actual cause instead of a misleading "✓ HelloAck
// received" diagnostic paired with a secret 1-exit.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "velox-shared/controltransport/pb"
)

const (
	// helloAckTimeout caps the wait for the master's HelloAck. The master
	// synchronously emits it inside the bidi-stream handler (handler.go
	// ~line 349) once the allowlist/credential_hash gates pass, so a
	// 15-second ceiling is plenty even on a busy master.
	helloAckTimeout = 15 * time.Second

	// defaultHeartbeatInterval matches the protocol expectation that
	// workers send heartbeats at ~5 s cadence. Tight enough to be useful
	// for `/api/v1/workers` dashboards.
	defaultHeartbeatInterval = 5 * time.Second
)

func main() {
	master := flag.String("master", "localhost:50051", "master gRPC target host:port")
	workerID := flag.String("worker-id", "dev-hello-client-1", "worker_id advertised in Hello (also drives credential hash)")
	workerName := flag.String("worker-name", "dev-hello-client", "human-friendly name surfaced in worker list")
	hostnameOverride := flag.String("hostname", "", "override os.Hostname() in Hello (default: real hostname)")
	protocolVersion := flag.String("protocol-version", "v3", "protocol_version string")
	executorID := flag.String("executor-id", "dev.scene.composite.v1", "executors[0].id advertised in Hello.capabilities")
	workerSecret := flag.String("worker-secret", "", "VELOX_WORKER_SECRET — combined with worker-id to produce credential_hash (sha256 hex)")
	credentialOverride := flag.String("credential-hash", "", "override credential_hash directly (skips sha256 derivation; for non-secret dev fixtures only)")

	tlsCert := flag.String("tls-cert", "", "path to client certificate (PEM). Together with --tls-key + --tls-ca enables mTLS.")
	tlsKey := flag.String("tls-key", "", "path to client private key (PEM).")
	tlsCA := flag.String("tls-ca", "", "path to CA certificate (PEM) used to verify the master's cert.")

	heartbeatWindow := flag.Duration("heartbeat-window", 0, "if >0, send synthetic heartbeats for this duration after HelloAck, then send Goodbye and exit (e.g. 60s)")
	heartbeatInterval := flag.Duration("heartbeat-interval", defaultHeartbeatInterval, "heartbeat interval (used when --heartbeat-window > 0)")

	flag.Parse()

	logger := log.New(os.Stderr, "dev-hello-client ", log.LstdFlags|log.Lmicroseconds)

	if err := run(logger, runParams{
		Master:             *master,
		WorkerID:           *workerID,
		WorkerName:         *workerName,
		Hostname:           *hostnameOverride,
		ProtocolVersion:    *protocolVersion,
		ExecutorID:         *executorID,
		WorkerSecret:       *workerSecret,
		CredentialOverride: *credentialOverride,
		TLSCert:            *tlsCert,
		TLSKey:             *tlsKey,
		TLSCA:              *tlsCA,
		HeartbeatWindow:    *heartbeatWindow,
		HeartbeatInterval:  *heartbeatInterval,
	}); err != nil {
		logger.Fatalf("dev-hello-client: %v", err)
	}
}

// runParams bundles all CLI inputs into a single struct so main() and
// run() stay readable and the values travel together to each helper.
type runParams struct {
	Master             string
	WorkerID           string
	WorkerName         string
	Hostname           string
	ProtocolVersion    string
	ExecutorID         string
	WorkerSecret       string
	CredentialOverride string
	TLSCert            string
	TLSKey             string
	TLSCA              string
	// HeartbeatWindow drives the post-HelloAck phase. Zero ⇒ exit
	// cleanly after HelloAck (the regime most useful for CI assertions
	// where the operator asks "did the wire handshake succeed?").
	HeartbeatWindow   time.Duration
	HeartbeatInterval time.Duration
}

// run is the linear, testable entry point: validate flags → dial →
// open stream → send Hello → await HelloAck → optional heartbeat loop.
//
// State ownership: shutdownState is a value-type local mutated only
// from this main control goroutine (no sync needed). drainStream and
// runHeartbeatLoop are passed &state where they need to flip a flag.
func run(logger *log.Logger, p runParams) error {
	// ── 1. Flag validation ----------------------------------------------------
	useTLS := p.TLSCert != "" || p.TLSKey != "" || p.TLSCA != ""
	switch {
	case useTLS && (p.TLSCert == "" || p.TLSKey == "" || p.TLSCA == ""):
		return errors.New("mTLS requires --tls-cert AND --tls-key AND --tls-ca (all three, partial triples will not load)")
	case p.HeartbeatWindow < 0:
		return fmt.Errorf("--heartbeat-window must be >= 0 (got %s)", p.HeartbeatWindow)
	case p.HeartbeatInterval <= 0:
		return fmt.Errorf("--heartbeat-interval must be > 0 (got %s)", p.HeartbeatInterval)
	case strings.TrimSpace(p.WorkerID) == "":
		return errors.New("--worker-id is required and must be non-empty")
	}

	// Construct the credential hash. Precedence:
	//   1. --credential-hash (explicit, dev-only escape hatch)
	//   2. SHA-256(worker_id + ":" + worker_secret) — matches the
	//      master-side recomputation in ingest.ValidateIdentityTuple.
	credentialHash := p.CredentialOverride
	switch {
	case credentialHash == "" && p.WorkerSecret != "":
		h := sha256.Sum256([]byte(p.WorkerID + ":" + p.WorkerSecret))
		credentialHash = hex.EncodeToString(h[:])
	case credentialHash == "":
		// No secret supplied: surface a WARN so the operator sees this
		// is a dev-bypass-grade credential, NOT a real registration.
		logger.Printf("WARN: no --worker-secret supplied; credential_hash will be \"dev-no-secret:%s\" — handshake is dev-bypass only and will be REJECTED by any master with VELOX_ALLOWED_WORKERS populated", p.WorkerID)
		credentialHash = "dev-no-secret:" + p.WorkerID
	}

	hostname := p.Hostname
	if hostname == "" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			hostname = "dev-hello-client-local"
		} else {
			hostname = h
		}
	}

	// ── 2. Build dial options --------------------------------------------------
	dialOpts, err := buildDialOptions(p.TLSCert, p.TLSKey, p.TLSCA)
	if err != nil {
		return fmt.Errorf("build dial options: %w", err)
	}

	// ── 3. Context + signal wiring --------------------------------------------
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// PR 2 — shutdownState is the single flag board drainStream consults
	// to decide whether a terminal recv err is a normal local close
	// (exit 0) or a real error (exit non-0). Each flag flips in only
	// one place below:
	//   helloAckReceived ← after the await-and-match loop in §6
	//   localCancelSent  ← immediately before each drainStream call
	//                       (we, the worker, are initiating shutdown)
	//   goodbyeSent      ← inside drainStream, right after stream.CloseSend()
	//                       (CloseSend IS the wire-level goodbye; setting
	//                       it there unifies fast-mode and heartbeat-mode
	//                       teardowns)
	var state shutdownState

	logger.Printf("connecting to %s (mTLS=%t, worker_id=%q)", p.Master, useTLS, p.WorkerID)
	conn, err := grpc.NewClient(p.Master, dialOpts...)
	if err != nil {
		cancel()
		return fmt.Errorf("grpc.NewClient: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Propagate the worker_id as outbound gRPC metadata so operators
	// reading master logs see it in the gRPC handler header trace
	// without parsing the first envelope.
	streamCtx := metadata.AppendToOutgoingContext(ctx, "worker-id", p.WorkerID)
	client := pb.NewWorkerControlClient(conn)
	stream, err := client.Stream(streamCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("Stream: %w", err)
	}

	// ── 4. Recv goroutine ------------------------------------------------------
	// Bidi streams require concurrent senders and receivers. We drain
	// master→worker envelopes into a channel so the main goroutine
	// can wait on HelloAck without blocking on later (Ping,
	// ConfigurationUpdate) server-originated frames.
	recvCh := make(chan recvResult, 16)
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			env, err := stream.Recv()
			recvCh <- recvResult{env: env, err: err}
			if err != nil {
				return
			}
		}
	}()

	// ── 5. Send Hello ----------------------------------------------------------
	helloSeq := int64(1)
	helloMsg := &pb.WorkerToMasterEnvelope{
		MessageId:       fmt.Sprintf("hello-%d", time.Now().UnixNano()),
		WorkerId:        p.WorkerID,
		SequenceNumber:  helloSeq,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: p.ProtocolVersion,
		Msg: &pb.WorkerToMasterEnvelope_Hello{
			Hello: &pb.Hello{
				WorkerName:     p.WorkerName,
				Hostname:       hostname,
				Version:        "dev",
				BundleVersion:  "dev",
				EngineVersion:  "dev",
				CredentialHash: credentialHash,
				Capabilities:   buildCapabilities(p.ExecutorID),
			},
		},
	}
	if err := stream.Send(helloMsg); err != nil {
		cancel()
		return fmt.Errorf("send Hello: %w", err)
	}
	logger.Printf("Hello sent (worker_id=%q, credential_hash=%q) — awaiting HelloAck…", p.WorkerID, credentialHash)

	// ── 6. Wait for HelloAck (bounded) ----------------------------------------
	helloAckCtx, helloAckCancel := context.WithTimeout(ctx, helloAckTimeout)
	defer helloAckCancel()
helloAckLoop:
	for {
		select {
		case <-helloAckCtx.Done():
			cancel()
			return fmt.Errorf("HelloAck not received within %s", helloAckTimeout)
		case r := <-recvCh:
			if r.err != nil {
				cancel()
				return fmt.Errorf("recv master envelope: %w", r.err)
			}
			if r.env.GetHelloAck() != nil {
				// flag the helloAck side of the shutdownState so
				// drainStream downstream classifies this as a
				// worker-initiated close (see PR 2 notes above)
				state.helloAckReceived = true
				break helloAckLoop
			}
			// Forward anything else that arrived *before* HelloAck
			// (rare; usually nothing). Useful for debugging
			// mis-routed masters.
			logger.Printf("non-Ack envelope received early: %T", r.env.GetMsg())
		}
	}
	logger.Printf("✓ HelloAck received from master (worker registered in registry)")

	// ── 7. Optional heartbeat window ------------------------------------------
	if p.HeartbeatWindow <= 0 {
		logger.Printf("done — no heartbeat window requested (use --heartbeat-window to keep the registration live)")
		// Fast-mode teardown: the cancelled ctx will surface to the
		// recv goroutine as codes.Canceled / context.Canceled. We
		// claim localCancelSent here because the deferred cancel()
		// is the proximate cause.
		state.localCancelSent = true
		return drainStream(stream, recvDone, recvCh, &state, logger)
	}

	logger.Printf("entering heartbeat window: total=%s interval=%s", p.HeartbeatWindow, p.HeartbeatInterval)
	hbDone := make(chan struct{})
	go runHeartbeatLoop(ctx, stream, hbDone, p, &helloSeq, logger)

	windowEnd := time.NewTimer(p.HeartbeatWindow)
	defer windowEnd.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Printf("interrupted — sending Goodbye")
			state.localCancelSent = true
			cancel()
			// Drain the heartbeat goroutine BEFORE reading *seq below.
			// Without this happens-before edge, `helloSeq+1` races with
			// the goroutine's `*seq++`, and `go test -race` flags it as
			// a real race (semantic blast radius is nil because master
			// dedups by worker_id+seq, but the race itself is real).
			<-hbDone
			sendGoodbye(stream, helloSeq+1, p.WorkerID, logger)
			return drainStream(stream, recvDone, recvCh, &state, logger)
		case <-windowEnd.C:
			logger.Printf("heartbeat window elapsed — sending Goodbye")
			state.localCancelSent = true
			cancel()
			<-hbDone
			sendGoodbye(stream, helloSeq+1, p.WorkerID, logger)
			return drainStream(stream, recvDone, recvCh, &state, logger)
		case r := <-recvCh:
			if r.err != nil {
				// PR 2: every terminal recv err flows through
				// isExpectedLocalClose. The full-context paths (cancel
				// + CloseSend happened) gate on ALL THREE state bits,
				// so a kick that arrives before we set
				// localCancelSent (e.g. eviction mid-session on a
				// fast-mode client) propagates correctly as exit-1.
				if isExpectedLocalClose(r.err, &state) {
					<-hbDone
					return drainStream(stream, recvDone, recvCh, &state, logger)
				}
				// PR FIX (P0.3 of the audit recap): heartbeat-phase
				// terminal recv err. ANY non-normal-exit code
				// (codes.PermissionDenied, codes.Unauthenticated,
				// codes.Unknown, codes.Unavailable, codes.Internal, ...)
				// means the master kicked us. Don't `continue` and
				// silently absorb it — surface with exit != 0 so the
				// operator reads the real cause instead of a misleading
				// "✓ HelloAck" verdict at window-end.
				logger.Printf("FATAL: unexpected recv error during heartbeat phase: %v", r.err)
				state.localCancelSent = true
				cancel() // also unblocks runHeartbeatLoop via ctx.Done()
				<-hbDone
				// Inline tail-cleanup mirrors drainStream's body but
				// deliberately does NOT consult recvCh for
				// classification: the recv goroutine can race-with-us
				// and push a post-cancel codes.Canceled frame which,
				// under an all-3-true state, would re-classify as a
				// "normal exit" and mask the real cause.
				if stream != nil {
					_ = stream.CloseSend()
				}
				state.goodbyeSent = true
				select {
				case <-recvDone:
				case <-time.After(2 * time.Second):
					logger.Printf("WARN: recv goroutine did not exit within 2s after CloseSend (likely master is wedged on the auth path)")
				}
				for {
					select {
					case <-recvCh: // discard — verdict already decided
					default:
						return r.err
					}
				}
			}
			// We logged HelloAck already; log anything else to expose
			// unexpected master-driven traffic.
			logger.Printf("master→client: %T", r.env.GetMsg())
		case <-hbDone:
			// Heartbeat loop returned on its own (shouldn't normally
			// happen — ctx drives it).
			state.localCancelSent = true
			return drainStream(stream, recvDone, recvCh, &state, logger)
		}
	}
}

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
