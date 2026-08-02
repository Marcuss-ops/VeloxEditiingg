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
//
// File split:
//   - main.go          : package doc, main(), runParams, run() (the
//     linear handshake flow).
//   - client_stream.go : buildDialOptions, buildCapabilities,
//     runHeartbeatLoop, sendGoodbye, recvResult, shutdownState,
//     isExpectedLocalClose, drainStream.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
