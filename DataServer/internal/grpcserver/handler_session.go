// Package grpcserver / handler_session.go
//
// workerSession and outboundMessage types plus helpers for placement,
// capabilities and executor tracking. Extracted from handler.go to keep
// the core types file focused.
package grpcserver

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
)

// outboundMessage wraps a protobuf envelope with optional callbacks
// for the sessionWriter. OnSent is called after a successful stream.Send;
// nil means no callback. This enables #1 fix: commands are marked delivered
// only after the real network write, not after safeSend puts them in the
// in-memory channel.
type outboundMessage struct {
	Envelope *pb.MasterToWorkerEnvelope
	OnSent   func() // Called after successful stream.Send; nil if not needed
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID  string
	sessionID string
	stream    grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	done      chan struct{}
	doneOnce  sync.Once          // P0 #6: prevents double-close on session teardown/reconnect
	cancel    context.CancelFunc // cancels the session context to terminate old goroutines

	// gRPC request context (carries trace context via otelgrpc).
	// Scorecard v2 / Step 15c: handlers use this instead of context.Background()
	// so spans have proper parent-child trace relationships.
	ctx context.Context

	// Serialized output: all stream.Send() calls go through sendCh → sessionWriter.
	// No other goroutine may call stream.Send() directly.
	sendCh chan *outboundMessage

	// writerErr is a small (cap 1) channel used by sessionWriter to signal
	// a stream.Send() failure back to the Stream() main loop. Phase 4.2
	// requirement: a network-level send error MUST terminate the session,
	// otherwise pending offers can be left orphaned silently. The main loop
	// reads writerErr inside its select and triggers a teardown on receipt.
	writerErr chan error // Job offering synchronization (Issue 4 fix).
	// PR #4: replaced pendingOffer (job-based) with pendingTaskOffer (task-based).
	pendingTaskOffer *taskgraph.TaskWithSpec // TaskOffer sent, awaiting TaskAccepted/TaskRejected
	claimMu          sync.Mutex              // serializes the claim+send+set flow; also guards pendingTaskOffer r/w

	// Worker capacity tracking (atomic — Phase 4.1 fix). The handleHeartbeat
	// goroutine writes them, sendPushJobOffer reads them under claimMu. Using
	// atomic.Int32 makes the read lock-free and race-clean in `-race`.
	maxParallelJobs atomic.Int32
	activeJobsCount atomic.Int32

	// supportedJobTypes is updated by handleHeartbeat from the worker's
	// capabilities and read by collectAllowedJobTypes under claimMu.
	// atomic.Value avoids RWMutex overhead while remaining race-clean.
	supportedJobTypes atomic.Value // []string

	// Sequence numbers for replay protection (Issue 7 fix).
	lastRecvSeq int64 // last received sequence number from worker

	// Placement snapshot fields: typed executor map, capability map,
	// and their revision counter. Populated at Hello time and updated
	// on heartbeat-driven re-advertisement. The placement snapshot is
	// built from these fields under RLock so the snapshot is always
	// consistent without blocking the main message loop.
	executorsMu sync.RWMutex
	executors   map[placement.ExecutorKey]struct{}

	capabilitiesMu sync.RWMutex
	capabilities   map[string]bool

	capabilityRevision atomic.Uint64

	ready    atomic.Bool
	draining atomic.Bool

	lastHeartbeatUnix atomic.Int64

	// Version correlation (Step 4 / Velox Metrics Center): software
	// versions reported by the worker via heartbeat, stored on the
	// session so they can be stamped on task_attempts at report time.
	gitSHA        atomic.Value // string
	workerVersion atomic.Value // string
	engineVersion atomic.Value // string
	ffmpegVersion atomic.Value // string
}

// placementSnapshot builds an immutable WorkerSnapshot from the in-memory
// session state. The snapshot is consistent at a single instant (executors
// and capabilities read under their respective RLock). The caller must
// NOT hold any session mutex when calling this method.
func (s *workerSession) placementSnapshot(workerID string) placement.WorkerSnapshot {
	s.executorsMu.RLock()
	executors := make(map[placement.ExecutorKey]struct{}, len(s.executors))
	for key := range s.executors {
		executors[key] = struct{}{}
	}
	s.executorsMu.RUnlock()

	s.capabilitiesMu.RLock()
	caps := make(map[string]bool, len(s.capabilities))
	for key, enabled := range s.capabilities {
		caps[key] = enabled
	}
	s.capabilitiesMu.RUnlock()

	return placement.WorkerSnapshot{
		WorkerID:           workerID,
		SessionID:          s.sessionID,
		Ready:              s.ready.Load(),
		Draining:           s.draining.Load(),
		SessionAlive:       true,
		MaxParallelJobs:    int(s.maxParallelJobs.Load()),
		ActiveJobs:         int(s.activeJobsCount.Load()),
		Executors:          executors,
		Capabilities:       caps,
		CapabilityRevision: s.capabilityRevision.Load(),
		LastHeartbeat: time.Unix(
			s.lastHeartbeatUnix.Load(),
			0,
		).UTC(),
	}
}

// replaceCapabilities atomically replaces the session's executor and
// capability maps with the parsed values from the Hello handshake.
// It bumps the capability revision so any pending claim that was
// built from a stale snapshot can be detected by the fencing check.
func (s *workerSession) replaceCapabilities(
	executors map[placement.ExecutorKey]struct{},
	capabilities map[string]bool,
) {
	s.executorsMu.Lock()
	s.executors = executors
	s.executorsMu.Unlock()

	s.capabilitiesMu.Lock()
	s.capabilities = capabilities
	s.capabilitiesMu.Unlock()

	s.capabilityRevision.Add(1)
}

func maxParallelJobsFromCapabilities(capsMap map[string]interface{}) int {
	if capsMap == nil {
		return 0
	}
	if mpj, ok := capsMap["max_parallel_jobs"]; ok {
		switch v := mpj.(type) {
		case float64:
			return int(v)
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		}
	}
	if host, ok := capsMap["host"].(map[string]interface{}); ok {
		if mpj, ok := host["max_parallel_jobs"]; ok {
			switch v := mpj.(type) {
			case float64:
				return int(v)
			case int:
				return v
			case int32:
				return int(v)
			case int64:
				return int(v)
			}
		}
	}
	return 0
}

// invalidateExecutor removes a single executor key from the session's
// executor map and bumps the capability revision. Called when the
// worker rejects a task with reason="unsupported_executor" — the
// placement snapshot said the worker supports this executor, but the
// worker disagrees. Invalidating prevents further offers of the same
// incompatible executor until the next Hello re-advertises it.
func (s *workerSession) invalidateExecutor(key placement.ExecutorKey) {
	s.executorsMu.Lock()
	delete(s.executors, key)
	s.executorsMu.Unlock()

	s.capabilityRevision.Add(1)
}
