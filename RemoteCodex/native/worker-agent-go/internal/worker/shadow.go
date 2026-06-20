// Package worker — shadow mode (PR11): dual-transport observation and comparison.
// shadow.go implements the shadow session: a gRPC transport that receives
// JobOffer/Command messages but NEVER sends JobAccepted — purely observes
// and compares timing with the primary HTTP polling transport.
package worker

import (
	"context"
	"time"

	pb "velox-shared/controltransport/pb"
	"velox-shared/controltransport"
)

// shadowSessionLifecycle manages the shadow gRPC transport lifecycle.
// Runs in a background goroutine. If the shadow transport fails, the
// session is retried without affecting the primary transport.
func (w *Worker) shadowSessionLifecycle(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		default:
		}

		w.logger.Info("[SHADOW] Starting shadow session — connecting gRPC transport")
		transport := w.shadowTransportFactory()
		if transport == nil {
			w.logger.Warn("[SHADOW] Shadow transport factory returned nil — retrying in 30s")
			select {
			case <-time.After(30 * time.Second):
				continue
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			}
		}

		hello := w.buildHello()
		if err := transport.Connect(ctx, hello); err != nil {
			w.logger.Warn("[SHADOW] Connect failed (will retry): %v", err)
			_ = transport.Close()
			select {
			case <-time.After(30 * time.Second):
				continue
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			}
		}

		w.shadowActive.Store(true)
		w.logger.Info("[SHADOW] Shadow session connected — starting receive loop")
		recvCh, errCh, err := transport.Receive(ctx)
		if err != nil {
			w.logger.Warn("[SHADOW] Receive failed: %v", err)
			_ = transport.Close()
			select {
			case <-time.After(30 * time.Second):
				continue
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			}
		}

		// Run shadow receive loop — blocks until the stream ends.
		w.shadowReceiveLoop(ctx, recvCh, errCh)

		_ = transport.Close()
		w.shadowActive.Store(false)
		w.logger.Info("[SHADOW] Shadow session ended — will reconnect in 30s")

		select {
		case <-time.After(30 * time.Second):
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		}
	}
}

// shadowReceiveLoop processes incoming messages from the shadow gRPC stream.
// It ONLY observes — never sends JobAccepted, never mutates worker state.
// JobOffers are recorded for comparison with the primary transport's claims.
func (w *Worker) shadowReceiveLoop(ctx context.Context, recvCh <-chan controltransport.ControlMessage, errCh <-chan error) {
	w.logger.Info("[SHADOW] Shadow receive loop started — observing only")

	sweepTicker := time.NewTicker(5 * time.Minute)
	defer sweepTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[SHADOW] Shadow receive loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Info("[SHADOW] Shadow receive loop exiting (stop signal)")
			return

		case streamErr, ok := <-errCh:
			if ok && streamErr != nil {
				w.logger.Warn("[SHADOW] Shadow transport error: %v", streamErr)
			} else {
				w.logger.Info("[SHADOW] Shadow transport closed")
			}
			return

		case msg, ok := <-recvCh:
			if !ok {
				w.logger.Warn("[SHADOW] Shadow receive channel closed")
				return
			}
			w.handleShadowMessage(msg)

		case <-sweepTicker.C:
			w.sweepStaleShadowJobs()
		}
	}
}

// handleShadowMessage dispatches shadow-side messages for observation only.
func (w *Worker) handleShadowMessage(msg controltransport.ControlMessage) {
	switch msg.Type {
	case controltransport.MsgJobOffer:
		w.handleShadowJobOffer(msg)

	case controltransport.MsgCommand:
		// Extract command name: try TypedPayload first (gRPC), fall back to Payload.
		cmdName := ""
		if cmd, ok := msg.TypedPayload.(*pb.Command); ok && cmd != nil {
			cmdName = cmd.GetCommand()
		}
		if cmdName == "" {
			if p := msg.Payload; p != nil {
				if s, ok := p["command"].(string); ok {
					cmdName = s
				}
			}
		}
		w.logger.Info("[SHADOW] Command observed: %s (via gRPC)", cmdName)

	case controltransport.MsgPing:
		// Reply with a basic heartbeat to keep the stream alive.
		// Do NOT send a full heartbeat — shadow is read-only.
		w.logger.Debug("[SHADOW] Ping received — replying with shadow heartbeat")

	case controltransport.MsgHelloAck:
		w.logger.Debug("[SHADOW] HelloAck received — shadow session confirmed")

	case controltransport.MsgDrain:
		w.logger.Info("[SHADOW] Drain observed via shadow stream")

	default:
		w.logger.Debug("[SHADOW] Observed message: %s", msg.Type)
	}
}

// handleShadowJobOffer records a JobOffer from the shadow transport for
// later comparison with the primary transport's claims.
// The shadow transport is always gRPC, so we try TypedPayload (*pb.JobOffer)
// first and fall back to Payload for edge cases.
func (w *Worker) handleShadowJobOffer(msg controltransport.ControlMessage) {
	jobID := ""

	// Try typed proto payload first (gRPC transport).
	if offer, ok := msg.TypedPayload.(*pb.JobOffer); ok && offer != nil {
		jobID = offer.GetJobId()
	}

	// Fall back to generic Payload map.
	if jobID == "" {
		if p := msg.Payload; p != nil {
			if id, ok := p["job_id"].(string); ok {
				jobID = id
			}
		}
	}

	if jobID == "" {
		w.logger.Warn("[SHADOW] JobOffer without job_id — cannot track")
		return
	}

	w.shadowSeenMu.Lock()
	w.shadowSeen[jobID] = time.Now()
	w.shadowSeenMu.Unlock()

	w.shadowOffers.Add(1)
	w.logger.Info("[SHADOW] JobOffer observed: %s (via gRPC — #%d total)", jobID, w.shadowOffers.Load())
}

// recordPrimaryJobSeen is called by the primary receiveLoop when a job is
// received via HTTP polling. It compares the jobID against the shadow transport's
// seen-jobs map and records match/mismatch metrics.
func (w *Worker) recordPrimaryJobSeen(jobID string) {
	w.shadowSeenMu.Lock()
	shadowTime, ok := w.shadowSeen[jobID]
	if ok {
		delete(w.shadowSeen, jobID) // Clean up after match
	}
	w.shadowSeenMu.Unlock()

	if ok {
		latency := time.Since(shadowTime)
		w.shadowMatches.Add(1)
		w.logger.Info("[SHADOW] Match: %s | Push→Poll latency: %v (shadow offer at %s)",
			jobID, latency.Round(time.Millisecond), shadowTime.Format(time.RFC3339))
	} else {
		w.shadowMismatches.Add(1)
		w.logger.Info("[SHADOW] Mismatch: %s arrived via HTTP polling but was NOT seen on shadow stream",
			jobID)
	}
}

// sweepStaleShadowJobs removes entries older than 10 minutes from the shadowSeen
// map. This prevents memory leaks from jobs that were observed on the shadow
// stream but never claimed by the primary transport.
func (w *Worker) sweepStaleShadowJobs() {
	cutoff := time.Now().Add(-10 * time.Minute)
	w.shadowSeenMu.Lock()
	defer w.shadowSeenMu.Unlock()

	var removed int
	for jobID, seenAt := range w.shadowSeen {
		if seenAt.Before(cutoff) {
			delete(w.shadowSeen, jobID)
			removed++
		}
	}
	if removed > 0 {
		w.logger.Debug("[SHADOW] Swept %d stale shadow job entries (older than 10 min)", removed)
	}
}

// ShadowMetrics represents a snapshot of shadow mode comparison metrics.
type ShadowMetrics struct {
	Offers    int64 `json:"shadow_offers"`
	Matches   int64 `json:"shadow_matches"`
	Mismatches int64 `json:"shadow_mismatches"`
}

// GetShadowMetrics returns structured shadow mode metrics.
func (w *Worker) GetShadowMetrics() ShadowMetrics {
	return ShadowMetrics{
		Offers:    w.shadowOffers.Load(),
		Matches:   w.shadowMatches.Load(),
		Mismatches: w.shadowMismatches.Load(),
	}
}

// isShadowModeActive returns true when the shadow gRPC transport is connected
// and actively observing. Uses atomic bool set by shadowSessionLifecycle.
func (w *Worker) isShadowModeActive() bool {
	return w.shadowActive.Load()
}
