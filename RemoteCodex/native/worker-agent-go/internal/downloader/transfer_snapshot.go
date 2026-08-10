package downloader

import (
	"errors"
	"math"
	"sync"
	"time"
)

// subscribe registers a snapshot subscriber. The returned channel delivers
// every published snapshot (non-blocking: a slow consumer drops intermediate
// snapshots, never stalls the transfer). The unsubscribe func removes the
// subscription and is idempotent.
//
// When the transfer is already terminal (settled before the subscriber
// arrived), the channel delivers the terminal snapshot exactly once and is
// closed, so a late subscriber never blocks on an empty channel.
func (t *Transfer) subscribe() (<-chan DownloadSnapshot, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state.Terminal() {
		ch := make(chan DownloadSnapshot, 1)
		ch <- t.snapshotLocked(t.now())
		close(ch)
		return ch, func() {}
	}
	t.subSeq++
	id := t.subSeq
	ch := make(chan DownloadSnapshot, 32)
	t.subscribers[id] = ch
	// A subscriber may attach after the first streamed chunk has already
	// been published (for example, an observer first waits for the upstream
	// request). Replay the current in-flight progress so it cannot miss the
	// only progress edge before a deliberately blocked test/server chunk.
	// A zero-byte live snapshot is omitted because the next state/progress
	// publication will carry it and this preserves the existing throttle
	// contract for subscribers that attach before transfer bytes arrive.
	if t.bytesDownloaded > 0 {
		ch <- t.snapshotLocked(t.now())
	}
	var once sync.Once
	unsub := func() {
		once.Do(func() {
			t.mu.Lock()
			delete(t.subscribers, id)
			t.mu.Unlock()
		})
	}
	return ch, unsub
}

// notifyOperational emits the manager-wide view when configured.
func (t *Transfer) notifyOperational() {
	if t.onOperationalSnapshot != nil {
		t.onOperationalSnapshot()
	}
}

// publish sends the current snapshot to every subscriber. The snapshot is
// built UNDER the transfer mutex (snapshotLocked's contract) so concurrent
// writers (setState/addWaiter/finish from other goroutines) never race the
// read. Sends are non-blocking: a subscriber with a full buffer misses this
// snapshot.
func (t *Transfer) publish(now time.Time) {
	t.mu.Lock()
	snap := t.snapshotLocked(now)
	for id, ch := range t.subscribers {
		select {
		case ch <- snap:
		default:
			if snap.State.Terminal() {
				// Terminal snapshots are contractual: make room by dropping
				// the oldest intermediate update, then deliver the terminal
				// snapshot before closing the channel.
				select {
				case <-ch:
				default:
				}
				ch <- snap
			}
		}
		if snap.State.Terminal() {
			close(ch)
			delete(t.subscribers, id)
		}
	}
	t.mu.Unlock()
}

// snapshot builds the observable snapshot. Caller must not hold t.mu.
func (t *Transfer) snapshot(now time.Time) DownloadSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(now)
}

// snapshotLocked builds the snapshot under the transfer mutex.
func (t *Transfer) snapshotLocked(now time.Time) DownloadSnapshot {
	state := t.state
	queuePos := 0
	if state == DownloadQueued {
		queuePos = int(t.queueSeq)
	}
	s := DownloadSnapshot{
		TransferID:         t.transferID,
		TransferGeneration: t.transferGeneration,
		AssetKey:           t.Key,
		AssetID:            t.req.AssetID,
		Role:               t.req.Role,
		State:              state,
		BytesDownloaded:    t.bytesDownloaded,
		BytesTotal:         t.bytesTotal,
		Attempt:            t.attempt,
		QueuePosition:      queuePos,
		SharedWaiters:      len(t.waiters),
		QueuedAt:           t.queuedAt,
		StartedAt:          t.startedAt,
		UpdatedAt:          t.updatedAt,
		CompletedAt:        t.completedAt,
		CacheHit:           t.cacheHit,
		ErrorCode:          errorCodeOf(t.err),
		ErrorDetail:        errorDetailOf(t.err),
		CheckpointSequence: t.checkpointSequence,
		JobIDs:             sortedJobIDs(t.jobRefs),
		JobRefs:            snapshotJobRefs(t.jobRefs),
		TaskID:             t.req.TaskID,
		SceneIDs:           sortedSceneIDs(t.sceneIDs),
		MIMEType:           t.req.MIMEType,
		SHA256:             t.req.SHA256,
	}
	if t.bytesTotal > 0 {
		s.ProgressPercent = float64(t.bytesDownloaded) / float64(t.bytesTotal) * 100
	}
	s.ThroughputBytesPerSecond = t.throughputBPS
	if t.bytesTotal > 0 && t.throughputBPS > 0 {
		remaining := t.bytesTotal - t.bytesDownloaded
		if remaining > 0 {
			s.ETASeconds = int64(math.Ceil(float64(remaining) / t.throughputBPS))
		}
	}
	return s
}

// errorCodeOf derives a stable, low-cardinality error code from a transfer
// error ("" for nil).
func errorCodeOf(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, errTransferCancelled):
		return "transfer_cancelled"
	case errors.Is(err, ErrVerify):
		return "verify_failed"
	case errors.Is(err, ErrPermanent):
		return "permanent"
	case errors.Is(err, ErrRetryable):
		return "retryable"
	default:
		return "failed"
	}
}

// errorDetailOf returns the error text ("" for nil).
func errorDetailOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
