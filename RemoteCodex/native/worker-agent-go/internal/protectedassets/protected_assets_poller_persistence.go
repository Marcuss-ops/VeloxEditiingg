package protectedassets

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"

	"velox-worker-agent/pkg/api"
)

// WaitReady blocks until the current registration session has received a
// valid protected-assets snapshot or ctx is cancelled. A lost session
// re-arms the channel, so a reconnect cannot inherit the previous session's
// cleanup permission.
func (p *ProtectedAssetsPoller) WaitReady(ctx context.Context) error {
	if p == nil {
		return errors.New("protectedassets.ProtectedAssetsPoller.WaitReady: nil poller")
	}
	for {
		p.mu.RLock()
		sessionGated := p.sessionGated
		p.mu.RUnlock()
		if sessionGated && !p.sessionAuthenticated() {
			p.invalidateReadiness()
		}
		p.readyMu.Lock()
		if p.ready {
			p.readyMu.Unlock()
			return nil
		}
		ch := p.readyCh
		p.readyMu.Unlock()
		if ch == nil {
			return errors.New("protectedassets.ProtectedAssetsPoller.WaitReady: barrier is not initialized")
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// IsReady reports whether the current registration session has a valid
// protection baseline.
func (p *ProtectedAssetsPoller) IsReady() bool {
	if p == nil {
		return false
	}
	p.readyMu.Lock()
	defer p.readyMu.Unlock()
	return p.ready
}
func (p *ProtectedAssetsPoller) sessionAuthenticated() bool {
	if !telemetry.GlobalReady().Snapshot().Registered {
		return false
	}
	client, ok := p.Client.(interface{ AuthToken() string })
	return ok && strings.TrimSpace(client.AuthToken()) != ""
}
func (p *ProtectedAssetsPoller) invalidateReadiness() {
	p.mu.Lock()
	p.sessionEpoch++
	p.lastPollErr = ErrProtectedSnapshotSessionUnavailable
	p.mu.Unlock()
	p.readyMu.Lock()
	if p.ready {
		// The ready channel is already closed for the completed session;
		// waiters have been released. Replace it with an open channel for
		// the new session so future waiters cannot inherit old readiness.
		p.ready = false
		p.readyCh = make(chan struct{})
	}
	p.readyMu.Unlock()
	telemetry.MarkCacheProtectionReady(false)
}

func (p *ProtectedAssetsPoller) recordPollError(err error) {
	p.mu.Lock()
	p.lastPollErr = err
	p.mu.Unlock()
}

// applySnapshot atomically swaps the in-memory snapshot reference.
// Centralised so the grow-only-by-reference invariant lives in
// one place — future enhancements (e.g. monotonic-version
// checking) plug in here without touching TickOnce.
func (p *ProtectedAssetsPoller) applySnapshot(snap *api.ProtectedAssetSnapshot, expectedEpoch uint64) error {
	generatedAt, err := time.Parse(time.RFC3339Nano, snap.GeneratedAt)
	if err != nil {
		// runTickOnce validates GeneratedAt before calling applySnapshot;
		// retain a defensive guard so readiness can never open from an
		// invalid snapshot if this helper is reused later.
		return ErrProtectedSnapshotInvalid
	}
	p.mu.Lock()
	if p.sessionGated && p.sessionEpoch != expectedEpoch {
		p.mu.Unlock()
		return ErrProtectedSnapshotSessionUnavailable
	}
	if p.sessionGated && !p.sessionAuthenticated() {
		p.mu.Unlock()
		p.invalidateReadiness()
		return ErrProtectedSnapshotSessionUnavailable
	}
	p.snap = snap
	p.lastPollErr = nil

	// Serialize the complete session transition under p.mu. A disconnect
	// cannot increment sessionEpoch or publish cache_protection_ready=false
	// until this snapshot has either opened the barrier or completed its
	// transition; an old response can therefore never reopen a newer session.
	// Open the cleanup barrier first, then publish readiness: a probe may
	// briefly report not-ready while cleanup is permitted, but never the
	// reverse.
	telemetry.SetProtectedSnapshotGeneratedAt(generatedAt)
	p.readyMu.Lock()
	p.ready = true
	if p.readyCh == nil {
		p.readyCh = make(chan struct{})
	}
	select {
	case <-p.readyCh:
	default:
		close(p.readyCh)
	}
	p.readyMu.Unlock()
	telemetry.MarkCacheProtectionReady(true)
	p.mu.Unlock()
	if p.OnSuccess != nil {
		p.OnSuccess(snap)
	}
	return nil
}

// Snapshot returns the most recent good snapshot pointer (or
// nil if no successful poll has happened yet). Thread-safe.
// The returned pointer is shared and must NOT be mutated by the
// caller; the poller is the sole writer.
func (p *ProtectedAssetsPoller) Snapshot() *api.ProtectedAssetSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snap
}

// Current adapts the poller's last successful snapshot to the
// workercache.SnapshotSource contract. A failed poll returns the last
// snapshot together with lastPollErr, so cleanup remains fail-safe while
// readiness and diagnostics can still inspect the last valid snapshot.
func (p *ProtectedAssetsPoller) Current(_ context.Context) (time.Time, []string, error) {
	p.mu.RLock()
	snap := p.snap
	pollErr := p.lastPollErr
	p.mu.RUnlock()
	if snap == nil {
		if pollErr != nil {
			return time.Time{}, nil, pollErr
		}
		return time.Time{}, nil, nil
	}
	generatedAt, err := time.Parse(time.RFC3339Nano, snap.GeneratedAt)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("parse protected-assets generated_at: %w", err)
	}
	ids := append([]string(nil), snap.ProtectedAssetKeys...)
	if pollErr != nil {
		return generatedAt.UTC(), ids, pollErr
	}
	return generatedAt.UTC(), ids, nil
}
