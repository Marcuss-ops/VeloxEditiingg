package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// runTickOnce is the shared fetch-and-notify helper used by both
// Run (with a label prefix for log identification) and
// TickOnce (with empty label for direct-call observability).
//
// Failure semantics are identical in both paths:
//   - p.snap is UNTOUCHED.
//   - p.OnError fires (if set) with the underlying error.
//   - The error is returned to the caller.
func (p *ProtectedAssetsPoller) runTickOnce(ctx context.Context, label string) error {
	snap, err := p.Client.GetProtectedAssets(ctx)
	if err != nil {
		p.mu.Lock()
		p.lastPollErr = err
		p.mu.Unlock()
		if p.OnError != nil {
			if label != "" {
				p.OnError(fmt.Errorf("%s: %w", label, err))
			} else {
				p.OnError(err)
			}
		}
		return err
	}
	if snap == nil {
		p.mu.Lock()
		p.lastPollErr = ErrProtectedSnapshotNil
		p.mu.Unlock()
		if p.OnError != nil {
			p.OnError(ErrProtectedSnapshotNil)
		}
		return ErrProtectedSnapshotNil
	}
	generatedAt, parseErr := time.Parse(time.RFC3339Nano, snap.GeneratedAt)
	if snap.GeneratedAt == "" || parseErr != nil {
		err := ErrProtectedSnapshotInvalid
		if parseErr != nil {
			err = fmt.Errorf("%w: %v", ErrProtectedSnapshotInvalid, parseErr)
		}
		p.mu.Lock()
		p.lastPollErr = err
		p.mu.Unlock()
		if p.OnError != nil {
			p.OnError(err)
		}
		return err
	}
	if p.SnapshotMaxAge > 0 && time.Since(generatedAt) > p.SnapshotMaxAge {
		p.mu.Lock()
		p.lastPollErr = ErrProtectedSnapshotStale
		p.mu.Unlock()
		if p.OnError != nil {
			p.OnError(ErrProtectedSnapshotStale)
		}
		return ErrProtectedSnapshotStale
	}
	p.mu.Lock()
	sessionGated := p.sessionGated
	sessionEpoch := p.sessionEpoch
	p.mu.Unlock()
	if sessionGated {
		ready := telemetry.GlobalReady().Snapshot()
		authenticated := false
		if client, ok := p.Client.(interface{ AuthToken() string }); ok {
			authenticated = strings.TrimSpace(client.AuthToken()) != ""
		}
		if !ready.Registered || !authenticated {
			p.invalidateReadiness()
			return ErrProtectedSnapshotSessionUnavailable
		}
	}
	if previous := p.Snapshot(); previous != nil {
		previousAt, previousErr := time.Parse(time.RFC3339Nano, previous.GeneratedAt)
		if previousErr == nil {
			older := false
			switch {
			case previous.Version > 0 && snap.Version > 0:
				// Version is authoritative when both snapshots carry it;
				// timestamps may come from a clock-skewed master.
				older = snap.Version < previous.Version ||
					(snap.Version == previous.Version && !generatedAt.After(previousAt))
			default:
				older = !generatedAt.After(previousAt)
			}
			if older {
				p.mu.Lock()
				p.lastPollErr = ErrProtectedSnapshotStale
				p.mu.Unlock()
				if p.OnError != nil {
					p.OnError(ErrProtectedSnapshotStale)
				}
				return ErrProtectedSnapshotStale
			}
		}
	}
	if err := p.applySnapshot(snap, sessionEpoch); err != nil {
		p.recordPollError(err)
		if p.OnError != nil {
			p.OnError(err)
		}
		return err
	}
	return nil
}

// waitForRegistration blocks the first request until the worker session is
// live and the shared API client has a bearer token. The client token may be
// populated before the gRPC Hello/Ack (for example from environment config),
// so checking both conditions prevents an early unauthenticated GET during
// the normal bootstrap race.
