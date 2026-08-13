package protectedassets

import (
	"context"
	"errors"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// Run blocks until ctx is cancelled. The first tick fires
// immediately on entry (so the snapshot is fresh on boot); subsequent
// ticks fire on the periodic ticker OR on ctx cancellation.
//
// Tick failures surface via OnError (if set) — runTickOnce owns
// the OnError firing so a single tick produces exactly one OnError
// event, regardless of whether the caller drove TickOnce or Run.
// Cancellation returns ctx.Err() verbatim.
//
// Returns ctx.Err() on clean cancellation, nil on a misconstructed
// poller (nil Client / zero Interval) — both are programmer
// errors caught at boot, NOT poll failures.
func (p *ProtectedAssetsPoller) Run(ctx context.Context) error {
	if p.Client == nil {
		return errors.New("protectedassets.ProtectedAssetsPoller.Run: nil Client")
	}
	if p.Interval <= 0 {
		return errors.New("protectedassets.ProtectedAssetsPoller.Run: zero Interval (call NewProtectedAssetsPoller instead of zeroing after construction)")
	}
	p.mu.Lock()
	p.sessionGated = true
	p.mu.Unlock()

	// Registration establishes the worker credential/session used by the
	// protected-assets endpoint. Do not poll before that gate is open: an
	// early request would be an avoidable 401 during bootstrap. Direct
	// TickOnce callers remain available for deterministic tests.
	if err := p.waitForRegistration(ctx); err != nil {
		return err
	}

	// Initial bootstrap fetch is retried until a valid 2xx snapshot is
	// received or the session context is cancelled. Once the registration
	// and authentication gate opens, a normal bootstrap must not expose a
	// transient 401 as the first observed poll result.
	for {
		if err := p.runTickOnce(ctx, "protectedassets.ProtectedAssetsPoller: initial tick"); err == nil {
			break
		}
		if err := waitContextTimer(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Re-check registration on every poll. A reconnect can invalidate
			// the data-plane credential after the initial barrier opened.
			if err := p.waitForRegistration(ctx); err != nil {
				return err
			}
			// Discard the returned error: runTickOnce already fired OnError
			// if it was set. The Run loop never stops on a poll failure.
			p.runTickOnce(ctx, "protectedassets.ProtectedAssetsPoller: tick")
		}
	}
}

// TickOnce runs one fetch synchronously and updates p.snap on
// success. Returns the underlying error to the caller verbatim
// (caller decides what to do with it; tests assert on it directly).
//
// Public so tests drive deterministically without the loop
// overhead — see the *_test.go file for the contract tests.
//
// CONTRACT:
//   - On success: p.snap is replaced; OnSuccess fires (if set);
//     fn returns nil.
//   - On failure: p.snap is UNAFFECTED; OnError fires (if set)
//     with the underlying error verbatim (no double-wrap); fn
//     returns the error.
//
// Run uses the shared runTickOnce helper with a label prefix so
// log lines from Run's automatic ticks carry "initial
// protected-assets poll" / "protected-assets poll" context;
// TickOnce passes an empty label so callers driving it directly
// see the raw error in OnError without prefix noise.
func (p *ProtectedAssetsPoller) TickOnce(ctx context.Context) error {
	return p.runTickOnce(ctx, "")
}

// runTickOnce is the shared fetch-and-notify helper used by both
// Run (with a label prefix for log identification) and
// TickOnce (with empty label for direct-call observability).
//
// Failure semantics are identical in both paths:
//   - p.snap is UNTOUCHED.
//   - p.OnError fires (if set) with the underlying error.
//   - The error is returned to the caller.
func (p *ProtectedAssetsPoller) waitForRegistration(ctx context.Context) error {
	for {
		registered := telemetry.GlobalReady().Snapshot().Registered
		// Do not assume an arbitrary ProtectedAssetsAPI is authenticated:
		// production uses *api.Client, but a fake or alternate client must
		// explicitly expose its current bearer token before Run may issue
		// the first request.
		authenticated := false
		if client, ok := p.Client.(interface{ AuthToken() string }); ok {
			authenticated = strings.TrimSpace(client.AuthToken()) != ""
		}
		if registered && authenticated {
			return nil
		}
		p.invalidateReadiness()
		if err := waitContextTimer(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

// waitContextTimer waits without retaining a timer until its deadline after
// the context has already been cancelled. This helper is used by the retry
// loops above, where time.After would allocate a timer on every iteration.
func waitContextTimer(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// WaitReady blocks until the current registration session has received a
// valid protected-assets snapshot or ctx is cancelled. A lost session
// re-arms the channel, so a reconnect cannot inherit the previous session's
// cleanup permission.
