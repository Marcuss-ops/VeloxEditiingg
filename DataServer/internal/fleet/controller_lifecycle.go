package fleet

// controller_lifecycle.go: Run / Start / Stop / Done for the FleetController.
// Split out of controller.go; the type + constructor live in controller.go,
// dispatch in controller_tick.go, and persistence in controller_persistence.go.

import (
	"context"
	"errors"
	"time"
)

// Run blocks until ctx is cancelled or Stop() is called. Matches
// the supervisor.Runner.Run signature so the production boot
// wires the FleetController directly via the supervisor (no
// goroutine spawning inside the Run callback — the supervisor
// owns the goroutine).
//
// On EITHER clean-exit path (ctx-done or Stop) Run returns nil:
// this is critical so the supervisor does not treat graceful
// shutdown as a transient failure (otherwise the supervisor
// would backoff-and-retry on SIGINT, which is wrong).
//
// Production wiring (cmd/server/bootstrap_composition.go)
// registers Run as a ClassRestartable runner; tests drive Tick
// directly when they need deterministic control over the
// dispatch surface.
func (c *FleetController) Run(ctx context.Context) error {
	if c == nil {
		return errors.New("fleet: nil controller")
	}
	if c.store == nil {
		return ErrStoreNotConfigured
	}
	ticker := time.NewTicker(c.tickI)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.cancel:
			return nil
		case <-ticker.C:
			c.Tick(ctx)
		}
	}
}

// Start launches Run in a goroutine and exposes Done() for
// shutdown synchronisation. Idempotent: a second Start returns
// ErrAlreadyRunning.
//
// Tests that want explicit lifecycle hooks (Start/Stop/Done)
// drive the controller here; production boot prefers Run
// directly via the supervisor because the supervisor owns the
// goroutine and the run-done channel.
func (c *FleetController) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("fleet: nil controller")
	}
	if c.store == nil {
		return ErrStoreNotConfigured
	}
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return ErrAlreadyRunning
	}
	c.running = true
	c.cancel = make(chan struct{})
	c.done = make(chan struct{})
	c.mu.Unlock()

	go func() {
		defer close(c.done)
		_ = c.Run(ctx)
	}()
	return nil
}

// Stop signals the tick goroutine to exit. Idempotent: a second
// call on an already-stopped controller returns nil (the cancel
// channel is one-shot; a second close would panic). The
// caller-facing contract is "you may call Stop at most once
// per Start".
func (c *FleetController) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return
	}
	close(c.cancel)
	c.running = false
}

// Done returns a channel that closes when the tick goroutine
// has fully exited. runUntilShutdown's graceful teardown waits
// on this with a 15-second budget per bootstrap.go:runUntilShutdown.
//
// For a never-started controller, Done returns a pre-closed
// channel so the caller does NOT need to special-case nil.
// (init via the Start path is the production case; never-started
// is a test fixture.)
func (c *FleetController) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return c.done
}
