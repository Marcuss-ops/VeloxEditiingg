package main

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// BackgroundRunner is the minimal interface for a named, context-driven
// long-running component. The Run method blocks until ctx is cancelled,
// then returns cleanly. Components that require a ticker or loop manage
// that internally (the supervisor only owns the lifecycle).
type BackgroundRunner interface {
	Name() string
	Run(ctx context.Context) error
}

// RunnerFunc is a convenience adapter so simple goroutine loops
// (zombie reaper, reconciler) can satisfy BackgroundRunner without a
// dedicated struct.
type RunnerFunc struct {
	name string
	fn   func(ctx context.Context) error
}

func (r RunnerFunc) Name() string                  { return r.name }
func (r RunnerFunc) Run(ctx context.Context) error { return r.fn(ctx) }

// BackgroundSupervisor owns a set of BackgroundRunners and provides
// coordinated start / graceful-stop lifecycle.
//
// All runners share a single context tree: Run creates a cancellable
// context and starts every registered runner in its own goroutine.
// Shutdown cancels that context and waits (with a bounded timeout) for
// every runner to return.
//
// If any runner returns a non-nil error BEFORE the context is cancelled,
// the supervisor logs it but continues — a failing runner must not kill
// the surviving ones (the operator may still want the other runners
// alive while debugging the broken one). When *all* runners return
// (cleanly or with error), Run unblocks.
type BackgroundSupervisor struct {
	runners []BackgroundRunner
}

// NewBackgroundSupervisor creates an empty supervisor.
func NewBackgroundSupervisor() *BackgroundSupervisor {
	return &BackgroundSupervisor{}
}

// Register adds a runner to the supervisor. Duplicate names are rejected
// (must-fail at composition time — a misconfigured supervisor is a
// startup bug, not a runtime recovery scenario).
func (s *BackgroundSupervisor) Register(r BackgroundRunner) error {
	if r == nil {
		return fmt.Errorf("supervisor: nil runner")
	}
	name := r.Name()
	if name == "" {
		return fmt.Errorf("supervisor: runner %T has empty Name()", r)
	}
	for _, existing := range s.runners {
		if existing.Name() == name {
			return fmt.Errorf("supervisor: duplicate runner name %q", name)
		}
	}
	s.runners = append(s.runners, r)
	log.Printf("[SUPERVISOR] registered runner: %s", name)
	return nil
}

// Run starts every registered runner in its own goroutine and blocks
// until ALL runners have exited (either because ctx was cancelled from
// the outside, or because all runners returned on their own).
//
// If ctx is already cancelled when Run is called, it returns
// immediately without starting any runners.
func (s *BackgroundSupervisor) Run(ctx context.Context) error {
	if len(s.runners) == 0 {
		log.Printf("[SUPERVISOR] no runners registered — supervisor idle")
		<-ctx.Done()
		return ctx.Err()
	}

	supCtx, supCancel := context.WithCancel(ctx)
	defer supCancel()

	var wg sync.WaitGroup
	wg.Add(len(s.runners))

	for _, r := range s.runners {
		r := r
		go func() {
			defer wg.Done()
			log.Printf("[SUPERVISOR] starting runner: %s", r.Name())
			if err := r.Run(supCtx); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				log.Printf("[SUPERVISOR] runner %s exited with error: %v", r.Name(), err)
			} else {
				log.Printf("[SUPERVISOR] runner %s exited cleanly", r.Name())
			}
		}()
	}

	log.Printf("[SUPERVISOR] %d runners started", len(s.runners))
	wg.Wait()
	log.Printf("[SUPERVISOR] all runners stopped")
	return nil
}

// Shutdown is intentionally removed — the supervisor lifecycle is managed
// externally by the caller (runServer). The caller cancels the parent
// context passed to Run, waits on a supervisorDone channel, and enforces
// its own timeout. An additional Shutdown method on the supervisor would
// be misleading because it cannot guarantee all runners have stopped
// without also holding the context that drives them.

// Len returns the number of registered runners (diagnostic).
func (s *BackgroundSupervisor) Len() int {
	return len(s.runners)
}

// Names returns the name of every registered runner (for /ready checks).
func (s *BackgroundSupervisor) Names() []string {
	names := make([]string, len(s.runners))
	for i, r := range s.runners {
		names[i] = r.Name()
	}
	return names
}
