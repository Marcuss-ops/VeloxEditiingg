package supervisor

// supervisor_run.go: Run / runLoop / safeCall / sleepCtx — the
// orchestration loop of the Supervisor. Split out of supervisor.go;
// types live in supervisor_types.go and the diagnostics surface in
// supervisor_diagnostics.go.

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Run starts every registered runner in its own goroutine and blocks
// until ALL runners have exited.
//
// Returns nil when the parent ctx was cancelled (graceful shutdown).
// Returns a non-nil error when a ClassCritical runner exhausts its
// retry budget; in that case the supervisor has already cancelled its
// internal ctx so every OTHER runner is torn down at the same time.
func (s *Supervisor) Run(ctx context.Context) error {
	if len(s.runners) == 0 {
		log.Printf("[SUPERVISOR] no runners registered — supervisor idle")
		<-ctx.Done()
		return ctx.Err()
	}

	supCtx, supCancel := context.WithCancel(ctx)
	defer supCancel()

	var wg sync.WaitGroup
	wg.Add(len(s.runners))

	var fatalMu sync.Mutex
	var fatalErr error

	for _, r := range s.runners {
		r := r
		// Pre-mark the runner as STARTING so a /ready check before
		// runLoop starts doesn't gate-fail spuriously.
		s.mu.Lock()
		s.states[r.Name] = RunnerStarting
		s.mu.Unlock()
		go func() {
			defer wg.Done()
			defer func() {
				s.mu.Lock()
				// Preserve RunnerFailed set by runLoop on exhaustion —
				// only demote to Stopped if the runner exited cleanly.
				if s.states[r.Name] != RunnerFailed {
					s.states[r.Name] = RunnerStopped
				}
				s.mu.Unlock()
			}()
			s.runLoop(supCtx, &fatalMu, &fatalErr, r, supCancel)
		}()
	}

	log.Printf("[SUPERVISOR] %d runners started", len(s.runners))
	wg.Wait()
	log.Printf("[SUPERVISOR] all runners stopped")
	fatalMu.Lock()
	defer fatalMu.Unlock()
	return fatalErr
}

// runLoop is the per-runner goroutine body. Honours the runner's Class
// and Policy: OneShot runs once; Restartable retries bounded; Critical
// retries forever (or up to the positive MaxRetries budget) and on
// exhaustion cancels the supervisor-internal ctx so every OTHER runner
// is torn down at the same time.
func (s *Supervisor) runLoop(
	ctx context.Context,
	fatalMu *sync.Mutex,
	fatalErr *error,
	r Runner,
	supCancel context.CancelFunc,
) {
	attempt := 0
	// maxR is the EFFECTIVE retry budget (raw MaxRetries mapped through
	// effectiveMaxRetries). It's used in the log lines that communicate
	// the retry ceiling to operators — the "retry N/ceiling" format
	// needs the effective budget so ClassCritical/MaxRetries=0 reads
	// as "retry 1/inf" (infinite) rather than "retry 1/0" (which would
	// misleadingly suggest the runner has no retries left). The exit
	// decision itself goes through shouldExitAfterFailure which takes
	// the raw Policy.MaxRetries.
	maxR := effectiveMaxRetries(r.Class, r.Policy.MaxRetries)
	for {
		s.mu.Lock()
		s.states[r.Name] = RunnerRunning
		s.mu.Unlock()

		log.Printf("[SUPERVISOR] starting runner: name=%s class=%s attempt=%d/%s",
			r.Name, r.Class.String(), attempt+1, retryCeilingString(maxR))
		err := safeCall(ctx, r.Run, r.Policy.RestartOnPanic, r.Name)

		if ctx.Err() != nil {
			log.Printf("[SUPERVISOR] runner %s exiting: %v", r.Name, ctx.Err())
			return
		}

		// nil err with a LIVE ctx is a false-success path for permanent
		// runners (ClassRestartable / ClassCritical). Remap to
		// ErrUnexpectedExit so the restart machinery kicks in rather
		// than marking the runner STOPPED and silently leaving the
		// master with a dead delivery / forwarding / outbox pipeline.
		if err == nil && r.Class != ClassOneShot {
			err = ErrUnexpectedExit
			log.Printf("[SUPERVISOR] runner %s returned nil err with live ctx (class=%s); treating as ErrUnexpectedExit",
				r.Name, r.Class.String())
		}

		if err == nil {
			s.mu.Lock()
			s.states[r.Name] = RunnerStopped
			s.mu.Unlock()
			log.Printf("[SUPERVISOR] runner %s exited cleanly", r.Name)
			return
		}

		attempt++

		s.mu.Lock()
		s.states[r.Name] = RunnerFailed
		s.mu.Unlock()

		switch r.Class {
		case ClassOneShot:
			log.Printf("[SUPERVISOR] runner %s one-shot failed (NOT restarted): class=%s err=%v",
				r.Name, r.Class.String(), err)
			return

		case ClassRestartable:
			if shouldExitAfterFailure(r.Class, r.Policy.MaxRetries, attempt) {
				log.Printf("[SUPERVISOR] runner %s restartable budget EXHAUSTED after %d attempts; removing from supervisor: class=%s last_err=%v",
					r.Name, r.Policy.MaxRetries, r.Class.String(), err)
				return
			}
			s.mu.Lock()
			s.states[r.Name] = RunnerBackingOff
			s.mu.Unlock()
			delay := r.Policy.backoffFor(attempt)
			log.Printf("[SUPERVISOR] runner %s FAILED (restartable); sleeping %s before retry %d/%s: err=%v",
				r.Name, delay, attempt+1, retryCeilingString(maxR), err)
			if !sleepCtx(ctx, delay) {
				log.Printf("[SUPERVISOR] runner %s restartable: ctx cancelled during backoff", r.Name)
				return
			}

		case ClassCritical:
			if shouldExitAfterFailure(r.Class, r.Policy.MaxRetries, attempt) {
				log.Printf("[SUPERVISOR] runner %s CRITICAL budget EXHAUSTED after %d attempts; cancelling supervisor: class=%s last_err=%v",
					r.Name, r.Policy.MaxRetries, r.Class.String(), err)
				fatalMu.Lock()
				*fatalErr = fmt.Errorf("supervisor: critical runner %q exhausted %d retries: %w", r.Name, r.Policy.MaxRetries, err)
				fatalMu.Unlock()
				supCancel()
				return
			}
			s.mu.Lock()
			s.states[r.Name] = RunnerBackingOff
			s.mu.Unlock()
			delay := r.Policy.backoffFor(attempt)
			log.Printf("[SUPERVISOR] runner %s FAILED (critical); sleeping %s before retry %d/%s: err=%v",
				r.Name, delay, attempt+1, retryCeilingString(maxR), err)
			if !sleepCtx(ctx, delay) {
				log.Printf("[SUPERVISOR] runner %s critical: ctx cancelled during backoff", r.Name)
				return
			}

		default:
			log.Printf("[SUPERVISOR] runner %s unknown class=%d; treating as one-shot: err=%v",
				r.Name, int(r.Class), err)
			return
		}
	}
}

func retryCeilingString(n int) string {
	if n < 0 {
		return "inf"
	}
	return fmt.Sprintf("%d", n)
}

// safeCall invokes fn under an optional panic recovery. When
// restartOnPanic is true, a recovered panic is converted to an error
// so the restart loop can treat it identically to a normal failure.
// When false, the panic propagates upward (intended for tests).
func safeCall(ctx context.Context, fn func(context.Context) error, restartOnPanic bool, name string) (err error) {
	if !restartOnPanic {
		return fn(ctx)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("supervisor runner %q panicked: %v", name, r)
		}
	}()
	return fn(ctx)
}

// sleepCtx blocks for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
