// Package outbox/dispatcher — polling loop that claims events from
// outbox_events, dispatches them to handlers registered in the Registry,
// and applies the retry / failure rules of PR 8.
//
// Guarantees
// ----------
//   * Survives dispatcher crashes: each event is claimed atomically with
//     status=PROCESSING + locked_by/locked_until. If a dispatcher dies
//     with the row locked, the next dispatcher's Claim sees
//     (locked_until < now) and re-claims it.
//   * Event marked PROCESSED only after Handler.Handle returns a nil
//     error. Transient retryable errors leave the row at status=PROCESSING
//     with extended lock_until. Permanent errors OR attempt_count
//     exceeding MaxAttempts move the event to status=FAILED.
//   * No double-effect: handlers must be idempotent. The dispatcher does
//     not deduplicate beyond what the database provides — by primary
//     key. If a producer accidentally writes two rows with the same
//     event_id, the second is an immediate INSERT-constraint failure.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Config holds dispatcher behaviour knobs.
type Config struct {
	// PollInterval is how often the loop awakens to attempt a claim.
	// Zero defaults to 1s.
	PollInterval time.Duration
	// BatchSize caps the number of rows claimed per cycle. Zero defaults
	// to 32 — same default as the store layer.
	BatchSize int
	// LockDuration is how long a claim reservation remains valid before
	// another dispatcher may re-claim. Zero defaults to 30s.
	LockDuration time.Duration
	// MaxAttempts is the per-event cap before transitioning to FAILED.
	// Zero defaults to 5.
	MaxAttempts int
}

func (c *Config) applyDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 32
	}
	if c.LockDuration <= 0 {
		c.LockDuration = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
}

// Dispatcher polls the Store and invokes handlers from the Registry.
type Dispatcher struct {
	store    *Store
	registry *Registry
	cfg      Config
	logger   *log.Logger

	// identity (used as locked_by)
	id string

	mu      sync.Mutex
	running bool
	cancel  chan struct{}
}

// NewDispatcher builds a dispatcher (does not run it).
func NewDispatcher(store *Store, registry *Registry, cfg Config) *Dispatcher {
	cfg.applyDefaults()
	return &Dispatcher{
		store:    store,
		registry: registry,
		cfg:      cfg,
		logger:   log.Default(),
		id:       fmt.Sprintf("dispatcher-%d", time.Now().UnixNano()),
	}
}

// Run starts the polling loop. Blocks until ctx is cancelled or Stop
// is called. A panic inside the loop is caught and a tick is skipped.
//
// Run is safe to call once per Dispatcher; concurrent Run calls return
// immediately.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return errors.New("outbox: dispatcher already running")
	}
	d.running = true
	d.cancel = make(chan struct{})
	ticker := time.NewTicker(d.cfg.PollInterval)
	d.mu.Unlock()
	defer ticker.Stop()

	d.logger.Printf("[OUTBOX] dispatcher started (poll=%s batch=%d lock=%s max_attempts=%d)",
		d.cfg.PollInterval, d.cfg.BatchSize, d.cfg.LockDuration, d.cfg.MaxAttempts)

	for {
		select {
		case <-ctx.Done():
			d.logger.Printf("[OUTBOX] context done — stopping")
			d.markStopped()
			return nil
		case <-d.cancel:
			d.logger.Printf("[OUTBOX] stop requested — exiting")
			d.markStopped()
			return nil
		case <-ticker.C:
			d.safeTick(ctx)
		}
	}
}

// Stop signals the dispatcher to exit at the next iteration. No-op if
// not running.
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.running {
		return
	}
	close(d.cancel)
}

// safeTick swallows panics per-cycle so a buggy handler cannot kill
// the loop for the entire master process.
func (d *Dispatcher) safeTick(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Printf("[OUTBOX] PANIC in tick: %v", r)
		}
	}()
	if err := d.Poll(ctx); err != nil {
		d.logger.Printf("[OUTBOX] poll error: %v", err)
	}
}

func (d *Dispatcher) markStopped() {
	d.mu.Lock()
	d.running = false
	d.mu.Unlock()
}

// Poll claims one batch of events and dispatches each one in turn.
//
// Exposed for tests; production code calls Run.
func (d *Dispatcher) Poll(ctx context.Context) error {
	lockUntil := time.Now().Add(d.cfg.LockDuration).UTC()
	events, err := d.store.Claim(ctx, d.id, lockUntil, d.cfg.BatchSize)
	if err != nil {
		return err
	}
	for _, e := range events {
		d.dispatchEvent(ctx, e, lockUntil)
	}
	return nil
}

// dispatchEvent runs one handler invocation and applies the rules from
// the PR 8 spec paragraph:
//
//   "evento marcato PROCESSED solo dopo successo"
//   "dispatcher crash"          → row stays PROCESSING; lock expiry re-claims it
//   "handler fallisce"          → permanent → FAILED, transient → retry
//   "handler retry"             → extend lock, re-attempt up to MaxAttempts
//   "handler idempotente"       → up to caller; dispatcher counts attempts
//
// Panic handling: a panic inside Handle MUST NOT leave the row in PROCESSING
// with stale locked_by/locked_until forever. We mark FAILED inside the defer
// so the operator sees the broken handler and the dispatch loop survives.
func (d *Dispatcher) dispatchEvent(ctx context.Context, e Event, lockUntil time.Time) {
	var panicked bool
	var panicVal any
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicVal = r
			d.logger.Printf("[OUTBOX] PANIC in handler (event_id=%s type=%s): %v",
				e.EventID, e.EventType, r)
			if mkErr := d.store.MarkFailed(ctx, e.EventID,
				fmt.Sprintf("panic: %v", r)); mkErr != nil {
				d.logger.Printf("[OUTBOX] mark failed (panic) error: %v", mkErr)
			}
		}
	}()

	h, err := d.registry.Lookup(e.EventType)
	if err != nil || panicked {
		if !panicked {
			d.logger.Printf("[OUTBOX] no handler for %q (event_id=%s) — marking FAILED",
				e.EventType, e.EventID)
			if mkErr := d.store.MarkFailed(ctx, e.EventID, err.Error()); mkErr != nil {
				d.logger.Printf("[OUTBOX] mark failed error: %v", mkErr)
			}
		}
		return
	}

	err = h.Handle(ctx, e)
	if err == nil && !panicked {
		if mkErr := d.store.MarkProcessed(ctx, e.EventID); mkErr != nil {
			d.logger.Printf("[OUTBOX] mark processed error: %v", mkErr)
		}
		return
	}
	if panicked {
		return // already handled in defer
	}

	// Handler returned non-nil.
	var hErr *HandlerError
	isTransient := errors.As(err, &hErr) && hErr.Transient

	// Permanent error: FAILED immediately.
	if !isTransient {
		d.logger.Printf("[OUTBOX] permanent handler failure event_id=%s type=%s: %v",
			e.EventID, e.EventType, err)
		if mkErr := d.store.MarkFailed(ctx, e.EventID, err.Error()); mkErr != nil {
			d.logger.Printf("[OUTBOX] mark failed error: %v", mkErr)
		}
		return
	}

	// Transient error: extend lock_until so the event re-enters the
	// ready queue once the lock window opens. If attempt_count has
	// already crossed MaxAttempts, mark FAILED instead.
	if e.AttemptCount >= d.cfg.MaxAttempts {
		d.logger.Printf("[OUTBOX] max attempts (%d) reached event_id=%s — FAILED",
			d.cfg.MaxAttempts, e.EventID)
		if mkErr := d.store.MarkFailed(ctx, e.EventID,
			fmt.Sprintf("max attempts reached: %v", err)); mkErr != nil {
			d.logger.Printf("[OUTBOX] mark failed error: %v", mkErr)
		}
		return
	}

	// Release the lock so the next Claim sees the row again immediately.
	if extErr := d.store.ExtendLock(ctx, e.EventID, lockUntil, err.Error()); extErr != nil {
		d.logger.Printf("[OUTBOX] extend lock error: %v", extErr)
	}
	_ = panicVal // silence unused variable warnings
}
