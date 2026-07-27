// Package protectedasset is the master-side periodic snapshot of
// "the next N dispatchable jobs' Drive clip IDs". Workers read this
// snapshot via the HTTP handler (Pass 6) to know which assets they
// MUST NOT delete from their local cache.
//
// Service is a thin in-memory cache:
//
//   - written by Refresh() (one-shot operation, deterministic)
//   - ticked by Run() (production loop, NOT unit-tested)
//   - read by Snapshot() (safe under -race)
//
// Layering note: the actual SELECT lives in shared/dispatchable so
// the master scheduler AND the snapshot service consume identical
// WHERE/ORDER BY. This Service owns only the in-memory union +
// sort + version bookkeeping.
//
// Race model:
//
//   - Refresh() takes the write lock for the duration of the
//     struct-swap. Version increments INSIDE the write lock so
//     a reader observes a monotonically increasing number that
//     corresponds to the same row data it sees.
//   - Snapshot() takes the read lock briefly, copies the struct
//     by value (Snapshot is ~5 fields, ~144 bytes worst case).
//   - Many snapshot readers + one writer → RWMutex is the right
//     primitive: RLock is cheap and contention-free under readers.
package protectedasset

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"velox-shared/assetref"
	"velox-shared/dispatchable"
)

// Snapshot is the in-memory representation of "what the worker
// cleaner MUST keep this round". Returned as-is from HTTP handlers
// (see Pass 6).
type Snapshot struct {
	Version       uint64    `json:"version"`
	GeneratedAt   time.Time `json:"generated_at"`
	LookaheadJobs int       `json:"lookahead_jobs"`
	DriveFileIDs  []string  `json:"drive_file_ids"`
}

// Repo is the data-source interface the Service consumes. We use this
// indirection (rather than dispatchable.Querier directly) so:
//
//   - tests can inject a fake Repo without bringing in go-sqlmock or
//     hand-rolling *sql.Rows;
//   - the Service is decoupled from the SELECT implementation in
//     shared/dispatchable — a future query-builder switch in dispatchable
//     does not change this package's tests.
//
// Production wiring: a 1-line adapter
//
//	repo := protectedasset.RepoFunc(func(ctx context.Context, limit int) ([]dispatchable.Job, error) {
//	    return dispatchable.ListNextDispatchableJobs(ctx, db, limit)
//	})
type Repo interface {
	ListNextDispatchableJobs(ctx context.Context, limit int) ([]dispatchable.Job, error)
}

// RepoFunc adapts a plain function to the Repo interface. Used to
// keep production wiring a one-liner (see package doc).
type RepoFunc func(ctx context.Context, limit int) ([]dispatchable.Job, error)

// ListNextDispatchableJobs forwards to the underlying function.
func (f RepoFunc) ListNextDispatchableJobs(ctx context.Context, limit int) ([]dispatchable.Job, error) {
	return f(ctx, limit)
}

// Service stores the last computed snapshot. Methods are safe for
// concurrent use.
//
// Equivalent-zero-value behaviour:
//   - A freshly constructed Service's Snapshot() returns the zero
//     Snapshot (Version == 0, GeneratedAt == IsZero(), empty IDs).
//   - HTTP handlers MUST treat Version == 0 as "snapshot not yet
//     generated" and respond 503 (Pass 6 aligns with this).
type Service struct {
	mu       sync.RWMutex
	snapshot Snapshot

	repo    Repo
	limit   int
	now     func() time.Time
	onError func(error) // nil-safe hook for Run loop Refresh failures
}

// DefaultLookahead is the design-doc canonical limit. Both the
// scheduler and the snapshot service converge on this number.
const DefaultLookahead = 10

// NewService constructs the service. limit <= 0 falls back to
// DefaultLookahead. clock defaults to time.Now().UTC() and is
// overridable via the (test-only) SetClock.
func NewService(repo Repo, limit int) *Service {
	if repo == nil {
		panic("protectedasset.NewService: nil Repo")
	}
	if limit <= 0 {
		limit = DefaultLookahead
	}
	return &Service{
		repo:  repo,
		limit: limit,
		now:   func() time.Time { return time.Now().UTC() },
	}
}

// SetClock replaces the clock function. Used by tests for
// deterministic GeneratedAt. Not safe for concurrent use; intended
// for constructor-time wiring.
func (s *Service) SetClock(now func() time.Time) *Service {
	s.now = now
	return s
}

// WithErrorHandler attaches a callback that receives every error
// surfaced by the Run() loop on a failed Refresh. The hook is
// called from the Run goroutine and MUST be cheap & non-blocking
// (forwarding to slog/metrics is the canonical use); pass nil to
// unregister (the default).
//
// Pass 6 (HTTP handler) should wire this BEFORE the handler is
// exposed, otherwise a chronically broken Refresh loop fails
// silently and the endpoint keeps serving a stale snapshot.
func (s *Service) WithErrorHandler(hook func(error)) *Service {
	s.onError = hook
	return s
}

// Refresh reads the repo, unions Drive IDs across the candidate
// payloads, sorts ascending, and atomically swaps the snapshot for
// a new monotonically-versioned one.
//
// Errors from the repo are propagated to the caller; Run() logs and
// continues. A failed Refresh leaves the previous snapshot intact
// (we never half-update).
func (s *Service) Refresh(ctx context.Context) error {
	jobs, err := s.repo.ListNextDispatchableJobs(ctx, s.limit)
	if err != nil {
		return fmt.Errorf("protectedasset.Refresh: repo: %w", err)
	}

	protected := make(map[string]struct{}, len(jobs))
	for _, j := range jobs {
		for id := range assetref.ExtractDriveFileIDs(j.Payload) {
			protected[id] = struct{}{}
		}
	}

	ids := make([]string, 0, len(protected))
	for id := range protected {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	s.mu.Lock()
	s.snapshot = Snapshot{
		Version:       s.snapshot.Version + 1,
		GeneratedAt:   s.now(),
		LookaheadJobs: len(jobs),
		DriveFileIDs:  ids,
	}
	s.mu.Unlock()
	return nil
}

// Snapshot returns the current snapshot by value. Safe for
// concurrent readers.
func (s *Service) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

// Run drives Refresh in a ticker loop until ctx is done. Interval
// must be positive; 0 / negative returns an error (use Refresh
// directly if you want manual ticks). Errors from Refresh are
// swallowed — this is the production polish loop and a single
// DB hiccup must not panic or crash the master.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("protectedasset.Run: interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Refresh(ctx); err != nil {
				if s.onError != nil {
					s.onError(err)
				}
				// Loop continues regardless — a single failed tick
				// must NOT halt the periodic poll. Observability is
				// the caller's job; Pass 6 routes to slog/metrics.
			}
		}
	}
}
