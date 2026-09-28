// Package observability / job_signals.go
//
// Focused reads behind the two runtime alert rules that the operator
// review asked for after the "job stuck in PENDING for 7 minutes with
// 4 idle workers" incident. Both are deliberately narrow: one scalar
// each, both nil- and absence-aware so a deployment without the
// optional reader degrades to "rule does not fire" instead of a
// fabricated healthy number.
package observability

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/jobs"
)

// JobQueueReader is the optional refinement of JobReader behind the
// stuck-job alert: the instantaneous queue snapshot (pending, running,
// oldest pending age). *store.SQLiteJobRepository satisfies it; a
// reader that does not is not an error — the rule simply stays quiet.
type JobQueueReader interface {
	QueueSnapshot(ctx context.Context, now time.Time) (jobs.QueueSnapshot, error)
}

// PrefetchFailureReader counts journaled prefetch failure events in a
// window. It is the SQL twin of the Prometheus family
// velox_prefetch_failures_total: the alert engine runs in-process but
// outside the metrics registry, so the durable event journal is its
// read surface for the same fact.
type PrefetchFailureReader interface {
	CountRecentPrefetchFailures(ctx context.Context, since time.Time) (int64, error)
}

// OldestPendingJobAge returns the age of the oldest PENDING job as of
// `now`, plus whether the queue reader is wired at all (second return
// value). A wired reader with an empty queue returns (0, true): age
// zero means "nothing is waiting", absence of the reader means "we do
// not know" — the two must never collapse into the same alert input.
func (s *Service) OldestPendingJobAge(ctx context.Context, now time.Time) (time.Duration, bool, error) {
	if s == nil || s.jobs == nil {
		return 0, false, nil
	}
	reader, ok := s.jobs.(JobQueueReader)
	if !ok {
		return 0, false, nil
	}
	snapshot, err := reader.QueueSnapshot(ctx, now)
	if err != nil {
		return 0, true, fmt.Errorf("observability: job queue snapshot: %w", err)
	}
	return snapshot.OldestPendingAge, true, nil
}

// RecentPrefetchFailures returns how many prefetch failure events were
// journaled inside `window`, plus whether the reader is wired.
func (s *Service) RecentPrefetchFailures(ctx context.Context, window time.Duration) (int64, bool, error) {
	if s == nil || s.prefetchFailures == nil {
		return 0, false, nil
	}
	count, err := s.prefetchFailures.CountRecentPrefetchFailures(ctx, time.Now().Add(-window))
	if err != nil {
		return 0, true, fmt.Errorf("observability: recent prefetch failures: %w", err)
	}
	return count, true, nil
}
