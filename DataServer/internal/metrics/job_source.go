// Package metrics / job_source.go
//
// The job-queue read surface the metrics supervisor consumes. It is
// declared here (consumer-side) so internal/metrics keeps its
// no-dependency-on-store contract: the production implementation is
// *store.SQLiteJobRepository, satisfied structurally — no import from
// store into metrics, no interface leakage in either direction.
package metrics

import (
	"context"
	"time"

	"velox-server/internal/jobs"
)

// JobQueueSource answers the two questions the job-queue families
// exist for:
//
//   - QueueSnapshot: how many jobs are waiting / in flight right now
//     and how long the oldest waiting one has been there (`now` is
//     passed in so the caller owns the clock domain);
//   - RecentTerminalJobs: which jobs reached a terminal state since a
//     watermark, oldest first — the input for the outcome counters and
//     the phase-attributed end-to-end latency histogram.
//
// Both methods are read-only and cheap enough for a 15s supervisor
// tick. Wiring is optional: a nil source keeps the supervisor green
// and simply leaves the job families un-stamped (their absence on
// /metrics is the honest signal that the source is not wired).
type JobQueueSource interface {
	QueueSnapshot(ctx context.Context, now time.Time) (jobs.QueueSnapshot, error)
	RecentTerminalJobs(ctx context.Context, since time.Time, limit int) ([]jobs.CompletionRecord, error)
}
