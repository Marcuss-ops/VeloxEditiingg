package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
)

type canonicalPhaseMetric struct {
	phase           string
	durationMS      int64
	eventCount      int64
	cpuMS           float64
	queueWaitMS     float64
	bytesIn         int64
	bytesOut        int64
	frames          int64
	maxDurationMS   int64
	firstStartedAt  time.Time
	lastCompletedAt time.Time
}

// persistCanonicalPhaseMetrics stores the phase-level facts needed for a
// bottleneck report. It deliberately aggregates only the producer-declared
// canonical Phase field; component/action detail remains in the existing
// summary and event tables.
func persistCanonicalPhaseMetrics(ctx context.Context, tx *sql.Tx, attemptID string, identity phaseTimingIdentity, timings []taskattempts.PhaseTimingDetailed) error {
	byPhase := make(map[string]*canonicalPhaseMetric)
	for _, timing := range timings {
		if timing.Phase == "" {
			continue
		}
		m := byPhase[timing.Phase]
		if m == nil {
			m = &canonicalPhaseMetric{phase: timing.Phase, maxDurationMS: timing.DurationMS}
			byPhase[timing.Phase] = m
		}
		m.durationMS += timing.DurationMS
		m.eventCount++
		m.cpuMS += timing.CPUMS
		m.queueWaitMS += timing.QueueWaitMS
		m.bytesIn += timing.BytesIn
		m.bytesOut += timing.BytesOut
		m.frames += timing.Frames
		if timing.DurationMS > m.maxDurationMS {
			m.maxDurationMS = timing.DurationMS
		}
		if !timing.StartedAt.IsZero() && (m.firstStartedAt.IsZero() || timing.StartedAt.Before(m.firstStartedAt)) {
			m.firstStartedAt = timing.StartedAt
		}
		if !timing.CompletedAt.IsZero() && (m.lastCompletedAt.IsZero() || timing.CompletedAt.After(m.lastCompletedAt)) {
			m.lastCompletedAt = timing.CompletedAt
		}
	}
	for _, m := range byPhase {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO task_attempt_phase_metrics (
				attempt_id, job_id, task_id, worker_id, phase, duration_ms,
				event_count, cpu_ms, queue_wait_ms, bytes_in, bytes_out, frames,
				max_duration_ms, first_started_at, last_completed_at, calculated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(attempt_id, phase) DO UPDATE SET
				job_id=excluded.job_id, task_id=excluded.task_id,
				worker_id=excluded.worker_id, duration_ms=excluded.duration_ms,
				event_count=excluded.event_count, cpu_ms=excluded.cpu_ms,
				queue_wait_ms=excluded.queue_wait_ms, bytes_in=excluded.bytes_in,
				bytes_out=excluded.bytes_out, frames=excluded.frames,
				max_duration_ms=excluded.max_duration_ms,
				first_started_at=excluded.first_started_at,
				last_completed_at=excluded.last_completed_at,
				calculated_at=excluded.calculated_at`,
			attemptID, identity.JobID, identity.TaskID, identity.WorkerID,
			m.phase, m.durationMS, m.eventCount, m.cpuMS, m.queueWaitMS,
			m.bytesIn, m.bytesOut, m.frames, m.maxDurationMS,
			formatTimingTime(m.firstStartedAt), formatTimingTime(m.lastCompletedAt), nowRFC3339Nano())
		if err != nil {
			return fmt.Errorf("task ingest canonical phase metrics insert %s: %w", m.phase, err)
		}
	}
	return nil
}
