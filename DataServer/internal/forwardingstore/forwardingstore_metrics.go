package forwardingstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"velox-server/internal/storecore"
)

// ForwardingQueueMetrics is a point-in-time snapshot of the forwarding
// queue health used by the forwarding runner and exposed via the metrics
// supervisor. The runner calls this on a slower cadence (30s) to avoid
// COUNT/strftime queries on every 5s tick.
type ForwardingQueueMetrics struct {
	// QueueDepth is the count of rows in PENDING or RETRY_WAIT status.
	QueueDepth int64
	// OldestPendingAge is the age of the oldest PENDING row.
	OldestPendingAge time.Duration
}

// GetForwardingQueueMetrics returns the current queue depth and oldest
// pending age for creator_forwardings.
func (s *SQLiteForwardingStore) GetForwardingQueueMetrics(ctx context.Context) (ForwardingQueueMetrics, error) {
	var m ForwardingQueueMetrics

	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM creator_forwardings
		 WHERE status IN ('PENDING', 'RETRY_WAIT')`,
	).Scan(&m.QueueDepth)
	if err != nil {
		return m, storecore.WrapDBInfrastructure("GetForwardingQueueMetrics depth", err)
	}

	var ageSeconds sql.NullInt64
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(
		    CAST((strftime('%s','now') - strftime('%s', created_at)) AS INTEGER),
		    0)
		 FROM creator_forwardings
		 WHERE status = 'PENDING'
		 ORDER BY created_at ASC LIMIT 1`,
	).Scan(&ageSeconds)
	if errors.Is(err, sql.ErrNoRows) {
		// An empty pending queue is a valid metrics state, not a
		// database failure. Keep the zero-value age.
		return m, nil
	}
	if err != nil {
		return m, storecore.WrapDBInfrastructure("GetForwardingQueueMetrics oldest", err)
	}
	if ageSeconds.Valid && ageSeconds.Int64 > 0 {
		m.OldestPendingAge = time.Duration(ageSeconds.Int64) * time.Second
	}
	return m, nil
}
