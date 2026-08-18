package store

import (
	"context"

	"velox-server/internal/forwardingstore"
)

// store_creator_forwardings_metrics.go delegates the forwarding queue health
// snapshot to the internal/forwardingstore leaf and re-exports the snapshot
// type. The SQL lives in the leaf.

// ForwardingQueueMetrics is a type alias for the canonical
// forwardingstore.ForwardingQueueMetrics snapshot.
type ForwardingQueueMetrics = forwardingstore.ForwardingQueueMetrics

// GetForwardingQueueMetrics returns the current queue depth and oldest
// pending age for creator_forwardings.
func (s *SQLiteStore) GetForwardingQueueMetrics(ctx context.Context) (ForwardingQueueMetrics, error) {
	return s.forwarding.GetForwardingQueueMetrics(ctx)
}
