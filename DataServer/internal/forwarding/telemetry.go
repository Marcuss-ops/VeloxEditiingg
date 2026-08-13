// Package forwarding provides the CreatorForwardingRunner.
package forwarding

// Telemetry is the forwarding runner's narrow operational-observability seam.
// Counters are monotonic; the queue gauges are approximate point-in-time
// values refreshed by the Run loop. Implementations must use bounded labels:
// job_id, forwarding_id, remote URLs and error messages are intentionally
// absent so the Prometheus registry stays low-cardinality.
type Telemetry interface {
	RecordClaimed(count int64)
	RecordForwarded()
	RecordFailed()
	RecordRetried()
	ObserveQueue(depth, oldestPendingAgeSeconds int64)
}
