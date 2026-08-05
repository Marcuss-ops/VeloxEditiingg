package downloader

// operational.go contains the manager-wide, low-cardinality view used by
// worker telemetry and Grafana. It intentionally contains no asset, job, task,
// URL, or hash identifiers.

// OperationalSnapshot is the current aggregate state of one worker's asset
// download manager. Bytes and rates are sums across registered transfers;
// ETA is the longest active transfer ETA. CoalescedRequestsTotal is a
// monotonically increasing manager-local count of callers that joined an
// already-running transfer.
type OperationalSnapshot struct {
	ActiveTransfers   int
	QueuedTransfers   int
	ReadyTransfers    int
	FailedTransfers   int
	CacheHitTransfers int

	BytesDownloaded int64
	BytesTotal      int64
	ThroughputBPS   float64
	ETASeconds      int64

	CoalescedRequestsTotal uint64
}
