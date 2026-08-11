package metrics

import "database/sql"

// OperationalTelemetry contains the low-cardinality measurements needed to
// separate renderer time from delivery and persistence time. It is kept
// separate from the scorecard collector so store/delivery packages can depend
// on a tiny structural interface without importing this package.
type OperationalTelemetry struct {
	deliveryQueueMS       *Family
	deliveryUploadMS      *Family
	deliveryTotalMS       *Family
	deliveryRetries       *Family
	deliveryTimeouts      *Family
	deliveryBytes         *Family
	deliveryUploadMbps    *Family
	deliveryProviderError *Family

	dbWriteWaitMS        *Family
	dbTransactionMS      *Family
	dbBusy               *Family
	dbBusyTimeout        *Family
	dbRetries            *Family
	dbWriteOperations    *Family
	dbReadOperations     *Family
	dbLongestTransaction *Family
	dbOpenConnections    *Family
	dbInUseConnections   *Family
	dbIdleConnections    *Family
	dbWaitCount          *Family
	dbWaitDurationMS     *Family

	cacheLookups         *Family
	cacheHits            *Family
	cacheMisses          *Family
	cacheUniqueAssets    *Family
	cacheInvariantErrors *Family

	// Phase A1.5: attempt-scoped asset download volume hoisted from the
	// worker's CacheResolver (task_attempt_cache_stats, migration 147).
	cacheDownloadCount *Family
	cacheDownloadBytes *Family
}

// NewOperationalTelemetry registers the delivery, database and cache
// measurement families on reg. Families use no high-cardinality identifiers;
// provider and error_code are closed/normalized values at the caller boundary.
func NewOperationalTelemetry(reg *Registry) *OperationalTelemetry {
	t := &OperationalTelemetry{}
	if reg == nil {
		return t
	}
	t.deliveryQueueMS = NewHistogramFamily("velox_delivery_queue_ms", "Delivery queue wait in milliseconds", []string{"provider"}, []float64{10, 50, 100, 250, 500, 1000, 5000, 15000, 60000})
	t.deliveryUploadMS = NewHistogramFamily("velox_delivery_upload_ms", "Provider delivery operation duration in milliseconds", []string{"provider", "status"}, []float64{10, 50, 100, 250, 500, 1000, 5000, 15000, 60000, 300000})
	t.deliveryTotalMS = NewHistogramFamily("velox_delivery_total_ms", "End-to-end delivery duration in milliseconds", []string{"provider", "status"}, []float64{10, 50, 100, 250, 500, 1000, 5000, 15000, 60000, 300000})
	t.deliveryRetries = NewCounterFamily("velox_delivery_retry_count", "Delivery attempts that scheduled a retry", []string{"provider"})
	t.deliveryTimeouts = NewCounterFamily("velox_delivery_timeout_count", "Delivery operations terminated by timeout", []string{"provider"})
	t.deliveryBytes = NewCounterFamily("velox_delivery_bytes_uploaded", "Bytes successfully delivered", []string{"provider"})
	t.deliveryUploadMbps = NewHistogramFamily("velox_delivery_upload_mbps", "Successful delivery throughput in megabits per second", []string{"provider"}, []float64{0.1, 1, 5, 10, 25, 50, 100, 500, 1000})
	t.deliveryProviderError = NewCounterFamily("velox_delivery_provider_error_count", "Delivery provider errors by stable code", []string{"provider", "error_code"})

	t.dbWriteWaitMS = NewHistogramFamily("velox_db_write_wait_ms", "Time spent waiting to begin a database write transaction", []string{}, []float64{0.1, 1, 5, 10, 25, 50, 100, 250, 1000, 5000})
	t.dbTransactionMS = NewHistogramFamily("velox_db_transaction_ms", "Database write transaction duration", []string{}, []float64{0.1, 1, 5, 10, 25, 50, 100, 250, 1000, 5000, 30000})
	t.dbBusy = NewCounterFamily("velox_db_busy_count", "Database busy or locked errors", []string{})
	t.dbBusyTimeout = NewCounterFamily("velox_db_busy_timeout_count", "Database busy or locked errors returned after the configured busy timeout", []string{})
	t.dbRetries = NewCounterFamily("velox_db_retry_count", "Database operation retries", []string{})
	t.dbWriteOperations = NewCounterFamily("velox_db_write_operations", "Observed database write operations", []string{})
	t.dbReadOperations = NewCounterFamily("velox_db_read_operations", "Observed database read operations", []string{})
	t.dbLongestTransaction = NewGaugeFamily("velox_db_longest_transaction_ms", "Longest observed database transaction duration in milliseconds", []string{})
	t.dbOpenConnections = NewGaugeFamily("velox_db_open_connections", "Current database pool open connections", []string{})
	t.dbInUseConnections = NewGaugeFamily("velox_db_in_use_connections", "Current database pool connections in use", []string{})
	t.dbIdleConnections = NewGaugeFamily("velox_db_idle_connections", "Current database pool idle connections", []string{})
	t.dbWaitCount = NewGaugeFamily("velox_db_wait_count", "Cumulative database pool connection waits", []string{})
	t.dbWaitDurationMS = NewGaugeFamily("velox_db_wait_duration_ms", "Cumulative database pool wait duration in milliseconds", []string{})

	t.cacheLookups = NewCounterFamily("velox_cache_lookups_total", "Cache lookups by result", []string{"result"})
	t.cacheHits = NewCounterFamily("velox_cache_hits_total", "Cache hits", []string{})
	t.cacheMisses = NewCounterFamily("velox_cache_misses_total", "Cache misses", []string{})
	t.cacheUniqueAssets = NewGaugeFamily("velox_unique_assets_requested", "Unique assets requested by the latest observed attempt", []string{})
	t.cacheInvariantErrors = NewCounterFamily("velox_cache_invariant_violations_total", "Cache lookup accounting invariant violations", []string{})
	t.cacheDownloadCount = NewCounterFamily("velox_cache_downloads_total", "Completed local asset downloads per attempt", []string{})
	t.cacheDownloadBytes = NewCounterFamily("velox_cache_download_bytes_total", "Bytes downloaded into the local asset cache per attempt", []string{})

	for _, f := range []*Family{
		t.deliveryQueueMS, t.deliveryUploadMS, t.deliveryTotalMS, t.deliveryRetries,
		t.deliveryTimeouts, t.deliveryBytes, t.deliveryUploadMbps, t.deliveryProviderError,
		t.dbWriteWaitMS, t.dbTransactionMS, t.dbBusy, t.dbBusyTimeout, t.dbRetries,
		t.dbWriteOperations, t.dbReadOperations, t.dbLongestTransaction,
		t.dbOpenConnections, t.dbInUseConnections, t.dbIdleConnections,
		t.dbWaitCount, t.dbWaitDurationMS,
		t.cacheLookups, t.cacheHits, t.cacheMisses, t.cacheUniqueAssets, t.cacheInvariantErrors,
		t.cacheDownloadCount, t.cacheDownloadBytes,
	} {
		reg.Register(f)
	}
	return t
}

// ObserveDBStats projects the cumulative sql.DB pool counters into gauges so
// operators can distinguish SQLite lock errors from ordinary connection-pool
// queueing. The values intentionally have no labels.
func (t *OperationalTelemetry) ObserveDBStats(stats sql.DBStats) {
	if t == nil || t.dbOpenConnections == nil {
		return
	}
	t.dbOpenConnections.GaugeSet([]string{}, int64(stats.OpenConnections))
	t.dbInUseConnections.GaugeSet([]string{}, int64(stats.InUse))
	t.dbIdleConnections.GaugeSet([]string{}, int64(stats.Idle))
	t.dbWaitCount.GaugeSet([]string{}, stats.WaitCount)
	t.dbWaitDurationMS.GaugeSet([]string{}, stats.WaitDuration.Milliseconds())
}

func (t *OperationalTelemetry) ObserveDelivery(provider string, queueMS, uploadMS, totalMS float64, status string) {
	if t == nil || t.deliveryQueueMS == nil {
		return
	}
	provider = stableLabel(provider, "unknown")
	status = stableLabel(status, "unknown")
	if queueMS >= 0 {
		t.deliveryQueueMS.Observe([]string{provider}, queueMS)
	}
	if uploadMS >= 0 {
		t.deliveryUploadMS.Observe([]string{provider, status}, uploadMS)
	}
	if totalMS >= 0 {
		t.deliveryTotalMS.Observe([]string{provider, status}, totalMS)
	}
}

func (t *OperationalTelemetry) RecordDeliveryRetry(provider string) {
	if t != nil && t.deliveryRetries != nil {
		t.deliveryRetries.Inc([]string{stableLabel(provider, "unknown")}, 1)
	}
}

func (t *OperationalTelemetry) RecordDeliveryTimeout(provider string) {
	if t != nil && t.deliveryTimeouts != nil {
		t.deliveryTimeouts.Inc([]string{stableLabel(provider, "unknown")}, 1)
	}
}

func (t *OperationalTelemetry) RecordDeliveryUpload(provider string, bytes int64, mbps float64) {
	if t == nil || bytes <= 0 {
		return
	}
	provider = stableLabel(provider, "unknown")
	t.deliveryBytes.Inc([]string{provider}, uint64(bytes))
	if mbps > 0 {
		t.deliveryUploadMbps.Observe([]string{provider}, mbps)
	}
}

func (t *OperationalTelemetry) RecordDeliveryProviderError(provider, code string) {
	if t != nil && t.deliveryProviderError != nil {
		t.deliveryProviderError.Inc([]string{stableLabel(provider, "unknown"), stableLabel(code, "UNKNOWN")}, 1)
	}
}

func (t *OperationalTelemetry) ObserveDBTransaction(waitMS, transactionMS float64, busy, busyTimeout, retried bool, writeOps, readOps uint64) {
	if t == nil || t.dbTransactionMS == nil {
		return
	}
	if waitMS >= 0 {
		t.dbWriteWaitMS.Observe([]string{}, waitMS)
	}
	if transactionMS >= 0 {
		t.dbTransactionMS.Observe([]string{}, transactionMS)
		t.dbLongestTransaction.GaugeMax([]string{}, int64(transactionMS))
	}
	if busy {
		t.dbBusy.Inc([]string{}, 1)
	}
	if busyTimeout {
		t.dbBusyTimeout.Inc([]string{}, 1)
	}
	if retried {
		t.dbRetries.Inc([]string{}, 1)
	}
	if writeOps > 0 {
		t.dbWriteOperations.Inc([]string{}, writeOps)
	}
	if readOps > 0 {
		t.dbReadOperations.Inc([]string{}, readOps)
	}
}

func (t *OperationalTelemetry) RecordDBOperation(write bool) {
	if t == nil {
		return
	}
	if write {
		t.dbWriteOperations.Inc([]string{}, 1)
	} else {
		t.dbReadOperations.Inc([]string{}, 1)
	}
}

func (t *OperationalTelemetry) RecordDBRetry() {
	if t != nil && t.dbRetries != nil {
		t.dbRetries.Inc([]string{}, 1)
	}
}

func (t *OperationalTelemetry) RecordCacheSnapshot(uniqueAssets, lookups, hits, misses int64) {
	if t == nil || t.cacheLookups == nil {
		return
	}
	if lookups < 0 || hits < 0 || misses < 0 || lookups != hits+misses {
		t.cacheInvariantErrors.Inc([]string{}, 1)
		return
	}
	t.cacheLookups.Inc([]string{"hit"}, uint64(hits))
	t.cacheLookups.Inc([]string{"miss"}, uint64(misses))
	t.cacheHits.Inc([]string{}, uint64(hits))
	t.cacheMisses.Inc([]string{}, uint64(misses))
	if uniqueAssets >= 0 {
		t.cacheUniqueAssets.GaugeSet([]string{}, uniqueAssets)
	}
}

// RecordCacheDownloads records the attempt-scoped asset download volume
// (Phase A1.5). Counter families are per-attempt cumulative; the supervisor
// feeds one terminal attempt per call so increments land exactly once.
func (t *OperationalTelemetry) RecordCacheDownloads(count, bytes int64) {
	if t == nil || t.cacheDownloadCount == nil {
		return
	}
	if count < 0 || bytes < 0 {
		t.cacheInvariantErrors.Inc([]string{}, 1)
		return
	}
	if count > 0 {
		t.cacheDownloadCount.Inc([]string{}, uint64(count))
	}
	if bytes > 0 {
		t.cacheDownloadBytes.Inc([]string{}, uint64(bytes))
	}
}

func stableLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
