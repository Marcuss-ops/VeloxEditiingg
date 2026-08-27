package telemetry

import "sync"

// PrometheusMetrics is the shared registry state for all metric families.
type PrometheusMetrics struct {
	mu sync.RWMutex

	jobQueueWaitMs          *HistogramVec
	jobDispatchMs           *HistogramVec
	jobRuntimeMs            *HistogramVec
	jobCompleteAckMs        *HistogramVec
	jobIdempotencyConflicts *CounterVec
	jobRetryCount           *HistogramVec
	jobResumeSuccess        *CounterVec
	jobResumeTotal          *CounterVec

	assetCacheHit                    *CounterVec
	assetCacheMiss                   *CounterVec
	assetCacheHitsCanonical          *CounterVec
	assetCacheMissesCanonical        *CounterVec
	assetCacheRequests               *CounterVec
	assetCacheEvictions              *CounterVec
	assetCacheEvictedBytes           *CounterVec
	assetCacheDiskUsagePercent       *GaugeVec
	assetCacheDownloads              *CounterVec
	assetCacheDownloadBytes          *CounterVec
	assetCacheDownloadMS             *HistogramVec
	assetDownloadSecondsCanonical    *HistogramVec
	assetCacheVerifyMS               *HistogramVec
	assetCacheCleanupMS              *HistogramVec
	assetCacheCleanupSkip            *CounterVec
	assetCacheProtectedSkipCanonical *CounterVec
	assetCacheSizeBytes              *GaugeVec
	assetCacheEntries                *GaugeVec
	assetCacheDuplicateDownloads     *CounterVec
	assetCacheDuplicateDownloadBytes *CounterVec
	leaseAcquires                    *CounterVec
	leaseReleases                    *CounterVec
	leaseRenewals                    *CounterVec
	leaseRetries                     *CounterVec
	leaseCleanupFailures             *CounterVec
	prefetchRequested                *CounterVec
	prefetchDownloaded               *CounterVec
	prefetchDownloadedBytes          *CounterVec
	prefetchUseful                   *CounterVec
	prefetchWastedBytes              *CounterVec
	prefetchQueueWaitSeconds         *HistogramVec
	prefetchResolveSeconds           *HistogramVec
	prefetchReadyLeadSeconds         *HistogramVec
	prefetchActive                   *GaugeVec
	prefetchQueueDepth               *GaugeVec

	// Warm-assembly KPI families. All use the static "total" label so
	// worker/job/asset identifiers never become Prometheus series.
	assemblyPrefetchCacheHits        *CounterVec
	assemblyPrefetchDownloadBytes    *CounterVec
	assemblyPrefetchMS               *HistogramVec
	assemblyAssetsReadyAtExecution   *GaugeVec
	assemblyAssetsMissingAtExecution *GaugeVec
	assemblyExecutionDownloadMS      *HistogramVec

	workerErrorsTotal                  *CounterVec
	assetDownloadActive                *GaugeVec
	assetDownloadQueued                *GaugeVec
	assetDownloadReady                 *GaugeVec
	assetDownloadFailed                *GaugeVec
	assetDownloadCacheHits             *GaugeVec
	assetDownloadBytes                 *GaugeVec
	assetDownloadTotalBytes            *GaugeVec
	assetDownloadThroughput            *GaugeVec
	assetDownloadChunksActive          *GaugeVec
	assetDownloadChunkThroughput       *GaugeVec
	assetDownloadETA                   *GaugeVec
	assetDownloadCoalesced             *CounterVec
	workerActiveJobs                   *GaugeVec
	workerStatus                       *GaugeVec
	fallbackCount                      *CounterVec
	pythonEmergencyPath                *CounterVec
	renderSeconds                      *HistogramVec
	artifactUploadSeconds              *HistogramVec
	artifactLockWaitSeconds            *HistogramVec
	renderUploadOverlapSeconds         *HistogramVec
	progressiveFirstPartStartedSeconds *HistogramVec
	progressivePartsBeforeRenderEnd    *HistogramVec
	progressiveBytesBeforeRenderEnd    *HistogramVec
	muxToOpenMicroseconds              *HistogramVec
	taskResultSubmitSeconds            *HistogramVec
	taskResultAckSeconds               *HistogramVec
	taskResultAcksTotal                *CounterVec
	telemetryInvalidEvents             *CounterVec

	artifactTmpfsReservedBytes *GaugeVec
	artifactTmpfsSpillTotal    *CounterVec
	artifactTmpfsSpillBytes    *CounterVec
	artifactNvmeFallback       *CounterVec

	attemptCPUTimeMs    *CounterVec
	attemptPeakRSSBytes *GaugeVec
	attemptIOBytes      *CounterVec
	attemptFrames       *CounterVec
	attemptProcesses    *CounterVec

	// Per-job resource attribution.
	jobPeakRssDeltaBytes *GaugeVec
	jobCpuCoreSeconds    *CounterVec
	jobPrefetchBytes     *CounterVec
	jobPublishBytes      *CounterVec
	jobOpenFdsPeak       *GaugeVec
	jobPageFaults        *CounterVec
	jobIoWaitMs          *CounterVec

	// Pending-offer dedup counters. Static "total" label — no worker/job
	// identifiers leak into Prometheus series.
	offerDuplicateTotal       *CounterVec
	offerReplacedTotal        *CounterVec
	offerStaleTotal           *CounterVec
	offerIdentityConflictTotal *CounterVec
	offerReconciledTotal      *CounterVec

	// Resource admission controller metrics.
	admissionRejectionsTotal *CounterVec
	backpressureEventsTotal  *CounterVec

	// Resource admission live diagnostics — gauges updated every heartbeat
	// cycle so Prometheus /graph can show real-time RSS pressure and
	// throttle state without querying the worker API.
	admissionRSSPressurePct   *GaugeVec
	admissionRSSBytes         *GaugeVec
	admissionThrottledRender  *GaugeVec
	admissionThrottledPrefetch *GaugeVec
	admissionThrottledPublish *GaugeVec

	// File descriptor gauges.
	workerOpenFds      *GaugeVec
	workerMaxFds       *GaugeVec
	workerFdUtilization *GaugeVec

	// Prefetch corruption.
	prefetchCorruptedTotal *CounterVec

	// Network admission controller metrics.
	networkSaturationIngress *GaugeVec
	networkSaturationEgress  *GaugeVec
	networkConsumerBytes     *GaugeVec
	networkThrottleMS        *GaugeVec
}
