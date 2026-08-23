package assembly

// PreparationMetrics describes the latency that eager preparation is meant
// to remove from the final execution critical path.
type PreparationMetrics struct {
	PrefetchStartedAt        string `json:"prefetch_started_at,omitempty"`
	PrefetchCompletedAt      string `json:"prefetch_completed_at,omitempty"`
	PrefetchMS               int64  `json:"prefetch_ms,omitempty"`
	PrefetchBytes            uint64 `json:"prefetch_bytes,omitempty"`
	PrefetchCacheHits        uint64 `json:"prefetch_cache_hits,omitempty"`
	PrefetchDownloadBytes    uint64 `json:"prefetch_download_bytes,omitempty"`
	FinalManifestReceivedAt  string `json:"final_manifest_received_at,omitempty"`
	ExecutionStartedAt       string `json:"execution_started_at,omitempty"`
	ReadyToExecutionMS       int64  `json:"ready_to_execution_ms,omitempty"`
	AssetsReadyAtExecution   uint64 `json:"assets_ready_at_execution,omitempty"`
	AssetsMissingAtExecution uint64 `json:"assets_missing_at_execution,omitempty"`
	ExecutionDownloadMS      int64  `json:"execution_download_ms,omitempty"`
	ConcatMS                 int64  `json:"concat_ms,omitempty"`
}
