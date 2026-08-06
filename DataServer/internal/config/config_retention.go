package config

// loadRetentionConfig reads the retention window env vars for the
// auxiliary tables the heartbeat path writes to.
//
//   - VELOX_RETENTION_WORKER_METRICS_DAYS  (default 7)
//   - VELOX_RETENTION_WORKER_EVENTS_DAYS   (default 30)
//   - VELOX_RETENTION_WORKER_RESOURCE_RAW_DAYS (default 90)
//   - VELOX_RETENTION_WORKER_RESOURCE_ROLLUP_DAYS (default 365)
//
// The resource windows can be set to 0 to opt out of the corresponding
// resource-table prune pass. Negative values and malformed values fall back
// to the configured default; they do not disable pruning. The prune helpers
// in DataServer/internal/store honor the zero opt-out by skipping the DELETE
// pass entirely (no SQL emitted).
//
// Worker metric/event retention keeps its existing minimum of 1. Resource
// retention uses minimum 0 so an explicit zero can disable only that prune
// pass; malformed values still fall back to the defaults.
func loadRetentionConfig(raw RawConfig) RetentionConfig {
	return RetentionConfig{
		WorkerMetricsDays:        raw.Int("VELOX_RETENTION_WORKER_METRICS_DAYS", 7, 1),
		WorkerEventsDays:         raw.Int("VELOX_RETENTION_WORKER_EVENTS_DAYS", 30, 1),
		WorkerResourceRawDays:    raw.Int("VELOX_RETENTION_WORKER_RESOURCE_RAW_DAYS", 90, 0),
		WorkerResourceRollupDays: raw.Int("VELOX_RETENTION_WORKER_RESOURCE_ROLLUP_DAYS", 365, 0),
		// 0 opts out for the resource tables. Defaults: 7 / 30 / 90 / 365.
	}
}

// Resource opt-out semantics: an explicit zero for either resource
// retention variable means "operator disabled that prune pass". Negative
// values do not opt out; intFromEnv rejects them and returns the default.
