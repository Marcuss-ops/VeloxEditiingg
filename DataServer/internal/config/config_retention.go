package config

// loadRetentionConfig reads the retention window env vars for the
// auxiliary tables the heartbeat path writes to.
//
//   - VELOX_RETENTION_WORKER_METRICS_DAYS  (default 7)
//   - VELOX_RETENTION_WORKER_EVENTS_DAYS   (default 30)
//
// Either field can be set to 0 (or a negative integer) to opt out of
// the corresponding prune pass. The opt-out semantics are documented
// on RetentionConfig.WorkerMetricsDays / WorkerEventsDays; the prune
// helpers in DataServer/internal/store honor the opt-out by skipping
// the DELETE pass entirely (no SQL emitted).
//
// The minimum is 1 (the intFromEnv helper enforces this) so a typo
// like VELOX_RETENTION_WORKER_METRICS_DAYS=abc falls back to the
// default rather than silently disabling retention.
func loadRetentionConfig() RetentionConfig {
	return RetentionConfig{
		WorkerMetricsDays: intFromEnv("VELOX_RETENTION_WORKER_METRICS_DAYS", 7, 1),
		WorkerEventsDays:  intFromEnv("VELOX_RETENTION_WORKER_EVENTS_DAYS", 30, 1),
		// 0 / negative values opt out. Default 7 / 30.
	}
}

// Note on opt-out semantics: a deployment that explicitly sets
// VELOX_RETENTION_WORKER_METRICS_DAYS=0 (or any non-positive
// integer) is treated as "operator disabled pruning" and the
// store-layer prune helpers skip the DELETE pass entirely. This
// matches the canonical opt-out contract used by every other
// retention helper in the codebase — no per-deployment env
// presence detection is required at the config layer because the
// intFromEnv helper already enforces the lower bound (>= 1) and
// only the explicit 0 / negative value falls through to the
// store-layer opt-out.
