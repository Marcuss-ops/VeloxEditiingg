package config

import "strings"

func loadWorkersConfig(raw RawConfig) WorkersConfig {
	c := WorkersConfig{
		MaxJobAttempts:   3,
		HeartbeatTimeout: 900,
		VersionNumber:    "v1.0.6",
	}
	// parseCommaList drops empty tokens and trims whitespace; the
	// canonical two-worker validator (ValidateProductionWorkers) is
	// invoked by Config.Validate() — see internal/config/config.go.
	c.AllowedWorkerIDs = parseCommaList(raw.Get("VELOX_ALLOWED_WORKERS"))
	c.MaxJobAttempts = raw.Int("VELOX_MAX_JOB_ATTEMPTS", 3, 1)
	c.BundleDir = raw.Get("VELOX_WORKER_BUNDLE_DIR")
	c.CodeVersion = raw.Get("VELOX_CODE_VERSION")
	c.VersionNumber = strings.TrimSpace(raw.Get("VELOX_VERSION_NUMBER"))
	if c.VersionNumber == "" {
		c.VersionNumber = "v1.0.6"
	}
	if c.CodeVersion == "" {
		c.CodeVersion = c.VersionNumber
	}
	c.HeartbeatTimeout = raw.Int("VELOX_WORKER_HEARTBEAT_TIMEOUT", 900, 1)
	c.ScriptDir = raw.Get("VELOX_SCRIPT_DIR")
	if ips := raw.Get("VELOX_ALLOWED_WORKER_IPS"); ips != "" {
		c.AllowedIPs = parseCommaList(ips)
	}
	// Operator-driven deterministic-pick for placement matching: when
	// set, the placement matcher emits RejectPlacementPinExcluded for
	// every worker_id != the pin (Bootstrap wires this to the Handler
	// via Handler.SetPlacementPin). Empty value keeps the matcher in
	// its stateless default.
	c.PlacementPinWorkerID = strings.TrimSpace(raw.Get("VELOX_PLACEMENT_PIN_WORKER_ID"))
	// STALE / PARTITIONED thresholds for the persistent state machine
	// owned by store_worker_runtime_recovery.go (single-writer tx).
	// Defaults match the canonical read-side thresholds in
	// workers/registry_query.go (ConnectionStaleThreshold = 150s,
	// ConnectionDisconnectedThreshold = 5min) so the persistent
	// mirror and the read-time derivation stay aligned.
	c.StaleThresholdSeconds = raw.Int("VELOX_WORKER_STALE_THRESHOLD_SECONDS", 150, 1)
	c.PartitionThresholdSeconds = raw.Int("VELOX_WORKER_PARTITION_THRESHOLD_SECONDS", 300, 1)
	return c
}
