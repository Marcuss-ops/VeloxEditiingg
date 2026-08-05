// Package grpcserver / session_capabilities.go
//
// Parsing of the executor capability report produced by the worker's
// executor.BuildCapabilityReport() → api.CapabilityReport.AsMap().
// The wire format is a map[string]interface{} with a top-level
// "executors" key containing a []interface{} of per-executor objects.
//
// Only the typed executors block drives placement decisions; legacy
// supported_job_types flags are intentionally ignored.
package grpcserver

import (
	"fmt"
	"sort"
	"strings"

	"velox-shared/controltransport"
)

// parseExecutorCapabilities extracts the (executor_id, executor_version)
// pairs from the raw capability map the worker sent in its Hello message.
//
// Expected wire shape (produced by worker-agent-go/pkg/api.CapabilityReport.AsMap):
//
//	{
//	  "executors": [
//	    {"id": "scene.composite.v1", "version": 1, "resource_class": "gpu", ...},
//	    ...
//	  ],
//	  ...
//	}
//
// Returns an empty map (not nil) when the executors key is absent —
// the caller decides whether that makes the worker ineligible.
// Returns an error when the executors block is present but malformed
// (wrong type, missing id/version, or version <= 0).
func parseExecutorCapabilities(raw map[string]interface{}) (controltransport.ExecutorRegistry, error) {
	if raw == nil {
		return controltransport.ExecutorRegistry{}, fmt.Errorf("capability report is required")
	}
	schemaVersion, ok := capabilityInteger(raw["schema_version"])
	if !ok || schemaVersion != controltransport.CapabilitySchemaVersion {
		return controltransport.ExecutorRegistry{}, fmt.Errorf("unsupported capability schema_version %v", raw["schema_version"])
	}
	executors, ok := raw["executors"]
	if !ok {
		return controltransport.ExecutorRegistry{}, fmt.Errorf("capability report executors array is required")
	}
	registry, err := controltransport.ExecutorRegistryFromLegacyStrict(map[string]interface{}{"executors": executors})
	if err != nil {
		return controltransport.ExecutorRegistry{}, fmt.Errorf("decode executor registry: %w", err)
	}
	return registry, nil
}

func capabilityInteger(value interface{}) (int, bool) {
	switch n := value.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), n == float64(int(n))
	case float32:
		return int(n), n == float32(int(n))
	default:
		return 0, false
	}
}

func extractAssetCacheKeys(raw map[string]interface{}) []string {
	value, ok := raw["asset_cache_keys"]
	if !ok {
		return nil
	}
	var keys []string
	switch list := value.(type) {
	case []interface{}:
		for _, item := range list {
			if key, ok := item.(string); ok && strings.TrimSpace(key) != "" {
				keys = append(keys, strings.TrimSpace(key))
			}
		}
	case []string:
		for _, key := range list {
			if strings.TrimSpace(key) != "" {
				keys = append(keys, strings.TrimSpace(key))
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// capabilitiesBoolMap is retained as a local compatibility name for the
// protobuf decoder, but returns the typed feature set. Executor metadata and
// host capacity are deliberately excluded because they have dedicated typed
// projections.
func capabilitiesBoolMap(raw map[string]interface{}) controltransport.CapabilitySet {
	result := make(controltransport.CapabilitySet, 0, len(raw))
	for key, val := range raw {
		if enabled, ok := val.(bool); ok && enabled {
			result = append(result, key)
		}
	}
	sort.Strings(result)
	return result
}
