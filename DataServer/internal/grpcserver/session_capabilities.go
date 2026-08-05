// Package grpcserver / session_capabilities.go
//
// Parsing of the executor capability report produced by the worker's
// executor.BuildCapabilityReport() → api.CapabilityReport.AsMap().
// The wire format is a map[string]interface{} with a top-level
// "executors" key containing a []interface{} of per-executor objects.
//
// supported_job_types is NOT used as the primary source — only
// the typed executors block drives placement decisions.
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
	registry, err := controltransport.ExecutorRegistryFromLegacy(raw)
	if err != nil {
		return controltransport.ExecutorRegistry{}, fmt.Errorf("decode executor registry: %w", err)
	}
	return registry, nil
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

// capabilitiesBoolMap normalises the raw capability map to a map[string]bool
// by extracting only the boolean entries. Non-boolean values (arrays,
// objects, numbers, strings) are silently dropped — they are metadata
// (executors, host, max_parallel_jobs) not capability flags.
func capabilitiesBoolMap(raw map[string]interface{}) map[string]bool {
	result := make(map[string]bool, len(raw))
	for key, val := range raw {
		if b, ok := val.(bool); ok {
			result[key] = b
		}
	}
	return result
}

// extractSupportedJobTypes parses a supported_job_types value from a
// capabilities map extracted from protobuf Struct. structpb normalises
// Go slices to []interface{}, so both Worker→Master paths (Hello
// capabilities and heartbeat Extra) share this helper.
func extractSupportedJobTypes(capsMap map[string]interface{}) []string {
	sjt, ok := capsMap["supported_job_types"]
	if !ok {
		return nil
	}
	switch list := sjt.(type) {
	case []interface{}:
		types := make([]string, 0, len(list))
		for _, item := range list {
			if s, ok := item.(string); ok {
				types = append(types, s)
			}
		}
		return types
	case []string:
		return list
	}
	return nil
}
