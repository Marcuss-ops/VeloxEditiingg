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

	"velox-server/internal/placement"
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
func parseExecutorCapabilities(raw map[string]interface{}) (map[placement.ExecutorKey]struct{}, error) {
	executorsRaw, ok := raw["executors"]
	if !ok {
		return make(map[placement.ExecutorKey]struct{}), nil
	}

	execList, ok := executorsRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("executors key is not an array (got %T)", executorsRaw)
	}

	result := make(map[placement.ExecutorKey]struct{}, len(execList))
	for i, item := range execList {
		entry, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("executors[%d]: not an object (got %T)", i, item)
		}

		id, _ := entry["id"].(string)
		if id == "" {
			return nil, fmt.Errorf("executors[%d]: missing or empty \"id\"", i)
		}

		version := 0
		switch v := entry["version"].(type) {
		case float64:
			version = int(v)
		case int:
			version = v
		case int64:
			version = int(v)
		default:
			return nil, fmt.Errorf("executors[%d] (%s): version must be a number (got %T)", i, id, entry["version"])
		}

		if version <= 0 {
			return nil, fmt.Errorf("executors[%d] (%s): version must be positive (got %d)", i, id, version)
		}

		key := placement.ExecutorKey{ID: id, Version: version}
		result[key] = struct{}{}
	}

	return result, nil
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
