package controltransport

// CapabilitySchemaVersion is the canonical version of the worker capability
// report exchanged at the control-transport boundary.
const CapabilitySchemaVersion = 1

// CapabilityReport is the canonical worker capability snapshot. Executor
// implementations live in Executors; Features are named protocol/admission
// capabilities. Host is operational metadata used for capacity, not an
// executor identity.
type CapabilityReport struct {
	SchemaVersion       int                  `json:"schema_version"`
	Executors           []ExecutorCapability `json:"executors"`
	Host                HostInfo             `json:"host"`
	Features            []string             `json:"features,omitempty"`
	AssetCacheKeys      []string             `json:"asset_cache_keys,omitempty"`
	AssetCacheTruncated bool                 `json:"asset_cache_keys_truncated,omitempty"`
}

// HostInfo is the typed host metadata carried alongside executor capabilities.
type HostInfo struct {
	WorkerID        string `json:"worker_id"`
	Hostname        string `json:"hostname"`
	CPUCount        int    `json:"cpu_count"`
	MaxParallelJobs int    `json:"max_parallel_jobs"`
	HasGPU          bool   `json:"has_gpu"`
	RAMBytes        int64  `json:"ram_bytes"`
	DiskFreeBytes   int64  `json:"disk_free_bytes"`
}

// CapabilitySet is the typed set of named protocol capabilities used by
// placement and admission. It deliberately has no free-form values.
type CapabilitySet []string

func (s CapabilitySet) Has(want string) bool {
	for _, value := range s {
		if value == want {
			return true
		}
	}
	return false
}

func (s CapabilitySet) All() []string {
	out := append([]string(nil), s...)
	return out
}

// AsMap is the sole conversion from the typed report to the protobuf Struct
// boundary. Internal worker/master code should use the typed fields instead.
func (r CapabilityReport) AsMap() map[string]interface{} {
	executors := make([]interface{}, 0, len(r.Executors))
	for _, e := range r.Executors {
		item := map[string]interface{}{
			"id":             e.ID,
			"version":        e.Version,
			"resource_class": e.ResourceClass,
			"temporal_mode":  e.TemporalMode,
			"deterministic":  e.Deterministic,
			"cacheable":      e.Cacheable,
			"supports_alpha": e.SupportsAlpha,
		}
		if len(e.OutputTypes) > 0 {
			outputs := make([]interface{}, 0, len(e.OutputTypes))
			for _, output := range e.OutputTypes {
				outputs = append(outputs, output)
			}
			item["output_types"] = outputs
		}
		executors = append(executors, item)
	}
	host := map[string]interface{}{
		"worker_id":         r.Host.WorkerID,
		"hostname":          r.Host.Hostname,
		"cpu_count":         r.Host.CPUCount,
		"max_parallel_jobs": r.Host.MaxParallelJobs,
		"has_gpu":           r.Host.HasGPU,
		"ram_bytes":         r.Host.RAMBytes,
		"disk_free_bytes":   r.Host.DiskFreeBytes,
	}
	out := map[string]interface{}{
		"schema_version": r.SchemaVersion,
		"executors":      executors,
		"host":           host,
	}
	for _, feature := range r.Features {
		if feature != "" {
			out[feature] = true
		}
	}
	if len(r.AssetCacheKeys) > 0 {
		keys := make([]interface{}, 0, len(r.AssetCacheKeys))
		for _, key := range r.AssetCacheKeys {
			keys = append(keys, key)
		}
		out["asset_cache_keys"] = keys
	}
	if r.AssetCacheTruncated {
		out["asset_cache_keys_truncated"] = true
	}
	return out
}
