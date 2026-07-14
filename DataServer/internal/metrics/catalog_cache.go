// Package metrics / catalog_cache.go
//
// Storage family — metrics for the local cache (cache.*) and the
// blob store (blob.*). Both families measure the same kind of
// observability surface (hits/misses, bytes, entries, corruption)
// for two different storage layers, so they are kept together.
package metrics

// cacheMetricDefinitions returns cache.* + blob.* definitions.
// Cache first (local), then blob (remote/blob-store).
func cacheMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Cache metrics ────────────────────────────────────────────────
		{
			Name: "cache.hits", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of local cache hit events",
		},
		{
			Name: "cache.misses", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of local cache miss events",
		},
		{
			Name: "cache.evictions", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of cache eviction events",
		},
		{
			Name: "cache.corruptions", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of cache corruption events detected",
		},
		{
			Name: "cache.entries", Unit: "count", Component: CompCache, Kind: KindGauge,
			Description: "Current number of entries in the local cache",
		},
		{
			Name: "cache.bytes", Unit: "bytes", Component: CompCache, Kind: KindGauge,
			Description: "Current size of the local cache in bytes",
		},
		{
			Name: "cache.pinned", Unit: "count", Component: CompCache, Kind: KindGauge,
			Description: "Number of pinned (non-evictable) entries in the local cache",
		},
		{
			Name: "cache.asset_hit_count", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of asset cache hits (granular per-category counter)",
		},
		{
			Name: "cache.asset_miss_count", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of asset cache misses (granular per-category counter)",
		},
		{
			Name: "cache.blob_hit_count", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of blob cache hits (granular per-category counter)",
		},
		{
			Name: "cache.blob_miss_count", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of blob cache misses (granular per-category counter)",
		},
		{
			Name: "cache.render_hit_count", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Number of render cache hits (granular per-category counter)",
		},
		{
			Name: "cache.byte_hit_ratio", Unit: "ratio", Component: CompCache, Kind: KindGauge,
			Description: "Ratio of bytes served from local cache vs total bytes downloaded",
		},
		{
			Name: "cache.requests_total", Unit: "count", Component: CompCache, Kind: KindCounter,
			Description: "Total cache requests by result (hit, miss, corrupt)",
		},
		{
			Name: "cache.bytes_total", Unit: "bytes", Component: CompCache, Kind: KindCounter,
			Description: "Total cache bytes by result (hit, miss)",
		},
		// ── Blob store metrics ───────────────────────────────────────────
		{
			Name: "blob.publish", Unit: "count", Component: CompBlob, Kind: KindCounter,
			Description: "Number of successful blob publish operations",
		},
		{
			Name: "blob.publish_failed", Unit: "count", Component: CompBlob, Kind: KindCounter,
			Description: "Number of failed blob publish operations",
		},
		{
			Name: "blob.fetch", Unit: "count", Component: CompBlob, Kind: KindCounter,
			Description: "Number of successful blob fetch operations",
		},
		{
			Name: "blob.fetch_miss", Unit: "count", Component: CompBlob, Kind: KindCounter,
			Description: "Number of blob fetch misses (blob not found)",
		},
		{
			Name: "blob.fetch_corruption", Unit: "count", Component: CompBlob, Kind: KindCounter,
			Description: "Number of blob fetch corruption events detected",
		},
		{
			Name: "blob.entries", Unit: "count", Component: CompBlob, Kind: KindGauge,
			Description: "Current number of entries in the blob store",
		},
		{
			Name: "blob.bytes", Unit: "bytes", Component: CompBlob, Kind: KindGauge,
			Description: "Current size of the blob store in bytes",
		},
	}
}
