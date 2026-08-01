// Package metrics / collector_cache.go
//
// Cache request/state families, sliced out of collector.go so the
// Collector struct definition stays focused on registration.
// Recorded by RecordAttempt (request counters) and RecordWorker
// (per-worker entries/size/evictions/corruptions).
package metrics

// initCacheFamilies creates the cache request/state families. Called
// once from NewCollector at boot.
func (c *Collector) initCacheFamilies() {
	c.cacheHits = NewCounterFamily("velox_cache_requests_total",
		"Cache requests by result", []string{"result"})
	c.cacheBytes = NewCounterFamily("velox_cache_bytes_total",
		"Cache bytes by result", []string{"result"})
	c.cacheEntries = NewGaugeFamily("velox_cache_entries",
		"Current cache entries", []string{"worker_id"})
	c.cacheSizeBytes = NewGaugeFamily("velox_cache_size_bytes",
		"Current cache size in bytes", []string{"worker_id"})
	c.cacheEvictions = NewCounterFamily("velox_cache_evictions_total",
		"Cache evictions", []string{"worker_id"})
	c.cacheEvictedBytes = NewCounterFamily("velox_cache_evicted_bytes_total",
		"Bytes evicted from cache", []string{"worker_id"})
	c.cacheCorruptions = NewCounterFamily("velox_cache_corruption_total",
		"Cache corruption events", []string{"worker_id"})
}

// cacheFamilies returns the cache subset registered by NewCollector
// via allFamilies.
func (c *Collector) cacheFamilies() []*Family {
	return []*Family{
		c.cacheHits, c.cacheBytes, c.cacheEntries,
		c.cacheSizeBytes, c.cacheEvictions, c.cacheEvictedBytes,
		c.cacheCorruptions,
	}
}
