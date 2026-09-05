// Package news / fetcher_test.go
//
// Regression tests for the Fetcher cache: the map is now mutex-guarded
// (concurrent readers/writers no longer panic) and bounded (distinct queries
// cannot grow it without limit; expired entries are dropped on sight).
package news

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestFetcherCacheConcurrentAccess runs parallel cache hits/stores against one
// Fetcher. Run under -race; the previous unguarded map write panicked here.
func TestFetcherCacheConcurrentAccess(t *testing.T) {
	f := NewFetcher(nil)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := strings.Repeat("q", 1+i%8)
			f.storeNews(key, []NewsItem{{Title: key}})
			_, _ = f.cachedNews(key)
		}(i)
	}
	wg.Wait()

	f.mu.Lock()
	size := len(f.cache)
	f.mu.Unlock()
	if size == 0 {
		t.Fatal("cache empty after stores")
	}
}

// TestFetcherCacheExpiryDropsStaleEntries asserts an expired entry is removed
// rather than served (and rather than lingering forever).
func TestFetcherCacheExpiryDropsStaleEntries(t *testing.T) {
	f := NewFetcher(nil)
	f.storeNews("query", []NewsItem{{Title: "stale"}})

	// Force expiry without sleeping for the real TTL.
	f.mu.Lock()
	f.cache["query"].expiresAt = time.Now().Add(-time.Second)
	f.mu.Unlock()

	if _, ok := f.cachedNews("query"); ok {
		t.Fatal("expired entry was served")
	}
	f.mu.Lock()
	_, stillPresent := f.cache["query"]
	f.mu.Unlock()
	if stillPresent {
		t.Fatal("expired entry was not evicted from the map")
	}
}

// TestFetcherCacheBounded asserts the eviction cap: after storing more
// distinct queries than maxCacheEntries, the map never exceeds the cap.
func TestFetcherCacheBounded(t *testing.T) {
	f := NewFetcher(nil)
	for i := 0; i < maxCacheEntries+50; i++ {
		f.storeNews(strings.Repeat("q", 1+i%64)+"-"+string(rune('a'+i%26))+string(rune('A'+i%26))+time.Now().Format("150405.000000000"), []NewsItem{{Title: "x"}})
	}
	f.mu.Lock()
	size := len(f.cache)
	f.mu.Unlock()
	if size > maxCacheEntries {
		t.Fatalf("cache size %d exceeds cap %d", size, maxCacheEntries)
	}
}
