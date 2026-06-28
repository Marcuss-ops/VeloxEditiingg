// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// Cache provides a SQLite-backed cache with TTL support.
//
// PR-YT-REPO: this is the **API response cache** — a legitimate
// in-RAM map keyed by cache key, with the canonical SQLite-backed
// store as the durable fallback. This is NOT the deprecated in-memory
// groups/channels mirror (PR15.4 removal); the two are different
// concerns and only the latter was a data-drift liability.
//
// NewCache is non-variadic and requires a Repository. SetStore is
// gone — the repo is wired at construction and held for the lifetime
// of the Cache.
type Cache struct {
	mu   sync.RWMutex
	data map[string]cacheEntry
	ttl  time.Duration
	repo Repository
}

type cacheEntry struct {
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// NewCache creates a new cache instance backed by the canonical
// Repository (Repository is required). Defaults to 2-hour TTL when
// `ttl` is 0.
func NewCache(dataDir string, ttl time.Duration, repo Repository) *Cache {
	if ttl == 0 {
		ttl = 2 * time.Hour
	}
	if repo == nil {
		// Fail-closed: a Cache without a repo would silently swallow
		// every Set because the SQLite write path gets nil-rejected.
		// Panicking surfaces the misconfiguration at boot rather than
		// at first request.
		panic("youtube.NewCache: Repository is required (PR-YT-REPO drops SetStore)")
	}

	c := &Cache{
		data: make(map[string]cacheEntry),
		ttl:  ttl,
		repo: repo,
	}
	c.load()
	return c
}

// load reads cache from SQLite (without preloading all entries).
func (c *Cache) load() error {
	return nil
}

// save writes cache to SQLite.
func (c *Cache) save() {
	for key, entry := range c.data {
		dataJSON, _ := json.Marshal(entry.Data)
		if err := c.repo.SetYouTubeCache(key, entry.Timestamp, string(dataJSON)); err != nil {
			log.Printf("[WARN] YouTube cache: SQLite save error for key %s: %v", key, err)
		}
	}
}

// Get retrieves a cached value if not expired, falling back to SQLite
// if not in memory.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	entry, ok := c.data[key]
	c.mu.RUnlock()

	if ok {
		if time.Since(time.Unix(entry.Timestamp, 0)) <= c.ttl {
			return entry.Data, true
		}
	}

	timestamp, dataJSON, err := c.repo.GetYouTubeCache(key)
	if err == nil && dataJSON != "" {
		var data interface{}
		if err := json.Unmarshal([]byte(dataJSON), &data); err == nil {
			if time.Since(time.Unix(timestamp, 0)) <= c.ttl {
				c.mu.Lock()
				c.data[key] = cacheEntry{Timestamp: timestamp, Data: data}
				c.mu.Unlock()
				return data, true
			}
		}
	}
	return nil, false
}

// Set stores a value in cache.
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = cacheEntry{
		Timestamp: time.Now().Unix(),
		Data:      value,
	}
	go c.save()
}

// Delete removes a key from cache.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
}

// Clear removes all entries from cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[string]cacheEntry)
	go c.save()
}

// Cleanup removes expired entries.
func (c *Cache) Cleanup() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	count := 0
	for key, entry := range c.data {
		if now.Sub(time.Unix(entry.Timestamp, 0)) > c.ttl {
			delete(c.data, key)
			count++
		}
	}
	if count > 0 {
		go c.save()
	}
	return count
}

// --- Feed Cache (10 hour TTL, no file persistence) ---

// FeedCache provides an in-memory cache for video feeds.
// No file persistence — the cache is rebuilt on each server restart.
// TTL is 10 hours by default.
type FeedCache struct {
	mu   sync.RWMutex
	data map[string]feedCacheEntry
	ttl  time.Duration
}

type feedCacheEntry struct {
	Timestamp int64         `json:"timestamp"`
	Data      *FeedResponse `json:"data"`
}

// NewFeedCache creates a new feed cache with 10 hour TTL.
func NewFeedCache(dataDir string) *FeedCache {
	return &FeedCache{
		data: make(map[string]feedCacheEntry),
		ttl:  10 * time.Hour,
	}
}

// Get retrieves a cached feed if not expired.
func (fc *FeedCache) Get(key string) (*FeedResponse, bool) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	entry, ok := fc.data[key]
	if !ok {
		return nil, false
	}
	if time.Since(time.Unix(entry.Timestamp, 0)) > fc.ttl {
		return nil, false
	}
	return entry.Data, true
}

// Set stores a feed in cache.
func (fc *FeedCache) Set(key string, feed *FeedResponse) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.data[key] = feedCacheEntry{
		Timestamp: time.Now().Unix(),
		Data:      feed,
	}
}

// Clear removes all entries from feed cache.
func (fc *FeedCache) Clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.data = make(map[string]feedCacheEntry)
}
