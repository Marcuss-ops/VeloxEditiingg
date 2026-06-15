// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// Cache provides a SQLite-backed cache with TTL support
type Cache struct {
	mu    sync.RWMutex
	data  map[string]cacheEntry
	ttl   time.Duration
	store YouTubeStore
}

type cacheEntry struct {
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// NewCache creates a new cache instance
func NewCache(dataDir string, ttl time.Duration, store ...YouTubeStore) *Cache {
	// Default to 2 hours TTL
	if ttl == 0 {
		ttl = 2 * time.Hour
	}

	var ytStore YouTubeStore
	if len(store) > 0 {
		ytStore = store[0]
	}

	c := &Cache{
		data:  make(map[string]cacheEntry),
		ttl:   ttl,
		store: ytStore,
	}

	// Load existing cache from SQLite
	c.load()

	return c
}

// SetStore sets the SQLite store for cache persistence
func (c *Cache) SetStore(store YouTubeStore) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = store
}

// load reads cache from SQLite (without preloading all entries).
func (c *Cache) load() error {
	// Entries are fetched on-demand via Get() with SQLite fallback.
	return nil
}

// save writes cache to SQLite.
func (c *Cache) save() {
	if c.store != nil {
		for key, entry := range c.data {
			dataJSON, _ := json.Marshal(entry.Data)
			if err := c.store.SetYouTubeCache(key, entry.Timestamp, string(dataJSON)); err != nil {
				log.Printf("[WARN] YouTube cache: SQLite save error for key %s: %v", key, err)
			}
		}
	}
}

// Get retrieves a cached value if not expired, falling back to SQLite if not in memory.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	entry, ok := c.data[key]
	c.mu.RUnlock()

	if ok {
		// Check if expired
		if time.Since(time.Unix(entry.Timestamp, 0)) <= c.ttl {
			return entry.Data, true
		}
	}

	// Fallback: try SQLite if not found or expired in memory
	if c.store != nil {
		timestamp, dataJSON, err := c.store.GetYouTubeCache(key)
		if err == nil && dataJSON != "" {
			// Unmarshal the cached data
			var data interface{}
			if err := json.Unmarshal([]byte(dataJSON), &data); err == nil {
				// Check TTL
				if time.Since(time.Unix(timestamp, 0)) <= c.ttl {
					// Populate back into memory
					c.mu.Lock()
					c.data[key] = cacheEntry{Timestamp: timestamp, Data: data}
					c.mu.Unlock()
					return data, true
				}
			}
		}
	}

	return nil, false
}

// Set stores a value in cache
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = cacheEntry{
		Timestamp: time.Now().Unix(),
		Data:      value,
	}

	// Save asynchronously to not block
	go c.save()
}

// Delete removes a key from cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
}

// Clear removes all entries from cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[string]cacheEntry)

	go c.save()
}

// Cleanup removes expired entries
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

// --- Feed Cache (10 hour TTL) ---

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

// Get retrieves a cached feed if not expired
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

// Set stores a feed in cache
func (fc *FeedCache) Set(key string, feed *FeedResponse) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.data[key] = feedCacheEntry{
		Timestamp: time.Now().Unix(),
		Data:      feed,
	}
}

// Clear removes all entries from feed cache
func (fc *FeedCache) Clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.data = make(map[string]feedCacheEntry)
}
