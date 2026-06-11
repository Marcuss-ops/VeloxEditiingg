// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cache provides a file-backed cache with TTL support
type Cache struct {
	mu       sync.RWMutex
	data     map[string]cacheEntry
	filePath string
	ttl      time.Duration
}

type cacheEntry struct {
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// NewCache creates a new cache instance
func NewCache(dataDir string, ttl time.Duration) *Cache {
	// Default to 2 hours TTL
	if ttl == 0 {
		ttl = 2 * time.Hour
	}

	// Ensure cache directory exists
	cacheDir := filepath.Join(dataDir, "youtube")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("[WARN] YouTube cache: could not create directory: %v", err)
	}

	filePath := filepath.Join(cacheDir, "youtube_api_cache.json")

	c := &Cache{
		data:     make(map[string]cacheEntry),
		filePath: filePath,
		ttl:      ttl,
	}

	// Load existing cache
	c.load()

	return c
}

// load reads cache from file
func (c *Cache) load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return json.Unmarshal(data, &c.data)
}

// save writes cache to file
func (c *Cache) save() error {
	data, err := json.MarshalIndent(c.data, "", "  ")
	if err != nil {
		return err
	}

	tempPath := c.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, c.filePath)
}

// Get retrieves a cached value if not expired
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.data[key]
	if !ok {
		return nil, false
	}

	// Check if expired
	if time.Since(time.Unix(entry.Timestamp, 0)) > c.ttl {
		return nil, false
	}

	return entry.Data, true
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
	go func() {
		if err := c.save(); err != nil {
			log.Printf("[WARN] YouTube cache: save error: %v", err)
		}
	}()
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

	go func() {
		if err := c.save(); err != nil {
			log.Printf("[WARN] YouTube cache: clear save error: %v", err)
		}
	}()
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
		go func() {
			if err := c.save(); err != nil {
				log.Printf("[WARN] YouTube cache: cleanup save error: %v", err)
			}
		}()
	}

	return count
}

// --- Feed Cache (10 hour TTL) ---

// FeedCache provides a separate cache for video feeds
type FeedCache struct {
	mu       sync.RWMutex
	data     map[string]feedCacheEntry
	filePath string
	ttl      time.Duration
}

type feedCacheEntry struct {
	Timestamp int64         `json:"timestamp"`
	Data      *FeedResponse `json:"data"`
}

// NewFeedCache creates a new feed cache with 10 hour TTL
func NewFeedCache(dataDir string) *FeedCache {
	cacheDir := filepath.Join(dataDir, "analytics")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("[WARN] Feed cache: could not create directory: %v", err)
	}

	fc := &FeedCache{
		data:     make(map[string]feedCacheEntry),
		filePath: filepath.Join(cacheDir, "feed_cache.json"),
		ttl:      10 * time.Hour,
	}

	fc.load()
	return fc
}

// load reads feed cache from file
func (fc *FeedCache) load() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	data, err := os.ReadFile(fc.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return json.Unmarshal(data, &fc.data)
}

// save writes feed cache to file
func (fc *FeedCache) save() error {
	data, err := json.MarshalIndent(fc.data, "", "  ")
	if err != nil {
		return err
	}

	tempPath := fc.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, fc.filePath)
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

	go func() {
		if err := fc.save(); err != nil {
			log.Printf("[WARN] Feed cache: save error: %v", err)
		}
	}()
}

// Clear removes all entries from feed cache
func (fc *FeedCache) Clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.data = make(map[string]feedCacheEntry)

	go func() {
		if err := fc.save(); err != nil {
			log.Printf("[WARN] Feed cache: clear save error: %v", err)
		}
	}()
}
