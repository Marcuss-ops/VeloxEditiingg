package darkeditor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/cache"
)

// CacheService provides caching for Dark Editor operations
type CacheService struct {
	redis *cache.Service
	ttl   time.Duration
}

// NewCacheService creates a new cache service for Dark Editor
func NewCacheService(redis *cache.Service, ttl time.Duration) *CacheService {
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	return &CacheService{
		redis: redis,
		ttl:   ttl,
	}
}

// ============== FILTER CACHING ==============

// FilterCacheKey generates a cache key for filter operations
func (c *CacheService) FilterCacheKey(filename, filterType string, value float64) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%f", filename, filterType, value)))
	return fmt.Sprintf("dark_editor:filter:%s", hex.EncodeToString(hash[:]))
}

// GetCachedFilter retrieves a cached filter result
func (c *CacheService) GetCachedFilter(ctx context.Context, filename, filterType string, value float64) (*CachedFilterResult, error) {
	key := c.FilterCacheKey(filename, filterType, value)
	data, err := c.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	var result CachedFilterResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CacheFilterResult caches a filter result
func (c *CacheService) CacheFilterResult(ctx context.Context, filename, filterType string, value float64, result CachedFilterResult) error {
	key := c.FilterCacheKey(filename, filterType, value)
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.redis.Set(ctx, key, data, c.ttl)
}

// CachedFilterResult represents a cached filter operation result
type CachedFilterResult struct {
	Filename   string    `json:"filename"`
	URL        string    `json:"url"`
	FilterType string    `json:"filter_type"`
	Value      float64   `json:"value"`
	CreatedAt  time.Time `json:"created_at"`
}

// ============== UPSCALE CACHING ==============

// UpscaleCacheKey generates a cache key for upscale operations
func (c *CacheService) UpscaleCacheKey(filename string, scale float64) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:upscale:%f", filename, scale)))
	return fmt.Sprintf("dark_editor:upscale:%s", hex.EncodeToString(hash[:]))
}

// GetCachedUpscale retrieves a cached upscale result
func (c *CacheService) GetCachedUpscale(ctx context.Context, filename string, scale float64) (*CachedUpscaleResult, error) {
	key := c.UpscaleCacheKey(filename, scale)
	data, err := c.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	var result CachedUpscaleResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CacheUpscaleResult caches an upscale result
func (c *CacheService) CacheUpscaleResult(ctx context.Context, filename string, scale float64, result CachedUpscaleResult) error {
	key := c.UpscaleCacheKey(filename, scale)
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.redis.Set(ctx, key, data, c.ttl)
}

// CachedUpscaleResult represents a cached upscale operation result
type CachedUpscaleResult struct {
	Filename  string    `json:"filename"`
	URL       string    `json:"url"`
	Scale     float64   `json:"scale"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	CreatedAt time.Time `json:"created_at"`
}

// ============== BACKGROUND REMOVAL CACHING ==============

// BackgroundRemovalCacheKey generates a cache key for background removal operations
func (c *CacheService) BackgroundRemovalCacheKey(filename, model string) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:remove_bg:%s", filename, model)))
	return fmt.Sprintf("dark_editor:remove_bg:%s", hex.EncodeToString(hash[:]))
}

// GetCachedBackgroundRemoval retrieves a cached background removal result
func (c *CacheService) GetCachedBackgroundRemoval(ctx context.Context, filename, model string) (*CachedBackgroundRemovalResult, error) {
	key := c.BackgroundRemovalCacheKey(filename, model)
	data, err := c.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	var result CachedBackgroundRemovalResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CacheBackgroundRemovalResult caches a background removal result
func (c *CacheService) CacheBackgroundRemovalResult(ctx context.Context, filename, model string, result CachedBackgroundRemovalResult) error {
	key := c.BackgroundRemovalCacheKey(filename, model)
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.redis.Set(ctx, key, data, c.ttl)
}

// CachedBackgroundRemovalResult represents a cached background removal result
type CachedBackgroundRemovalResult struct {
	Filename  string    `json:"filename"`
	URL       string    `json:"url"`
	Model     string    `json:"model"`
	CreatedAt time.Time `json:"created_at"`
}

// ============== TRANSFORM CACHING ==============

// TransformCacheKey generates a cache key for transform operations
func (c *CacheService) TransformCacheKey(filename string, params TransformParams) string {
	paramsJSON, _ := json.Marshal(params)
	hash := sha256.Sum256(append([]byte(filename+":"), paramsJSON...))
	return fmt.Sprintf("dark_editor:transform:%s", hex.EncodeToString(hash[:]))
}

// GetCachedTransform retrieves a cached transform result
func (c *CacheService) GetCachedTransform(ctx context.Context, filename string, params TransformParams) (*CachedTransformResult, error) {
	key := c.TransformCacheKey(filename, params)
	data, err := c.redis.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	var result CachedTransformResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CacheTransformResult caches a transform result
func (c *CacheService) CacheTransformResult(ctx context.Context, filename string, params TransformParams, result CachedTransformResult) error {
	key := c.TransformCacheKey(filename, params)
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.redis.Set(ctx, key, data, c.ttl)
}

// TransformParams represents transform operation parameters
type TransformParams struct {
	Rotate     float64 `json:"rotate,omitempty"`
	FlipX      bool    `json:"flip_x,omitempty"`
	FlipY      bool    `json:"flip_y,omitempty"`
	ScaleX     float64 `json:"scale_x,omitempty"`
	ScaleY     float64 `json:"scale_y,omitempty"`
	TranslateX int     `json:"translate_x,omitempty"`
	TranslateY int     `json:"translate_y,omitempty"`
}

// CachedTransformResult represents a cached transform result
type CachedTransformResult struct {
	Filename  string          `json:"filename"`
	URL       string          `json:"url"`
	Params    TransformParams `json:"params"`
	CreatedAt time.Time       `json:"created_at"`
}

// ============== CACHE INVALIDATION ==============

// InvalidateFile removes all cached operations for a file
func (c *CacheService) InvalidateFile(ctx context.Context, filename string) error {
	pattern := fmt.Sprintf("dark_editor:*:%s*", filename)
	return c.redis.DeleteByPattern(ctx, pattern)
}

// InvalidateAll removes all Dark Editor cache entries
func (c *CacheService) InvalidateAll(ctx context.Context) error {
	return c.redis.DeleteByPattern(ctx, "dark_editor:*")
}

// ============== TASK QUEUE HELPERS ==============

// QueueBackgroundRemoval queues a background removal task
func (c *CacheService) QueueBackgroundRemoval(ctx context.Context, taskID, filename, model string) error {
	task := cache.Task{
		ID:   taskID,
		Type: "remove_bg",
		Payload: map[string]interface{}{
			"filename": filename,
			"model":    model,
		},
	}
	return c.redis.QueueTask(ctx, "dark_editor_tasks", task)
}

// QueueUpscale queues an upscale task
func (c *CacheService) QueueUpscale(ctx context.Context, taskID, filename string, scale float64) error {
	task := cache.Task{
		ID:   taskID,
		Type: "upscale",
		Payload: map[string]interface{}{
			"filename": filename,
			"scale":    scale,
		},
	}
	return c.redis.QueueTask(ctx, "dark_editor_tasks", task)
}

// QueueGenerate queues an AI generation task
func (c *CacheService) QueueGenerate(ctx context.Context, taskID, prompt string, params map[string]interface{}) error {
	task := cache.Task{
		ID:   taskID,
		Type: "generate",
		Payload: map[string]interface{}{
			"prompt": prompt,
			"params": params,
		},
	}
	return c.redis.QueueTask(ctx, "dark_editor_tasks", task)
}

// GetTask retrieves a task by ID
func (c *CacheService) GetTask(ctx context.Context, taskID string) (*cache.Task, error) {
	return c.redis.GetTask(ctx, taskID)
}

// UpdateTaskStatus updates a task's status
func (c *CacheService) UpdateTaskStatus(ctx context.Context, taskID, status string, result map[string]interface{}, errMsg string) error {
	return c.redis.UpdateTaskStatus(ctx, taskID, status, result, errMsg)
}

// ============== PUB/SUB HELPERS ==============

// PublishTaskCompletion publishes a task completion notification
func (c *CacheService) PublishTaskCompletion(ctx context.Context, taskID, taskType, status string, result map[string]interface{}) error {
	return c.redis.Publish(ctx, "dark_editor:task_completed", map[string]interface{}{
		"task_id":   taskID,
		"task_type": taskType,
		"status":    status,
		"result":    result,
		"timestamp": time.Now(),
	})
}

// PublishProgress publishes a progress update
func (c *CacheService) PublishProgress(ctx context.Context, taskID string, progress float64, message string) error {
	return c.redis.Publish(ctx, "dark_editor:progress:"+taskID, map[string]interface{}{
		"task_id":   taskID,
		"progress":  progress,
		"message":   message,
		"timestamp": time.Now(),
	})
}
