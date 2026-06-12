// Package cache provides Redis-based caching and task queue for Dark Editor operations.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Service provides Redis cache and task queue operations.
type Service struct {
	client *redis.Client
}

// NewService creates a new Redis cache service.
func NewService(addr, password string, db int) (*Service, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Service{client: client}, nil
}

// Close closes the Redis connection.
func (s *Service) Close() error {
	return s.client.Close()
}

// Get retrieves a value from cache.
func (s *Service) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

// Set stores a value in cache with TTL.
func (s *Service) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.client.Set(ctx, key, value, ttl).Err()
}

// Delete removes a key from cache.
func (s *Service) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

// DeleteByPattern removes keys matching a pattern.
func (s *Service) DeleteByPattern(ctx context.Context, pattern string) error {
	var cursor uint64
	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := s.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}

// Task represents a background task in the queue.
type Task struct {
	ID      string                 `json:"id"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload,omitempty"`
	Status  string                 `json:"status,omitempty"`
	Result  map[string]interface{} `json:"result,omitempty"`
	Error   string                 `json:"error,omitempty"`
}

// QueueTask adds a task to the queue.
func (s *Service) QueueTask(ctx context.Context, queue string, task Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.LPush(ctx, queue, data).Err()
}

// GetTask retrieves a task by ID.
func (s *Service) GetTask(ctx context.Context, taskID string) (*Task, error) {
	data, err := s.client.Get(ctx, "task:"+taskID).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// UpdateTaskStatus updates a task's status and result.
func (s *Service) UpdateTaskStatus(ctx context.Context, taskID, status string, result map[string]interface{}, errMsg string) error {
	data, _ := json.Marshal(map[string]interface{}{
		"status": status,
		"result": result,
		"error":  errMsg,
	})
	return s.client.Set(ctx, "task:"+taskID, data, 24*time.Hour).Err()
}

// DequeueTask pops a task from the queue (blocking with timeout).
func (s *Service) DequeueTask(ctx context.Context, queue string, timeout time.Duration) (*Task, error) {
	result, err := s.client.BRPop(ctx, timeout, queue).Result()
	if err != nil {
		return nil, err
	}
	if len(result) < 2 {
		return nil, fmt.Errorf("empty response from queue %s", queue)
	}
	var task Task
	if err := json.Unmarshal([]byte(result[1]), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// Publish sends a message to a Redis pub/sub channel.
func (s *Service) Publish(ctx context.Context, channel string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, channel, data).Err()
}
