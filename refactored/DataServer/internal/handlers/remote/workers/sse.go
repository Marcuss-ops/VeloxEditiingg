package workers

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/queue"
)

// SSEEvent represents a server-sent event
type SSEEvent struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// SSEBroker manages SSE client subscriptions
type SSEBroker struct {
	mu      sync.RWMutex
	clients map[chan SSEEvent]struct{}
	workers *queue.FileQueue
}

// NewSSEBroker creates a new SSE broker
func NewSSEBroker(fq *queue.FileQueue) *SSEBroker {
	return &SSEBroker{
		clients: make(map[chan SSEEvent]struct{}),
		workers: fq,
	}
}

// Subscribe adds a new client channel
func (b *SSEBroker) Subscribe() chan SSEEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan SSEEvent, 64)
	b.clients[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a client channel
func (b *SSEBroker) Unsubscribe(ch chan SSEEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.clients, ch)
	close(ch)
}

// Broadcast sends an event to all connected clients
func (b *SSEBroker) Broadcast(event SSEEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- event:
		default:
			// Client channel full, skip to avoid blocking
		}
	}
}

// NotifyJobStatus broadcasts a job status update
func (b *SSEBroker) NotifyJobStatus(jobID, status, workerID string) {
	b.Broadcast(SSEEvent{
		Type: "job_status",
		Data: gin.H{
			"job_id":    jobID,
			"status":    status,
			"worker":    workerID,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		},
	})
}

// NotifyWorkerStatus broadcasts a worker status update
func (b *SSEBroker) NotifyWorkerStatus(workerID, status string) {
	b.Broadcast(SSEEvent{
		Type: "worker_status",
		Data: gin.H{
			"worker_id": workerID,
			"status":    status,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		},
	})
}

// SSEHandler returns a Gin handler for SSE connections
func (b *SSEBroker) SSEHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Set SSE headers
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")

		flusher, ok := c.Writer.(http.Flusher)
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming not supported"})
			return
		}

		// Subscribe to events
		ch := b.Subscribe()
		defer b.Unsubscribe(ch)

		// Send initial snapshot
		if b.workers != nil {
			ctx := c.Request.Context()
			stats, _ := b.workers.Stats(ctx)
			pending := int64(0)
			processing := int64(0)
			if stats != nil {
				if v, ok := stats["pending"]; ok {
					pending = v
				}
				if v, ok := stats["processing"]; ok {
					processing = v
				}
			}
			c.SSEvent("message", gin.H{
				"type": "snapshot",
				"data": gin.H{
					"pending":    pending,
					"processing": processing,
					"timestamp":  time.Now().UTC().Format(time.RFC3339),
				},
			})
			flusher.Flush()
		}

		// Stream events
		for {
			select {
			case <-c.Request.Context().Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				c.SSEvent("message", event)
				flusher.Flush()
			case <-time.After(30 * time.Second):
				// Send keepalive
				fmt.Fprintf(c.Writer, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	}
}
