package remoteengine

import (
	"net/http"
	"time"
)

// Config holds remote engine configuration
type Config struct {
	URL       string
	Token     string
	TimeoutMS int
	// Retries is the number of retry attempts after the initial request.
	// Total attempts = Retries + 1.
	Retries int
}

// Client is the remote engine client
type Client struct {
	config     Config
	httpClient *http.Client
}

// SimpleScriptRequest is the input for simple script generation
type SimpleScriptRequest struct {
	Topic     string            `json:"topic"`
	Language  string            `json:"language,omitempty"`
	Style     string            `json:"style,omitempty"`
	Duration  int               `json:"duration,omitempty"` // seconds
	Variables map[string]string `json:"variables,omitempty"`
}

// SimpleScriptResponse is the output
type SimpleScriptResponse struct {
	OK       bool   `json:"ok"`
	Script   string `json:"script"`
	Title    string `json:"title,omitempty"`
	Error    string `json:"error,omitempty"`
	TraceID  string `json:"trace_id,omitempty"`
	Provider string `json:"provider,omitempty"`
}

// BatchScriptRequest is the input for batch generation
type BatchScriptRequest struct {
	Topics    []string          `json:"topics"`
	Language  string            `json:"language,omitempty"`
	Style     string            `json:"style,omitempty"`
	Variables map[string]string `json:"variables,omitempty"`
}

// BatchScriptResponse is the output
type BatchScriptResponse struct {
	OK      bool              `json:"ok"`
	Scripts []GeneratedScript `json:"scripts,omitempty"`
	Error   string            `json:"error,omitempty"`
	TraceID string            `json:"trace_id,omitempty"`
}

// GeneratedScript represents a single generated script
type GeneratedScript struct {
	Topic  string `json:"topic"`
	Script string `json:"script"`
	Title  string `json:"title,omitempty"`
	Error  string `json:"error,omitempty"`
}

// PipelineStatusResponse is the status of a pipeline job
type PipelineStatusResponse struct {
	OK        bool                   `json:"ok"`
	TraceID   string                 `json:"trace_id"`
	Status    string                 `json:"status"` // pending, running, completed, failed
	Progress  float64                `json:"progress,omitempty"`
	Result    map[string]interface{} `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt time.Time              `json:"created_at,omitempty"`
	UpdatedAt time.Time              `json:"updated_at,omitempty"`
}
