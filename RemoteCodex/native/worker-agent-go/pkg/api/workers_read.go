package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// =====================================================================
// Per-worker read-model API methods
// =====================================================================
//
// The master exposes three operator-facing GET endpoints under
// /api/v1/workers/{worker_id}/{metrics,sessions,events}. They were
// added in commit 044a401 (RW-PROD-005) and are covered by
// DataServer-side handler tests + swag annotations. This file
// exposes the same surface to out-of-process consumers through
// pkg/api: three stable single-method interfaces, three typed
// response structs mirroring the JSON shape byte-for-byte, and
// three thin *Client methods that reuse the existing *Client.doRequest
// plumbing (auth, retry, circuit breaker).
//
// SECURITY posture: the typed DTOs here are intentionally a separate
// mirror from DataServer/internal/handlers/server/api/workers_dto.go.
// The remote-codex module is a separate Go module — typed DTOs cannot
// be shared via Go imports. The mirror contract is pinned by the
// happy-path tests in workers_read_test.go; any shape drift there
// MUST be reflected here in the same atomic commit.
//
// All three methods honour the *Client's auth token (SetAuthToken)
// and retry policy. They add no new headers, no undocumented query
// params, and no transport concerns beyond what the existing
// doRequest path already implements.

// ---------------------------------------------------------------------
// Options structs
// ---------------------------------------------------------------------

// WorkerMetricsListOptions configures the per-worker metrics GET
// query parameters. The zero value is well-defined: empty Since +
// Limit=0 means no query params (server returns the most recent
// rows up to its default page size).
type WorkerMetricsListOptions struct {
	// Since is an optional RFC3339 lower bound on sampled_at.
	// Empty string = server-side default (no filter).
	Since string
	// Limit is an optional page size. 0 = server-side default
	// (DefaultListLimit, clamped to [1, MaxListLimit]).
	Limit int
}

// WorkerSessionsListOptions configures the per-worker sessions GET
// query parameters. The zero value is well-defined.
type WorkerSessionsListOptions struct {
	// IncludeRevoked controls whether revoked sessions are
	// included. Default (false) is the canonical operational
	// posture: revoked sessions can carry token-hash metadata
	// the operator did not ask for.
	IncludeRevoked bool
	// Limit is an optional page size. 0 = server-side default.
	Limit int
}

// WorkerEventsListOptions configures the per-worker events GET
// query parameters. The zero value is well-defined.
type WorkerEventsListOptions struct {
	// EventType is an optional exact match on event_type. The
	// master handler does NOT enforce a whitelist, so an unknown
	// event type simply returns zero matching rows.
	EventType string
	// Since is an optional RFC3339 lower bound on created_at.
	Since string
	// Limit is an optional page size. 0 = server-side default.
	Limit int
}

// ---------------------------------------------------------------------
// Stable interfaces — "accept interfaces, return structs"
// ---------------------------------------------------------------------

// WorkerMetricsAPI is the stable interface for the per-worker
// metrics GET endpoint. Consumers depend on this interface rather
// than the *Client concrete type so the implementation can be
// swapped (mock, fake, real) without breaking callers.
type WorkerMetricsAPI interface {
	// GetWorkerMetrics fetches the most recent metrics samples
	// for the named worker. Honours the *Client's auth token
	// and retry policy. Returns (nil, error) on transport
	// failure or non-2xx response.
	GetWorkerMetrics(ctx context.Context, workerID string, opts WorkerMetricsListOptions) (*WorkerMetricsList, error)
}

// WorkerSessionsAPI is the stable interface for the per-worker
// sessions GET endpoint.
type WorkerSessionsAPI interface {
	GetWorkerSessions(ctx context.Context, workerID string, opts WorkerSessionsListOptions) (*WorkerSessionsList, error)
}

// WorkerEventsAPI is the stable interface for the per-worker
// events GET endpoint.
type WorkerEventsAPI interface {
	GetWorkerEvents(ctx context.Context, workerID string, opts WorkerEventsListOptions) (*WorkerEventsList, error)
}

// Compile-time guard: *Client must satisfy all three stable
// interfaces. A future refactor that drops a method or breaks a
// signature will be caught at build time (much louder than a
// runtime type assertion failure inside a downstream package).
var (
	_ WorkerMetricsAPI  = (*Client)(nil)
	_ WorkerSessionsAPI = (*Client)(nil)
	_ WorkerEventsAPI   = (*Client)(nil)
)

// ---------------------------------------------------------------------
// Typed response structs — JSON tags pin the wire shape
// (mirror of DataServer workers_dto.go).
// ---------------------------------------------------------------------

// MetricSample mirrors MetricSampleResponse field-for-field.
// Pointer-typed optional fields (LoadAverage / ProcessRSSBytes /
// NetworkRxBytes / NetworkTxBytes) render a NULL value as an
// omitted key on the wire, matching the DataServer handler's
// NULL-handling contract.
type MetricSample struct {
	SampledAt           string   `json:"sampled_at"`
	SessionID           string   `json:"session_id,omitempty"`
	ConnectionStatus    string   `json:"connection_status"`
	ActiveTasks         int64    `json:"active_tasks"`
	TaskSlots           int64    `json:"task_slots"`
	CPUUtilizationRatio float64  `json:"cpu_utilization_ratio"`
	MemoryUsedBytes     int64    `json:"memory_used_bytes"`
	DiskFreeBytes       int64    `json:"disk_free_bytes"`
	LoadAverage         *float64 `json:"load_average,omitempty"`
	ProcessRSSBytes     *int64   `json:"process_rss_bytes,omitempty"`
	NetworkRxBytes      *int64   `json:"network_rx_bytes,omitempty"`
	NetworkTxBytes      *int64   `json:"network_tx_bytes,omitempty"`
}

// WorkerMetricsList mirrors WorkerMetricsListResponse.
type WorkerMetricsList struct {
	WorkerID string         `json:"worker_id"`
	Count    int            `json:"count"`
	Metrics  []MetricSample `json:"metrics"`
}

// Session mirrors SessionResponse.
type Session struct {
	SessionID        string `json:"session_id"`
	WorkerID         string `json:"worker_id"`
	SessionType      string `json:"session_type"`
	Status           string `json:"status"`
	IPAddress        string `json:"ip_address"`
	Revoked          bool   `json:"revoked"`
	ProtocolVersion  string `json:"protocol_version"`
	BundleVersion    string `json:"bundle_version,omitempty"`
	CreatedAt        string `json:"created_at"`
	ExpiresAt        string `json:"expires_at"`
	ConnectedAt      string `json:"connected_at,omitempty"`
	LastSeenAt       string `json:"last_seen_at,omitempty"`
	DisconnectedAt   string `json:"disconnected_at,omitempty"`
	DisconnectReason string `json:"disconnect_reason,omitempty"`
}

// WorkerSessionsList mirrors WorkerSessionsListResponse.
type WorkerSessionsList struct {
	WorkerID string    `json:"worker_id"`
	Count    int       `json:"count"`
	Sessions []Session `json:"sessions"`
}

// Event mirrors EventResponse.
type Event struct {
	EventID    string         `json:"event_id"`
	WorkerID   string         `json:"worker_id,omitempty"`
	SessionID  string         `json:"session_id,omitempty"`
	JobID      string         `json:"job_id,omitempty"`
	TaskID     string         `json:"task_id,omitempty"`
	AttemptID  string         `json:"attempt_id,omitempty"`
	EventType  string         `json:"event_type"`
	Severity   string         `json:"severity"`
	ReasonCode string         `json:"reason_code,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	CreatedAt  string         `json:"created_at"`
}

// WorkerEventsList mirrors WorkerEventsListResponse.
type WorkerEventsList struct {
	WorkerID string  `json:"worker_id"`
	Count    int     `json:"count"`
	Events   []Event `json:"events"`
}

// ---------------------------------------------------------------------
// Path constants for the per-worker read endpoints. Centralised
// so a future path rename (e.g. /api/v2/workers/...) only touches
// one place.
// ---------------------------------------------------------------------

const (
	workerMetricsPathFmt  = "/api/v1/workers/%s/metrics"
	workerSessionsPathFmt = "/api/v1/workers/%s/sessions"
	workerEventsPathFmt   = "/api/v1/workers/%s/events"
)

// ---------------------------------------------------------------------
// Implementation methods on *Client
// ---------------------------------------------------------------------

// GetWorkerMetrics fetches /api/v1/workers/{worker_id}/metrics.
// workerID is escaped via net/url PathEscape so opaque ids with
// reserved characters do not break the path contract.
//
// Honours the *Client's auth token (SetAuthToken), retry policy,
// and circuit breaker via the existing doRequest path. The body
// argument is nil because this is a GET.
func (c *Client) GetWorkerMetrics(ctx context.Context, workerID string, opts WorkerMetricsListOptions) (*WorkerMetricsList, error) {
	body, err := c.doRequest(ctx, http.MethodGet, workerReadPathWithQuery(workerMetricsPathFmt, workerID, buildMetricsQuery(opts)), nil)
	if err != nil {
		return nil, err
	}
	var resp WorkerMetricsList
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode worker metrics response: %w", err)
	}
	return &resp, nil
}

// GetWorkerSessions fetches /api/v1/workers/{worker_id}/sessions.
func (c *Client) GetWorkerSessions(ctx context.Context, workerID string, opts WorkerSessionsListOptions) (*WorkerSessionsList, error) {
	body, err := c.doRequest(ctx, http.MethodGet, workerReadPathWithQuery(workerSessionsPathFmt, workerID, buildSessionsQuery(opts)), nil)
	if err != nil {
		return nil, err
	}
	var resp WorkerSessionsList
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode worker sessions response: %w", err)
	}
	return &resp, nil
}

// GetWorkerEvents fetches /api/v1/workers/{worker_id}/events.
func (c *Client) GetWorkerEvents(ctx context.Context, workerID string, opts WorkerEventsListOptions) (*WorkerEventsList, error) {
	body, err := c.doRequest(ctx, http.MethodGet, workerReadPathWithQuery(workerEventsPathFmt, workerID, buildEventsQuery(opts)), nil)
	if err != nil {
		return nil, err
	}
	var resp WorkerEventsList
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode worker events response: %w", err)
	}
	return &resp, nil
}

// ---------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------

// workerReadPathWithQuery formats the worker-id path safely and
// appends a query string only when at least one parameter is set.
// Centralised because the three GET methods follow the same shape.
// Currently cannot fail (fmt.Sprintf + url.Values.Encode are
// infallible on simple types) so the signature returns just
// `string` to keep the GET methods flat.
func workerReadPathWithQuery(format, workerID string, q url.Values) string {
	// net/url.PathEscape escapes reserved chars in path segments
	// (e.g. '/', '?') per RFC 3986 §3.3.
	path := fmt.Sprintf(format, url.PathEscape(workerID))
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return path
}

func buildMetricsQuery(opts WorkerMetricsListOptions) url.Values {
	q := url.Values{}
	if opts.Since != "" {
		q.Set("since", opts.Since)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	return q
}

func buildSessionsQuery(opts WorkerSessionsListOptions) url.Values {
	q := url.Values{}
	if opts.IncludeRevoked {
		q.Set("include_revoked", "true")
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	return q
}

func buildEventsQuery(opts WorkerEventsListOptions) url.Values {
	q := url.Values{}
	if opts.EventType != "" {
		q.Set("event_type", opts.EventType)
	}
	if opts.Since != "" {
		q.Set("since", opts.Since)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	return q
}
