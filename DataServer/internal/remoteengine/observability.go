package remoteengine

import (
	"context"

	"velox-server/internal/logging"
)

// RetryMetrics is the remote engine client's narrow retry-observability
// seam. Counters are monotonic and label values are closed enums (request
// method, RemoteErrorClass) so the Prometheus registry stays
// low-cardinality: job/trace ids, URLs and error messages are intentionally
// absent. A nil sink (the default for tests and un-wired callers) is a
// no-op.
type RetryMetrics interface {
	RecordRequest(method string)
	RecordRetry(class string)
	RecordFailure(class string)
}

// WithLogger wires a structured logger for the client's operator-facing
// events. The constructor installs logging.NewLogger("remoteengine");
// tests inject a custom (or nil) logger to silence or redirect output.
func (c *Client) WithLogger(l *logging.Logger) *Client {
	if c != nil {
		c.logger = l
	}
	return c
}

// WithMetrics wires the retry/request measurement sink (e.g.
// metrics.RemoteEngineTelemetry registered on the Prometheus Registry).
// Without it the client emits no /metrics series; structured logs and
// tracing spans still work.
func (c *Client) WithMetrics(m RetryMetrics) *Client {
	if c != nil {
		c.metrics = m
	}
	return c
}

// logInfo/logWarn are nil-safe structured emit helpers so a nil injected
// logger (tests) never panics. They thread ctx so the logger can inject
// trace_id/span_id when an active span is present (GAP 4).
func (c *Client) logInfo(ctx context.Context, code string, fields map[string]interface{}) {
	if c != nil && c.logger != nil {
		c.logger.InfoContext(ctx, code, fields)
	}
}

func (c *Client) logWarn(ctx context.Context, code string, fields map[string]interface{}) {
	if c != nil && c.logger != nil {
		c.logger.WarnContext(ctx, code, fields)
	}
}

func (c *Client) logError(ctx context.Context, code string, fields map[string]interface{}) {
	if c != nil && c.logger != nil {
		c.logger.ErrorContext(ctx, code, fields)
	}
}

// recordRequest/recordRetry/recordFailure keep the Prometheus sink
// in lockstep at each observation point; a nil sink is a no-op.
func (c *Client) recordRequest(method string) {
	if c != nil && c.metrics != nil {
		c.metrics.RecordRequest(method)
	}
}

func (c *Client) recordRetry(class string) {
	if c != nil && c.metrics != nil {
		c.metrics.RecordRetry(class)
	}
}

func (c *Client) recordFailure(class string) {
	if c != nil && c.metrics != nil {
		c.metrics.RecordFailure(class)
	}
}
