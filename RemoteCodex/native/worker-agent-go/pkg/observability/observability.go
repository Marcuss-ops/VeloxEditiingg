// Package observability exposes the small observability boundary shared by
// public worker packages. The implementation remains owned by the worker's
// internal telemetry/tracing packages; callers depend only on these narrow
// contracts and never on internal paths.
package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"velox-worker-agent/internal/oteltrace"
	"velox-worker-agent/internal/telemetry"
)

// CacheMetrics is the cache-specific metrics surface required by public
// cache implementations. It deliberately avoids exposing the concrete
// Prometheus registry or worker-wide metric state.
type CacheMetrics interface {
	RecordCacheRequest(result string)
	RecordCacheEvictions(reason string, count int)
}

// CacheMetricsProvider returns the process metrics sink used by the worker
// composition root. The concrete implementation is hidden behind the port.
func CacheMetricsProvider() CacheMetrics {
	return telemetry.GetPrometheusMetrics()
}

// StartSpan starts a worker span through the canonical tracer.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return oteltrace.StartSpan(ctx, name, attrs...)
}

// AttrJobID creates the canonical job-id span attribute.
func AttrJobID(id string) attribute.KeyValue {
	return oteltrace.AttrJobID(id)
}
