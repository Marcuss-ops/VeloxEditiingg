// Package telemetry provides OpenTelemetry distributed tracing for Velox.
//
// Scorecard v2 / Step 15: the tracer provider is configurable via
// VELOX_OTEL_EXPORTER env var:
//
//	""        (default) — no-op tracer (zero overhead)
//	"stdout"  — prints spans to stderr (dev/debug)
//	"otlp"    — exports to OTLP collector (production, requires
//	           VELOX_OTEL_ENDPOINT)
//
// Scorecard v2 / Step 15c: W3C TraceContext propagation is initialized
// globally so gRPC interceptors (otelgrpc) can extract/inject trace
// context from inbound/outbound gRPC metadata automatically.
//
// Spans are NEVER created with high-cardinality labels (job_id,
// task_id go into span attributes, not the span name).
package telemetry

import (
	"context"
	"log"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var (
	tracer     trace.Tracer
	tracerOnce sync.Once

	// propagatorInit ensures W3C propagation is set exactly once.
	// Called from initStdoutTracer (when VELOX_OTEL_EXPORTER=stdout)
	// and from any future otlp/jaeger path. The no-op default path
	// (VELOX_OTEL_EXPORTER="") does NOT set it — when tracing is off,
	// propagation is also off (zero overhead).
	propagatorOnce sync.Once
)

// Tracer returns the global Velox tracer. Safe for concurrent use.
// The first call initializes the tracer provider based on VELOX_OTEL_EXPORTER.
func Tracer() trace.Tracer {
	tracerOnce.Do(func() {
		tracer = initTracer()
	})
	return tracer
}

// initTracer reads VELOX_OTEL_EXPORTER and returns the appropriate tracer.
// Default is no-op (zero overhead when tracing is disabled).
func initTracer() trace.Tracer {
	exporter := os.Getenv("VELOX_OTEL_EXPORTER")

	switch exporter {
	case "stdout":
		return initStdoutTracer()
	case "otlp":
		log.Printf("[TELEMETRY] OTLP exporter requested but not yet wired — falling back to no-op")
		return noop.NewTracerProvider().Tracer("velox-server")
	default:
		return noop.NewTracerProvider().Tracer("velox-server")
	}
}

// initPropagator sets the global TextMapPropagator to W3C TraceContext.
// Called exactly once from initStdoutTracer (and future otlp paths).
// The propagation package provides TraceContext{} which handles the
// standard "traceparent" and "tracestate" headers.
func initPropagator() {
	propagatorOnce.Do(func() {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		log.Printf("[TELEMETRY] W3C TraceContext propagator initialized")
	})
}

// initStdoutTracer creates a tracer that prints spans to stderr.
// Also initializes the W3C propagator so gRPC context propagation works.
func initStdoutTracer() trace.Tracer {
	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Printf("[TELEMETRY] stdout exporter init failed: %v — falling back to no-op", err)
		return noop.NewTracerProvider().Tracer("velox-server")
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("velox-server"),
		semconv.ServiceVersion(os.Getenv("VELOX_VERSION")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)

	// Initialize W3C TraceContext propagation so otelgrpc can
	// extract/inject traceparent from gRPC metadata.
	initPropagator()

	log.Printf("[TELEMETRY] stdout tracer provider + W3C propagator initialized")
	return tp.Tracer("velox-server")
}

// ── Span Helpers ───────────────────────────────────────────────────────

// StartSpan is the canonical span-starter for Velox. It wraps
// Tracer().Start() with standard service attributes.
// spanName should be a low-cardinality operation name (e.g. "schedule_task",
// "claim_task", "ingest_result").
func StartSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, spanName, trace.WithAttributes(attrs...))
}

// SpanFromContext extracts the current span from context.
// Returns a no-op span if no span is in context.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// TraceIDFromContext returns the W3C trace ID (32 hex chars) from the
// current span context, or "" if no span is active.
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span == nil || !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

// SpanIDFromContext returns the W3C span ID (16 hex chars) from the
// current span context, or "" if no span is active.
func SpanIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span == nil || !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().SpanID().String()
}

// ── Common span attribute keys ────────────────────────────────────────

// Low-cardinality attributes safe for all spans.
var (
	AttrService       = attribute.String("service.name", "velox-server")
	AttrSpanKind      = func(kind string) attribute.KeyValue { return attribute.String("span.kind", kind) }
	AttrJobID         = func(id string) attribute.KeyValue { return attribute.String("velox.job_id", id) }
	AttrTaskID        = func(id string) attribute.KeyValue { return attribute.String("velox.task_id", id) }
	AttrAttemptID     = func(id string) attribute.KeyValue { return attribute.String("velox.attempt_id", id) }
	AttrWorkerID      = func(id string) attribute.KeyValue { return attribute.String("velox.worker_id", id) }
	AttrLeaseID       = func(id string) attribute.KeyValue { return attribute.String("velox.lease_id", id) }
	AttrExecutorID    = func(id string) attribute.KeyValue { return attribute.String("velox.executor_id", id) }
	AttrAttemptNumber = func(n int) attribute.KeyValue { return attribute.Int("velox.attempt_number", n) }
	AttrStatus        = func(s string) attribute.KeyValue { return attribute.String("velox.status", s) }
	AttrErrorCode     = func(c string) attribute.KeyValue { return attribute.String("velox.error_code", c) }
)
