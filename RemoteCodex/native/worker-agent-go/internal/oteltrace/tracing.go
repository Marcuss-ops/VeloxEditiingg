// Package oteltrace provides OpenTelemetry distributed tracing for the
// Velox worker agent.
//
// Scorecard v2 / Step 15: the tracer provider is configurable via
// VELOX_OTEL_EXPORTER env var:
//
//	""        (default) — no-op tracer (zero overhead)
//	"stdout"  — prints spans to stderr (dev/debug)
//
// The worker propagates trace context via context.Context (not gRPC
// metadata — the worker receives context from the gRPC interceptor
// when the master propagates via otelgrpc).
package oteltrace

import (
	"context"
	"log"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var (
	tracer     trace.Tracer
	tracerOnce sync.Once
)

// Tracer returns the global worker tracer. Safe for concurrent use.
func Tracer() trace.Tracer {
	tracerOnce.Do(func() {
		tracer = initTracer()
	})
	return tracer
}

func initTracer() trace.Tracer {
	exporter := os.Getenv("VELOX_OTEL_EXPORTER")
	switch exporter {
	case "stdout":
		return initStdoutTracer()
	default:
		return noop.NewTracerProvider().Tracer("velox-worker-agent")
	}
}

func initStdoutTracer() trace.Tracer {
	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Printf("[OTELTRACE] stdout exporter init failed: %v", err)
		return noop.NewTracerProvider().Tracer("velox-worker-agent")
	}
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("velox-worker-agent"),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	log.Printf("[OTELTRACE] worker stdout tracer initialized")
	return tp.Tracer("velox-worker-agent")
}

// StartSpan starts a span with the given name and optional attributes.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

// ── Common attribute constructors ──────────────────────────────────────

func AttrJobID(id string) attribute.KeyValue     { return attribute.String("velox.job_id", id) }
func AttrTaskID(id string) attribute.KeyValue    { return attribute.String("velox.task_id", id) }
func AttrWorkerID(id string) attribute.KeyValue  { return attribute.String("velox.worker_id", id) }
func AttrStatus(s string) attribute.KeyValue     { return attribute.String("velox.status", s) }
func AttrExecutorID(id string) attribute.KeyValue { return attribute.String("velox.executor_id", id) }
