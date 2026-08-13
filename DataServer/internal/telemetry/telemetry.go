// Package telemetry provides OpenTelemetry distributed tracing for Velox.
//
// Scorecard v2 / Step 15: the tracer provider is configurable via
// VELOX_OTEL_EXPORTER env var:
//
//	""        (default) — no-op tracer (zero overhead)
//	"stdout"  — prints spans to stderr (dev/debug)
//	"otlp"    — exports to OTLP collector via gRPC (production, requires
//	           VELOX_OTEL_ENDPOINT)
//
// Scorecard v2 / Step 17: OTLP exporter now wired. Set
// VELOX_OTEL_EXPORTER=otlp and VELOX_OTEL_ENDPOINT=host:port
// (e.g. "otel-collector:4317") for production tracing.
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
	"errors"
	"fmt"
	"log"
	"sync"

	"velox-server/internal/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var (
	// propagatorOnce ensures W3C propagation is set exactly once per process.
	// The no-op default path does NOT set it — zero overhead when tracing is off.
	propagatorOnce sync.Once

	globalTelemetryState = struct {
		sync.RWMutex
		instance        *Telemetry
		configured      bool
		tracerRequested bool
	}{instance: newTelemetry(config.TelemetryConfig{Insecure: true}, true)}
)

// ErrConfigureAfterTracer indicates that the Configure-before-Tracer contract
// was violated. Reconfiguration after a tracer has been requested is rejected
// instead of silently racing with or replacing a live provider.
var ErrConfigureAfterTracer = errors.New("telemetry: Configure must run before Tracer")

// ErrAlreadyConfigured indicates that the process-wide facade was configured
// more than once. Tests and components that need independent configurations
// should use NewTelemetry rather than resetting package state.
var ErrAlreadyConfigured = errors.New("telemetry: already configured")

// Telemetry owns one immutable configuration and one lazily initialized
// tracer. Instances are the preferred seam for tests and embedded consumers;
// they cannot contaminate one another through package-global reset state.
type Telemetry struct {
	config        config.TelemetryConfig
	installGlobal bool
	once          sync.Once
	tracer        trace.Tracer

	providerMu sync.Mutex
	provider   *sdktrace.TracerProvider
	initErr    error
}

// NewTelemetry creates an isolated telemetry instance. Configure the instance
// by value at construction time, then safely share it between goroutines.
// Isolated instances never replace the process-wide OpenTelemetry provider.
func NewTelemetry(cfg config.TelemetryConfig) *Telemetry {
	return newTelemetry(cfg, false)
}

func newTelemetry(cfg config.TelemetryConfig, installGlobal bool) *Telemetry {
	return &Telemetry{config: cfg, installGlobal: installGlobal}
}

// Configure applies centrally parsed telemetry settings to the process-wide
// facade. It must run before the first global Tracer call and may run only
// once. Use NewTelemetry for isolated configurations in tests.
func Configure(cfg config.TelemetryConfig) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	globalTelemetryState.Lock()
	defer globalTelemetryState.Unlock()
	if globalTelemetryState.tracerRequested {
		return ErrConfigureAfterTracer
	}
	if globalTelemetryState.configured {
		return ErrAlreadyConfigured
	}
	globalTelemetryState.instance = newTelemetry(cfg, true)
	globalTelemetryState.configured = true
	return nil
}

func validateConfig(cfg config.TelemetryConfig) error {
	switch cfg.Exporter {
	case "", "stdout":
		return nil
	case "otlp":
		if cfg.Endpoint == "" {
			return errors.New("telemetry: OTLP exporter requires VELOX_OTEL_ENDPOINT")
		}
		return nil
	default:
		return fmt.Errorf("telemetry: unsupported exporter %q", cfg.Exporter)
	}
}

// Tracer returns the process-wide Velox tracer. The facade records the first
// request before initializing the instance, so a concurrent Configure cannot
// race with initialization or silently change the selected configuration.
func Tracer() trace.Tracer {
	globalTelemetryState.Lock()
	globalTelemetryState.tracerRequested = true
	instance := globalTelemetryState.instance
	globalTelemetryState.Unlock()
	return instance.Tracer()
}

func globalConfig() config.TelemetryConfig {
	globalTelemetryState.RLock()
	defer globalTelemetryState.RUnlock()
	return globalTelemetryState.instance.Config()
}

// Shutdown closes the provider owned by the process-wide facade. Bootstrap
// should defer this after Configure so OTLP/stdout batchers flush during an
// orderly server shutdown instead of surviving as leaked goroutines.
func Shutdown(ctx context.Context) error {
	globalTelemetryState.RLock()
	instance := globalTelemetryState.instance
	globalTelemetryState.RUnlock()
	return instance.Shutdown(ctx)
}

// Config returns the immutable configuration captured by this instance.
// It is primarily useful for wiring and tests; callers cannot mutate it.
func (t *Telemetry) Config() config.TelemetryConfig {
	if t == nil {
		return config.TelemetryConfig{}
	}
	return t.config
}

// ReadinessError returns the initialization failure for a requested exporter.
// An empty exporter is an intentional DISABLED state and is ready by design.
func (t *Telemetry) ReadinessError() error {
	if t == nil {
		return errors.New("telemetry: instance is nil")
	}
	t.providerMu.Lock()
	defer t.providerMu.Unlock()
	return t.initErr
}

// GlobalReadinessError reports whether the configured process-wide exporter is
// usable. It is consumed by the server readiness gate.
func GlobalReadinessError() error {
	globalTelemetryState.RLock()
	instance := globalTelemetryState.instance
	globalTelemetryState.RUnlock()
	return instance.ReadinessError()
}

// Tracer returns this instance's tracer. The first call initializes the
// provider based only on the configuration captured by NewTelemetry.
func (t *Telemetry) Tracer() trace.Tracer {
	if t == nil {
		return noop.NewTracerProvider().Tracer("velox-server")
	}
	t.once.Do(func() {
		t.tracer = t.initTracer()
	})
	return t.tracer
}

// Shutdown releases exporter resources owned by this instance. It is safe to
// call more than once. Calling it before the first Tracer call initializes the
// instance first, ensuring a newly-created exporter cannot leak after shutdown.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_ = t.Tracer()
	t.providerMu.Lock()
	provider := t.provider
	t.provider = nil
	t.providerMu.Unlock()
	if provider == nil {
		return nil
	}
	return provider.Shutdown(ctx)
}

func (t *Telemetry) setProvider(provider *sdktrace.TracerProvider) {
	t.providerMu.Lock()
	t.provider = provider
	t.providerMu.Unlock()
}

func (t *Telemetry) setInitError(err error) {
	t.providerMu.Lock()
	t.initErr = err
	t.providerMu.Unlock()
}

// initTracer reads the immutable instance configuration. Default is no-op
// (zero overhead when tracing is disabled).
func (t *Telemetry) initTracer() trace.Tracer {
	switch t.config.Exporter {
	case "stdout":
		return t.initStdoutTracer()
	case "otlp":
		return t.initOTLPTracer()
	default:
		return noop.NewTracerProvider().Tracer("velox-server")
	}
}

// initPropagator sets the global TextMapPropagator to W3C TraceContext.
// Called exactly once from initStdoutTracer and initOTLPTracer.
func initPropagator() {
	propagatorOnce.Do(func() {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		log.Printf("[TELEMETRY] W3C TraceContext propagator initialized")
	})
}

// buildResource constructs the canonical Resource (service.name,
// service.version) for both stdout and OTLP tracer providers.
func (t *Telemetry) buildResource() *resource.Resource {
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("velox-server"),
		semconv.ServiceVersion(t.config.Version),
	)
	return res
}

// initStdoutTracer creates a tracer that prints spans to stderr.
// Also initializes the W3C propagator so gRPC context propagation works.
func (t *Telemetry) initStdoutTracer() trace.Tracer {
	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.setInitError(fmt.Errorf("telemetry: stdout exporter init: %w", err))
		log.Printf("[TELEMETRY] stdout exporter init failed: %v — readiness will remain MISCONFIGURED", err)
		return noop.NewTracerProvider().Tracer("velox-server")
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(t.buildResource()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	t.setProvider(tp)
	if t.installGlobal {
		otel.SetTracerProvider(tp)
		initPropagator()
	}

	log.Printf("[TELEMETRY] stdout tracer provider + W3C propagator initialized")
	return tp.Tracer("velox-server")
}

// initOTLPTracer creates a tracer that exports spans to an OTLP
// collector via gRPC. Reads VELOX_OTEL_ENDPOINT (host:port, e.g.
// "otel-collector:4317"). Uses insecure credentials by default;
// set VELOX_OTEL_INSECURE=false to require TLS (not yet wired).
func (t *Telemetry) initOTLPTracer() trace.Tracer {
	endpoint := t.config.Endpoint
	if endpoint == "" {
		t.setInitError(errors.New("telemetry: OTLP exporter endpoint is empty"))
		log.Printf("[TELEMETRY] OTLP exporter requested but endpoint is empty — readiness will remain MISCONFIGURED")
		return noop.NewTracerProvider().Tracer("velox-server")
	}

	log.Printf("[TELEMETRY] OTLP gRPC exporter connecting to %s", endpoint)

	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	if t.config.Insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(context.Background(), options...)
	if err != nil {
		t.setInitError(fmt.Errorf("telemetry: OTLP exporter init: %w", err))
		log.Printf("[TELEMETRY] OTLP gRPC exporter init failed: %v — readiness will remain MISCONFIGURED", err)
		return noop.NewTracerProvider().Tracer("velox-server")
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(t.buildResource()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	t.setProvider(tp)
	if t.installGlobal {
		otel.SetTracerProvider(tp)
		initPropagator()
	}

	log.Printf("[TELEMETRY] OTLP gRPC tracer provider + W3C propagator initialized — endpoint=%s", endpoint)
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
