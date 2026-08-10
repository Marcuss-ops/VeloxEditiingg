package telemetry

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	"velox-server/internal/config"
)

func TestTelemetryInstancesAreIsolated(t *testing.T) {
	t.Parallel()

	first := NewTelemetry(config.TelemetryConfig{
		Exporter: "",
		Version:  "first",
	})
	second := NewTelemetry(config.TelemetryConfig{
		Exporter:                  "",
		Version:                   "second",
		MeasureEnqueueAllocations: true,
	})

	if got := first.Config().Version; got != "first" {
		t.Fatalf("first config version = %q, want first", got)
	}
	if got := second.Config().Version; got != "second" {
		t.Fatalf("second config version = %q, want second", got)
	}
	if first.Config().MeasureEnqueueAllocations {
		t.Fatal("first instance inherited second instance configuration")
	}
	if !second.Config().MeasureEnqueueAllocations {
		t.Fatal("second instance lost its configuration")
	}
	if first.Tracer() == nil || second.Tracer() == nil {
		t.Fatal("isolated instances returned nil tracer")
	}
	t.Cleanup(func() {
		if err := first.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown first telemetry: %v", err)
		}
		if err := second.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown second telemetry: %v", err)
		}
	})
}

func TestTelemetryTracerLazyInitializationIsConcurrentSafe(t *testing.T) {
	t.Parallel()

	telemetry := NewTelemetry(config.TelemetryConfig{Exporter: ""})
	t.Cleanup(func() { _ = telemetry.Shutdown(context.Background()) })
	const callers = 32
	tracers := make(chan interface{}, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			tracers <- telemetry.Tracer()
		}()
	}
	wg.Wait()
	close(tracers)

	var first reflect.Value
	for tracer := range tracers {
		if tracer == nil {
			t.Fatal("concurrent Tracer call returned nil")
		}
		value := reflect.ValueOf(tracer)
		if !first.IsValid() {
			first = value
			continue
		}
		if !value.Type().Comparable() || !first.Type().Comparable() || value.Interface() != first.Interface() {
			t.Fatal("concurrent Tracer calls returned different tracer instances")
		}
	}
}

func TestTelemetryNilInstanceReturnsNoopTracer(t *testing.T) {
	t.Parallel()

	var telemetry *Telemetry
	tracer := telemetry.Tracer()
	if tracer == nil {
		t.Fatal("nil Telemetry returned nil tracer")
	}
	// The concrete no-op type is intentionally not part of the public
	// contract; starting a span is the stable behavior required here.
	_, span := tracer.Start(context.Background(), "nil-telemetry")
	if span == nil {
		t.Fatal("nil Telemetry tracer did not return a span")
	}
	span.End()
}

func TestGlobalConfigureBeforeTracerContract(t *testing.T) {
	if os.Getenv("VELOX_TELEMETRY_CONTRACT_HELPER") == "1" {
		if err := Configure(config.TelemetryConfig{Exporter: "", Version: "contract"}); err != nil {
			t.Fatalf("Configure before Tracer: %v", err)
		}
		if Tracer() == nil {
			t.Fatal("Tracer returned nil after Configure")
		}
		if err := Configure(config.TelemetryConfig{Version: "late"}); !errors.Is(err, ErrConfigureAfterTracer) {
			t.Fatalf("late Configure error = %v, want %v", err, ErrConfigureAfterTracer)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGlobalConfigureBeforeTracerContract$", "-test.v")
	cmd.Env = append(os.Environ(), "VELOX_TELEMETRY_CONTRACT_HELPER=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated contract subprocess failed: %v\n%s", err, output)
	}
}
