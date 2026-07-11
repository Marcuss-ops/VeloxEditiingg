package deliveries

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"velox-server/internal/store"
)

func TestClassifyError_Nil(t *testing.T) {
	if got := ClassifyError(nil); got != ErrorClassTransient {
		t.Fatalf("nil error should be transient, got %d", got)
	}
}

func TestClassifyError_Permanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrProviderPermanent", ErrProviderPermanent},
		{"ErrProviderNotConfigured", ErrProviderNotConfigured},
		{"wrapped permanent", fmt.Errorf("outer: %w", ErrProviderPermanent)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != ErrorClassPermanent {
				t.Fatalf("want ErrorClassPermanent, got %d", got)
			}
		})
	}
}

func TestClassifyError_Auth(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrProviderAuth", ErrProviderAuth},
		{"wrapped auth", fmt.Errorf("outer: %w", ErrProviderAuth)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != ErrorClassAuth {
				t.Fatalf("want ErrorClassAuth, got %d", got)
			}
		})
	}
}

func TestClassifyError_RateLimit(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrProviderRateLimit", ErrProviderRateLimit},
		{"wrapped rate limit", fmt.Errorf("outer: %w", ErrProviderRateLimit)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != ErrorClassRateLimit {
				t.Fatalf("want ErrorClassRateLimit, got %d", got)
			}
		})
	}
}

func TestClassifyError_Transient(t *testing.T) {
	err := errors.New("network timeout")
	if got := ClassifyError(err); got != ErrorClassTransient {
		t.Fatalf("plain error should be transient, got %d", got)
	}
}

func TestRegistry_RegisterResolveNames(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}

	// Resolve on empty registry
	if _, err := r.Resolve("drive"); !errors.Is(err, ErrProviderNotConfigured) {
		t.Fatalf("want ErrProviderNotConfigured, got %v", err)
	}

	// Register a provider
	r.Register(&stubProvider{name: "drive"})

	p, err := r.Resolve("drive")
	if err != nil {
		t.Fatalf("Resolve drive: %v", err)
	}
	if p.Name() != "drive" {
		t.Fatalf("want name 'drive', got %q", p.Name())
	}

	// Names
	names := r.Names()
	if len(names) != 1 || names[0] != "drive" {
		t.Fatalf("want [drive], got %v", names)
	}

	// Register another
	r.Register(&stubProvider{name: "youtube"})
	names = r.Names()
	if len(names) != 2 {
		t.Fatalf("want 2 names, got %d", len(names))
	}

	// Re-registration with same name overwrites
	r.Register(&stubProvider{name: "drive"})
	p, _ = r.Resolve("drive")
	if p.Name() != "drive" {
		t.Fatalf("want overwritten name 'drive', got %q", p.Name())
	}

	// Register with different name doesn't overwrite "drive"
	r.Register(&stubProvider{name: "s3"})
	p, _ = r.Resolve("drive")
	if p.Name() != "drive" {
		t.Fatalf("drive should still resolve to original, got %q", p.Name())
	}
}

func TestRegistry_NilReceiver(t *testing.T) {
	var r *Registry
	if _, err := r.Resolve("x"); !errors.Is(err, ErrProviderNotConfigured) {
		t.Fatalf("nil registry resolve should return ErrProviderNotConfigured, got %v", err)
	}
	if names := r.Names(); names != nil {
		t.Fatalf("nil registry Names should return nil, got %v", names)
	}
	r.Register(nil) // should not panic
}

func TestProviderError_Unwrap(t *testing.T) {
	cause := errors.New("root cause")
	pe := &ProviderError{
		Class:   ErrorClassPermanent,
		Message: "upload failed",
		Cause:   cause,
	}
	if pe.Error() != "root cause" {
		t.Fatalf("want cause error string, got %q", pe.Error())
	}
	if !errors.Is(pe, cause) {
		t.Fatal("should unwrap to cause")
	}
}

func TestProviderError_NoCause(t *testing.T) {
	pe := &ProviderError{
		Class:   ErrorClassTransient,
		Message: "upload failed",
	}
	if pe.Error() != "upload failed" {
		t.Fatalf("want message string, got %q", pe.Error())
	}
	if pe.Unwrap() != nil {
		t.Fatal("Unwrap should return nil when no cause")
	}
}

// backoffForAttempt tests
func TestRunnerConfig_BackoffForAttempt(t *testing.T) {
	cfg := DefaultRunnerConfig()

	tests := []struct {
		attempt int
		want    int // expected seconds approximately
	}{
		{1, 30},
		{2, 120},
		{3, 600},
		{4, 1800},
		{5, 1800}, // clamped to last entry
		{100, 1800},
	}

	for _, tt := range tests {
		d := cfg.backoffForAttempt(tt.attempt)
		if d <= 0 {
			t.Fatalf("attempt %d: want positive backoff, got %v", tt.attempt, d)
		}
	}
}

func TestDefaultRunnerConfig(t *testing.T) {
	cfg := DefaultRunnerConfig()
	if cfg.PollInterval <= 0 {
		t.Fatal("PollInterval should be positive")
	}
	if cfg.LeaseDuration <= 0 {
		t.Fatal("LeaseDuration should be positive")
	}
	if cfg.MaxAttempts <= 0 {
		t.Fatal("MaxAttempts should be positive")
	}
	if cfg.ClaimBatch <= 0 {
		t.Fatal("ClaimBatch should be positive")
	}
	if len(cfg.BackoffSchedule) == 0 {
		t.Fatal("BackoffSchedule should not be empty")
	}
}

func TestNewDeliveryRunner_Defaults(t *testing.T) {
	r := NewDeliveryRunner(nil, nil, nil, "")
	if r == nil {
		t.Fatal("NewDeliveryRunner returned nil")
	}
	if r.cfg == nil {
		t.Fatal("cfg should be set to defaults")
	}
	if r.registry == nil {
		t.Fatal("registry should be set to empty")
	}
	if r.identity == "" {
		t.Fatal("identity should be auto-generated")
	}
}

func TestDeliveryRunner_Stop(t *testing.T) {
	r := NewDeliveryRunner(nil, nil, nil, "test-runner")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)
	r.Stop()
	// Should not block or panic
}

// stubProvider is a minimal Provider for tests.
type stubProvider struct {
	name string
}

func (s *stubProvider) Name() string { return s.name }
func (s *stubProvider) Deliver(_ context.Context, _ *store.Artifact, _ *Destination, _, _ string) (*Result, error) {
	return &Result{Success: true}, nil
}
