// Package transport — fallback transport that tries gRPC first, then HTTP polling.
package transport

import (
	"context"
	"fmt"
	"sync"

	"velox-shared/controltransport"
	"velox-worker-agent/pkg/logger"
)

// FallbackTransport implements ControlTransport by trying a primary transport
// (gRPC stream) first. If Connect() fails, it transparently falls back to a
// secondary transport (HTTP polling). Once connected, all operations are
// delegated to the active transport.
//
// This enables a zero-downtime migration from HTTP polling to gRPC streaming:
//   - Workers configured for gRPC still work even if the gRPC endpoint is down
//   - No worker restart needed — the fallback happens at connection time
//   - The next re-registration cycle will retry gRPC
type FallbackTransport struct {
	primary   controltransport.ControlTransport
	secondary controltransport.ControlTransport
	active    controltransport.ControlTransport
	mu        sync.Mutex
	connected bool
	log       *logger.Logger
}

// NewFallbackTransport creates a transport that tries primary first,
// falling back to secondary on connection failure.
func NewFallbackTransport(primary, secondary controltransport.ControlTransport, log *logger.Logger) *FallbackTransport {
	return &FallbackTransport{
		primary:   primary,
		secondary: secondary,
		log:       log,
	}
}

// Connect tries primary transport first. On failure, falls back to secondary.
func (f *FallbackTransport) Connect(ctx context.Context, hello controltransport.WorkerHello) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Try primary (gRPC) first
	if err := f.primary.Connect(ctx, hello); err != nil {
		f.log.Warn("[TRANSPORT] gRPC connection failed, falling back to HTTP polling: %v", err)

		// Fall back to secondary (HTTP polling)
		if err2 := f.secondary.Connect(ctx, hello); err2 != nil {
			return fmt.Errorf("fallback transport: both primary and secondary failed: primary=%w, secondary=%v", err, err2)
		}

		f.active = f.secondary
		f.connected = true
		f.log.Info("[TRANSPORT] Connected via HTTP polling (fallback mode)")
		return nil
	}

	f.active = f.primary
	f.connected = true
	f.log.Info("[TRANSPORT] Connected via gRPC stream")
	return nil
}

// Receive delegates to the active transport.
func (f *FallbackTransport) Receive(ctx context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.connected || f.active == nil {
		return nil, nil, controltransport.ErrNotConnected
	}
	return f.active.Receive(ctx)
}

// Send delegates to the active transport.
func (f *FallbackTransport) Send(ctx context.Context, msg controltransport.ControlMessage) error {
	f.mu.Lock()
	active := f.active
	f.mu.Unlock()

	if active == nil {
		return controltransport.ErrNotConnected
	}
	return active.Send(ctx, msg)
}

// Close closes the active transport.
func (f *FallbackTransport) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.connected = false

	var errs []error
	if f.primary != nil {
		if err := f.primary.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if f.secondary != nil {
		if err := f.secondary.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	f.active = nil

	if len(errs) > 0 {
		return fmt.Errorf("fallback close errors: %v", errs)
	}
	return nil
}
