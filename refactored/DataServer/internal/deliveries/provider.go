// Package deliveries is the delivery provider abstraction layer.
//
// Goal:
//   * Decouple Velox's main flow from Drive / YouTube specifics.
//   * Allow providers to live as long-lived adapters (DriveProvider,
//     YouTubeProvider) or skeleton stubs (S3Provider, LocalExportProvider)
//     that return ErrProviderNotConfigured until a future deployment adds
//     the wiring.
//   * Make DeliveryRunner the single durable entry point for "push this
//     artifact to this destination", so a process restart mid-upload does
//     not lose work.
//
// The Provider contract is intentionally minimal: Name + Deliver.
// Adapters wrap their domain-specific errors into ProviderError so the
// runner can decide retry classification (transient vs permanent) without
// string-matching driver-specific messages.
package deliveries

import (
	"context"
	"errors"

	"velox-server/internal/store"
)

// ErrProviderNotConfigured is the sentinel returned by skeleton providers
// (S3, LocalExport) until environment wiring is added. The runner treats
// this as a permanent failure with status FAILED, no retry.
var ErrProviderNotConfigured = errors.New("deliveries: provider not configured")

// ErrProviderPermanent marks an error as non-retryable. The runner will
// move the delivery into FAILED and stop claiming it.
var ErrProviderPermanent = errors.New("deliveries: permanent error")

// Provider is the single contract every adapter must implement.
// Adapters do NOT touch the database directly: the runner handles claim,
// lease, outbox, and idempotency. The adapter's only job is to push the
// artifact to the destination.
type Provider interface {
	// Name returns the canonical provider identifier (e.g. "drive",
	// "youtube", "s3", "local_export"). Registry keys are case-sensitive.
	Name() string

	// Deliver performs the upload. Implementations must:
	//   * be idempotent under retry — re-running the same (artifact,
	//     destination) MUST NOT cause data duplication
	//   * honor ctx cancellation between byte ranges / chunks so a
	//     runner shutdown is responsive
	//   * return ProviderError (or wrap one) so the runner can classify
	//     the failure. Returning a plain error means the runner treats
	//     it as transient and retries with backoff.
	Deliver(ctx context.Context, artifact *store.Artifact, destination *Destination) (*Result, error)
}

// Destination is the typed view of a delivery_destinations row.
//
// `Configuration` is the JSON blob deserialized into a typed structure;
// adapters can re-marshal it via the embedded raw if they need simple
// field access without a dedicated struct.
type Destination struct {
	DestinationID  string
	Provider       string
	AccountID      string
	FolderID       string
	ChannelID      string
	Language       string
	Name           string
	Enabled        bool
	Configuration  []byte
	ConfigurationJSON string
}

// Result captures the post-upload state. RemoteID/RemoteURL are the
// canonical identifiers the runner persists on job_deliveries so the
// JobViewAssembler can surface them on the legacy view.
type Result struct {
	Success      bool
	RemoteID     string
	RemoteURL    string
	ProviderMeta map[string]interface{}
}

// Registry is the in-process lookup of providers keyed by canonical name.
// The runner resolves destinations through this map.
type Registry struct {
	providers map[string]Provider
}

// NewRegistry builds an empty registry. Use Register to add providers.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds a provider under its Name() into the registry. Re-registration
// overwrites the previous mapping (useful for tests).
func (r *Registry) Register(p Provider) {
	if r == nil || p == nil {
		return
	}
	r.providers[p.Name()] = p
}

// Resolve returns the provider for the registered name, or
// ErrProviderNotConfigured if no provider matches.
func (r *Registry) Resolve(name string) (Provider, error) {
	if r == nil {
		return nil, ErrProviderNotConfigured
	}
	p, ok := r.providers[name]
	if !ok {
		return nil, ErrProviderNotConfigured
	}
	return p, nil
}

// Names returns the names of all registered providers (sorted by insertion
// for stable test snapshots).
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.providers))
	for k := range r.providers {
		out = append(out, k)
	}
	return out
}

// ClassifyError tags an arbitrary error as transient (retryable) or
// permanent (no-retry). The default classification returns the error
// wrapped in ErrProviderPermanent so the runner never silently retries
// forever; adapters that want different treatment must return errors
// that wrap the sentinel.
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrProviderNotConfigured) || errors.Is(err, ErrProviderPermanent) {
		return ErrProviderPermanent
	}
	// Transient: surface verbatim so the runner can apply backoff.
	return err
}
