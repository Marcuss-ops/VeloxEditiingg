package runtimeassets

import (
	"context"

	"velox-worker-agent/internal/downloader"
)

// Binding is a worker-local materialization of one canonical asset identity.
// Paths never belong in CompiledRenderPlanV2; they exist only for execution.
type Binding struct {
	AssetID string
	Path    string
	SHA256  string
	Size    int64
	// Verified is true only when the canonical worker asset resolver has
	// already completed the size/SHA-256 verification before projecting the
	// binding. Executors still stat the file, but do not reread a warm cache
	// blob solely to repeat the same integrity scan for every job.
	Verified bool
	// Origin records how this asset was materialized: warm_cache, prefetch,
	// or runtime_download. Propagated from PreparedAssetMetadata by the
	// fast-assembly binding path so certification tests can assert the
	// resolution origin without re-deriving it.
	Origin downloader.ResolutionOrigin
}

// Bindings maps canonical asset IDs to verified local files.
type Bindings map[string]Binding

type bindingsContextKey struct{}

// WithBindings attaches a defensive copy of runtime asset bindings to ctx.
func WithBindings(ctx context.Context, bindings Bindings) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, bindingsContextKey{}, clone(bindings))
}

// FromContext returns a defensive copy of bindings carried by ctx.
func FromContext(ctx context.Context) (Bindings, bool) {
	if ctx == nil {
		return nil, false
	}
	bindings, ok := ctx.Value(bindingsContextKey{}).(Bindings)
	if !ok || bindings == nil {
		return nil, false
	}
	return clone(bindings), true
}

func clone(bindings Bindings) Bindings {
	if bindings == nil {
		return nil
	}
	out := make(Bindings, len(bindings))
	for id, binding := range bindings {
		out[id] = binding
	}
	return out
}
