package runtimeassets

import (
	"context"

	"velox-worker-agent/internal/downloader"
)

// Binding is a worker-local materialization of one canonical asset identity.
// Paths never belong in CompiledRenderPlanV2; they exist only for execution.
type Binding struct {
	AssetID string
	// CacheKey is the exact asset identity registered by the common resolver
	// in the worker cache. It can differ from AssetID when a Drive locator is
	// resolved from a plan identity.
	CacheKey string
	Path     string
	SHA256   string
	Size     int64
	// Verified means the binding's declared identity was checked before
	// projection. For a full binding this is a complete size/SHA-256 check;
	// for Sparse it only means the size and each materialized range were
	// checked, as detailed by VerifiedRanges. Executors still stat the file,
	// but do not reread a warm full cache blob solely to repeat the same scan.
	Verified bool
	// Origin records how this asset was materialized: warm_cache, prefetch,
	// or runtime_download. Propagated from PreparedAssetMetadata by the
	// fast-assembly binding path so certification tests can assert the
	// resolution origin without re-deriving it.
	Origin downloader.ResolutionOrigin
	// Sparse marks a source materialized by media-aware HTTP Range reads. Such
	// a file has the declared size and the container index plus the required
	// sample ranges, but its holes are not real source bytes: SHA256 is the
	// immutable identity declared by the plan, not a full-file digest
	// recomputed on this host. Executors must never emit a full-file hash for
	// a Sparse binding.
	Sparse bool
	// VerifiedRanges means every materialized byte was checked against block
	// hashes bound to the full-source certificate. Holes remain unverified bytes.
	VerifiedRanges bool
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
