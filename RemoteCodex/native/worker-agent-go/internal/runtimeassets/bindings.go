package runtimeassets

import "context"

// Binding is a worker-local materialization of one canonical asset identity.
// Paths never belong in CompiledRenderPlanV2; they exist only for execution.
type Binding struct {
	AssetID string
	Path    string
	SHA256  string
	Size    int64
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
