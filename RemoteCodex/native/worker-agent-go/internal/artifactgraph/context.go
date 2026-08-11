package artifactgraph

import "context"

type graphContextKey struct{}

// WithGraph returns a child context carrying the attempt's artifact graph.
// The dispatch path (internal/worker/task_dispatch.go) injects the per-attempt
// graph here so executors can consume it via GraphFromContext — same pattern
// as telemetry.WithAttemptEventMachine. nil-safe.
func WithGraph(ctx context.Context, g *Graph) context.Context {
	if ctx == nil || g == nil {
		return ctx
	}
	return context.WithValue(ctx, graphContextKey{}, g)
}

// GraphFromContext returns the attempt's artifact graph, or nil when absent.
func GraphFromContext(ctx context.Context) *Graph {
	if ctx == nil {
		return nil
	}
	g, _ := ctx.Value(graphContextKey{}).(*Graph)
	return g
}
