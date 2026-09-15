package pipeline

import "context"

// CompileCompletedHook is invoked after a pipeline has produced a valid,
// non-empty render plan and immediately before rendering starts. It lets the
// caller close its plan/compile telemetry span at the actual phase boundary;
// the pipeline package does not depend on the worker telemetry package.
type CompileCompletedHook func()

type compileCompletedHookKey struct{}

// WithCompileCompletedHook binds a callback that RunWithMetrics invokes after
// compilation and before the native render begins.
func WithCompileCompletedHook(ctx context.Context, hook CompileCompletedHook) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, compileCompletedHookKey{}, hook)
}

func compileCompletedHookFromContext(ctx context.Context) CompileCompletedHook {
	if ctx == nil {
		return nil
	}
	hook, _ := ctx.Value(compileCompletedHookKey{}).(CompileCompletedHook)
	return hook
}
