// PR-3.5 — tests for the functional-options constructor pattern
// that surfaces executor.Registry to the buildHello / sendHeartbeat
// paths. The Option pattern itself MUST stay backward-compatible:
// every existing caller of New(cfg, version) keeps working.
package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/config"
)

// regStubExecutor is a minimal Executor used in this file's tests.
// We don't import the executor package's test helpers because
// that package has its own tests under internal/executor.
type regStubExecutor struct {
	descriptor executor.Descriptor
}

func (r *regStubExecutor) Descriptor() executor.Descriptor { return r.descriptor }
func (r *regStubExecutor) Validate(_ executor.TaskSpec) error {
	return r.descriptor.Validate()
}
func (r *regStubExecutor) Execute(_ context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	return executor.ExecutionResult{Status: "succeeded"}, nil
}

func newInsecureDevCfg(t *testing.T) *config.WorkerConfig {
	t.Helper()
	t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "true")
	return &config.WorkerConfig{
		WorkerID:          "tests",
		WorkerName:        "tests",
		WorkDir:           t.TempDir(),
		LogLevel:          "info",
		MasterURL:         "http://localhost:8000",
		ControlGRPCURL:    "localhost:9000",
		AllowInsecureGRPC: true,
	}
}

func TestNew_DefaultRegistryNonNil(t *testing.T) {
	// Backward-compat path: callers that don't pass any options still
	// receive a fully-formed Worker with a non-nil registry. The
	// buildHello path treats nil as empty; supplying a real registry
	// is strictly better than suffering a nil-deref crash before the
	// taskrunner can take over.
	w, err := New(newInsecureDevCfg(t), "test")
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NotNil(t, w.executorRegistry, "default registry must not be nil")
	require.Equal(t, 0, w.executorRegistry.Len(), "default registry must be empty")
}

func TestNew_WithRegistryWiresThrough(t *testing.T) {
	reg := executor.NewRegistry()
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID:            "scene.composite.v1",
		Version:       1,
		ResourceClass: executor.ResourceGPU,
		TemporalMode:  executor.TemporalGlobal,
	}})
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID:            "render.image.v1",
		Version:       3,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalFrameLocal,
	}})

	w, err := New(newInsecureDevCfg(t), "test", WithRegistry(reg))
	require.NoError(t, err)
	require.NotNil(t, w)

	// Pointer identity: the worker must hold the SAME registry, so
	// Register calls after New() are visible to buildHello.
	require.Same(t, reg, w.executorRegistry, "worker must hold the same registry pointer")
	require.Equal(t, 2, w.executorRegistry.Len())

	// Mutating through the original handle must be visible.
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID:            "audio.mix.v1",
		Version:       1,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalWindowed,
	}})
	require.Equal(t, 3, w.executorRegistry.Len(), "mid-run Register must be visible to worker")
}

func TestNew_WithRegistryNilOptionPanics(t *testing.T) {
	// Passing WithRegistry(nil) MUST panic loudly. This is the safety
	// net that prevents the silent-fallback-bug class (worker boots,
	// advertises zero executors, every job routes to dead-letter).
	require.PanicsWithValue(t,
		"worker.WithRegistry: registry must not be nil — pass an explicit *executor.Registry or omit WithRegistry",
		func() { WithRegistry(nil) },
		"WithRegistry(nil) must panic with a stable message")
}

func TestNew_WithoutRegistryUsesEmptyDefault(t *testing.T) {
	// Omitting WithRegistry entirely is the supported way to start with
	// an empty registry — no panic, just an empty default.
	w, err := New(newInsecureDevCfg(t), "test")
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NotNil(t, w.executorRegistry)
	require.Equal(t, 0, w.executorRegistry.Len())
}

func TestNew_WithNilOptionIgnored(t *testing.T) {
	// A nil Option func must not panic; functional options are
	// always user-supplied so a stray nil is recoverable.
	w, err := New(newInsecureDevCfg(t), "test", nil)
	require.NoError(t, err)
	require.NotNil(t, w)
	require.Equal(t, 0, w.executorRegistry.Len())
}
