// PR-3.5 — asserts buildHello's `executors` sub-key comes EXACTLY
// from Registry.Descriptors() sorted by (ID, Version); the master-side
// CapabilitySchemaVersion=1 decoder depends on this ordering.
//
// REVISION HISTORY
//
//	v1: initial hello-matches-registry tests + supported_job_types
//	    compat shim.
//	v2 (post-review): dropped supported_job_types mirror; extracted
//	    Worker.capabilitiesMap() as the single source of truth for
//	    hello + heartbeat; top-level max_parallel_jobs mirrors
//	    host.max_parallel_jobs (sourced from same limiter field).
//	v3 (FINAL): added missing pkg/api import; dropped redundant
//	    byte-stable-key-order test (covered by
//	    TestBuildHello_DeterministicAcrossRegistrations); simplified
//	    the dead-code sweep.
package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/api"
)

func assertHelloMatchesRegistry(t *testing.T, w *Worker) {
	t.Helper()
	hello := w.buildHello()
	req := require.New(t)

	// schema_version key MUST be present and equal to the closed constant.
	sv, ok := hello.Capabilities["schema_version"]
	req.True(ok, "missing schema_version key")
	req.EqualValues(api.CapabilitySchemaVersion, sv, "schema_version must equal api.CapabilitySchemaVersion")

	// executors list must contain exactly what Descriptors() reports.
	execsRaw, ok := hello.Capabilities["executors"]
	req.True(ok, "missing executors key")
	execs, ok := execsRaw.([]interface{})
	req.True(ok, "executors must be a []interface{}")
	descs := w.executorRegistry.Descriptors()
	req.Equal(len(descs), len(execs), "executors length must match Descriptors() length")

	for i, d := range descs {
		got, ok := execs[i].(map[string]interface{})
		req.True(ok, "executor at index %d not a map", i)
		req.Equal(d.ID, got["id"], "executor ID at index %d", i)
		req.EqualValues(d.Version, got["version"], "executor version at index %d", i)
		req.Equal(string(d.ResourceClass), got["resource_class"], "executor resource_class at index %d", i)
		req.Equal(string(d.TemporalMode), got["temporal_mode"], "executor temporal_mode at index %d", i)
		req.Equal(d.Deterministic, got["deterministic"], "deterministic at index %d", i)
		req.Equal(d.Cacheable, got["cacheable"], "cacheable at index %d", i)
		req.Equal(d.SupportsAlpha, got["supports_alpha"], "supports_alpha at index %d", i)
	}

	// max_parallel_jobs MUST come from the limiter (single source of
	// truth) and MUST be byte-identical to host.max_parallel_jobs so a
	// master reading either location sees the same value.
	topMax, ok := hello.Capabilities["max_parallel_jobs"].(int)
	req.True(ok, "max_parallel_jobs must be int")
	hostBlock, ok := hello.Capabilities["host"].(map[string]interface{})
	req.True(ok, "host block must be a map")
	hostMax, ok := hostBlock["max_parallel_jobs"].(int)
	req.True(ok, "host.max_parallel_jobs must be int")
	req.Equal(topMax, hostMax, "max_parallel_jobs and host.max_parallel_jobs must agree")
	req.Equal(w.concurrencyLimiter.MaxActiveJobs(), topMax, "max_parallel_jobs must come from the limiter")

	// supported_job_types is GONE in PR-3.5. Hard cutover — master
	// decoders will see the new schema and must adapt.
	_, hasLegacy := hello.Capabilities["supported_job_types"]
	req.False(hasLegacy, "supported_job_types must be removed (PR-3.5 hard cutover)")

	// Legacy boolean keys are GONE.
	for _, banned := range []string{
		"render_scene_image",
		"render_clip_stock",
		"upload_drive",
		"ffmpeg",
		"cpp_engine",
	} {
		req.NotContains(hello.Capabilities, banned, "legacy boolean %q must be removed in PR-3.5", banned)
	}
}

func TestBuildHello_MatchesRegistryContents(t *testing.T) {
	reg := executor.NewRegistry()
	// Deliberately unsorted registration order — sort responsibility
	// belongs to registry.Descriptors(), NOT to buildHello.
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID: "zzz.last", Version: 1, ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalFrameLocal,
	}})
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID: "aaa.first", Version: 2, ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalFrameLocal,
	}})
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID: "aaa.first", Version: 1, ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalFrameLocal,
	}})
	reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
		ID: "middle.one", Version: 9, ResourceClass: executor.ResourceGPU, TemporalMode: executor.TemporalGlobal,
	}})

	w, err := New(newInsecureDevCfg(t), "test", WithRegistry(reg))
	require.NoError(t, err)
	assertHelloMatchesRegistry(t, w)

	// Sanity-check the expected order: aaa.first@1, aaa.first@2, middle.one@9, zzz.last@1
	execs, _ := w.buildHello().Capabilities["executors"].([]interface{})
	require.Len(t, execs, 4)
	require.Equal(t, "aaa.first", execs[0].(map[string]interface{})["id"])
	require.EqualValues(t, 1, execs[0].(map[string]interface{})["version"])
	require.Equal(t, "aaa.first", execs[1].(map[string]interface{})["id"])
	require.EqualValues(t, 2, execs[1].(map[string]interface{})["version"])
	require.Equal(t, "middle.one", execs[2].(map[string]interface{})["id"])
	require.EqualValues(t, 9, execs[2].(map[string]interface{})["version"])
	require.Equal(t, "zzz.last", execs[3].(map[string]interface{})["id"])
}

func TestBuildHello_EmptyRegistryExecutesClean(t *testing.T) {
	// Empty registry must produce a valid hello with empty arrays —
	// not nil, not missing keys. Encoding the payload must not panic
	// or omit the executors key (master would default to "missing = false").
	w, err := New(newInsecureDevCfg(t), "test")
	require.NoError(t, err)

	hello := w.buildHello()
	execs, ok := hello.Capabilities["executors"]
	require.True(t, ok)
	require.Equal(t, []interface{}{}, execs)

	// JSON encoding must succeed and produce a deterministic byte stream.
	a, err := json.Marshal(hello.Capabilities)
	require.NoError(t, err)
	b, err := json.Marshal(hello.Capabilities)
	require.NoError(t, err)
	require.Equal(t, string(a), string(b), "hello must be byte-stable for empty registries")
}

func TestBuildHello_DeterministicAcrossRegistrations(t *testing.T) {
	// Two workers built with the SAME registry contents produce
	// byte-identical hello maps. This is the "stable hello across
	// boots" invariant.
	buildSame := func() map[string]interface{} {
		reg := executor.NewRegistry()
		reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
			ID: "scene.composite.v1", Version: 1,
			ResourceClass: executor.ResourceGPU, TemporalMode: executor.TemporalGlobal,
			Deterministic: true, Cacheable: true,
		}})
		reg.MustRegister(&regStubExecutor{descriptor: executor.Descriptor{
			ID: "render.image.v1", Version: 3,
			ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalFrameLocal,
		}})
		w, err := New(newInsecureDevCfg(t), "test", WithRegistry(reg))
		require.NoError(t, err)
		return w.buildHello().Capabilities
	}
	a := buildSame()
	b := buildSame()
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	hash := func(s string) string {
		h := sha256.Sum256([]byte(s))
		return hex.EncodeToString(h[:])
	}
	require.Equal(t, hash(string(ab)), hash(string(bb)), "hello JSON must be byte-identical across boots")
}

func TestBuildHello_UsesLimiterNotDetectForMaxParallel(t *testing.T) {
	// Hello reads from w.concurrencyLimiter.MaxActiveJobs(); a
	// ConfigurationUpdate immediately changes what hello would emit
	// on the NEXT attempt. The buildHello path MUST NOT call
	// detectMaxParallelJobs (which reads NumCPU directly).
	w, err := New(newInsecureDevCfg(t), "test")
	require.NoError(t, err)
	w.concurrencyLimiter.SetMaxActiveJobs(7)
	hello := w.buildHello()
	require.EqualValues(t, 7, hello.Capabilities["max_parallel_jobs"])
	require.EqualValues(t, 7, hello.Capabilities["host"].(map[string]interface{})["max_parallel_jobs"])
}
