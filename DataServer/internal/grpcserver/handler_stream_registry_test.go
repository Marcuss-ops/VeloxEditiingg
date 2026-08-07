package grpcserver

import (
	"context"
	"testing"

	workersreg "velox-server/internal/workers"

	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
)

// TestRegisterHelloCapabilitiesInRegistry_PopulatesDeclaredMaxSlots is the
// regression test for the task_slots=0 read-model bug: the gRPC hello path
// stored the worker capability report on the control session and in the
// runtime snapshot DB but never forwarded it into the in-memory registry.
// Without the bridge, GET /api/v1/workers surfaced task_slots=0 even though
// the worker advertised capabilities.host.max_parallel_jobs=1. This test
// locks the bridge: after the hello-forwarding helper runs, the registry
// read model must expose DeclaredMaxSlots == the advertised value.
func TestRegisterHelloCapabilitiesInRegistry_PopulatesDeclaredMaxSlots(t *testing.T) {
	ctx := context.Background()
	reg := workersreg.New(nil) // in-memory registry, no SQLite
	h := NewHandler(reg, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})

	capsStruct, err := structpb.NewStruct(map[string]any{
		"host": map[string]any{"max_parallel_jobs": 2},
		"executors": []any{
			map[string]any{"id": "scene.composite.v1", "version": 1},
		},
	})
	if err != nil {
		t.Fatalf("build hello capabilities: %v", err)
	}

	h.registerHelloCapabilitiesInRegistry(ctx, "worker-bridge-test", "worker-bridge", "10.0.0.9", "v3",
		capsStruct.AsMap(), &pb.Hello{
			Version:       "abc123",
			BundleVersion: "v1.0.6",
			EngineVersion: "engine-1",
			Capabilities:  capsStruct,
			WorkerName:    "worker-bridge",
		})

	info := reg.GetWorker(ctx, "worker-bridge-test")
	if info == nil {
		t.Fatal("worker not registered in in-memory registry after hello bridge")
	}
	if info.DeclaredMaxSlots != 2 {
		t.Fatalf("DeclaredMaxSlots = %d, want 2 (advertised host.max_parallel_jobs)", info.DeclaredMaxSlots)
	}
	if info.ExecutorCapabilities.IsEmpty() {
		t.Fatal("executor capabilities not decoded into the registry read model")
	}
	if info.CodeVersion != "abc123" {
		t.Fatalf("CodeVersion = %q, want abc123", info.CodeVersion)
	}
}

// TestRegisterHelloCapabilitiesInRegistry_EmptyReportIsNoop ensures the
// bridge degrades safely when the hello carries no capability report: it
// must not panic and must not fabricate capacity.
func TestRegisterHelloCapabilitiesInRegistry_EmptyReportIsNoop(t *testing.T) {
	ctx := context.Background()
	reg := workersreg.New(nil)
	h := NewHandler(reg, nil, nil, nil, nil, nil, nil, &HandlerConfig{PushMode: true})

	h.registerHelloCapabilitiesInRegistry(ctx, "worker-empty-caps", "worker-empty", "10.0.0.10", "v3",
		map[string]interface{}{}, &pb.Hello{})

	if info := reg.GetWorker(ctx, "worker-empty-caps"); info != nil {
		t.Fatalf("worker registered with empty capabilities; want no registration: %+v", info)
	}
}
