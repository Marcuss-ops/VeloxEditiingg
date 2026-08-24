package grpcserver

import (
	"fmt"
	"testing"

	"velox-shared/controltransport"
)

func TestParseExecutorCapabilitiesReturnsTypedRegistry(t *testing.T) {
	registry, err := parseExecutorCapabilities(map[string]interface{}{
		"schema_version": controltransport.CapabilitySchemaVersion,
		"executors": []interface{}{
			map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
		},
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !registry.Has("scene.composite.v1", 1) {
		t.Fatalf("registry does not contain parsed executor: %+v", registry.All())
	}
}

func TestParseExecutorCapabilitiesRejectsMalformedVersion(t *testing.T) {
	_, err := parseExecutorCapabilities(map[string]interface{}{
		"schema_version": controltransport.CapabilitySchemaVersion,
		"executors": []interface{}{
			map[string]interface{}{"id": "broken", "version": "one"},
		},
	})
	if err == nil {
		t.Fatal("malformed executor version accepted")
	}
}

func TestParseExecutorCapabilitiesMissingBlockIsRejected(t *testing.T) {
	_, err := parseExecutorCapabilities(map[string]interface{}{
		"schema_version": controltransport.CapabilitySchemaVersion,
		controltransport.CapabilityCanonicalPayloadV2: true,
	})
	if err == nil {
		t.Fatal("missing executors array must be rejected")
	}
}

func TestWorkerSessionPlacementSnapshotNeverMixesCapabilityGenerations(t *testing.T) {
	sess := &workerSession{workerID: "generation-worker"}
	initial, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: "generation-0", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	sess.replaceCapabilities(initial, controltransport.CapabilitySet{"generation-0"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 10000; i++ {
			generation := fmt.Sprintf("generation-%d", i)
			registry, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: generation, Version: 1})
			if err != nil {
				return
			}
			sess.replaceCapabilities(registry, controltransport.CapabilitySet{generation})
		}
	}()

	for {
		select {
		case <-done:
			return
		default:
		}
		snapshot := sess.placementSnapshot(sess.workerID)
		executors := snapshot.ExecutorRegistry.All()
		if len(executors) != 1 || len(snapshot.Capabilities) != 1 || executors[0].ID != snapshot.Capabilities[0] {
			t.Fatalf("mixed placement generation: executors=%+v capabilities=%v revision=%d", executors, snapshot.Capabilities, snapshot.CapabilityRevision)
		}
	}
}

func TestWorkerSessionCapabilityRevisionTracksReplacements(t *testing.T) {
	sess := &workerSession{}
	first, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: "first", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: "second", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	sess.replaceCapabilities(first, nil)
	if got := sess.capabilityRevision.Load(); got != 1 {
		t.Fatalf("revision after first replacement = %d, want 1", got)
	}
	sess.replaceExecutorRegistry(second)
	if got := sess.capabilityRevision.Load(); got != 2 {
		t.Fatalf("revision after heartbeat replacement = %d, want 2", got)
	}
	if !sess.executors.Has("second", 1) || sess.executors.Has("first", 1) {
		t.Fatalf("replacement registry = %+v, want only second@1", sess.executors.All())
	}
}
