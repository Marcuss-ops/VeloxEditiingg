package grpcserver

import (
	"testing"

	"velox-shared/controltransport"
)

func TestParseExecutorCapabilitiesReturnsTypedRegistry(t *testing.T) {
	registry, err := parseExecutorCapabilities(map[string]interface{}{
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
		"executors": []interface{}{
			map[string]interface{}{"id": "broken", "version": "one"},
		},
	})
	if err == nil {
		t.Fatal("malformed executor version accepted")
	}
}

func TestParseExecutorCapabilitiesMissingBlockIsEmpty(t *testing.T) {
	registry, err := parseExecutorCapabilities(map[string]interface{}{
		controltransport.CapabilityCanonicalPayloadV2: true,
	})
	if err != nil {
		t.Fatalf("parse missing block: %v", err)
	}
	if !registry.IsEmpty() {
		t.Fatalf("missing executors should produce empty registry: %+v", registry.All())
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
