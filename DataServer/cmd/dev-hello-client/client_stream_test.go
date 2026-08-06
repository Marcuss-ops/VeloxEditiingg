package main

import (
	"testing"

	"velox-shared/controltransport"
)

func TestBuildCapabilitiesCanonicalReport(t *testing.T) {
	caps := buildCapabilities("scene.composite.v1").AsMap()

	if got := caps["schema_version"]; got != float64(controltransport.CapabilitySchemaVersion) {
		t.Fatalf("schema_version = %v, want %d", got, controltransport.CapabilitySchemaVersion)
	}
	if got := caps[controltransport.CapabilityCanonicalPayloadV2]; got != true {
		t.Fatalf("canonical payload capability = %v, want true", got)
	}

	executors, ok := caps["executors"].([]interface{})
	if !ok || len(executors) != 1 {
		t.Fatalf("executors = %#v, want one executor object", caps["executors"])
	}
	executor, ok := executors[0].(map[string]interface{})
	if !ok {
		t.Fatalf("executor[0] = %#v, want object", executors[0])
	}
	if executor["id"] != "scene.composite.v1" || executor["version"] != float64(1) {
		t.Fatalf("executor[0] = %#v, want scene.composite.v1@1", executor)
	}
}

func TestBuildHeartbeatExtraNestsCanonicalReport(t *testing.T) {
	extra := buildHeartbeatExtra("scene.composite.v1").AsMap()
	caps, ok := extra["capabilities"].(map[string]interface{})
	if !ok {
		t.Fatalf("heartbeat extra capabilities = %#v, want nested object", extra["capabilities"])
	}
	host, ok := caps["host"].(map[string]interface{})
	if !ok {
		t.Fatalf("nested host capabilities = %#v, want object", caps["host"])
	}
	if got := host["max_parallel_jobs"]; got != float64(1) {
		t.Fatalf("nested max_parallel_jobs = %v, want 1", got)
	}
}
