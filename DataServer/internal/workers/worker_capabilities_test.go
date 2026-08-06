package workers

import (
	"context"
	"testing"
)

func TestRegisterWorkerHydratesReleaseIdentityFromCapabilities(t *testing.T) {
	reg := New(nil)
	err := reg.RegisterWorker(context.Background(), "worker-rel", "worker", "127.0.0.1", map[string]interface{}{
		"capabilities": map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(3)},
			},
			"release_identity": map[string]interface{}{
				"image_digest":      "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
				"source_commit":     "fbbf7c1",
				"source_hash":       "f106b007d8f827ae667dfbe1c1a1a31eff4647a71cf2de53b125d7347dbf51ca",
				"bundle_hash":       "f106b007d8f827ae667dfbe1c1a1a31eff4647a71cf2de53b125d7347dbf51ca",
				"engine_sha256":     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				"software_version":  "v1.2.20",
				"protocol_version":  "v3",
				"capability_schema": float64(1),
			},
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	info := reg.GetWorker(context.Background(), "worker-rel")
	if info == nil {
		t.Fatal("worker not registered")
	}
	ri := info.ReleaseIdentity
	if ri.SourceHash != "f106b007d8f827ae667dfbe1c1a1a31eff4647a71cf2de53b125d7347dbf51ca" {
		t.Errorf("SourceHash = %q, want the certificate value", ri.SourceHash)
	}
	if ri.ProtocolVersion != "v3" {
		t.Errorf("ProtocolVersion = %q, want v3", ri.ProtocolVersion)
	}
	if ri.SoftwareVersion != "v1.2.20" {
		t.Errorf("SoftwareVersion = %q, want v1.2.20", ri.SoftwareVersion)
	}
	if ri.CapabilitySchema != 1 {
		t.Errorf("CapabilitySchema = %d, want 1", ri.CapabilitySchema)
	}
	if ri.EngineSHA256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("EngineSHA256 = %q, want certificate engine hash", ri.EngineSHA256)
	}
}

func TestRegisterWorkerHydratesTypedExecutorRegistryFromLegacyPayload(t *testing.T) {
	reg := New(nil)
	err := reg.RegisterWorker(context.Background(), "worker-typed", "worker", "127.0.0.1", map[string]interface{}{
		"capabilities": map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(3)},
			},
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	info := reg.GetWorker(context.Background(), "worker-typed")
	if info == nil {
		t.Fatal("worker not registered")
	}
	if !info.ExecutorRegistrySnapshot().Has("scene.composite.v1", 3) {
		t.Fatalf("typed registry missing legacy executor: %+v", info.ExecutorRegistrySnapshot().All())
	}
}
