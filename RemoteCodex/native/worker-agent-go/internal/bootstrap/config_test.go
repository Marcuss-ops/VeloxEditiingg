package bootstrap

import "testing"

func TestResolveEngineVersionIgnoresPersistedConfigValue(t *testing.T) {
	t.Setenv("VELOX_ENGINE_VERSION", "")
	if got := resolveEngineVersion("v1.2.29-canonical"); got != "v1.2.29-canonical" {
		t.Fatalf("resolveEngineVersion() = %q, want build version", got)
	}
}

func TestResolveEngineVersionHonorsExplicitRuntimeEngineVersion(t *testing.T) {
	t.Setenv("VELOX_ENGINE_VERSION", "v1.2.30-native")
	if got := resolveEngineVersion("v1.2.29-canonical"); got != "v1.2.30-native" {
		t.Fatalf("resolveEngineVersion() = %q, want explicit runtime version", got)
	}
}
