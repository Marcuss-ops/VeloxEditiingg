package config

import (
	"os"
	"testing"
	"time"
)

func TestRawConfigTypedParsingAndDefaults(t *testing.T) {
	raw := NewRawConfig(map[string]string{
		"INT":      " 7 ",
		"BOOL":     "yes",
		"DURATION": "250ms",
		"FLOAT":    "1.25",
		"INVALID":  "not-a-number",
	})
	if got := raw.Int("INT", 1, 1); got != 7 {
		t.Fatalf("Int = %d, want 7", got)
	}
	if !raw.Bool("BOOL", false) {
		t.Fatal("Bool did not parse yes")
	}
	if got := raw.Duration("DURATION", time.Second); got != 250*time.Millisecond {
		t.Fatalf("Duration = %s, want 250ms", got)
	}
	if got := raw.Float("FLOAT", 2, 0); got != 1.25 {
		t.Fatalf("Float = %v, want 1.25", got)
	}
	if got := raw.Int("INVALID", 9, 1); got != 9 {
		t.Fatalf("invalid Int = %d, want fallback 9", got)
	}
	if got := raw.Duration("MISSING", 3*time.Second); got != 3*time.Second {
		t.Fatalf("missing Duration = %s, want fallback", got)
	}
}

func TestRawConfigSnapshotIsIsolatedFromInputMutation(t *testing.T) {
	values := map[string]string{"VELOX_MASTER_URL": "https://master.example"}
	raw := NewRawConfig(values)
	values["VELOX_MASTER_URL"] = "https://mutated.example"
	if got := raw.Get("VELOX_MASTER_URL"); got != "https://master.example" {
		t.Fatalf("raw snapshot changed after source mutation: %q", got)
	}
	cfg := FromRaw(NewRawConfig(map[string]string{
		"VELOX_DB_PATH":         "/tmp/velox.db",
		"VELOX_GRPC_PORT":       "50051",
		"VELOX_ALLOWED_WORKERS": "worker-a,worker-b",
		"VELOX_MASTER_URL":      "https://master.example",
	}))
	if string(cfg.ControlPlane.RESTPublic) != "https://master.example" {
		t.Fatalf("typed Config lost public endpoint: %q", cfg.ControlPlane.RESTPublic)
	}
}

func TestRawConfigSourceIsCapturedInSnapshot(t *testing.T) {
	raw := NewRawConfig(map[string]string{"VELOX_TASKGRAPH_TICK": "7s"})
	if got := raw.Source("VELOX_TASKGRAPH_TICK"); got != SourceEnv {
		t.Fatalf("explicit raw source = %q, want %q", got, SourceEnv)
	}
	if got := raw.Source("MISSING"); got != SourceDefault {
		t.Fatalf("missing raw source = %q, want %q", got, SourceDefault)
	}
}

func TestRawConfigFromEnvFileCapturesSourceWithoutGlobalState(t *testing.T) {
	key := "VELOX_RAW_CONFIG_SOURCE_TEST"
	oldValue, wasSet := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(key, oldValue)
		} else {
			_ = os.Unsetenv(key)
		}
	})
	path := t.TempDir() + "/runtime.env"
	if err := os.WriteFile(path, []byte(key+"=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := RawConfigFromEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := raw.Get(key); got != "from-file" {
		t.Fatalf("raw value = %q, want from-file", got)
	}
	if got := raw.Source(key); got != SourceFile {
		t.Fatalf("raw source = %q, want %q", got, SourceFile)
	}
	// A subsequent environment snapshot must not mutate the first snapshot's
	// provenance, even though the process environment now contains the key.
	next := RawConfigFromEnv()
	if got := raw.Source(key); got != SourceFile {
		t.Fatalf("first snapshot source changed after next load: %q", got)
	}
	if got := next.Source(key); got != SourceEnv {
		t.Fatalf("next snapshot source = %q, want %q", got, SourceEnv)
	}
}
