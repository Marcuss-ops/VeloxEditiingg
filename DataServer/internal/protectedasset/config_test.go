// Package protectedasset — ServiceEnv loader test matrix.

package protectedasset

import (
	"strings"
	"testing"
	"time"
)

func TestLoadServiceEnv_DefaultsMatchSpec(t *testing.T) {
	t.Setenv("VELOX_CACHE_LOOKAHEAD_JOBS", "")
	t.Setenv("VELOX_CACHE_SNAPSHOT_INTERVAL", "")

	got, err := LoadServiceEnv()
	if err != nil {
		t.Fatalf("LoadServiceEnv: %v", err)
	}
	if got.LookaheadJobs != DefaultLookahead {
		t.Errorf("LookaheadJobs=%d want %d", got.LookaheadJobs, DefaultLookahead)
	}
	if got.SnapshotInterval != 30*time.Second {
		t.Errorf("SnapshotInterval=%v want 30s", got.SnapshotInterval)
	}
}

func TestLoadServiceEnv_OverrideValid(t *testing.T) {
	t.Setenv("VELOX_CACHE_LOOKAHEAD_JOBS", "25")
	t.Setenv("VELOX_CACHE_SNAPSHOT_INTERVAL", "1m30s")

	got, err := LoadServiceEnv()
	if err != nil {
		t.Fatalf("LoadServiceEnv: %v", err)
	}
	if got.LookaheadJobs != 25 {
		t.Errorf("LookaheadJobs=%d want 25", got.LookaheadJobs)
	}
	if got.SnapshotInterval != 90*time.Second {
		t.Errorf("SnapshotInterval=%v want 1m30s", got.SnapshotInterval)
	}
}

func TestLoadServiceEnv_OverrideInvalidLookahead(t *testing.T) {
	t.Setenv("VELOX_CACHE_LOOKAHEAD_JOBS", "ten")
	t.Setenv("VELOX_CACHE_SNAPSHOT_INTERVAL", "")

	_, err := LoadServiceEnv()
	if err == nil {
		t.Fatalf("LoadServiceEnv: expected error on VELOX_CACHE_LOOKAHEAD_JOBS=ten")
	}
	if !strings.Contains(err.Error(), "VELOX_CACHE_LOOKAHEAD_JOBS") {
		t.Errorf("err=%v; want substring VELOX_CACHE_LOOKAHEAD_JOBS", err)
	}
}

func TestLoadServiceEnv_OverrideInvalidInterval(t *testing.T) {
	t.Setenv("VELOX_CACHE_LOOKAHEAD_JOBS", "")
	t.Setenv("VELOX_CACHE_SNAPSHOT_INTERVAL", "forever")

	_, err := LoadServiceEnv()
	if err == nil {
		t.Fatalf("LoadServiceEnv: expected error on VELOX_CACHE_SNAPSHOT_INTERVAL=forever")
	}
	if !strings.Contains(err.Error(), "VELOX_CACHE_SNAPSHOT_INTERVAL") {
		t.Errorf("err=%v; want substring VELOX_CACHE_SNAPSHOT_INTERVAL", err)
	}
}
