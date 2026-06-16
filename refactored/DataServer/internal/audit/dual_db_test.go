package audit

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func touchFile(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("SQLite dummy"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

func TestCheckDualDatabase_NoSources(t *testing.T) {
	tmp := t.TempDir()
	runtimePath := filepath.Join(tmp, "velox.db")
	touchFile(t, runtimePath, time.Now())

	rep := CheckDualDatabase(tmp, runtimePath, 24*time.Hour)
	if rep.SamePathHit {
		t.Fatalf("expected SamePathHit=false on clean run, got true")
	}
	if rep.SourceNewerThanRuntime {
		t.Fatalf("expected SourceNewerThanRuntime=false on clean run, got true")
	}
	if len(rep.Warnings) != 0 {
		t.Fatalf("expected no warnings on clean run, got %v", rep.Warnings)
	}
	for _, s := range rep.Sources {
		if s.Exists {
			t.Fatalf("expected all sources missing, got exists=%v for %s", s.Exists, s.Path)
		}
	}
}

func TestCheckDualDatabase_SamePath(t *testing.T) {
	tmp := t.TempDir()
	// Runtime DB happens to live at <tmp>/data/velox.db, which equals one
	// of the well-known source candidates.
	runtimePath := filepath.Join(tmp, "data", "velox.db")
	touchFile(t, runtimePath, time.Now())

	rep := CheckDualDatabase(tmp, runtimePath, 24*time.Hour)
	if !rep.SamePathHit {
		t.Fatalf("expected SamePathHit=true when runtime == candidate, got false; sources=%+v", rep.Sources)
	}
	if len(rep.Warnings) == 0 {
		t.Fatalf("expected warning for same-path hit")
	}
}

func TestCheckDualDatabase_RuntimeOlderThanSource(t *testing.T) {
	tmp := t.TempDir()
	runtimePath := filepath.Join(tmp, "velox.db")
	sourcePath := filepath.Join(tmp, "..", "data", "velox.db")
	now := time.Now()
	touchFile(t, runtimePath, now.Add(-72*time.Hour))
	touchFile(t, sourcePath, now)

	rep := CheckDualDatabase(tmp, runtimePath, 24*time.Hour)
	if !rep.SourceNewerThanRuntime {
		t.Fatalf("expected SourceNewerThanRuntime=true, got false; warnings=%v", rep.Warnings)
	}
	if rep.LagHours < 71 || rep.LagHours > 73 {
		t.Fatalf("expected LagHours around 72, got %v", rep.LagHours)
	}
}

func TestCheckDualDatabase_WithinThresholdIsSilent(t *testing.T) {
	tmp := t.TempDir()
	runtimePath := filepath.Join(tmp, "velox.db")
	sourcePath := filepath.Join(tmp, "..", "data", "velox.db")
	now := time.Now()
	touchFile(t, runtimePath, now.Add(-2*time.Hour))
	touchFile(t, sourcePath, now)

	rep := CheckDualDatabase(tmp, runtimePath, 24*time.Hour)
	if rep.SourceNewerThanRuntime {
		t.Fatalf("expected SourceNewerThanRuntime=false for 2h lag, got true")
	}
	if len(rep.Warnings) != 0 {
		t.Fatalf("expected no warnings within threshold, got %v", rep.Warnings)
	}
}

func TestCheckDualDatabase_ThresholdZeroDisables(t *testing.T) {
	tmp := t.TempDir()
	runtimePath := filepath.Join(tmp, "velox.db")
	sourcePath := filepath.Join(tmp, "..", "data", "velox.db")
	now := time.Now()
	touchFile(t, runtimePath, now.Add(-1000*time.Hour))
	touchFile(t, sourcePath, now)

	rep := CheckDualDatabase(tmp, runtimePath, 0)
	if rep.SourceNewerThanRuntime {
		t.Fatalf("threshold=0 should disable staleness check, got SourceNewerThanRuntime=true")
	}
}

func TestCheckDualDatabase_MissingRuntimeSilent(t *testing.T) {
	tmp := t.TempDir()
	runtimePath := filepath.Join(tmp, "velox.db") // never created
	rep := CheckDualDatabase(tmp, runtimePath, 24*time.Hour)
	if rep.SourceNewerThanRuntime || rep.SamePathHit {
		t.Fatalf("missing runtime should not warn, got %+v", rep)
	}
}

func TestCheckDualDatabase_EmptyDataDir(t *testing.T) {
	runtimePath := "/var/lib/velox/velox.db"
	rep := CheckDualDatabase("", runtimePath, 24*time.Hour)
	if len(rep.Sources) != 0 {
		t.Fatalf("empty dataDir should yield no candidates, got %+v", rep.Sources)
	}
}
