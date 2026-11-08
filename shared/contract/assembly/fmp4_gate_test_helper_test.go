package assembly

import (
	"os"
	"path/filepath"
	"testing"
)

func enableFMP4GateForTest(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fmp4-gate.json")
	payload := `{"version":1,"jobs":100,"certified_jobs":100,"recorded_at":"2026-11-08T00:00:00Z"}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write fMP4 gate evidence: %v", err)
	}
	t.Setenv("VELOX_FMP4_GATE_EVIDENCE", path)
}
