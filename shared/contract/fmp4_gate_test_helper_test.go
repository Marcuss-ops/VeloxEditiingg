package contract

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeFMP4GateEvidence records a benchmark-gate artifact for tests that need
// the streaming profile admitted.
func writeFMP4GateEvidence(t *testing.T, jobs, certified int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fmp4-gate.json")
	payload := fmt.Sprintf(`{"version":1,"jobs":%d,"certified_jobs":%d,"recorded_at":"2026-11-08T00:00:00Z"}`, jobs, certified)
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write fMP4 gate evidence: %v", err)
	}
	return path
}
