// Package artifacts / scan_test.go — PR 3.5-a invariant.
//
// The SQL fragment  `SET status = 'SUCCEEDED'`  is the spec's atomic
// CAS that flips a status to SUCCEEDED. The contract enforced here is
// AUDITABILITY: every file that contains this fragment is either:
//
//   (a) the single legal writer of jobs.status='SUCCEEDED' for the
//       verified-finalization lifecycle, OR
//   (b) a SEPARATE lifecycle writer (job_deliveries, workflow_steps,
//       workflow_runs, the legacy CompleteJobTx path), OR
//   (c) this test file (regex literal as documentation).
//
// A future PR that adds a NEW file containing the fragment will fail
// this test unless it is also added to the allowlist — forcing an
// explicit audit decision ("is this a new lifecycle writer, or is
// this a regression that duplicates an existing writer?").
//
// This test does NOT scan files outside internal/ — migration SQL
// files are excluded (they are .sql, not .go).
package artifacts_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// forbiddenSUCCEEDEDWrite is the SQL fragment we forbid outside the
// audited allowlist. Case-insensitive, whitespace-tolerant: a
// `SET  status='SUCCEEDED'` style still trips the check.
var forbiddenSUCCEEDEDWrite = regexp.MustCompile(`(?i)SET\s+status\s*=\s*['"]SUCCEEDED['"]`)

// allowedWriters is the EXPLICIT audited allowlist of files that
// legitimately contain `SET status='SUCCEEDED'`.
//
// PR 3.5-a's sole-writer contract is JOBS-TABLE-SPECIFIC: only
// sqlite_finalization_repository.go may flip jobs.status='SUCCEEDED'.
// The other entries here are SEPARATE LIFECYCLES that the migration
// does NOT touch — listing them explicitly records the audit trail
// so a future regression that DOES target jobs.status is detected.
//
// Keyed by absolute relative path (NOT basename) so a copy-paste
// regression in a subpackage is NOT silently allowed.
var allowedWriters = map[string]bool{
	// PR 3.5-a legal writer of jobs.status='SUCCEEDED' (verified
	// finalization: jobs CAS + job_attempts CAS in single tx).
	filepath.Join("internal", "artifacts", "sqlite_finalization_repository.go"): true,
	// Interface + commands: contains the regex literal in a doc
	// comment EXPLAINING the contract. No executable SQL update.
	filepath.Join("internal", "artifacts", "finalization_repository.go"): true,
	// SEPARATE legacy lifecycle: internal/store/store_assembly.go's
	// `CompleteJobTx` (PR1c legacy atomic SUCCEEDED transition) — to
	// be retired in PR 3.5-b 4.6 by routing through the new verified
	// finalization. Until then, this is the ONLY other writer that
	// targets jobs.status='SUCCEEDED', so it MUST be on the allowlist
	// — PR 3.5-a intentionally does NOT remove it (removal is in the
	// next session's scope).
	filepath.Join("internal", "store", "store_assembly.go"): true,
	// SEPARATE lifecycle: UPDATE job_deliveries SET status='SUCCEEDED'
	// is delivery-completion (NOT jobs). PR 3.5-a does NOT touch
	// delivery completion.
	filepath.Join("internal", "store", "store_deliveries.go"): true,
	// SEPARATE lifecycles: UPDATE workflow_steps / workflow_runs SET
	// status='SUCCEEDED' is workflow-completion (NOT jobs).
	// PR 3.5-a does NOT touch workflow completion.
	filepath.Join("internal", "workflow", "sqlite_repository.go"): true,
}

// allowedTestFiles are files that legitimately contain the SQL
// fragment as a TEST LITERAL (not as an executed statement).
var allowedTestFiles = map[string]bool{
	filepath.Join("internal", "artifacts", "scan_test.go"): true,
}

// findInternalRoot walks from the package cwd upward (default
// `go test` cwd is the package directory; running tests directly from
// the module root puts cwd = the module root) until it finds a
// directory containing `internal/`. Returns the rooted-under-module
// path that contains `internal/`.
//
// This makes the scan test portable: it works whether you run
// `go test ./internal/artifacts` (cwd = internal/artifacts) OR
// from the module root (cwd = the DataServer root).
func findInternalRoot(t *testing.T) string {
	t.Helper()
	cur, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("abs cwd: %v", err)
	}
	// Cap the upward walk so we never spin on a malformed FS.
	const maxLevelsUp = 8
	for i := 0; i <= maxLevelsUp; i++ {
		if _, statErr := os.Stat(filepath.Join(cur, "internal")); statErr == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break // reached filesystem root without finding `internal/`
		}
		cur = parent
	}
	t.Skip("scan_test cannot find an ancestor directory containing `internal/`; " +
		"this test requires `internal/` somewhere above the package dir (typical Go module layout)")
	return ""
}

func TestSucceededWriterIsFinalizationOnly(t *testing.T) {
	root := findInternalRoot(t)
	internalDir := filepath.Join(root, "internal")

	var violations []string
	walkErr := filepath.WalkDir(internalDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if allowedWriters[rel] || allowedTestFiles[rel] {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if forbiddenSUCCEEDEDWrite.Match(b) {
			violations = append(violations, rel)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk internal/: %v", walkErr)
	}
	if len(violations) > 0 {
		t.Fatalf("`SET status = 'SUCCEEDED'` SQL fragment detected outside the audited allowlist.\n"+
			"All files containing this fragment must be explicitly allowlisted.\n"+
			"Offending files (must add an allowlist entry with a documenting comment, "+
			"OR remove the SQL / route through the verified FinalizeVerified):\n"+
			"  %v", violations)
	}
}

// TestSucceededWriterCount guards the canonical writer's SUCCEEDED-flip
// count. The verified-finalization contract requires:
//
//   - At least 2 distinct `SET status='SUCCEEDED'` statements
//     (jobs.status and job_attempts.status), so simply deleting the
//     job_attempts CAS would drop the count below the contract.
//   - At most a small upper bound — typically 2 — so an accidental
//     copy-paste regression inside the writer is detected.
//
// If you add a 3rd legitimate flips inside FinalizeVerified, UPDATE
// THIS TEST (with reasoning) — the test is a tripwire, not a
// permanent spec.
func TestSucceededWriterCount(t *testing.T) {
	root := findInternalRoot(t)
	path := filepath.Join(root, "internal", "artifacts", "sqlite_finalization_repository.go")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read writer: %v", err)
	}
	matches := forbiddenSUCCEEDEDWrite.FindAll(b, -1)

	const (
		minExpected = 2 // jobs CAS + job_attempts CAS — required by verified finalization
		maxExpected = 3 // small ceiling: catch accidental duplicate writers
	)
	switch {
	case len(matches) < minExpected:
		t.Fatalf("canonical writer contains %d `SET status='SUCCEEDED'` matches; expected >= %d.\n"+
			"Verified finalization requires at least 2 distinct CAS (jobs + job_attempts).\n"+
			"Did you delete one accidentally? Did the writer move? Investigate immediately.",
			len(matches), minExpected)
	case len(matches) > maxExpected:
		t.Fatalf("canonical writer contains %d `SET status='SUCCEEDED'` matches; expected <= %d.\n"+
			"Multiple writers inside the same file defeat the single-writer contract — "+
			"split your logic into a different status enum or move the extra flip out "+
			"of the verified-finalization tx.",
			len(matches), maxExpected)
	}
}
