// Package artifacts / scan_test.go
//
// The SQL fragment  `SET status = 'SUCCEEDED'`  is the spec's atomic
// CAS that flips a status to SUCCEEDED. The contract enforced here is
// AUDITABILITY: every file that contains this fragment is either:
//
//	(a) the single legal writer of jobs.status='SUCCEEDED' for the
//	    verified-finalization lifecycle, OR
//	(b) a SEPARATE lifecycle writer (job_deliveries, workflow_steps,
//	    workflow_runs), OR
//	(c) this test file (regex literal as documentation).
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

// forbid any writer on job_attempts (case-insensitive,
// whitespace-tolerant). The runtime INSERT/UPDATE surface on
// job_attempts has been removed (per-attempt identity lives on
// task_attempts). READS are still permitted (SELECT is not in the
// regex).
var forbiddenJobAttemptsWrite = regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE)\s+job_attempts\b`)

// forbiddenSUCCEEDEDWrite is the SQL fragment we forbid outside the
// audited allowlist. Case-insensitive, whitespace-tolerant: a
// `SET  status='SUCCEEDED'` style still trips the check.
var forbiddenSUCCEEDEDWrite = regexp.MustCompile(`(?i)SET\s+status\s*=\s*['"]SUCCEEDED['"]`)

// allowedWriters is the EXPLICIT audited allowlist of files that
// legitimately contain `SET status='SUCCEEDED'`.
//
// Sole-writer contract: only sqlite_finalize_writer.go may flip
// jobs.status='SUCCEEDED'. The other entries here are SEPARATE
// lifecycles (delivery, workflow) — listing them explicitly records
// the audit trail so a future regression that DOES target jobs.status
// is detected.
//
// Keyed by absolute relative path (NOT basename) so a copy-paste
// regression in a subpackage is NOT silently allowed.
var allowedWriters = map[string]bool{
	// Sole writer of jobs.status='SUCCEEDED' for the verified-
	// finalization lifecycle (sole-writer enforced by scan_test.go).
	filepath.Join("internal", "artifacts", "sqlite_finalize_writer.go"): true,
	// Coordinator.CommitAttempt is the canonical atomic SUCCEEDED tx
	// writer for tasks + task_attempts + jobs in the Completion flow.
	filepath.Join("internal", "completion", "coordinator.go"): true,
	// UoW adapter: the SQL gateway the coordinator speaks through
	// (six typed repos bound to a single *sql.Tx). The
	// MarkSucceededIfTasksDone method's `SET status = 'SUCCEEDED'`
	// is the SAME atomic tx as coordinator.go — the SQL lives in
	// sqlite_uow.go while the orchestration (BeginTx → CAS → Commit)
	// lives in coordinator.go. The SQL-ownership shape guard
	// (scripts/ci/check-sql-ownership.sh) explicitly allows
	// `internal/completion/sqlite_uow.go` as the second canonical
	// SQL gateway alongside internal/store/**.
	filepath.Join("internal", "completion", "sqlite_uow.go"): true,
	// Interface + commands: contains the regex literal in a doc
	// comment EXPLAINING the contract. No executable SQL update.
	filepath.Join("internal", "artifacts", "finalization_repository.go"): true,
	// store_assembly.go's CompleteJobTx was removed. The only writer
	// of jobs.status='SUCCEEDED' in this package layer is now
	// sqlite_finalize_writer.go.
	// SEPARATE lifecycle: UPDATE job_deliveries SET status='SUCCEEDED'
	// is delivery-completion (NOT jobs).
	filepath.Join("internal", "store", "store_deliveries.go"):       true,
	filepath.Join("internal", "store", "store_deliveries_lease.go"): true,
	// SEPARATE lifecycles: UPDATE workflow_steps / workflow_runs SET
	// status='SUCCEEDED' is workflow-completion (NOT jobs).
	filepath.Join("internal", "workflow", "sqlite_repository.go"):         true,
	filepath.Join("internal", "workflow", "sqlite_repository_queries.go"): true,
	filepath.Join("internal", "workflow", "sqlite_repository_steps.go"):   true,
}

// allowedTestFiles are files that legitimately contain the SQL
// fragment as a TEST LITERAL (not as an executed statement).
var allowedTestFiles = map[string]bool{
	filepath.Join("internal", "artifacts", "scan_test.go"): true,
	// calendar_test.go uses direct SQL to set jobs.status='SUCCEEDED' as a
	// TEST FIXTURE — it tests calendar API status mapping, not the finalization
	// lifecycle. The full FinalizeVerified path requires a complex multi-table
	// fixture (upload session, artifact, job attempt in RENDER_FINISHED state)
	// which is out of scope for this API-level test.
	filepath.Join("internal", "handlers", "server", "calendar", "calendar_test.go"): true,
}

// allowedJobAttemptsLegacyWriters is the explicit allowlist of files
// that legitimately contain INSERT/UPDATE on job_attempts. Only test
// fixtures pre-populate the table for smoke tests of read-side
// consumers. Production source MUST stay empty here — adding a new
// entry reintroduces the retired runtime writer surface.
var allowedJobAttemptsLegacyWriters = map[string]bool{
	filepath.Join("internal", "artifacts", "scan_test.go"):                                                  true,
	filepath.Join("internal", "artifacts", "service_test.go"):                                               true,
	filepath.Join("internal", "handlers", "remote", "workers", "uploads", "video_upload_completed_test.go"): true,
}

// stripGoComments removes `// ... \n` and `/* ... */` comments from
// Go source bytes. The scan test runs the SQL-fragment regex against
// the stripped source so doc comments / backticked examples that
// reference the canonical SQL verbatim don't trip a false-positive.
//
// Per-line handling: skip until newline on `//`; toggle in/out on
// `/* ... */`. The block-comment parser is intentionally conservative
// (it does NOT understand strings, raw strings, or rune literals) —
// false negatives would re-flag legitimate SQL writers and the
// reviewer would catch that, whereas false positives block CI on a
// doc-only reference that cannot execute SQL.
func stripGoComments(src []byte) []byte {
	var out []byte
	inBlock := false
	for i := 0; i < len(src); i++ {
		if inBlock {
			if i+1 < len(src) && src[i] == '*' && src[i+1] == '/' {
				inBlock = false
				i++
			}
			continue
		}
		if i+1 < len(src) && src[i] == '/' && src[i+1] == '/' {
			// Skip to end of line, preserving the newline so line
			// counts in test output remain comparable.
			for i < len(src) && src[i] != '\n' {
				i++
			}
			if i < len(src) {
				out = append(out, '\n')
			}
			continue
		}
		if i+1 < len(src) && src[i] == '/' && src[i+1] == '*' {
			inBlock = true
			i++
			continue
		}
		out = append(out, src[i])
	}
	return out
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
		// Strip Go comments before regex matching so doc comments
		// that reference the canonical SUCCEEDED-flip SQL
		// (e.g. `SET status='SUCCEEDED'` in a backticked example)
		// don't trip a false positive. Same guard as
		// TestNoJobAttemptsWriter for symmetry.
		if forbiddenSUCCEEDEDWrite.Match(stripGoComments(b)) {
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
// count. Tripwire bounds:
//
//   - At least 1 distinct `SET status='SUCCEEDED'` statement (jobs).
//   - At most a small upper bound — small ceiling to catch a
//     accidental copy-paste regression inside the writer.
//
// If you add a legitimate 3rd flip, UPDATE this test (with
// reasoning) — the bound is a tripwire, not a permanent spec.
func TestSucceededWriterCount(t *testing.T) {
	root := findInternalRoot(t)
	path := filepath.Join(root, "internal", "artifacts", "sqlite_finalize_writer.go")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read writer: %v", err)
	}
	matches := forbiddenSUCCEEDEDWrite.FindAll(b, -1)

	const (
		// The legacy job_attempts CAS was removed from FinalizeVerified;
	// the canonical SUCCEEDED write is solely the jobs CAS.
		minExpected = 1
		maxExpected = 3 // small ceiling: catch accidental duplicate writers
	)
	switch {
	case len(matches) < minExpected:
		t.Fatalf("canonical writer contains %d `SET status='SUCCEEDED'` matches; expected >= %d.\n"+
			"Verified finalization requires at least 1 distinct CAS (jobs).\n"+
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

// TestNoJobAttemptsWriter: enforce that no production .go file under
// internal/ contains INSERT INTO job_attempts or UPDATE job_attempts
// SQL fragments. Test fixtures are explicitly allowlisted.
//
// READS (SELECT) are not in the regex.
func TestNoJobAttemptsWriter(t *testing.T) {
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
		// Allowlist check: test fixtures that exercise read-side
		// consumers + this guard's own definition file.
		rel, _ := filepath.Rel(root, path)
		if allowedJobAttemptsLegacyWriters[rel] {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		// Strip Go comments before regex matching so doc comments
		// that reference the canonical SQL (`UPDATE job_attempts`
		// inside a backticked example) don't trip a false positive.
		if forbiddenJobAttemptsWrite.Match(stripGoComments(b)) {
			violations = append(violations, rel)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk internal/: %v", walkErr)
	}
	if len(violations) > 0 {
		t.Fatalf("`INSERT INTO job_attempts` / `UPDATE job_attempts` SQL writer detected outside the legacy-fixture allowlist.\n"+
			"the runtime writer surface on job_attempts has been retired. Per-attempt identity lives on task_attempts.\n"+
			"Offending files (must be added to allowedJobAttemptsLegacyWriters with a documenting comment, "+
			"OR rewritten to use task_attempts):\n"+
			"  %v", violations)
	}
}
