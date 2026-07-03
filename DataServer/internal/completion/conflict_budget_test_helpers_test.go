// Package completion / conflict_budget_test_helpers.go
//
// Shared test-only helpers for the three conflict_budget test files
// (conflict_budget_test.go, conflict_budget_chaos_test.go,
// conflict_budget_busy_chaos_test.go). All three files are in
// `package completion`; putting the shared `testKey` constant in
// this dedicated file (rather than in one of the three test files)
// avoids the redeclaration error a per-file `const testKey =
// "test"` would produce and gives a single, greppable place to
// find the canonical "single shared key" used by legacy tests.
//
// Verdetto P0 #4 (Blocco 3) added a per-key parameter to
// ConflictBudget.Record; tests that pre-date the per-key design
// collapse into a single shared key because their scenarios assume
// a single streak. The new per-key isolation tests use distinct
// keys explicitly (e.g. "commit:alpha", "commit:beta") and do not
// reference testKey.
package completion

// testKey is the canonical key the legacy conflict-budget tests
// use. See the package-level comment above for why it lives here
// rather than in one of the three test files.
const testKey = "test"
