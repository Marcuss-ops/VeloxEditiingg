package youtube

// This file is a compatibility stub for two symbols that
// `cmd/server/bootstrap.go` still references:
//
//   - CanonicalOAuthTokenSubPath (a string)
//   - ConsolidateOAuthTokens(dataDir string, dryRun bool) (*ConsolidationResult, error)
//
// Both were previously defined in `migration_consolidate_tokens.go`,
// which was deleted by commit `62aded69` (S7 + S8 + S10 SQLite-only
// contract cluster) on the grounds of "no external callers". The
// `cmd/server/bootstrap.go` call site slipped through that grep (the
// import path was via the integration package alias, not a direct
// name match), so `go build ./...` has been failing on:
//   - bootstrap.go:179:66: undefined: integrationsYoutube.CanonicalOAuthTokenSubPath
//   - bootstrap.go:184: undefined: integrationsYoutube.ConsolidateOAuthTokens
//
// The runtime path is now SQLite-only (S6 verdict): the YAML / JSON
// token directory no longer participates in any boot-time write or
// rehydration. The legacy one-shot consolidation function is not
// re-introduced here because the operator can run it explicitly
// through the planned `velox migrate youtube-oauth-json` admin command,
// and rehydrating it as boot-time logic would resurrect the dual-write
// drift the verdict called out. The stub returns a zero-value result
// so the bootstrap code path compiles and the runtime stays clean.

// CanonicalOAuthTokenSubPath is the canonical on-disk location for
// system-managed OAuth tokens: <DataDir>/secrets/youtube/tokens/.
// Exported because bootstrap.go uses it as the MkdirAll target before
// the (now stubbed) consolidation call. The constant is the single
// authoritative value; do NOT hard-code "secrets/youtube/tokens" in
// calling code.
const CanonicalOAuthTokenSubPath = "secrets/youtube/tokens"

// ConsolidationResult reports what a `ConsolidateOAuthTokens`
// invocation did. Field names are preserved verbatim from the
// previously-deleted implementation so any consumers (operators'
// logs, future `velox migrate youtube-oauth-json` reporting) can still
// print them in the same shape.
type ConsolidationResult struct {
	// Found = tokens discovered in non-canonical locations.
	Found int
	// Moved = tokens relocated into CanonicalOAuthTokenSubPath.
	Moved int
	// Merged = tokens merged with an existing canonical copy (no fs move).
	Merged int
	// DeletedLegacyFiles = legacy copies removed after a successful merge.
	DeletedLegacyFiles int
	// RemovedEmptyDirs = legacy directories pruned after relocation.
	RemovedEmptyDirs int
	// Errors = per-file error messages collected during the run; the
	// function returns nil error unless the workflow itself failed.
	Errors []string
}

// ConsolidateOAuthTokens is the compatibility stub for the
// previously-deleted one-shot helper. It does no work at runtime
// because the SQLite-only contract (S6) makes boot-time JSON scanning
// / merging redundant: the OAuth routes no longer read from
// <DataDir>/secrets/youtube/tokens/*.json, and the manual admin
// command `velox migrate youtube-oauth-json` (to be implemented) is
// the canonical entry point for legacy migration. Keeping this stub
// compiles the bootstrap path; the contract is now explicit so a
// future contributor cannot accidentally re-introduce runtime JSON
// scanning by "fixing" the stub.
//
// dryRun is preserved as a parameter so a future implementation can
// flip it on for a dry-run report without touching the bootstrap callsite.
// dataDir is preserved for the same reason.
func ConsolidateOAuthTokens(dataDir string, dryRun bool) (*ConsolidationResult, error) {
	return &ConsolidationResult{
		Found: 0, Moved: 0, Merged: 0,
		DeletedLegacyFiles: 0, RemovedEmptyDirs: 0,
		Errors: nil,
	}, nil
}
