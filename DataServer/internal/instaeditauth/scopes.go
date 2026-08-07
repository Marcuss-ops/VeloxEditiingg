package instaeditauth

// =========================================================================
// instaeditauth/scopes — scope claim vocabulary for the InstaEdit→
// Velox BFF control JWT.
// =========================================================================
//
// These constants mirror the scopes declared on the mint side
// at InstaeditLogin/internal/veloxcontract/contract.go. They MUST stay
// in sync — a drift between the two repos surfaces as a 403 at the first
// BFF call, not at deploy time (the Velox verifier reads claims.Scopes
// from the JWT and matches them against the slice passed into
// Middleware(requiredScopes), so any mismatch is a hard fail).
//
// ROUTING:
//   jobs.read    → Velox routes that READ jobs (GET /api/v1/instaedit/jobs,
//                  GET .../jobs/{id}, GET .../jobs/{id}/deliveries)
//   jobs.write   → Velox routes that MUTATE jobs (POST .../jobs,
//                  POST .../jobs/{id}/cancel)
//   workers.read → Velox routes that READ workers (GET .../workers,
//                  GET .../workers/{id})
//   assets.read  → Velox routes that READ render assets (GET .../assets/{id})
//   assets.write → Velox routes that upload a render asset
//                  (PUT/POST /api/v1/instaedit/assets/*)
//
// VALUES MUST NOT contain a ":", a space, or be longer than
// 64 characters — the Go middleware logs them verbatim and we keep
// the wire format human-readable for the 403 body.

const (
	// ScopeJobsRead grants read access to rendering jobs and their
	// deliveries.
	ScopeJobsRead = "jobs.read"

	// ScopeJobsWrite grants write access to the job lifecycle
	// (create / update / cancel).
	ScopeJobsWrite = "jobs.write"

	// ScopeWorkersRead grants read access to compute workers.
	ScopeWorkersRead = "workers.read"

	// ScopeAssetsRead grants read access to render assets.
	ScopeAssetsRead = "assets.read"

	// ScopeAssetsWrite grants permission to upload a render asset
	// to Velox for a given workspace.
	ScopeAssetsWrite = "assets.write"

	// Editor scopes are reserved for the project-scoped bridge. They
	// never grant access to global groups, channels, or workspaces.
	ScopeEditorRead  = "editor.read"
	ScopeEditorWrite = "editor.write"
)

// AllScopesSuperset is the union of the BFF scopes. It is used by
// Velox middleware test fixtures that need a "guaranteed to pass"
// scope set to exercise unrelated code paths.
//
// Do NOT add new entries to this slice casually — every addition
// widens the grant set that the superset-fallback path produces,
// which weakens the audit-trail granularity on the Velox side.
// Use the per-route Middleware(requiredScopes) when adding new
// protected routes.
var AllScopesSuperset = []string{
	ScopeJobsRead,
	ScopeJobsWrite,
	ScopeWorkersRead,
	ScopeAssetsRead,
	ScopeAssetsWrite,
}
