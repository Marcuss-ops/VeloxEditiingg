package instaeditauth

// =========================================================================
// instaeditauth/scopes — scope claim vocabulary for the InstaEdit→
// Velox BFF control JWT.
// =========================================================================
//
// These constants mirror the four scopes declared on the mint side
// at InstaeditLogin/internal/veloxclient/auth.go. They MUST stay in
// sync — a drift between the two repos surfaces as a 403 at the first
// BFF call, not at deploy time (the Velox verifier reads claims.Scopes
// from the JWT and matches them against the slice passed into
// Middleware(requiredScopes), so any mismatch is a hard fail).
//
// ROUTING:
//   editor.project.read      → Velox routes that READ a dark-editor
//                              project / job / delivery / worker / asset
//                              (e.g. GET /api/v1/instaedit/editor/projects/{id},
//                              GET .../jobs, GET .../jobs/{id},
//                              GET .../jobs/{id}/deliveries,
//                              GET .../workers, GET .../workers/{id},
//                              GET .../assets/{id})
//   editor.project.write     → Velox routes that MUTATE a dark-editor
//                              project (POST .../jobs, POST .../jobs/{id}/cancel,
//                              PATCH on projects)
//   editor.asset.upload      → Velox routes that upload a render asset
//                              (PUT/POST /api/v1/instaedit/editor/assets/*)
//   youtube.session.publish  → Velox route that publishes a thumbnail
//                              update to YouTube (POST .../sessions/{id}/publish)
//
// VALUES MUST NOT contain a ":", a space, or be longer than
// 64 characters — the Go middleware logs them verbatim and we keep
// the wire format human-readable for the 403 body.

const (
	// ScopeEditorProjectRead grants read access to a dark-editor
	// project and its child resources (jobs, deliveries, workers, assets).
	ScopeEditorProjectRead = "editor.project.read"

	// ScopeEditorProjectRead grants write access to a dark-editor
	// project lifecycle (create / update / cancel).
	ScopeEditorProjectWrite = "editor.project.write"

	// ScopeEditorAssetUpload grants permission to upload a render
	// asset to Velox for a given project.
	ScopeEditorAssetUpload = "editor.asset.upload"

	// ScopeYouTubeSessionPublish grants permission to publish a
	// thumbnail update to YouTube through Velox.
	ScopeYouTubeSessionPublish = "youtube.session.publish"
)

// AllScopesSuperset is the union of the four editor scopes. It is
// used by:
//
//   - The InstaEdit BFF during the cutover window: when the
//     EditorBFFModule Proxy(...) call does not yet declare an
//     explicit per-operation scope, the BFF falls back to this
//     superset so the dark-editor UI keeps working while the
//     per-operation wiring lands in a followup commit.
//   - Velox middleware test fixtures that need a "guaranteed to pass"
//     scope set to exercise unrelated code paths.
//
// Do NOT add new entries to this slice casually — every addition
// widens the grant set that the superset-fallback path produces,
// which weakens the audit-trail granularity on the Velox side.
// Use the per-route Middleware(requiredScopes) when adding new
// protected routes.
var AllScopesSuperset = []string{
	ScopeEditorProjectRead,
	ScopeEditorProjectWrite,
	ScopeEditorAssetUpload,
	ScopeYouTubeSessionPublish,
}
