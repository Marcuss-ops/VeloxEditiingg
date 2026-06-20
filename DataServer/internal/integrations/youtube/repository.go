package youtube

// YouTubeRepository is the SINGLE canonical interface for YouTube
// persistent state.
//
// PR15.4: after dropping Storage.data.Groups (the in-RAM mirror of the
// groups/channels catalog) and the memory-vs-DB reconciler guards
// (safetyGuard ratio + empty-wipe guard + saveAllReconcile bypass),
// YouTubeRepository collapses to the SQL-only YouTubeStore contract
// declared in service.go. There is no longer a wider "unified" surface
// because the *Storage struct is now a thin SQL pass-through facade and
// does not own additional methods beyond what YouTubeStore provides.
//
// Migration status:
//
//   - This file previously declared a wider interface (YouTubeStore +
//     four in-memory methods) reserved for a "future composed
//     repository". PR15.4 deletes that wider surface and replaces it
//     with a type alias so any caller that asked for YouTubeRepository
//     transitively depends on YouTubeStore.
//
// Relationship to *SQLiteStore:
//
//   - store.SQLiteStore implements YouTubeStore (compile-time assertion
//     in store/interface_compliance_test.go).
//   - YouTubeRepository is a strict alias of YouTubeStore, so the same
//     compile-time assertion guarantees YouTubeRepository satisfaction.
//
// New code MUST depend on YouTubeStore directly. YouTubeRepository is
// kept as an alias for transition period (callers may still hold the
// older name).
type YouTubeRepository = YouTubeStore
