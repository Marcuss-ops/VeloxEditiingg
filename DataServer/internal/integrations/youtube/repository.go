package youtube

// YouTubeRepository is the SINGLE canonical wider interface that unifies
// YouTube persistent state. It is the strict superset of:
//
//   - YouTubeStore (canonical SQLite operations): channels, OAuth tokens,
//     groups v2, group memberships, API cache;
//   - four methods formerly owned by *Storage: LoadData / SaveData /
//     syncGroupLocked / saveAllReconcile (the in-memory group/map state
//     plus destructive reconciliation).
//
// Relationship to other interfaces:
//
//   - YouTubeStore (service.go) is the *narrow* SQL-only contract every
//     existing caller already depends on. It is preserved as-is — the
//     store package's *SQLiteStore satisfies it today.
//   - YouTubeRepository (this file) is the wider, single canonical home
//     for the unified-state concept. New code MUST depend on
//     YouTubeRepository directly. *SQLiteStore does NOT satisfy it (the
//     four in-memory methods belong to *Storage) — that is intentional:
//     sqlite-only callers stay on YouTubeStore, while the future single
//     concrete repository (which embeds a *SQLiteStore AND the Storage
//     in-memory state) will satisfy the wider interface.
//
// Migration status of this branch:
//   - The wider interface is defined (this file).
//   - *Storage satisfies the in-memory + reconciliation subset through
//     the four methods it already owns; the satisfaction is asserted by
//     the narrow compile-time guard at the bottom of this file.
//
// Follow-up commits (separate branches) will:
//   - introduce a Compose(...) helper that yields a struct satisfying
//     YouTubeRepository by combining a YouTubeStore + *Storage-like
//     in-memory state;
//   - migrate handlers / Service / module wiring to consume
//     YouTubeRepository instead of {Service.store, *Storage} pairs.
type YouTubeRepository interface {
	// SQL-only subset — embedded so YouTubeRepository is a strict superset
	// of YouTubeStore; no method duplication.
	YouTubeStore

	// In-memory + reconciling surface — absorbed from *Storage. These are
	// the four methods the user explicitly named in the unification claim.
	LoadData() *StorageData
	SaveData(data *StorageData) error
	syncGroupLocked(name string, g *Group) error
	saveAllReconcile() error
}

// Static interface-satisfaction guard for the methods *Storage
// actually owns in the unified repository: in-memory state +
// reconciliation. SQLite CRUD remains delegated via Storage's narrower
// store field, and *SQLiteStore does not satisfy the wide
// YouTubeRepository (intentionally — that role is reserved for the yet-
// to-be-introduced composed repository). The narrow guard keeps a
// compile-time check that the four in-memory methods stay on *Storage.
var _ interface {
	LoadData() *StorageData
	SaveData(data *StorageData) error
	syncGroupLocked(name string, g *Group) error
	saveAllReconcile() error
} = (*Storage)(nil)
