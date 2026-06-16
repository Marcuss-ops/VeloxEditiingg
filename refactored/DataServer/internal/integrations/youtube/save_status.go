package youtube

import (
	"errors"
	"time"
)

// ErrSaveRefusedBySafetyGuard is surfaced by the audit endpoint when a
// destructive save would risk wiping the persisted catalog. USED through
// the YouTubeStore / GroupStore chain. With the S11 Storage struct drop
// this error is ROUTED back from the GroupStore adapter's reconciliation
// path rather than emitted by an in-process save() call.
var ErrSaveRefusedBySafetyGuard = errors.New("save refused by safety guard: in-memory group set too small relative to DB")

// ErrGroupMembershipRefusedEmptyMemory is the per-group analogue of
// ErrSaveRefusedBySafetyGuard: a destructive membership diff that would
// wipe a group's DB rows because the in-memory state has shrunk to an
// empty slice. Surfaces from diffGroupMemberships-style operations.
var ErrGroupMembershipRefusedEmptyMemory = errors.New("group membership refused: empty in-memory channel slice would wipe persisted memberships")

// Safety-guard tuning constants. Kept exported (capitalized) because the
// audit handler surfaces them as part of its live_count / safety_guard
// response and any future tuning tool needs the same names to match.
const (
	safetyGuardMinRatio    = 0.5
	safetyGuardMinDBGroups = 4
)

// SaveStatus records the outcome of the most recent save / SyncGroup /
// SaveData / saveAllReconcile invocation. Surfaced by the
// /api/v1/audit/persistence endpoint so operators can verify the safety
// guard, the per-group path, and the live counts are coherent end-to-end.
//
// Persistence status was previously recorded by the legacy *Storage
// struct's saveWithStatus; the S11 cutover moves the same fields onto
// the GroupStore adapter (persistence_reconcile.go shall own the
// recordStatus call going forward) so /audit/persistence can keep
// reading the same JSON shape.
type SaveStatus struct {
	Timestamp    time.Time `json:"timestamp"`
	Operation    string    `json:"operation"`          // "save", "save_all_reconcile", "sync_group", "save_data", "cleanup"
	GroupTarget  string    `json:"group,omitempty"`    // name when Operation = "sync_group"
	Result       string    `json:"result"`             // "ok" | "refused_safety_guard" | "error: ..."
	Error        string    `json:"error,omitempty"`
	MemoryGroups int       `json:"memory_groups"`
	DBGroupCount int       `json:"db_group_count"`
	Ratio        float64   `json:"memory_db_ratio"`
	BypassGuard  bool      `json:"bypass_safety_guard,omitempty"`
}

// PersistenceResultXxx are the canonical result strings the
// /api/v1/audit/persistence handler surfaces. Defined as constants so
// other packages can match the same literals without stringly-typed
// duplication.
const (
	PersistenceResultOK                 = "ok"
	PersistenceResultRefusedSafetyGuard = "refused_safety_guard"
	PersistenceResultError              = "error"
)
