package youtube

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// ErrSaveRefusedBySafetyGuard is returned by save() when the in-memory group set
// is suspiciously smaller than the DB set, which would risk destroying
// persisted data on a rewrite.
var ErrSaveRefusedBySafetyGuard = errors.New("save refused by safety guard: in-memory group set too small relative to DB")

// ErrGroupMembershipRefusedEmptyMemory is returned by diffGroupMemberships when
// the in-memory channel slice for a group is empty while the DB has
// memberships for the same group. Used to refuse destructive wipes that could
// happen from a stale partial load.
var ErrGroupMembershipRefusedEmptyMemory = errors.New("group membership refused: empty in-memory channel slice would wipe persisted memberships")

// safetyGuardMinRatio is the minimum acceptable ratio of in-memory groups to
// DB groups. If the ratio falls below this and the DB has more than
// safetyGuardMinDBGroups groups, save() refuses the destructive rewrite.
const (
	safetyGuardMinRatio    = 0.5
	safetyGuardMinDBGroups = 4
)

// load reads data from canonical SQLite tables (youtube_groups_v2, youtube_channels).
func (s *Storage) load() error {
	if s.store == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	groupRows, err := s.store.ListYouTubeGroupsV2()
	if err != nil {
		return fmt.Errorf("load groups: %w", err)
	}

	for _, row := range groupRows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		groupType, _ := row["group_type"].(string)
		createdAt, _ := row["created_at"].(string)
		gid, _ := row["id"].(int64)

		createdAtTime := parseFlexTime(createdAt)

		// Merge duplicate groups by name, preferring 'upload' type and consolidating channels
		group, exists := s.data.Groups[name]
		if !exists {
			group = &Group{
				Name:      name,
				CreatedAt: createdAtTime,
				Channels:  []Channel{},
				GroupType: groupType,
			}
			s.data.Groups[name] = group
		} else {
			// If duplicate found, prioritize 'upload' as the primary type for the Suite
			if group.GroupType != "upload" && groupType == "upload" {
				group.GroupType = "upload"
			}
		}

		if gid > 0 {
			channelIDs, err := s.store.ListGroupChannelsV2(gid)
			if err == nil {
				for _, chID := range channelIDs {
					// Check if channel already exists in this merged group to avoid duplicates
					found := false
					for _, existingCh := range group.Channels {
						if existingCh.ID == chID {
							found = true
							break
						}
					}
					if found {
						continue
					}

					ch, err := s.store.GetYouTubeChannel(chID)
					if err == nil && ch != nil {
						channel := channelFromCanonicalRow(ch)
						if channel != nil {
							group.Channels = append(group.Channels, *channel)
						}
					} else {
						group.Channels = append(group.Channels, Channel{ID: chID})
					}
				}
			}
		}
	}

	niches, err := s.store.ListYouTubeTrackedNiches()
	if err == nil && len(niches) > 0 {
		s.data.TrackedNiches = niches
	}

	log.Printf("[OK] Loaded %d groups from canonical tables", len(s.data.Groups))
	return nil
}

// channelFromCanonicalRow converts a canonical youtube_channels row to a Channel.
func channelFromCanonicalRow(row map[string]interface{}) *Channel {
	id, _ := row["channel_id"].(string)
	if id == "" {
		return nil
	}
	return &Channel{
		ID:        id,
		Title:     asStringField(row, "title"),
		Name:      asStringField(row, "display_name"),
		URL:       asStringField(row, "channel_url"),
		Thumbnail: asStringField(row, "thumbnail_url"),
		Language:  asStringField(row, "language"),
		ViewCount: asInt64Field(row, "view_count"),
		SubCount:  asInt64Field(row, "subscriber_count"),
	}
}

func parseFlexTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// checkSafetyGuard returns an error if the in-memory group set is suspiciously
// smaller than the DB set. This prevents destructive rewrites from erasing
// persisted channels when memory gets corrupted, reduced, or loaded from a
// stale snapshot.
func (s *Storage) checkSafetyGuard() error {
	if s.store == nil {
		return nil
	}

	dbGroups, err := s.store.ListYouTubeGroupsV2()
	if err != nil || len(dbGroups) == 0 {
		return nil // nothing to protect
	}

	memGroups := 0
	for _, g := range s.data.Groups {
		if g != nil {
			memGroups++
		}
	}

	// Empty memory with non-empty DB: refuse.
	if memGroups == 0 {
		return fmt.Errorf("%w (memory=0, db=%d)", ErrSaveRefusedBySafetyGuard, len(dbGroups))
	}

	// Memory much smaller than DB: refuse.
	if len(dbGroups) > safetyGuardMinDBGroups {
		ratio := float64(memGroups) / float64(len(dbGroups))
		if ratio < safetyGuardMinRatio {
			return fmt.Errorf("%w (memory=%d, db=%d, ratio=%.2f)", ErrSaveRefusedBySafetyGuard, memGroups, len(dbGroups), ratio)
		}
	}
	return nil
}

// SaveStatus records the outcome of the most recent save() / SyncGroup /
// SaveData / saveAllReconcile invocation. Surfaced by the
// /api/v1/audit/persistence endpoint so operators can verify the safety
// guard, the per-group path, and the live counts are coherent end-to-end.
type SaveStatus struct {
	Timestamp     time.Time `json:"timestamp"`
	Operation     string    `json:"operation"`              // "save", "save_all_reconcile", "sync_group", "save_data", "cleanup"
	GroupTarget   string    `json:"group,omitempty"`        // name when Operation = "sync_group"
	Result        string    `json:"result"`                 // "ok" | "refused_safety_guard" | "error: ..."
	Error         string    `json:"error,omitempty"`
	MemoryGroups  int       `json:"memory_groups"`
	DBGroupCount  int       `json:"db_group_count"`
	Ratio         float64   `json:"memory_db_ratio"`
	BypassGuard   bool      `json:"bypass_safety_guard,omitempty"`
}

// save persists the full in-memory state to canonical SQLite tables using a
// non-destructive per-group diff. Untouched groups are not modified; groups
// present only in DB but no longer in memory are NOT deleted (full destructive
// reconciliation must be opted in via saveAllReconcile).
//
// A safety guard refuses the write if the in-memory group set looks
// suspiciously smaller than the DB set, preventing data loss when the
// in-memory state has been corrupted, partially loaded, or replaced with a
// stale snapshot.
func (s *Storage) save() error {
	return s.saveWithStatus("save", "", false)
}

// saveWithStatus is the shared core for save()/saveAllReconcile()/SyncGroup()/SaveData().
// Records the result (and the safety-guard statistics) on s.lastSave even on error.
//
// For op == "sync_group" the safety guard is intentionally skipped because
// the per-group diff can only touch the affected group — the global
// memory-vs-DB ratio is irrelevant for that operation and computing it costs
// an extra ListYouTubeGroupsV2 round-trip on every channel mutation. The
// safety guard still fires on save() / SaveData() / saveAllReconcile() (when
// bypassGuard is false).
func (s *Storage) saveWithStatus(op, groupName string, bypassGuard bool) (err error) {
	if s.store == nil {
		s.recordStatus(op, groupName, bypassGuard, nil, 0, 0, 0)
		return nil
	}

	memGroups := 0
	dbGroups := 0
	ratio := 0.0
	if op == "sync_group" {
		for _, g := range s.data.Groups {
			if g != nil {
				memGroups++
			}
		}
	} else {
		for _, g := range s.data.Groups {
			if g != nil {
				memGroups++
			}
		}
		if rows, derr := s.store.ListYouTubeGroupsV2(); derr == nil {
			dbGroups = len(rows)
		}
		if dbGroups > 0 {
			ratio = float64(memGroups) / float64(dbGroups)
		}
	}

	defer func() {
		s.recordStatus(op, groupName, bypassGuard, err, memGroups, dbGroups, ratio)
	}()

	if op != "sync_group" && !bypassGuard {
		if sgErr := s.checkSafetyGuard(); sgErr != nil {
			log.Printf("[WARN] Storage.%s() refused by safety guard: %v (use saveAllReconcile to override)", op, sgErr)
			err = sgErr
			return sgErr
		}
	}

	if bypassGuard {
		// saveAllReconcile path: destructive rewrite.
		for name, g := range s.data.Groups {
			if g == nil {
				continue
			}
			groupType := g.GroupType
			if groupType == "" {
				groupType = "manager"
			}
			groupID, gErr := s.store.UpsertYouTubeGroupV2(name, groupType, "", "")
			if gErr != nil {
				err = fmt.Errorf("save group %q: %w", name, gErr)
				return err
			}
			if gErr := s.replaceGroupMemberships(groupID, g); gErr != nil {
				err = gErr
				return err
			}
		}
		for _, niche := range s.data.TrackedNiches {
			if uErr := s.store.UpsertYouTubeTrackedNiche(niche); uErr != nil {
				err = fmt.Errorf("save tracked niche %q: %w", niche, uErr)
				return err
			}
		}
		return nil
	}

	if op == "sync_group" && groupName != "" {
		g, ok := s.data.Groups[groupName]
		if !ok || g == nil {
			// SyncGroup callers (e.g. per-operation handlers) already hold s.mu
			// and have validated the group exists; reaching here means a direct
			// call would no-op. Surface as ok so the safety-guard record shows a
			// clean run rather than a phantom error.
			for _, niche := range s.data.TrackedNiches {
				if uErr := s.store.UpsertYouTubeTrackedNiche(niche); uErr != nil {
					err = fmt.Errorf("save tracked niche %q: %w", niche, uErr)
					return err
				}
			}
			return nil
		}
		if syncErr := s.syncGroupLocked(groupName, g); syncErr != nil {
			err = syncErr
			return err
		}
		// also push tracked niches (they're orthogonal to groups)
		for _, niche := range s.data.TrackedNiches {
			if uErr := s.store.UpsertYouTubeTrackedNiche(niche); uErr != nil {
				err = fmt.Errorf("save tracked niche %q: %w", niche, uErr)
				return err
			}
		}
		return nil
	}

	// Default save path: per-group diff over every group.
	for name, g := range s.data.Groups {
		if g == nil {
			continue
		}
		if syncErr := s.syncGroupLocked(name, g); syncErr != nil {
			err = syncErr
			return err
		}
	}
	for _, niche := range s.data.TrackedNiches {
		if uErr := s.store.UpsertYouTubeTrackedNiche(niche); uErr != nil {
			err = fmt.Errorf("save tracked niche %q: %w", niche, uErr)
			return err
		}
	}
	return nil
}

// recordStatus normalises a save-status record atomically. Called from
// saveWithStatus (always) and from SaveData on the bypass path.
func (s *Storage) recordStatus(op, groupName string, bypassGuard bool, err error, mem, db int, ratio float64) {
	result := "ok"
	errMsg := ""
	if err != nil {
		if errors.Is(err, ErrSaveRefusedBySafetyGuard) {
			result = "refused_safety_guard"
		} else {
			result = "error"
			errMsg = err.Error()
		}
	}
	s.lastStatusMu.Lock()
	s.lastStatus = &SaveStatus{
		Timestamp:    time.Now().UTC(),
		Operation:    op,
		GroupTarget:  groupName,
		Result:       result,
		Error:        errMsg,
		MemoryGroups: mem,
		DBGroupCount: db,
		Ratio:        ratio,
		BypassGuard:  bypassGuard,
	}
	s.lastStatusMu.Unlock()
}

// LastSaveStatus returns a snapshot of the most recent save outcome, or nil
// if no save has been attempted yet. Safe to call concurrently.
func (s *Storage) LastSaveStatus() *SaveStatus {
	s.lastStatusMu.RLock()
	defer s.lastStatusMu.RUnlock()
	if s.lastStatus == nil {
		return nil
	}
	cp := *s.lastStatus
	return &cp
}

// saveAllReconcile forces a full destructive reconciliation: replaces every
// group and its memberships with the in-memory state. Use ONLY when you
// intentionally want to discard any DB-only state (e.g. one-off repair,
// import). Bypasses the safety guard.
//
// Caller requirement: this method acquires s.mu internally; do NOT hold it
// when calling, or you will deadlock.
func (s *Storage) saveAllReconcile() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveWithStatus("save_all_reconcile", "", true)
}

// SyncGroup persists a single group's in-memory state to SQLite using a
// non-destructive diff. Only the channels belonging to this group are touched.
// Use this instead of save() when only one group has been modified.
func (s *Storage) SyncGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store == nil {
		s.recordStatus("sync_group", name, false, nil, 0, 0, 0)
		return nil
	}
	if _, exists := s.data.Groups[name]; !exists {
		err := ErrGroupNotFound
		s.recordStatus("sync_group", name, false, err, 0, 0, 0)
		return err
	}
	return s.saveWithStatus("sync_group", name, false)
}

// syncGroupLocked must be called with s.mu held. Upserts the group row, upserts
// every channel row, and performs a non-destructive membership diff:
// adds channels that are in memory but missing from DB; removes channels that
// are in DB but no longer in memory for this group. Does NOT touch other groups.
func (s *Storage) syncGroupLocked(name string, g *Group) error {
	groupType := g.GroupType
	if groupType == "" {
		groupType = "manager"
	}

	groupID, err := s.store.UpsertYouTubeGroupV2(name, groupType, "", "")
	if err != nil {
		return fmt.Errorf("upsert group %q: %w", name, err)
	}

	if err := s.diffGroupMemberships(groupID, name, g); err != nil {
		return err
	}
	return nil
}

// diffGroupMemberships applies a non-destructive diff to the membership table
// for a single group. Channels in memory but not in DB are added; channels
// in DB but no longer in memory are removed.
func (s *Storage) diffGroupMemberships(groupID int64, groupName string, g *Group) error {
	desired := make(map[string]Channel, len(g.Channels))
	for _, ch := range g.Channels {
		if ch.ID == "" {
			continue
		}
		desired[ch.ID] = ch

		addedAt := ""
		if !ch.AddedAt.IsZero() {
			addedAt = ch.AddedAt.Format(time.RFC3339)
		}
		lastSync := ""
		if !ch.LastSync.IsZero() {
			lastSync = ch.LastSync.Format(time.RFC3339)
		}

		if err := s.store.UpsertYouTubeChannel(
			ch.ID, ch.Title, ch.Name, ch.URL, ch.Thumbnail,
			ch.Language, ch.Notes,
			ch.ViewCount, ch.SubCount,
			addedAt, lastSync,
		); err != nil {
			return fmt.Errorf("upsert channel %s: %w", safeChannelID(ch.ID), err)
		}
	}

	currentIDs, err := s.store.ListGroupChannelsV2(groupID)
	if err != nil {
		return fmt.Errorf("list memberships for group %q: %w", groupName, err)
	}

	// Empty-channel wipe guard: if the in-memory list for this group is empty
	// while the DB has memberships, refuse to remove every row. Catches stale
	// partial loads where a per-group mutation runs against an un-hydrated
	// Channels slice. The full safety guard on save() catches whole-table wipes;
	// this catches per-group wipes from un-hydrated per-group state.
	if len(desired) == 0 && len(currentIDs) > 0 {
		log.Printf("[WARN] diffGroupMemberships: empty desired slice for group %q (DB has %d memberships); refusing to remove every row to avoid destructive wipe", groupName, len(currentIDs))
		return fmt.Errorf("%w (group=%q, db_had=%d, memory_was_empty)", ErrGroupMembershipRefusedEmptyMemory, groupName, len(currentIDs))
	}

	// Remove stale memberships: in DB but no longer in memory for this group.
	for _, chID := range currentIDs {
		if _, ok := desired[chID]; ok {
			continue
		}
		if err := s.store.RemoveChannelFromGroupV2(groupID, chID); err != nil {
			return fmt.Errorf("remove stale membership %s from group %q: %w", safeChannelID(chID), groupName, err)
		}
	}

	// Add new memberships: in memory but not in DB.
	for chID := range desired {
		found := false
		for _, cid := range currentIDs {
			if cid == chID {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if err := s.store.AddChannelToGroupV2(groupID, chID); err != nil {
			return fmt.Errorf("add membership %s to group %q: %w", safeChannelID(chID), groupName, err)
		}
	}
	return nil
}

// replaceGroupMemberships wipes and re-creates all memberships for a single
// group. Used only by saveAllReconcile; do NOT call from per-operation paths
// because it deletes rows that may be already in the desired state.
func (s *Storage) replaceGroupMemberships(groupID int64, g *Group) error {
	if err := s.store.DeleteYouTubeGroupChannelsByGroupID(groupID); err != nil {
		return fmt.Errorf("clear memberships for group %q: %w", g.Name, err)
	}
	for _, ch := range g.Channels {
		addedAt := ""
		if !ch.AddedAt.IsZero() {
			addedAt = ch.AddedAt.Format(time.RFC3339)
		}
		lastSync := ""
		if !ch.LastSync.IsZero() {
			lastSync = ch.LastSync.Format(time.RFC3339)
		}
		if err := s.store.UpsertYouTubeChannel(
			ch.ID, ch.Title, ch.Name, ch.URL, ch.Thumbnail,
			ch.Language, ch.Notes,
			ch.ViewCount, ch.SubCount,
			addedAt, lastSync,
		); err != nil {
			return fmt.Errorf("upsert channel %s: %w", safeChannelID(ch.ID), err)
		}
		if err := s.store.AddChannelToGroupV2(groupID, ch.ID); err != nil {
			return fmt.Errorf("link channel %s to group %q: %w", safeChannelID(ch.ID), g.Name, err)
		}
	}
	return nil
}
