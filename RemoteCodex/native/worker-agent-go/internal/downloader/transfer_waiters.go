package downloader

import "errors"

// waiterKey identifies one logical Resolve call. The request metadata is
// retained separately in jobRefs; the unique ID prevents two simultaneous
// resolves from the same job/task from overwriting each other.
type waiterKey uint64

// errTransferCancelled is the sentinel surfaced to waiters when the transfer
// was cancelled (last waiter left or the manager closed).
var errTransferCancelled = errors.New("downloader: transfer cancelled")

// addWaiter registers one logical Resolve call. SharedWaiters in snapshots
// equals the number of live calls, even when request metadata is identical.
func (t *Transfer) addWaiter(id uint64, req DownloadRequest) {
	t.mu.Lock()
	t.waiters[waiterKey(id)] = struct{}{}
	if req.JobID != "" {
		refKey := req.JobID + "\x00" + req.TaskID
		ref := t.jobRefs[refKey]
		ref.JobID = req.JobID
		ref.TaskID = req.TaskID
		ref.SceneIDs = mergeStrings(ref.SceneIDs, req.SceneIDs)
		t.jobRefs[refKey] = ref
	}
	for _, sceneID := range req.SceneIDs {
		if sceneID != "" {
			t.sceneIDs[sceneID] = struct{}{}
		}
	}
	t.updatedAt = t.now()
	t.mu.Unlock()
}

// removeWaiter unregisters one waiter and reports whether it was the last one.
func (t *Transfer) removeWaiter(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.waiters, waiterKey(id))
	return len(t.waiters) == 0
}

// hasJobReference reports whether this transfer belongs to jobID's aggregate.
// Unlike the active waiter set, it remains true after Resolve returns so the
// job read model can report the completed asset.
func (t *Transfer) hasJobReference(jobID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ref := range t.jobRefs {
		if ref.JobID == jobID {
			return true
		}
	}
	return false
}
