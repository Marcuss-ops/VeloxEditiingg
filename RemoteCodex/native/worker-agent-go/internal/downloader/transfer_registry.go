package downloader

import (
	"sync"
	"time"

	"velox-shared/assetref"
)

// TransferRegistry owns the set of transfers, keyed by AssetKey. Terminal
// transfers are retained up to the manager's configured bound so recent
// Snapshot/JobSnapshot reads remain available without unbounded growth.
type TransferRegistry struct {
	mu        sync.Mutex
	transfers map[assetref.AssetKey]*Transfer
}

func newTransferRegistry() *TransferRegistry {
	return &TransferRegistry{transfers: make(map[assetref.AssetKey]*Transfer)}
}

// Get returns the transfer for key (nil when absent).
func (r *TransferRegistry) Get(key assetref.AssetKey) *Transfer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.transfers[key]
}

// GetOrCreate returns the transfer for key when it exists, otherwise creates
// one via mk and stores it. `created` reports whether mk ran.
func (r *TransferRegistry) GetOrCreate(key assetref.AssetKey, mk func() *Transfer) (t *Transfer, created bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.transfers[key]; existing != nil {
		return existing, false
	}
	t = mk()
	r.transfers[key] = t
	return t, true
}

// PruneTerminal retains at most max terminal transfers. Live transfers are
// never evicted. The oldest completed terminal entries are removed first.
func (r *TransferRegistry) PruneTerminal(max int) {
	if max <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	terminalCount := 0
	for _, t := range r.transfers {
		if t.isTerminal() {
			terminalCount++
		}
	}
	for terminalCount > max {
		var oldestKey assetref.AssetKey
		oldestAt := time.Time{}
		for key, t := range r.transfers {
			if !t.isTerminal() {
				continue
			}
			t.mu.Lock()
			completedAt := t.completedAt
			t.mu.Unlock()
			if oldestKey == "" || completedAt.Before(oldestAt) {
				oldestKey, oldestAt = key, completedAt
			}
		}
		if oldestKey == "" {
			break
		}
		delete(r.transfers, oldestKey)
		terminalCount--
	}
}

// Each visits every registered transfer (deterministic key order).
func (r *TransferRegistry) Each(f func(key assetref.AssetKey, t *Transfer)) {
	r.mu.Lock()
	keys := make([]assetref.AssetKey, 0, len(r.transfers))
	for k := range r.transfers {
		keys = append(keys, k)
	}
	r.mu.Unlock()
	for _, k := range keys {
		if t := r.Get(k); t != nil {
			f(k, t)
		}
	}
}
