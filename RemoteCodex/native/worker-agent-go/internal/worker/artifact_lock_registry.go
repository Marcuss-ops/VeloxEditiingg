package worker

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type artifactLockEntry struct {
	mu   sync.Mutex
	refs int
}

type ArtifactLockRegistry struct {
	mu    sync.Mutex
	locks map[string]*artifactLockEntry
}

func NewArtifactLockRegistry() *ArtifactLockRegistry {
	return &ArtifactLockRegistry{locks: make(map[string]*artifactLockEntry)}
}

func (r *ArtifactLockRegistry) Acquire(ctx context.Context, key string) (func(), error) {
	if r == nil || key == "" {
		return nil, fmt.Errorf("artifact lock: empty key or nil registry")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	entry := r.locks[key]
	if entry == nil {
		entry = &artifactLockEntry{}
		r.locks[key] = entry
	}
	entry.refs++
	r.mu.Unlock()
	locked := make(chan struct{})
	go func() { entry.mu.Lock(); close(locked) }()
	select {
	case <-locked:
		var once sync.Once
		return func() {
			once.Do(func() {
				entry.mu.Unlock()
				r.mu.Lock()
				entry.refs--
				if entry.refs == 0 {
					delete(r.locks, key)
				}
				r.mu.Unlock()
			})
		}, nil
	case <-ctx.Done():
		go func() {
			<-locked
			entry.mu.Unlock()
			r.mu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(r.locks, key)
			}
			r.mu.Unlock()
		}()
		return nil, ctx.Err()
	}
}

func (r *ArtifactLockRegistry) AcquireMany(ctx context.Context, keys []string) (func(), error) {
	unique := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("artifact lock: empty key")
		}
		unique[key] = struct{}{}
	}
	ordered := make([]string, 0, len(unique))
	for key := range unique {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	releases := make([]func(), 0, len(ordered))
	for _, key := range ordered {
		release, err := r.Acquire(ctx, key)
		if err != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return nil, err
		}
		releases = append(releases, release)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		})
	}, nil
}

// TryAcquireMany takes all requested artifact locks without waiting. It is
// used by opportunistic recovery so foreground publication remains responsive
// and lock contention is treated as a normal skip rather than an upload error.
func (r *ArtifactLockRegistry) TryAcquireMany(keys []string) (func(), bool) {
	if r == nil {
		return nil, false
	}
	unique := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			return nil, false
		}
		unique[key] = struct{}{}
	}
	ordered := make([]string, 0, len(unique))
	for key := range unique {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)

	releases := make([]func(), 0, len(ordered))
	for _, key := range ordered {
		r.mu.Lock()
		entry := r.locks[key]
		if entry == nil {
			entry = &artifactLockEntry{}
			if r.locks == nil {
				r.locks = make(map[string]*artifactLockEntry)
			}
			r.locks[key] = entry
		}
		entry.refs++
		r.mu.Unlock()

		if !entry.mu.TryLock() {
			r.mu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(r.locks, key)
			}
			r.mu.Unlock()
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return nil, false
		}

		capturedKey := key
		capturedEntry := entry
		releases = append(releases, func() {
			capturedEntry.mu.Unlock()
			r.mu.Lock()
			capturedEntry.refs--
			if capturedEntry.refs == 0 {
				delete(r.locks, capturedKey)
			}
			r.mu.Unlock()
		})
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		})
	}, true
}
