package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const defaultPublisherConcurrency = 4

// PublisherPool bounds concurrent publication work while serializing the
// lifecycle of one artifact/spool ID. Different artifacts may publish in
// parallel; foreground publish and resume for the same key cannot overlap.
type PublisherPool struct {
	sem chan struct{}

	mu    sync.Mutex
	locks map[string]*publisherKeyLock
}

type publisherKeyLock struct {
	refs int
	mu   sync.Mutex
}

func NewPublisherPool(concurrency int) *PublisherPool {
	if concurrency <= 0 {
		concurrency = defaultPublisherConcurrency
	}
	return &PublisherPool{
		sem:   make(chan struct{}, concurrency),
		locks: make(map[string]*publisherKeyLock),
	}
}

// Concurrency returns the configured maximum number of simultaneous
// publishers. It is useful for diagnostics and tests.
func (p *PublisherPool) Concurrency() int {
	if p == nil || p.sem == nil {
		return 0
	}
	return cap(p.sem)
}

func (p *PublisherPool) Acquire(ctx context.Context) error {
	if p == nil || p.sem == nil {
		return fmt.Errorf("publisher pool is not configured")
	}
	select {
	case p.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PublisherPool) Release() {
	if p == nil || p.sem == nil {
		return
	}
	select {
	case <-p.sem:
	default:
	}
}

func (p *PublisherPool) TryAcquire() bool {
	if p == nil || p.sem == nil {
		return false
	}
	select {
	case p.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// AcquireArtifact acquires the global publisher slot and the per-key lock.
// The returned function releases both in the reverse order and is safe to
// call once. An empty key is rejected because unkeyed serialization would
// recreate the old worker-wide mutex semantics.
func (p *PublisherPool) AcquireArtifact(ctx context.Context, key string) (func(), error) {
	if key == "" {
		return nil, fmt.Errorf("publisher artifact key is required")
	}
	if err := p.Acquire(ctx); err != nil {
		return nil, err
	}

	lock := p.keyLock(key)
	if err := acquireMutex(ctx, &lock.mu); err != nil {
		p.Release()
		p.releaseKeyLock(key, lock)
		return nil, err
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			lock.mu.Unlock()
			p.releaseKeyLock(key, lock)
			p.Release()
		})
	}, nil
}

func acquireMutex(ctx context.Context, mu *sync.Mutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keyed locks are held only after a publisher slot is acquired. Polling
	// keeps cancellation observable without spawning an unowned goroutine.
	for {
		if mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}
}

func (p *PublisherPool) keyLock(key string) *publisherKeyLock {
	p.mu.Lock()
	defer p.mu.Unlock()
	lock := p.locks[key]
	if lock == nil {
		lock = &publisherKeyLock{}
		p.locks[key] = lock
	}
	lock.refs++
	return lock
}

func (p *PublisherPool) releaseKeyLock(key string, lock *publisherKeyLock) {
	p.mu.Lock()
	defer p.mu.Unlock()
	lock.refs--
	if lock.refs == 0 {
		delete(p.locks, key)
	}
}
