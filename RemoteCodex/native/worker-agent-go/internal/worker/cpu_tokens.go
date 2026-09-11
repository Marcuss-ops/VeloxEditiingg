package worker

import (
	"context"
	"fmt"
)

// CPUTokenPool is a small weighted semaphore. One token represents one CPU
// core reserved for native render work. A render acquires all of its tokens
// before entering the executor, so the thread budget is an actual admission
// constraint rather than metadata passed only through environment variables.
//
// A buffered channel is used instead of a mutex/condition variable: partial
// acquisition is rolled back on cancellation and no goroutine is left behind
// to wake a waiter. The pool is intentionally local to a Worker process.
type CPUTokenPool struct {
	tokens chan struct{}
}

func NewCPUTokenPool(capacity int) *CPUTokenPool {
	if capacity < 1 {
		capacity = 1
	}
	p := &CPUTokenPool{tokens: make(chan struct{}, capacity)}
	for i := 0; i < capacity; i++ {
		p.tokens <- struct{}{}
	}
	return p
}

func (p *CPUTokenPool) Acquire(ctx context.Context, count int) error {
	if p == nil {
		return fmt.Errorf("cpu token pool is nil")
	}
	if count < 1 || count > cap(p.tokens) {
		return fmt.Errorf("invalid cpu token request: %d (capacity=%d)", count, cap(p.tokens))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	acquired := 0
	for acquired < count {
		select {
		case <-ctx.Done():
			p.Release(acquired)
			return ctx.Err()
		case <-p.tokens:
			acquired++
		}
	}
	return nil
}

func (p *CPUTokenPool) Release(count int) {
	if p == nil || count <= 0 {
		return
	}
	for i := 0; i < count; i++ {
		p.tokens <- struct{}{}
	}
}

func (p *CPUTokenPool) Capacity() int {
	if p == nil {
		return 0
	}
	return cap(p.tokens)
}
