package worker

import "context"

type PublisherPool struct {
	sem chan struct{}
}

func NewPublisherPool(concurrency int) *PublisherPool {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &PublisherPool{sem: make(chan struct{}, concurrency)}
}

func (p *PublisherPool) Acquire(ctx context.Context) error {
	select {
	case p.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PublisherPool) Release() {
	select {
	case <-p.sem:
	default:
	}
}

func (p *PublisherPool) TryAcquire() bool {
	select {
	case p.sem <- struct{}{}:
		return true
	default:
		return false
	}
}
