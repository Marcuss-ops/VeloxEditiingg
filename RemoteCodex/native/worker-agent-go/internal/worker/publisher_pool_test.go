package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPublisherPoolConcurrency(t *testing.T) {
	pool := NewPublisherPool(3)
	if got := pool.Concurrency(); got != 3 {
		t.Fatalf("Concurrency() = %d, want 3", got)
	}
	if got := NewPublisherPool(0).Concurrency(); got != defaultPublisherConcurrency {
		t.Fatalf("default Concurrency() = %d, want %d", got, defaultPublisherConcurrency)
	}
}

func TestPublisherPoolAcquireArtifactSerializesSameKey(t *testing.T) {
	pool := NewPublisherPool(2)
	first, err := pool.AcquireArtifact(context.Background(), "artifact-1")
	if err != nil {
		t.Fatal(err)
	}
	defer first()

	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		release, err := pool.AcquireArtifact(context.Background(), "artifact-1")
		if err == nil {
			close(acquired)
			release()
		}
		close(done)
	}()

	select {
	case <-acquired:
		t.Fatal("same artifact acquired concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	first()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("same artifact was not released")
	}
	<-done
}

func TestPublisherPoolAllowsDifferentKeysInParallel(t *testing.T) {
	pool := NewPublisherPool(2)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for _, key := range []string{"artifact-1", "artifact-2"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			unlock, err := pool.AcquireArtifact(context.Background(), key)
			if err != nil {
				t.Errorf("AcquireArtifact(%q): %v", key, err)
				return
			}
			defer unlock()
			ready <- struct{}{}
			<-release
		}(key)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("different artifacts did not acquire in parallel")
		}
	}
	close(release)
	wg.Wait()
}

func TestPublisherPoolAcquireArtifactCancellation(t *testing.T) {
	pool := NewPublisherPool(1)
	first, err := pool.AcquireArtifact(context.Background(), "artifact-1")
	if err != nil {
		t.Fatal(err)
	}
	defer first()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.AcquireArtifact(ctx, "artifact-2"); err == nil {
		t.Fatal("expected cancellation while publisher slot is occupied")
	}
}
