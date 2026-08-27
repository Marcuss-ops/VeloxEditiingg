package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestArtifactLockRegistrySerializesSameArtifact(t *testing.T) {
	r := NewArtifactLockRegistry()
	first, err := r.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer first()

	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		release, err := r.Acquire(context.Background(), "a")
		if err == nil {
			close(acquired)
			release()
		}
		close(done)
	}()
	select {
	case <-acquired:
		t.Fatal("same artifact acquired concurrently")
	case <-time.After(20 * time.Millisecond):
	}
	first()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("same artifact did not acquire after release")
	}
	<-done
}

func TestArtifactLockRegistryAllowsDifferentArtifacts(t *testing.T) {
	r := NewArtifactLockRegistry()
	ready := make(chan struct{}, 2)
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			release, err := r.Acquire(context.Background(), key)
			if err != nil {
				t.Errorf("Acquire(%s): %v", key, err)
				return
			}
			defer release()
			ready <- struct{}{}
			<-gate
		}(key)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("different artifacts did not acquire in parallel")
		}
	}
	close(gate)
	wg.Wait()
}

func TestArtifactLockRegistryAcquireManyDeduplicatesAndReleases(t *testing.T) {
	r := NewArtifactLockRegistry()
	release, err := r.AcquireMany(context.Background(), []string{"b", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := r.AcquireMany(context.Background(), []string{""}); err == nil {
		t.Fatal("expected empty key error")
	}
}

func TestArtifactLockRegistryCancellation(t *testing.T) {
	r := NewArtifactLockRegistry()
	release, err := r.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := r.Acquire(ctx, "a"); err == nil {
		t.Fatal("expected cancellation")
	}
}
