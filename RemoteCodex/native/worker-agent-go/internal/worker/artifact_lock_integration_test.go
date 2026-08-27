package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

// These tests exercise the exact lock acquisition shapes used by the
// foreground publisher and recovery path. They deliberately avoid the full
// Worker composition so they remain deterministic and isolate the contract
// owned by ArtifactLockRegistry.
func TestArtifactLockRegistryForegroundAndResumeSerializeSameArtifact(t *testing.T) {
	registry := NewArtifactLockRegistry()
	foregroundRelease, err := registry.AcquireMany(context.Background(), []string{"spool-a", "spool-a"})
	if err != nil {
		t.Fatal(err)
	}

	resumeAcquired := make(chan struct{})
	resumeDone := make(chan struct{})
	go func() {
		defer close(resumeDone)
		release, acquireErr := registry.Acquire(context.Background(), "spool-a")
		if acquireErr != nil {
			t.Errorf("resume acquire: %v", acquireErr)
			return
		}
		close(resumeAcquired)
		release()
	}()

	select {
	case <-resumeAcquired:
		t.Fatal("resume acquired while foreground held the same artifact")
	case <-time.After(25 * time.Millisecond):
	}

	foregroundRelease()
	select {
	case <-resumeAcquired:
	case <-time.After(time.Second):
		t.Fatal("resume did not acquire after foreground released")
	}
	<-resumeDone
}

func TestArtifactLockRegistryForegroundAndResumeAllowDifferentArtifacts(t *testing.T) {
	registry := NewArtifactLockRegistry()
	foregroundRelease, err := registry.AcquireMany(context.Background(), []string{"spool-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer foregroundRelease()

	resumeAcquired := make(chan struct{})
	resumeDone := make(chan struct{})
	go func() {
		defer close(resumeDone)
		release, acquireErr := registry.Acquire(context.Background(), "spool-b")
		if acquireErr != nil {
			t.Errorf("resume acquire: %v", acquireErr)
			return
		}
		close(resumeAcquired)
		release()
	}()

	select {
	case <-resumeAcquired:
	case <-time.After(time.Second):
		t.Fatal("resume for a different artifact was serialized behind foreground")
	}
	<-resumeDone
}

func TestArtifactLockRegistryAcquireManyDeterministicAcrossForegroundResume(t *testing.T) {
	registry := NewArtifactLockRegistry()
	firstRelease, err := registry.AcquireMany(context.Background(), []string{"spool-b", "spool-a", "spool-b"})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	started := make(chan struct{}, 2)
	for _, key := range []string{"spool-a", "spool-b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			release, acquireErr := registry.Acquire(context.Background(), key)
			if acquireErr != nil {
				t.Errorf("acquire %s: %v", key, acquireErr)
				return
			}
			started <- struct{}{}
			release()
		}(key)
	}

	select {
	case <-started:
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case <-started:
		t.Fatal("AcquireMany did not hold both unique artifact locks")
	case <-time.After(25 * time.Millisecond):
	}

	firstRelease()
	wg.Wait()
}
