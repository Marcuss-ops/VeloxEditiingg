package darkeditor

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBackgroundTaskRepositoryReturnsCopiesAndUpdatesSafely(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	r := newBackgroundTaskRepository(time.Hour, 100, 0, func() time.Time { return now })
	defer r.Close()

	r.Set(BackgroundRemovalStatus{
		TaskID:    "task-1",
		Status:    "pending",
		StartedAt: now,
	})
	got, ok := r.Get("task-1")
	if !ok {
		t.Fatal("Get(task-1) = not found")
	}
	got.Status = "tampered"
	got.EndedAt = now.Add(time.Minute)

	again, ok := r.Get("task-1")
	if !ok {
		t.Fatal("Get(task-1) after mutation = not found")
	}
	if again.Status != "pending" || !again.EndedAt.IsZero() {
		t.Fatalf("repository exposed internal state: %#v", again)
	}

	if !r.Update("task-1", func(status *BackgroundRemovalStatus) {
		status.Status = "completed"
		status.Filename = "output.png"
	}) {
		t.Fatal("Update(task-1) = false")
	}
	updated, ok := r.Get("task-1")
	if !ok || updated.Status != "completed" || updated.Filename != "output.png" {
		t.Fatalf("updated status = %#v, found=%v", updated, ok)
	}
}

func TestBackgroundTaskRepositoryConcurrentReadsWrites(t *testing.T) {
	r := newBackgroundTaskRepository(time.Hour, 1000, 0, time.Now)
	defer r.Close()

	const workers = 32
	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				id := fmt.Sprintf("task-%d", (worker*iterations+i)%100)
				r.Set(BackgroundRemovalStatus{TaskID: id, Status: "pending", StartedAt: time.Now()})
				_, _ = r.Get(id)
				_ = r.Update(id, func(status *BackgroundRemovalStatus) {
					status.Status = "processing"
				})
				_, _ = r.Get(id)
			}
		}()
	}
	wg.Wait()

	if got := r.Len(); got == 0 || got > 100 {
		t.Fatalf("repository length = %d, want 1..100", got)
	}
}

func TestBackgroundTaskRepositoryExpiresByTTL(t *testing.T) {
	current := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	r := newBackgroundTaskRepository(time.Minute, 10, 0, func() time.Time { return current })
	defer r.Close()

	r.Set(BackgroundRemovalStatus{TaskID: "expired", Status: "completed", StartedAt: current.Add(-time.Minute)})
	r.Set(BackgroundRemovalStatus{TaskID: "live", Status: "processing", StartedAt: current})

	if _, ok := r.Get("expired"); ok {
		t.Fatal("expired task was returned")
	}
	if _, ok := r.Get("live"); !ok {
		t.Fatal("live task was removed")
	}
	if got := r.Len(); got != 1 {
		t.Fatalf("repository length = %d, want 1", got)
	}
}

func TestBackgroundTaskRepositoryEvictsOldestWhenFull(t *testing.T) {
	current := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	r := newBackgroundTaskRepository(time.Hour, 2, 0, func() time.Time { return current })
	defer r.Close()

	r.Set(BackgroundRemovalStatus{TaskID: "oldest", Status: "completed", StartedAt: current})
	current = current.Add(time.Second)
	r.Set(BackgroundRemovalStatus{TaskID: "middle", Status: "completed", StartedAt: current})
	current = current.Add(time.Second)
	r.Set(BackgroundRemovalStatus{TaskID: "newest", Status: "completed", StartedAt: current})

	if _, ok := r.Get("oldest"); ok {
		t.Fatal("oldest task was not evicted")
	}
	if _, ok := r.Get("middle"); !ok {
		t.Fatal("middle task was evicted")
	}
	if _, ok := r.Get("newest"); !ok {
		t.Fatal("newest task was evicted")
	}
}

func TestBackgroundTaskRepositoryEvictsTerminalTasksBeforeActiveTasks(t *testing.T) {
	current := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	r := newBackgroundTaskRepository(time.Hour, 2, 0, func() time.Time { return current })
	defer r.Close()

	r.Set(BackgroundRemovalStatus{TaskID: "active-old", Status: "processing", StartedAt: current})
	current = current.Add(time.Second)
	r.Set(BackgroundRemovalStatus{TaskID: "terminal", Status: "completed", StartedAt: current})
	current = current.Add(time.Second)
	r.Set(BackgroundRemovalStatus{TaskID: "active-new", Status: "pending", StartedAt: current})

	if _, ok := r.Get("terminal"); ok {
		t.Fatal("terminal task was not evicted first")
	}
	if _, ok := r.Get("active-old"); !ok {
		t.Fatal("older active task was evicted while a terminal task was available")
	}
	if _, ok := r.Get("active-new"); !ok {
		t.Fatal("newer active task was evicted while a terminal task was available")
	}
}

func TestBackgroundTaskRepositoryCleanupStops(t *testing.T) {
	r := newBackgroundTaskRepository(time.Hour, 2, time.Millisecond, time.Now)
	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not stop cleanup goroutine")
	}
	// Close is idempotent.
	r.Close()
}
