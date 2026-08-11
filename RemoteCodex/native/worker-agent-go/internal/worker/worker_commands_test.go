package worker

import (
	"io"
	"sync"
	"testing"
	"time"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestStopAfterActiveTasksWaitsForDrainThenStops(t *testing.T) {
	w := &Worker{
		config:        &config.WorkerConfig{},
		logger:        logger.New(logger.InfoLevel, io.Discard),
		stopChan:      make(chan struct{}),
		activeTasks:   make(map[string]*ActiveTaskExecution),
		pendingTasks:  make(map[string]*PendingTaskExecution),
		activeTasksMu: sync.RWMutex{},
	}
	w.activeTasks["task-running"] = &ActiveTaskExecution{TaskID: "task-running"}

	done := make(chan struct{})
	go func() {
		w.stopAfterActiveTasks()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("restart stopped while an active task was still present")
	case <-time.After(150 * time.Millisecond):
	}

	w.activeTasksMu.Lock()
	delete(w.activeTasks, "task-running")
	w.activeTasksMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("restart did not stop after active task drained")
	}
	if !w.IsStopped() {
		t.Fatal("worker is not stopped after restart drain")
	}
}
