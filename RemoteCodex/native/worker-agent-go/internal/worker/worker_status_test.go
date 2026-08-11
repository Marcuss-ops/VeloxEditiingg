package worker

import (
	"io"
	"testing"

	"velox-worker-agent/pkg/logger"
)

func TestCancelJobRemovesPendingOffersAndPreservesOtherJobs(t *testing.T) {
	w := &Worker{
		logger:       logger.New(logger.InfoLevel, io.Discard),
		pendingTasks: make(map[string]*PendingTaskExecution),
		taskIDsByJob: make(map[string][]string),
		activeTasks:  make(map[string]*ActiveTaskExecution),
	}

	w.pendingTasks["task-cancel"] = &PendingTaskExecution{
		TaskID: "task-cancel",
		JobID:  "job-cancel",
	}
	w.pendingTasks["task-keep"] = &PendingTaskExecution{
		TaskID: "task-keep",
		JobID:  "job-keep",
	}

	if !w.cancelJob("job-cancel") {
		t.Fatal("cancelJob returned false for a pending offer")
	}

	if _, ok := w.pendingTasks["task-cancel"]; ok {
		t.Fatal("cancelled pending offer still consumes a worker slot")
	}
	if _, ok := w.pendingTasks["task-keep"]; !ok {
		t.Fatal("pending offer for another job was removed")
	}
}
