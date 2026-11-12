package jobdoctor

import (
	"fmt"
	"strings"
	"time"
)

type finalizationFailure struct{ taskID, code, message string }

func (f *finalizationFailure) detail() string {
	return fmt.Sprintf("task=%s finalization failed code=%s error=%s", f.taskID, f.code, f.message)
}

func findFinalizationFailure(tasks []*taskInfo) *finalizationFailure {
	for _, task := range tasks {
		if task.Spec != nil && strings.EqualFold(task.Spec.FinalizeStatus, "FAILED") {
			return &finalizationFailure{taskID: task.TaskID, code: task.Spec.FinalizeErrorCode, message: task.Spec.FinalizeError}
		}
	}
	return nil
}

func collectFailedTasks(tasks []*taskInfo) []FailedTask {
	var failures []FailedTask
	for _, task := range tasks {
		if !strings.EqualFold(task.Status, "FAILED") {
			continue
		}
		failure := FailedTask{TaskID: task.TaskID, ExecutorID: task.ExecutorID, WorkerID: task.WorkerID}
		for i := len(task.Attempts) - 1; i >= 0; i-- {
			if task.Attempts[i].ErrorCode != "" || task.Attempts[i].ErrorMessage != "" {
				failure.WorkerID = task.Attempts[i].WorkerID
				failure.ErrorCode = task.Attempts[i].ErrorCode
				failure.ErrorMessage = task.Attempts[i].ErrorMessage
				break
			}
		}
		failures = append(failures, failure)
	}
	return failures
}

func addFailedTaskEvidence(d *Diagnosis) {
	for _, failure := range d.FailedTasks {
		addEvidence(d, "task_attempts", fmt.Sprintf("task=%s executor=%s worker=%s error_code=%s error=%s", failure.TaskID, failure.ExecutorID, failure.WorkerID, failure.ErrorCode, failure.ErrorMessage))
	}
}

func hasScheduledFinalizationRetry(tasks []*taskInfo, now time.Time) bool {
	for _, task := range tasks {
		if task.Spec == nil || !strings.EqualFold(task.Spec.FinalizeStatus, "RETRY_WAIT") {
			continue
		}
		if retryAt, ok := parseTimestamp(task.Spec.FinalizeRetryAt); ok && retryAt.After(now) {
			return true
		}
	}
	return false
}
