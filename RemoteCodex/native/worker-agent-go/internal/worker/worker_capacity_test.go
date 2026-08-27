package worker

import (
	"testing"

	"velox-worker-agent/pkg/video/pipeline"
)

func TestCountRenderOccupyingTasksReleasesPublishingPhase(t *testing.T) {
	tasks := map[string]*ActiveTaskExecution{
		"render":    {OperationalPhase: pipeline.PhaseRendering},
		"publish":   {OperationalPhase: pipeline.PhasePublishing},
		"commit":    {OperationalPhase: pipeline.PhaseCommitWait},
		"unstarted": {},
	}
	if got := countRenderOccupyingTasks(tasks); got != 2 {
		t.Fatalf("render-occupying tasks = %d, want 2", got)
	}
}
