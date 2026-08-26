package worker

import (
	"context"
	"time"

	"velox-worker-agent/pkg/video/pipeline"
)

// task_progress.go owns the detailed-progress callback surface used by the
// dispatch path: the throttled JobProgress projection onto the active task
// and the lifecycle-edge emission (phase/segment changed/completed) into the
// attempt event machine. It holds the activeTasksMu lock while mutating the
// active task's progress.

// withJobProgressCallback returns a child context carrying the
// canonical progress callback that updates activeTask.Progress under
// the activeTasksMu lock. The callback uses taskID to dynamically
// look up the current entry — never the captured pointer — so a
// later replace would still route to the fresh entry.
func (w *Worker) withJobProgressCallback(parent context.Context, taskID string) context.Context {
	return pipeline.WithDetailedProgressCallback(parent, func(snapshot pipeline.ProgressSnapshot) {
		now := time.Now().UTC()
		w.activeTasksMu.Lock()
		if current := w.activeTasks[taskID]; current != nil {
			previous := current.Progress
			phaseChanged := previous.Phase != snapshot.Phase
			segmentChanged := previous.Segment != snapshot.Segment
			segmentCompleted := snapshot.SegmentCompleted &&
				(!previous.SegmentCompleted || previous.Segment != snapshot.Segment)
			identical := previous.Percent == snapshot.Percent &&
				previous.Scene == snapshot.Scene && previous.TotalScenes == snapshot.TotalScenes &&
				previous.Segment == snapshot.Segment && previous.TotalSegments == snapshot.TotalSegments &&
				previous.SegmentCompleted == snapshot.SegmentCompleted &&
				previous.Phase == snapshot.Phase && !segmentCompleted &&
				previous.FramesEncoded == snapshot.FramesEncoded &&
				previous.FramesDecoded == snapshot.FramesDecoded &&
				previous.FramesComposited == snapshot.FramesComposited &&
				previous.FfmpegSpeedX == snapshot.FfmpegSpeedX &&
				previous.ElapsedMS == snapshot.ElapsedMS &&
				cumulativeMetricsEqual(previous.CumulativeMetrics, snapshot.CumulativeMetrics)
			publishDue := !identical && (previous.LastPublishedAt.IsZero() ||
				now.Sub(previous.LastPublishedAt) >= 2*time.Second || phaseChanged || segmentChanged || segmentCompleted)

			metrics := make(map[string]float64, len(snapshot.CumulativeMetrics))
			for key, value := range snapshot.CumulativeMetrics {
				metrics[key] = value
			}
			// Keep the latest snapshot in the same canonical Attempt
			// projection even when heartbeat publication is throttled.
			// LastProgressAt describes the newest engine observation;
			// LastPublishedAt is only the local wake/throttle clock and
			// is never serialized as operator telemetry.
			if current.AttemptEvents != nil {
				// Emit lifecycle edges before the progress sample updates the
				// machine's last phase/segment context. Segment completion is
				// emitted after the sample because ProgressUpdated resets the
				// completion edge for the next segment.
				if phaseChanged {
					current.AttemptEvents.PhaseChanged(snapshot.Phase)
				}
				if segmentChanged {
					current.AttemptEvents.SegmentStarted(snapshot.Segment, snapshot.Phase)
				}
				current.AttemptEvents.ProgressUpdated(snapshot.Phase, snapshot.Segment, snapshot.Percent, snapshot.ElapsedMS, snapshot.FramesEncoded, now)
				if segmentCompleted {
					current.AttemptEvents.SegmentCompleted(snapshot.Segment, snapshot.Phase)
				}
			}
			current.Progress = JobProgress{
				Percent:     snapshot.Percent,
				Scene:       snapshot.Scene,
				TotalScenes: snapshot.TotalScenes, Segment: snapshot.Segment,
				TotalSegments:     snapshot.TotalSegments,
				SegmentCompleted:  snapshot.SegmentCompleted,
				Phase:             snapshot.Phase,
				Stage:             snapshot.Phase,
				FramesEncoded:     snapshot.FramesEncoded,
				FramesDecoded:     snapshot.FramesDecoded,
				FramesComposited:  snapshot.FramesComposited,
				FfmpegSpeedX:      snapshot.FfmpegSpeedX,
				ElapsedMS:         snapshot.ElapsedMS,
				LastProgressAt:    now,
				LastPublishedAt:   previous.LastPublishedAt,
				CumulativeMetrics: metrics,
			}
			if publishDue {
				current.Progress.LastPublishedAt = now
				w.wakeHeartbeat()
			}
		}
		w.activeTasksMu.Unlock()
	})
}

func cumulativeMetricsEqual(left, right map[string]float64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}
