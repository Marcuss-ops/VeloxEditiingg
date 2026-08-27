package worker

import (
	"context"
	"sync"
	"time"

	"velox-worker-agent/pkg/video/pipeline"
)

var artifactProgressByContext sync.Map

func (w *Worker) withJobProgressCallback(parent context.Context, taskID string) context.Context {
	return pipeline.WithArtifactWriteCallback(
		pipeline.WithDetailedProgressCallback(parent, w.detailedProgressCallback(taskID)),
		w.artifactWriteProgressCallback(taskID),
	)
}

func (w *Worker) detailedProgressCallback(taskID string) pipeline.DetailedProgressFunc {
	return func(snapshot pipeline.ProgressSnapshot) {
		now := time.Now().UTC()
		w.activeTasksMu.Lock()
		if current := w.activeTasks[taskID]; current != nil {
			previous := current.Progress
			phaseChanged := previous.Phase != snapshot.Phase
			segmentChanged := previous.Segment != snapshot.Segment
			segmentCompleted := snapshot.SegmentCompleted && (!previous.SegmentCompleted || previous.Segment != snapshot.Segment)
			identical := previous.Percent == snapshot.Percent && previous.Scene == snapshot.Scene && previous.TotalScenes == snapshot.TotalScenes && previous.Segment == snapshot.Segment && previous.TotalSegments == snapshot.TotalSegments && previous.SegmentCompleted == snapshot.SegmentCompleted && previous.Phase == snapshot.Phase && !segmentCompleted && previous.FramesEncoded == snapshot.FramesEncoded && previous.FramesDecoded == snapshot.FramesDecoded && previous.FramesComposited == snapshot.FramesComposited && previous.FfmpegSpeedX == snapshot.FfmpegSpeedX && previous.ElapsedMS == snapshot.ElapsedMS && cumulativeMetricsEqual(previous.CumulativeMetrics, snapshot.CumulativeMetrics)
			publishDue := !identical && (previous.LastPublishedAt.IsZero() || now.Sub(previous.LastPublishedAt) >= 2*time.Second || phaseChanged || segmentChanged || segmentCompleted)
			metrics := make(map[string]float64, len(snapshot.CumulativeMetrics))
			for key, value := range snapshot.CumulativeMetrics {
				metrics[key] = value
			}
			if current.AttemptEvents != nil {
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
			current.Progress = JobProgress{Percent: snapshot.Percent, Scene: snapshot.Scene, TotalScenes: snapshot.TotalScenes, Segment: snapshot.Segment, TotalSegments: snapshot.TotalSegments, SegmentCompleted: snapshot.SegmentCompleted, Phase: snapshot.Phase, Stage: snapshot.Phase, FramesEncoded: snapshot.FramesEncoded, FramesDecoded: snapshot.FramesDecoded, FramesComposited: snapshot.FramesComposited, FfmpegSpeedX: snapshot.FfmpegSpeedX, ElapsedMS: snapshot.ElapsedMS, LastProgressAt: now, LastPublishedAt: previous.LastPublishedAt, CumulativeMetrics: metrics}
			if publishDue {
				current.Progress.LastPublishedAt = now
				w.wakeHeartbeat()
			}
		}
		w.activeTasksMu.Unlock()
	}
}

func (w *Worker) artifactWriteProgressCallback(taskID string) pipeline.ArtifactWriteProgressFunc {
	return func(progress pipeline.ArtifactWriteProgress) {
		if progress.SafeOffsetBytes <= 0 && !progress.Finalized {
			return
		}
		if progress.Finalized && progress.FinalizedAt.IsZero() {
			progress.FinalizedAt = time.Now()
		}
		artifactProgressByContext.Store(taskID+"\x00"+progress.Artifact, progress)
		w.activeTasksMu.Lock()
		if current := w.activeTasks[taskID]; current != nil {
			metrics := current.Progress.CumulativeMetrics
			if metrics == nil {
				metrics = make(map[string]float64)
			}
			metrics["artifact.high_watermark_bytes"] = float64(progress.HighWatermarkBytes)
			metrics["artifact.safe_offset_bytes"] = float64(progress.SafeOffsetBytes)
			if progress.Finalized {
				metrics["artifact.finalized"] = 1
			} else {
				metrics["artifact.finalized"] = 0
			}
			current.Progress.CumulativeMetrics = metrics
			w.wakeHeartbeat()
		}
		w.activeTasksMu.Unlock()
	}
}

func artifactProgressForTask(ctx context.Context, artifact string) pipeline.ArtifactWriteProgress {
	if ctx == nil {
		return pipeline.ArtifactWriteProgress{}
	}
	taskID, _ := ctx.Value(progressTaskKey{}).(string)
	if value, ok := artifactProgressByContext.Load(taskID + "\x00" + artifact); ok {
		return value.(pipeline.ArtifactWriteProgress)
	}
	return pipeline.ArtifactWriteProgress{}
}

type progressTaskKey struct{}

func withProgressTaskID(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, progressTaskKey{}, taskID)
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
