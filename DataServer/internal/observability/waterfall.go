package observability

import (
	"encoding/json"
	"strconv"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

type rawWaterfallReport struct {
	Waterfall []struct {
		Name        string    `json:"name"`
		StartedAt   time.Time `json:"startedAt"`
		CompletedAt time.Time `json:"completedAt"`
		DurationMS  int64     `json:"durationMs"`
		Status      string    `json:"status"`
	} `json:"waterfall"`
}

type rawAttemptMilestone struct {
	Name       string          `json:"name"`
	Sequence   json.RawMessage `json:"sequence"`
	ElapsedMS  json.RawMessage `json:"elapsed_ms"`
	ElapsedMs  json.RawMessage `json:"elapsedMs"`
	OccurredAt string          `json:"occurred_at"`
}

type rawAttemptMilestoneReport struct {
	Milestones []rawAttemptMilestone `json:"milestones"`
}

// decodeAttemptWaterfall reads the worker's monotonic milestone timeline from
// the durable raw report. It deliberately returns no waterfall when the
// report predates milestone support; callers must expose that as unknown.
func decodeAttemptWaterfall(raw string, attemptID string, wallMS int64) *AttemptWaterfall {
	if raw == "" || wallMS < 0 {
		return nil
	}
	var report rawAttemptMilestoneReport
	if json.Unmarshal([]byte(raw), &report) != nil || len(report.Milestones) == 0 {
		return nil
	}
	samples := make([]sharedtelemetry.AttemptMilestoneSample, 0, len(report.Milestones))
	for _, milestone := range report.Milestones {
		elapsed := parseJSONInt(milestone.ElapsedMS)
		if elapsed == 0 {
			elapsed = parseJSONInt(milestone.ElapsedMs)
		}
		samples = append(samples, sharedtelemetry.AttemptMilestoneSample{
			Name: sharedtelemetry.AttemptMilestone(milestone.Name), Sequence: parseJSONUint(milestone.Sequence),
			ElapsedMS: elapsed, OccurredAt: milestone.OccurredAt,
		})
	}
	waterfall := BuildAttemptWaterfall(attemptID, samples, wallMS)
	return &waterfall
}

func parseJSONInt(raw json.RawMessage) int64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var value int64
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		value, _ = strconv.ParseInt(text, 10, 64)
	}
	return value
}

func parseJSONUint(raw json.RawMessage) uint64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var value uint64
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		value, _ = strconv.ParseUint(text, 10, 64)
	}
	return value
}

func decodeWaterfall(raw string, start, end *time.Time) ([]WaterfallStage, bool) {
	if raw == "" || start == nil || end == nil {
		return nil, false
	}
	var report rawWaterfallReport
	if json.Unmarshal([]byte(raw), &report) != nil || len(report.Waterfall) == 0 {
		return nil, false
	}
	result := make([]WaterfallStage, 0, len(report.Waterfall))
	var previous time.Time
	for _, stage := range report.Waterfall {
		if stage.Name == "" || stage.StartedAt.IsZero() || stage.CompletedAt.Before(stage.StartedAt) || stage.StartedAt.Before(*start) || stage.CompletedAt.After(*end) || (!previous.IsZero() && stage.StartedAt.Before(previous)) || stage.DurationMS != stage.CompletedAt.Sub(stage.StartedAt).Milliseconds() {
			return nil, false
		}
		result = append(result, WaterfallStage{Name: stage.Name, StartedAt: stage.StartedAt, CompletedAt: stage.CompletedAt, DurationMS: stage.DurationMS, Status: stage.Status})
		previous = stage.CompletedAt
	}
	return result, true
}
