package observability

import (
	"encoding/json"
	"time"
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
