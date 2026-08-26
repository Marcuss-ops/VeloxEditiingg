package observability

import (
	"testing"
	"time"
)

func TestDecodeWaterfallRejectsOverlapAndOutOfBounds(t *testing.T) {
	start := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	raw := `{"waterfall":[{"name":"a","startedAt":"2026-08-26T00:00:00Z","completedAt":"2026-08-26T00:00:05Z","durationMs":5000},{"name":"b","startedAt":"2026-08-26T00:00:04Z","completedAt":"2026-08-26T00:00:06Z","durationMs":2000}]}`
	if stages, valid := decodeWaterfall(raw, &start, &end); valid || stages != nil {
		t.Fatalf("overlapping waterfall = %#v, valid=%v; want rejection", stages, valid)
	}
}

func TestDecodeWaterfallAcceptsContainedSerialStages(t *testing.T) {
	start := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	raw := `{"waterfall":[{"name":"a","startedAt":"2026-08-26T00:00:01Z","completedAt":"2026-08-26T00:00:05Z","durationMs":4000},{"name":"b","startedAt":"2026-08-26T00:00:06Z","completedAt":"2026-08-26T00:00:08Z","durationMs":2000}]}`
	stages, valid := decodeWaterfall(raw, &start, &end)
	if !valid || len(stages) != 2 || stages[1].DurationMS != 2000 {
		t.Fatalf("contained waterfall = %#v, valid=%v; want two valid stages", stages, valid)
	}
}
