package metrics

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/taskattempts"
)

type failingAttemptReader struct {
	fail string
}

func (r failingAttemptReader) GetMetrics(context.Context, string) (*taskattempts.AttemptMetrics, error) {
	if r.fail == "metrics" {
		return nil, errors.New("metrics unavailable")
	}
	return &taskattempts.AttemptMetrics{}, nil
}

func (r failingAttemptReader) GetCacheStats(context.Context, string) (*taskattempts.AttemptCacheStats, error) {
	if r.fail == "cache" {
		return nil, errors.New("cache unavailable")
	}
	return nil, nil
}

func (r failingAttemptReader) GetCostBasis(context.Context, string) (*taskattempts.AttemptCostBasis, error) {
	if r.fail == "cost" {
		return nil, errors.New("cost unavailable")
	}
	return nil, nil
}

func (r failingAttemptReader) GetStatus(context.Context, string) (taskattempts.AttemptStatus, error) {
	if r.fail == "status" {
		return taskattempts.AttemptStatusPending, errors.New("status unavailable")
	}
	return taskattempts.AttemptStatusSucceeded, nil
}

func TestScanAttemptWithLabelsFailsClosedOnReaderErrors(t *testing.T) {
	for _, failure := range []string{"metrics", "cache", "cost", "status"} {
		t.Run(failure, func(t *testing.T) {
			collector := NewCollector(NewRegistry())
			if err := collector.ScanAttemptWithLabels(context.Background(), failingAttemptReader{fail: failure}, "attempt-1", "exec", "v1", "cpu"); err == nil {
				t.Fatalf("ScanAttemptWithLabels returned nil for %s reader failure", failure)
			}
		})
	}
}

func TestScanAttemptWithLabelsRejectsNilMetrics(t *testing.T) {
	collector := NewCollector(NewRegistry())
	reader := failingAttemptReader{}
	if err := collector.ScanAttemptWithLabels(context.Background(), nilMetricsAttemptReader{failingAttemptReader: reader}, "attempt-1", "exec", "v1", "cpu"); err == nil {
		t.Fatal("ScanAttemptWithLabels accepted nil metrics")
	}
}

type nilMetricsAttemptReader struct{ failingAttemptReader }

func (nilMetricsAttemptReader) GetMetrics(context.Context, string) (*taskattempts.AttemptMetrics, error) {
	return nil, nil
}
