package worker

import (
	"testing"
	"time"
)

func TestTaskFailureBackoffIsExponentialAndBounded(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{0, 2 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 30 * time.Second},
		{10, 30 * time.Second},
	} {
		if got := taskFailureBackoff(tc.attempt); got != tc.want {
			t.Errorf("taskFailureBackoff(%d)=%s want %s", tc.attempt, got, tc.want)
		}
	}
}
