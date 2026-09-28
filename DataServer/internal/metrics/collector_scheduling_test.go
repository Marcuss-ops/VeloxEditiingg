package metrics

import (
	"strings"
	"testing"

	"velox-server/internal/taskattempts"
)

// TestSchedulingWaitHistogramsAreExposed locks the exposure half of the
// catalog reconciliation: queue/lease/first-worker waits were declared
// histograms long before any family served them — now the endpoint
// serves all three, with the millisecond values the columns carry.
func TestSchedulingWaitHistogramsAreExposed(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)

	c.RecordAttempt(
		taskattempts.AttemptMetrics{QueueMS: 1200, LeaseWaitMS: 300, TimeToFirstWorkerMS: 1500},
		taskattempts.AttemptCacheStats{},
		nil,
		"scene.composite", "3", "default",
	)

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		"velox_queue_wait_ms_count 1",
		"velox_queue_wait_ms_sum 1200",
		"velox_lease_wait_ms_count 1",
		"velox_lease_wait_ms_sum 300",
		"velox_queue_time_to_first_worker_ms_count 1",
		"velox_queue_time_to_first_worker_ms_sum 1500",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestSchedulingWaitHistogramsSkipsUnreportedWaits: a zero column
// means "not reported" (legacy row, pre-074 worker), never an
// observation of an instantaneous dispatch.
func TestSchedulingWaitHistogramsSkipsUnreportedWaits(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)

	c.RecordAttempt(
		taskattempts.AttemptMetrics{},
		taskattempts.AttemptCacheStats{},
		nil,
		"scene.composite", "3", "default",
	)

	out := dumpRegistryAll(t, reg)
	for _, banned := range []string{
		"velox_queue_wait_ms_count 1",
		"velox_lease_wait_ms_count 1",
		"velox_queue_time_to_first_worker_ms_count 1",
	} {
		if strings.Contains(out, banned) {
			t.Errorf("zero-value wait produced an observation (%s):\n%s", banned, out)
		}
	}
}
