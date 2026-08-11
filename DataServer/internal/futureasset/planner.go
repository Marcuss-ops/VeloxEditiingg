// Package futureasset is the master-side planner seam. It accepts already
// placement-scoped future jobs and delegates deterministic snapshot shaping
// to the shared futureasset contract.
package futureasset

import (
	"fmt"
	"sync/atomic"
	"time"

	shared "velox-shared/futureasset"
)

type FutureAssetPlanner struct {
	now     func() time.Time
	version atomic.Uint64
	limits  shared.Limits
}

func NewPlanner(limits ...shared.Limits) *FutureAssetPlanner {
	configured := shared.Limits{}
	if len(limits) > 0 {
		configured = limits[0]
	}
	return &FutureAssetPlanner{now: func() time.Time { return time.Now().UTC() }, limits: configured}
}

// Build creates the next complete plan for one worker. The input must already
// be ordered N+1.. and filtered to jobs reserved for workerID; this package
// does not guess placement or mutate task lifecycle state.
func (p *FutureAssetPlanner) Build(workerID, currentJobID, planID string, jobs []shared.Job, ttl time.Duration) (shared.Plan, error) {
	if p == nil {
		return shared.Plan{}, fmt.Errorf("futureasset: nil planner")
	}
	if ttl <= 0 {
		return shared.Plan{}, fmt.Errorf("futureasset: plan ttl must be positive")
	}
	now := p.now().UTC()
	version := p.nextVersion(now)
	return shared.Build(shared.PlannerInput{
		Version: version, PlanID: planID, WorkerID: workerID,
		GeneratedAt: now, ExpiresAt: now.Add(ttl), CurrentJob: currentJobID,
		FutureJobs: jobs, Limits: p.limits,
	})
}

// nextVersion remains ordered across planner instances started at different
// times (for example after a Master restart), while the atomic floor keeps
// same-clock and clock-regression calls monotonic within one process.
func (p *FutureAssetPlanner) nextVersion(now time.Time) uint64 {
	candidate := uint64(now.UnixNano())
	for {
		previous := p.version.Load()
		if candidate <= previous {
			candidate = previous + 1
		}
		if p.version.CompareAndSwap(previous, candidate) {
			return candidate
		}
	}
}
