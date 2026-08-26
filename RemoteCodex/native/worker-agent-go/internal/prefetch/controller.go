// Package prefetch reconciles worker-scoped FutureAssetPlan snapshots.
//
// This package is intentionally a planning layer only. It does not download,
// verify, evict, or delete files. Those operations remain owned by the
// canonical CacheResolver, AssetDownloadManager, Transferer, and workercache.
package prefetch

import (
	"fmt"
	"sync"
	"time"

	"velox-shared/futureasset"
)

type Controller struct {
	mu       sync.Mutex
	workerID string
	now      func() time.Time
	version  uint64
	active   map[string]futureasset.Job
	protect  map[string]futureasset.ProtectedAsset
}

type ReconcileResult struct {
	Applied       bool
	Stale         bool
	Expired       bool
	Added         []futureasset.Job
	Removed       []futureasset.Job
	Reprioritized []futureasset.Job
}

func NewController(workerID string) *Controller {
	return &Controller{workerID: workerID, now: time.Now, active: make(map[string]futureasset.Job), protect: make(map[string]futureasset.ProtectedAsset)}
}

func (c *Controller) Apply(plan futureasset.Plan) (ReconcileResult, error) {
	if c == nil {
		return ReconcileResult{}, fmt.Errorf("prefetch: nil controller")
	}
	if err := plan.Validate(); err != nil {
		return ReconcileResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if plan.WorkerID != c.workerID {
		return ReconcileResult{}, fmt.Errorf("prefetch: plan worker_id=%q does not match worker=%q", plan.WorkerID, c.workerID)
	}
	if plan.Version <= c.version {
		return ReconcileResult{Stale: true}, nil
	}
	result := ReconcileResult{Applied: true}
	// Index the incoming plan's jobs once so the removal sweep below is O(1)
	// per active job instead of a linear scan of plan.PrefetchJobs per job
	// (the previous O(n²) findJob loop).
	scheduled := make(map[string]struct{}, len(plan.PrefetchJobs))
	for _, job := range plan.PrefetchJobs {
		scheduled[job.JobID] = struct{}{}
	}
	for jobID, old := range c.active {
		if _, ok := scheduled[jobID]; !ok {
			result.Removed = append(result.Removed, old)
			delete(c.active, jobID)
		}
	}
	if plan.Expired(c.now()) {
		result.Expired = true
	} else {
		for _, job := range plan.PrefetchJobs {
			old, exists := c.active[job.JobID]
			if !exists {
				c.active[job.JobID] = job
				result.Added = append(result.Added, job)
				continue
			}
			if old.Distance != job.Distance || old.ReservationID != job.ReservationID || old.TaskRevision != job.TaskRevision {
				c.active[job.JobID] = job
				result.Reprioritized = append(result.Reprioritized, job)
			}
		}
	}
	c.protect = make(map[string]futureasset.ProtectedAsset, len(plan.Protect))
	for _, asset := range plan.Protect {
		c.protect[asset.AssetKey] = asset
	}
	c.version = plan.Version
	return result, nil
}

func (c *Controller) Cancel(jobID, reservationID string, planVersion uint64) bool {
	if c == nil || jobID == "" || reservationID == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if planVersion != 0 && planVersion < c.version {
		return false
	}
	job, ok := c.active[jobID]
	if !ok || job.ReservationID != reservationID {
		return false
	}
	delete(c.active, jobID)
	return true
}

func (c *Controller) Version() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.version
}

func (c *Controller) ActiveJobs() []futureasset.Job {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]futureasset.Job, 0, len(c.active))
	for _, job := range c.active {
		out = append(out, job)
	}
	return out
}

func (c *Controller) ProtectedAssets() []futureasset.ProtectedAsset {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]futureasset.ProtectedAsset, 0, len(c.protect))
	for _, asset := range c.protect {
		out = append(out, asset)
	}
	return out
}
