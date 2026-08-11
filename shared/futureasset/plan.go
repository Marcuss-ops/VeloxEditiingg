// Package futureasset contains the shared, deterministic contract for
// worker-scoped future asset plans.
//
// A plan is a complete snapshot, not a stream of mutations. The master owns
// policy and placement; the worker only reconciles the snapshot. This package
// deliberately does not import the cache or downloader packages: prefetch is
// a later consumer of this contract, never a second byte pipeline.
package futureasset

import (
	"fmt"
	"sort"
	"strings"
	"time"

	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultPrefetchHorizon is the hard reservation horizon. Only these jobs
	// may cause a future asset request.
	DefaultPrefetchHorizon = 3
	// DefaultProtectionLookahead is the retention/protection horizon.
	DefaultProtectionLookahead = 10
)

// Limits controls the two intentionally different worker windows. It is
// carried in the snapshot so the master and worker validate the same policy.
type Limits struct {
	PrefetchHorizon     int
	ProtectionLookahead int
}

func (l Limits) withDefaults() Limits {
	if l.PrefetchHorizon <= 0 {
		l.PrefetchHorizon = DefaultPrefetchHorizon
	}
	if l.ProtectionLookahead <= 0 {
		l.ProtectionLookahead = DefaultProtectionLookahead
	}
	return l
}

// AssetManifest is the master's canonical integrity contract for one asset.
// SHA256 and SizeBytes are required before an asset is eligible for verified
// prefetch.
type AssetManifest struct {
	AssetKey  string
	AssetID   string
	SHA256    string
	SizeBytes int64
	MIMEType  string
	Role      string
}

// Job is a worker-pinned future job in canonical queue order.
type Job struct {
	JobID         string
	TaskID        string
	ReservationID string
	TaskRevision  int
	Distance      int
	Assets        []AssetManifest
}

// ProtectedAsset is an aggregate retention hint. The worker later maps each
// entry to its existing workercache reservation primitive.
type ProtectedAsset struct {
	AssetKey        string
	FutureRefCount  int
	NextUseDistance int
}

// Plan is a complete worker-scoped snapshot. PrefetchJobs are restricted to
// the hard horizon; Protect contains the wider retention lookahead.
type Plan struct {
	Version      uint64
	PlanID       string
	WorkerID     string
	GeneratedAt  time.Time
	ExpiresAt    time.Time
	CurrentJob   string
	PrefetchJobs []Job
	Protect      []ProtectedAsset
	Limits       Limits
}

// PlannerInput is already-placement-scoped input. The caller must provide
// only jobs reserved for WorkerID, in canonical queue order N+1 onward.
type PlannerInput struct {
	Version     uint64
	PlanID      string
	WorkerID    string
	GeneratedAt time.Time
	ExpiresAt   time.Time
	CurrentJob  string
	FutureJobs  []Job
	Limits      Limits
}

// Build validates and compiles one complete snapshot. It never performs I/O,
// changes task state, or talks to a downloader.
func Build(in PlannerInput) (Plan, error) {
	if strings.TrimSpace(in.WorkerID) == "" {
		return Plan{}, fmt.Errorf("futureasset: worker_id is required")
	}
	if in.Version == 0 {
		return Plan{}, fmt.Errorf("futureasset: version must be positive")
	}
	if strings.TrimSpace(in.PlanID) == "" {
		return Plan{}, fmt.Errorf("futureasset: plan_id is required")
	}
	if in.GeneratedAt.IsZero() || in.ExpiresAt.IsZero() || !in.ExpiresAt.After(in.GeneratedAt) {
		return Plan{}, fmt.Errorf("futureasset: generated_at/expires_at must be ordered")
	}

	limits := in.Limits.withDefaults()
	if limits.PrefetchHorizon > limits.ProtectionLookahead {
		return Plan{}, fmt.Errorf("futureasset: prefetch horizon cannot exceed protection lookahead")
	}
	plan := Plan{
		Version: in.Version, PlanID: strings.TrimSpace(in.PlanID), WorkerID: strings.TrimSpace(in.WorkerID),
		GeneratedAt: in.GeneratedAt, ExpiresAt: in.ExpiresAt, CurrentJob: in.CurrentJob,
		PrefetchJobs: make([]Job, 0, min(len(in.FutureJobs), DefaultPrefetchHorizon)),
		Protect:      make([]ProtectedAsset, 0),
		Limits:       limits,
	}
	protected := make(map[string]*ProtectedAsset)
	seenJobs := make(map[string]struct{}, len(in.FutureJobs))

	for index, inputJob := range in.FutureJobs {
		if index >= limits.ProtectionLookahead {
			break
		}
		job := inputJob
		if strings.TrimSpace(job.JobID) == "" || strings.TrimSpace(job.TaskID) == "" {
			return Plan{}, fmt.Errorf("futureasset: future job %d requires job_id and task_id", index)
		}
		if _, exists := seenJobs[job.JobID]; exists {
			return Plan{}, fmt.Errorf("futureasset: duplicate job_id %q", job.JobID)
		}
		seenJobs[job.JobID] = struct{}{}
		job.Distance = index + 1
		var err error
		job.Assets, err = canonicalAssets(job.Assets)
		if err != nil {
			return Plan{}, fmt.Errorf("futureasset: job %q: %w", job.JobID, err)
		}
		for _, asset := range job.Assets {
			if asset.AssetKey == "" {
				return Plan{}, fmt.Errorf("futureasset: job %q contains empty asset_key", job.JobID)
			}
			entry := protected[asset.AssetKey]
			if entry == nil {
				entry = &ProtectedAsset{AssetKey: asset.AssetKey, NextUseDistance: job.Distance}
				protected[asset.AssetKey] = entry
			}
			entry.FutureRefCount++
			if job.Distance < entry.NextUseDistance {
				entry.NextUseDistance = job.Distance
			}
		}
		if job.Distance <= limits.PrefetchHorizon {
			if job.ReservationID == "" {
				return Plan{}, fmt.Errorf("futureasset: prefetch job %q requires reservation_id", job.JobID)
			}
			for _, asset := range job.Assets {
				if asset.SHA256 == "" || asset.SizeBytes <= 0 {
					return Plan{}, fmt.Errorf("futureasset: prefetch asset %q in job %q lacks sha256/size", asset.AssetKey, job.JobID)
				}
			}
			plan.PrefetchJobs = append(plan.PrefetchJobs, job)
		}
	}

	for _, asset := range protected {
		plan.Protect = append(plan.Protect, *asset)
	}
	sort.Slice(plan.Protect, func(i, j int) bool { return plan.Protect[i].AssetKey < plan.Protect[j].AssetKey })
	return plan, nil
}

func canonicalAssets(in []AssetManifest) ([]AssetManifest, error) {
	byKey := make(map[string]AssetManifest, len(in))
	for _, asset := range in {
		asset.AssetKey = strings.TrimSpace(asset.AssetKey)
		if asset.AssetKey == "" {
			return nil, fmt.Errorf("empty asset_key")
		}
		if previous, exists := byKey[asset.AssetKey]; exists && (previous.SHA256 != asset.SHA256 || previous.SizeBytes != asset.SizeBytes) {
			return nil, fmt.Errorf("asset %q has conflicting integrity metadata", asset.AssetKey)
		}
		byKey[asset.AssetKey] = asset
	}
	out := make([]AssetManifest, 0, len(byKey))
	for _, asset := range byKey {
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AssetKey < out[j].AssetKey })
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Validate checks a received snapshot without compiling it. Expiry is
// intentionally not treated as a malformed plan; callers stop new work when
// it is expired and retain verified cache files.
func (p Plan) Validate() error {
	if p.WorkerID == "" || p.PlanID == "" || p.Version == 0 {
		return fmt.Errorf("futureasset: incomplete plan identity")
	}
	if p.GeneratedAt.IsZero() || p.ExpiresAt.IsZero() || !p.ExpiresAt.After(p.GeneratedAt) {
		return fmt.Errorf("futureasset: invalid plan timestamps")
	}
	limits := p.Limits.withDefaults()
	if len(p.PrefetchJobs) > limits.PrefetchHorizon {
		return fmt.Errorf("futureasset: prefetch horizon exceeds %d", limits.PrefetchHorizon)
	}
	seenJobs := make(map[string]struct{}, len(p.PrefetchJobs))
	for i, job := range p.PrefetchJobs {
		if job.Distance != i+1 || job.Distance > limits.PrefetchHorizon || job.ReservationID == "" {
			return fmt.Errorf("futureasset: prefetch job %q has invalid distance/reservation", job.JobID)
		}
		if job.JobID == "" || job.TaskID == "" {
			return fmt.Errorf("futureasset: prefetch job requires job_id and task_id")
		}
		if _, exists := seenJobs[job.JobID]; exists {
			return fmt.Errorf("futureasset: duplicate prefetch job %q", job.JobID)
		}
		seenJobs[job.JobID] = struct{}{}
		seenAssets := make(map[string]struct{}, len(job.Assets))
		for _, asset := range job.Assets {
			if asset.AssetKey == "" || asset.SHA256 == "" || asset.SizeBytes <= 0 {
				return fmt.Errorf("futureasset: prefetch job %q contains incomplete manifest", job.JobID)
			}
			if _, exists := seenAssets[asset.AssetKey]; exists {
				return fmt.Errorf("futureasset: duplicate asset %q in job %q", asset.AssetKey, job.JobID)
			}
			seenAssets[asset.AssetKey] = struct{}{}
		}
	}
	seenProtected := make(map[string]struct{}, len(p.Protect))
	for _, asset := range p.Protect {
		if asset.AssetKey == "" || asset.FutureRefCount <= 0 || asset.NextUseDistance <= 0 || asset.NextUseDistance > limits.ProtectionLookahead {
			return fmt.Errorf("futureasset: invalid protected asset %q", asset.AssetKey)
		}
		if _, exists := seenProtected[asset.AssetKey]; exists {
			return fmt.Errorf("futureasset: duplicate protected asset %q", asset.AssetKey)
		}
		seenProtected[asset.AssetKey] = struct{}{}
	}
	return nil
}

// ToProto maps the validated domain plan to the typed control-stream message.
func (p Plan) ToProto() *pb.FutureAssetPlan {
	out := &pb.FutureAssetPlan{
		Version: p.Version, PlanId: p.PlanID, WorkerId: p.WorkerID,
		GeneratedAt: timestamppb.New(p.GeneratedAt), ExpiresAt: timestamppb.New(p.ExpiresAt),
		CurrentJobId:        p.CurrentJob,
		PrefetchJobs:        make([]*pb.PrefetchJob, 0, len(p.PrefetchJobs)),
		ProtectAssets:       make([]*pb.ProtectedFutureAsset, 0, len(p.Protect)),
		PrefetchHorizon:     int32(p.Limits.withDefaults().PrefetchHorizon),
		ProtectionLookahead: int32(p.Limits.withDefaults().ProtectionLookahead),
	}
	for _, job := range p.PrefetchJobs {
		wireJob := &pb.PrefetchJob{JobId: job.JobID, TaskId: job.TaskID, ReservationId: job.ReservationID, TaskRevision: int32(job.TaskRevision), Distance: int32(job.Distance)}
		for _, asset := range job.Assets {
			wireJob.Assets = append(wireJob.Assets, &pb.PrefetchAsset{AssetKey: asset.AssetKey, AssetId: asset.AssetID, Sha256: asset.SHA256, SizeBytes: asset.SizeBytes, MimeType: asset.MIMEType, Role: asset.Role})
		}
		out.PrefetchJobs = append(out.PrefetchJobs, wireJob)
	}
	for _, asset := range p.Protect {
		out.ProtectAssets = append(out.ProtectAssets, &pb.ProtectedFutureAsset{AssetKey: asset.AssetKey, FutureRefCount: int32(asset.FutureRefCount), NextUseDistance: int32(asset.NextUseDistance)})
	}
	return out
}

// FromProto maps and validates a received typed plan.
func FromProto(in *pb.FutureAssetPlan) (Plan, error) {
	if in == nil {
		return Plan{}, fmt.Errorf("futureasset: nil plan")
	}
	p := Plan{Version: in.GetVersion(), PlanID: in.GetPlanId(), WorkerID: in.GetWorkerId(), CurrentJob: in.GetCurrentJobId(), Limits: Limits{PrefetchHorizon: int(in.GetPrefetchHorizon()), ProtectionLookahead: int(in.GetProtectionLookahead())}}
	if in.GetGeneratedAt() == nil || in.GetExpiresAt() == nil {
		return Plan{}, fmt.Errorf("futureasset: plan timestamps are required")
	}
	p.GeneratedAt, p.ExpiresAt = in.GetGeneratedAt().AsTime(), in.GetExpiresAt().AsTime()
	for _, wireJob := range in.GetPrefetchJobs() {
		if wireJob == nil {
			return Plan{}, fmt.Errorf("futureasset: nil prefetch job")
		}
		job := Job{JobID: wireJob.GetJobId(), TaskID: wireJob.GetTaskId(), ReservationID: wireJob.GetReservationId(), TaskRevision: int(wireJob.GetTaskRevision()), Distance: int(wireJob.GetDistance())}
		for _, wireAsset := range wireJob.GetAssets() {
			if wireAsset == nil {
				return Plan{}, fmt.Errorf("futureasset: nil prefetch asset")
			}
			job.Assets = append(job.Assets, AssetManifest{AssetKey: wireAsset.GetAssetKey(), AssetID: wireAsset.GetAssetId(), SHA256: wireAsset.GetSha256(), SizeBytes: wireAsset.GetSizeBytes(), MIMEType: wireAsset.GetMimeType(), Role: wireAsset.GetRole()})
		}
		p.PrefetchJobs = append(p.PrefetchJobs, job)
	}
	for _, wireAsset := range in.GetProtectAssets() {
		if wireAsset == nil {
			return Plan{}, fmt.Errorf("futureasset: nil protected asset")
		}
		p.Protect = append(p.Protect, ProtectedAsset{AssetKey: wireAsset.GetAssetKey(), FutureRefCount: int(wireAsset.GetFutureRefCount()), NextUseDistance: int(wireAsset.GetNextUseDistance())})
	}
	if err := p.Validate(); err != nil {
		return Plan{}, err
	}
	return p, nil
}

// Expired reports whether the plan is no longer allowed to start new work.
func (p Plan) Expired(now time.Time) bool { return !now.Before(p.ExpiresAt) }
