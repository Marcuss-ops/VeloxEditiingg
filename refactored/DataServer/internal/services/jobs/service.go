package jobs

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/services/joblifecycle"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// Service holds job read/use-case logic that should stay outside HTTP handlers.
type Service struct {
	cfg              *config.Config
	fileQ            *queue.FileQueue
	jobsRepo         store.JobsRepository
	logger           *queue.EventLogger
	reg              *workers.Registry
	masterBundleHash string
	lifecycle        *joblifecycle.Service
}

// SetMasterBundleHash sets the current master bundle hash for compatibility checks.
func (s *Service) SetMasterBundleHash(hash string) {
	s.masterBundleHash = hash
}

type ClaimRequest struct {
	WorkerID    string
	WorkerName  string
	ClientIP    string
	Drain       bool
	Schedulable bool
	JobType     string
}

type ClaimResult struct {
	JobID           string
	Payload         map[string]interface{}
	Reason          string
	LeaseID         string
	LeaseExpiresAt  string
	Attempt         int
	ContractVersion int
}

type SubmitResultRequest struct {
	JobID           string
	WorkerID        string
	Status          string
	Error           string
	Output          map[string]interface{}
	EndTime         string
	LeaseID         string
	Attempt         int
	ContractVersion int
	ArtifactID      string
	OutputSHA256    string
	IdempotencyKey  string
}

func NewService(cfg *config.Config, fileQ *queue.FileQueue, jobsRepo store.JobsRepository, logger *queue.EventLogger, reg *workers.Registry) *Service {
	maxRetries := 3
	if cfg != nil && cfg.Workers.MaxJobAttempts > 0 {
		maxRetries = cfg.Workers.MaxJobAttempts
	}
	lifecycleSvc := joblifecycle.NewService(fileQ.TransitionService(), fileQ.GetDBStore(), maxRetries)
	return &Service{
		cfg:       cfg,
		fileQ:     fileQ,
		jobsRepo:  jobsRepo,
		logger:    logger,
		reg:       reg,
		lifecycle: lifecycleSvc,
	}
}

func (s *Service) ClaimNextJob(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	if strings.TrimSpace(req.WorkerID) == "" {
		return &ClaimResult{Reason: "missing worker_id"}, nil
	}

	var workerInfo *workers.WorkerInfo
	if s.reg != nil {
		workerInfo = s.reg.GetWorker(ctx, req.WorkerID)
		if workerInfo == nil {
			return &ClaimResult{Reason: "Worker not registered"}, nil
		}
		if workerInfo.Drain {
			return &ClaimResult{Reason: "Worker draining"}, nil
		}
		if !workerInfo.Schedulable {
			return &ClaimResult{Reason: "Worker not schedulable"}, nil
		}
		if strings.EqualFold(strings.TrimSpace(workerInfo.Status), "offline") {
			return &ClaimResult{Reason: "Worker offline"}, nil
		}
		if strings.TrimSpace(workerInfo.CurrentJob) != "" && !strings.EqualFold(strings.TrimSpace(workerInfo.CurrentJob), req.WorkerID) {
			return &ClaimResult{Reason: "Worker busy"}, nil
		}
	}

	if s.cfg != nil && s.cfg.Workers.ForceSingleWorker != "" && s.cfg.Workers.ForceSingleWorker != "0" && !strings.EqualFold(s.cfg.Workers.ForceSingleWorker, "false") {
		allowed := req.WorkerID == s.cfg.Workers.ForceSingleWorker || req.ClientIP == s.cfg.Workers.ForceSingleWorker
		if !allowed {
			return &ClaimResult{Reason: "Single-worker mode active (" + s.cfg.Workers.ForceSingleWorker + ")"}, nil
		}
	}

	if s.cfg != nil {
		allowedWorkers := strings.TrimSpace(s.cfg.Workers.AllowedWorkers)
		if allowedWorkers != "" && !strings.EqualFold(allowedWorkers, "*") && !strings.EqualFold(allowedWorkers, "ALL") {
			parts := strings.Split(allowedWorkers, ",")
			allowlistOK := false
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				if req.ClientIP == p || req.WorkerID == p || req.WorkerName == p {
					allowlistOK = true
					break
				}
			}
			if !allowlistOK && s.cfg.Workers.AllowlistRegistered && s.reg != nil && s.reg.IsRegistered(ctx, req.WorkerID) {
				allowlistOK = true
			}
			if !allowlistOK {
				return &ClaimResult{Reason: "Worker not allowed"}, nil
			}
		}
	}

	if !req.Drain && s.reg != nil {
		if workerInfo == nil {
			return &ClaimResult{Reason: "Worker not registered"}, nil
		}
		if !req.Schedulable && !workerInfo.Schedulable {
			return &ClaimResult{Reason: "Worker not schedulable"}, nil
		}
	}

	// Worker compatibility check: reject if protocol/bundle/capabilities mismatch
	if workerInfo != nil {
		if reason := s.checkWorkerCompatibility(ctx, workerInfo, req.JobType); reason != "" {
			return &ClaimResult{Reason: "Worker incompatible: " + reason}, nil
		}
	}

	var (
		payload  map[string]interface{}
		job      *queue.Job
		jobID    string
		leaseID  string
		leaseExp string
		attempt  int
	)
	if s.fileQ != nil {
		var allowedJobTypes []string
		if workerInfo != nil {
			allowedJobTypes = workerInfo.GetSupportedJobTypes()
		}
		var claimErr error
		job, claimErr = s.fileQ.ClaimNextJob(ctx, req.WorkerID, allowedJobTypes)
		if claimErr != nil {
			return &ClaimResult{Reason: "lease failed"}, nil
		}
		if job == nil {
			return &ClaimResult{}, nil
		}
		jobID = job.JobID
		payload = job.Payload
		leaseID = job.LeaseID
		if exp, ok := payload["lease_expiry"].(string); ok {
			leaseExp = strings.TrimSpace(exp)
		}
		if leaseExp == "" {
			leaseExp = strings.TrimSpace(stringValue(payload["lease_expires_at"]))
		}
		attempt = job.Attempt
		if attempt == 0 {
			attempt = job.RetryCount
		}
	}

	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["created_at"] = normalizeJobTimestamp(job.CreatedAt)
	if ts := normalizeJobTimestamp(job.UpdatedAt); ts != "" {
		payload["updated_at"] = ts
	}
	if ts := normalizeJobTimestamp(job.StartedAt); ts != "" {
		payload["started_at"] = ts
	}
	if ts := normalizeJobTimestamp(job.CompletedAt); ts != "" {
		payload["completed_at"] = ts
	}
	if ts := normalizeJobTimestamp(job.AssignedAt); ts != "" {
		payload["assigned_at"] = ts
	}
	if ts := normalizeJobTimestamp(job.LeaseExpiry); ts != "" {
		payload["lease_expiry"] = ts
		payload["lease_expires_at"] = ts
	}
	if ts := normalizeJobTimestamp(job.LastErrorAt); ts != "" {
		payload["last_error_at"] = ts
	}
	if ts := normalizeJobTimestamp(job.FailedAt); ts != "" {
		payload["failed_at"] = ts
	}
	payload["job_id"] = jobID
	payload["id"] = jobID
	payload["render_plan_version"] = "v1"
	if leaseID != "" {
		payload["lease_id"] = leaseID
	}
	if leaseExp != "" {
		payload["lease_expiry"] = leaseExp
		payload["lease_expires_at"] = leaseExp
	}
	if attempt > 0 {
		payload["attempt"] = attempt
	}
	payload["contract_version"] = 2

	if s.reg != nil {
		if err := s.reg.Heartbeat(ctx, req.WorkerID, req.WorkerName, "busy", jobID, nil); err != nil {
			log.Printf("service: heartbeat failed for %s: %v", req.WorkerID, err)
		}
	}

	return &ClaimResult{
		JobID:           jobID,
		Payload:         payload,
		LeaseID:         leaseID,
		LeaseExpiresAt:  leaseExp,
		Attempt:         attempt,
		ContractVersion: 2,
	}, nil
}

func normalizeJobTimestamp(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case int:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case int32:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case int64:
		return time.Unix(t, 0).UTC().Format(time.RFC3339)
	case float32:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case float64:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case uint:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case uint32:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case uint64:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	default:
		return ""
	}
}

func (s *Service) SubmitResult(ctx context.Context, req SubmitResultRequest) (bool, error) {
	if req.ContractVersion != 0 && req.ContractVersion != 2 {
		return false, fmt.Errorf("unsupported contract version: %d", req.ContractVersion)
	}
	status := strings.ToLower(strings.TrimSpace(req.Status))
	if status == "" || status == "success" || status == "completed" {
		var err error
		if s.fileQ != nil {
			if leaseErr := s.ValidateJobLease(ctx, req.JobID, req.WorkerID, req.LeaseID); leaseErr != nil {
				return false, leaseErr
			}
			if len(req.Output) > 0 {
				logEntries := ExtractWorkerLogEntries(req.Output, req.WorkerID)
				if len(logEntries) > 0 {
					if logErr := s.fileQ.UpdateJobLogs(ctx, req.JobID, logEntries); logErr != nil {
						log.Printf("service: failed to update job logs for %s: %v", req.JobID, logErr)
					}
				}
			}

		// Use JobLifecycleService.SubmitResult instead of UpdateJobFields
		lifecycleRes := joblifecycle.CompleteJobResult{
			CompletedBy:    strings.TrimSpace(req.WorkerID),
			ArtifactID:     strings.TrimSpace(req.ArtifactID),
			OutputSHA256:   strings.TrimSpace(req.OutputSHA256),
			IdempotencyKey: strings.TrimSpace(req.IdempotencyKey),
			EndTime:        strings.TrimSpace(req.EndTime),
		}
		// Don't store worker_output or result_path_worker on the job row -
		// these belong on the artifact. The upload-completed handler or
		// DeliveryRunner handles artifact pathing.
		err = s.lifecycle.SubmitResult(ctx, req.JobID, lifecycleRes)
		}
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(req.WorkerID) != "" && s.reg != nil {
			if err := s.reg.Heartbeat(ctx, req.WorkerID, "", "online", "", nil); err != nil {
				log.Printf("service: online heartbeat failed for %s: %v", req.WorkerID, err)
			}
		}
		return s.fileQ != nil, nil
	}

	var err error
	if s.fileQ != nil {
		if leaseErr := s.ValidateJobLease(ctx, req.JobID, req.WorkerID, req.LeaseID); leaseErr != nil {
			return false, leaseErr
		}
		if len(req.Output) > 0 {
			logEntries := ExtractWorkerLogEntries(req.Output, req.WorkerID)
			if len(logEntries) > 0 {
				if logErr := s.fileQ.UpdateJobLogs(ctx, req.JobID, logEntries); logErr != nil {
					log.Printf("service: failed to update job logs on fail for %s: %v", req.JobID, logErr)
				}
			}
		}
		err = s.fileQ.FailJob(ctx, req.JobID, req.Error, req.WorkerID, true)
	}
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(req.WorkerID) != "" && s.reg != nil {
		if err := s.reg.Heartbeat(ctx, req.WorkerID, "", "online", "", nil); err != nil {
			log.Printf("service: online heartbeat (fail path) failed for %s: %v", req.WorkerID, err)
		}
	}
	return false, nil
}

func (s *Service) CompleteJob(ctx context.Context, jobID, workerID string) error {
	var err error
	if s.fileQ != nil {
		err = s.fileQ.CompleteJob(ctx, jobID)
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(workerID) != "" && s.reg != nil {
		if err := s.reg.Heartbeat(ctx, workerID, "", "online", "", nil); err != nil {
			log.Printf("service: complete heartbeat failed for %s: %v", workerID, err)
		}
	}
	return nil
}

func (s *Service) ValidateJobLease(ctx context.Context, jobID, workerID, leaseID string) error {
	if s.fileQ == nil || strings.TrimSpace(jobID) == "" {
		return nil
	}
	job, err := s.fileQ.GetJobAsMap(ctx, jobID)
	if err != nil || job == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	currentLease := strings.TrimSpace(stringValue(job["lease_id"]))
	if currentLease == "" {
		return nil
	}
	if strings.TrimSpace(leaseID) == "" {
		return fmt.Errorf("missing lease_id for job %s", jobID)
	}
	if !strings.EqualFold(strings.TrimSpace(currentLease), strings.TrimSpace(leaseID)) {
		return fmt.Errorf("lease mismatch for job %s", jobID)
	}
	if workerID != "" {
		if assigned := strings.TrimSpace(stringValue(job["assigned_to"])); assigned != "" && !strings.EqualFold(assigned, strings.TrimSpace(workerID)) {
			return fmt.Errorf("worker mismatch for job %s", jobID)
		}
	}
	return nil
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func (s *Service) FailJob(ctx context.Context, jobID, workerID, errMsg string) error {
	var attempt int
	if s.fileQ != nil {
		attempt, _ = s.fileQ.GetJobAttempt(ctx, jobID)
	}

	maxAttempts := 3
	if s.cfg != nil && s.cfg.Workers.MaxJobAttempts > 0 {
		maxAttempts = s.cfg.Workers.MaxJobAttempts
	}
	requeue := attempt < maxAttempts

	var err error
	if s.fileQ != nil {
		err = s.fileQ.FailJob(ctx, jobID, errMsg, workerID, requeue)
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(workerID) != "" && s.reg != nil {
		if err := s.reg.Heartbeat(ctx, workerID, "", "online", "", nil); err != nil {
			log.Printf("service: fail heartbeat failed for %s: %v", workerID, err)
		}
	}
	return nil
}
