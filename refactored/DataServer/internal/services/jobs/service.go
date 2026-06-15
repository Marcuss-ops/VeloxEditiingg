package jobs

import (
	"context"
	"log"
	"strings"

	"velox-server/internal/config"
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
	JobID   string
	Payload map[string]interface{}
	Reason  string
}

type SubmitResultRequest struct {
	JobID    string
	WorkerID string
	Status   string
	Error    string
	Output   map[string]interface{}
	EndTime  string
}

func NewService(cfg *config.Config, fileQ *queue.FileQueue, jobsRepo store.JobsRepository, logger *queue.EventLogger, reg *workers.Registry) *Service {
	return &Service{
		cfg:      cfg,
		fileQ:    fileQ,
		jobsRepo: jobsRepo,
		logger:   logger,
		reg:      reg,
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

	if s.cfg != nil && s.cfg.ForceSingleWorker != "" && s.cfg.ForceSingleWorker != "0" && !strings.EqualFold(s.cfg.ForceSingleWorker, "false") {
		allowed := req.WorkerID == s.cfg.ForceSingleWorker || req.ClientIP == s.cfg.ForceSingleWorker
		if !allowed {
			return &ClaimResult{Reason: "Single-worker mode active (" + s.cfg.ForceSingleWorker + ")"}, nil
		}
	}

	if s.cfg != nil {
		allowedWorkers := strings.TrimSpace(s.cfg.AllowedWorkers)
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
			if !allowlistOK && s.cfg.AllowlistAllowRegistered && s.reg != nil && s.reg.IsRegistered(ctx, req.WorkerID) {
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
		payload map[string]interface{}
		jobID   string
	)
	if s.fileQ != nil {
		var allowedJobTypes []string
		if workerInfo != nil {
			allowedJobTypes = workerInfo.GetSupportedJobTypes()
		}
		job, claimErr := s.fileQ.ClaimNextJob(ctx, req.WorkerID, allowedJobTypes)
		if claimErr != nil {
			return &ClaimResult{Reason: "lease failed"}, nil
		}
		if job == nil {
			return &ClaimResult{}, nil
		}
		jobID = job.JobID
		payload = job.Payload
	}

	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["job_id"] = jobID
	payload["id"] = jobID
	payload["render_plan_version"] = "v1"

	if s.reg != nil {
		if err := s.reg.Heartbeat(ctx, req.WorkerID, req.WorkerName, "busy", jobID, nil); err != nil {
			log.Printf("service: heartbeat failed for %s: %v", req.WorkerID, err)
		}
	}

	return &ClaimResult{
		JobID:   jobID,
		Payload: payload,
	}, nil
}

func (s *Service) SubmitResult(ctx context.Context, req SubmitResultRequest) (bool, error) {
	status := strings.ToLower(strings.TrimSpace(req.Status))
	if status == "" || status == "success" || status == "completed" {
		var err error
		if s.fileQ != nil {
			if len(req.Output) > 0 {
				logEntries := ExtractWorkerLogEntries(req.Output, req.WorkerID)
				if len(logEntries) > 0 {
					if err := s.fileQ.UpdateJobLogs(ctx, req.JobID, logEntries); err != nil {
						log.Printf("service: failed to update job logs for %s: %v", req.JobID, err)
					}
				}
			}

			updates := map[string]interface{}{
				"status": "COMPLETED",
			}
			if strings.TrimSpace(req.EndTime) != "" {
				updates["completed_at"] = strings.TrimSpace(req.EndTime)
			}
			if strings.TrimSpace(req.WorkerID) != "" {
				updates["completed_by"] = strings.TrimSpace(req.WorkerID)
			}
			if len(req.Output) > 0 {
				updates["worker_output"] = req.Output
				if path := ExtractOutputVideoPath(req.Output); path != "" {
					updates["result_path_worker"] = path
				}
			}
			err = s.fileQ.UpdateJobFields(ctx, req.JobID, updates)
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
		if len(req.Output) > 0 {
			logEntries := ExtractWorkerLogEntries(req.Output, req.WorkerID)
			if len(logEntries) > 0 {
				if err := s.fileQ.UpdateJobLogs(ctx, req.JobID, logEntries); err != nil {
					log.Printf("service: failed to update job logs on fail for %s: %v", req.JobID, err)
				}
			}
		}
		err = s.fileQ.FailJob(ctx, req.JobID, req.Error, true)
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

func (s *Service) FailJob(ctx context.Context, jobID, workerID, errMsg string) error {
	var attempt int
	if s.fileQ != nil {
		attempt, _ = s.fileQ.GetJobAttempt(ctx, jobID)
	}

	maxAttempts := 3
	if s.cfg != nil && s.cfg.MaxJobAttempts > 0 {
		maxAttempts = s.cfg.MaxJobAttempts
	}
	requeue := attempt < maxAttempts

	var err error
	if s.fileQ != nil {
		err = s.fileQ.FailJob(ctx, jobID, errMsg, requeue)
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
