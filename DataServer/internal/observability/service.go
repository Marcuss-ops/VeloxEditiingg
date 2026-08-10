package observability

import (
	"context"
	"fmt"

	"velox-server/internal/audittrail"
)

// Service is the read-only observability aggregation service.
type Service struct {
	tasks          TaskReader
	attempts       AttemptReader
	jobs           JobReader
	workers        WorkerReader
	versionMetrics VersionMetricsReader
	audit          AuditReader
	jobInspection  JobInspectionReader
	liveAttempts   LiveAttemptReader
}

// NewService constructs the observability aggregation service.
// Jobs and workers readers are optional (nil-safe) for backward compatibility
// with existing callers that only need task/attempt summarization.
func NewService(tasks TaskReader, attempts AttemptReader) (*Service, error) {
	if tasks == nil {
		return nil, fmt.Errorf("observability: task reader is required")
	}
	if attempts == nil {
		return nil, fmt.Errorf("observability: attempt reader is required")
	}
	return &Service{tasks: tasks, attempts: attempts}, nil
}

// WithJobs sets the job reader for aggregate queries (Overview).
func (s *Service) WithJobs(r JobReader) *Service { s.jobs = r; return s }

// WithWorkers sets the worker reader for worker queries.
func (s *Service) WithWorkers(r WorkerReader) *Service { s.workers = r; return s }

// WithVersionMetrics sets the version metrics reader for regression comparison.
func (s *Service) WithVersionMetrics(r VersionMetricsReader) *Service { s.versionMetrics = r; return s }

func (s *Service) WithAudit(r AuditReader) *Service { s.audit = r; return s }

// WithJobInspection wires the optional persistence-backed job details.
func (s *Service) WithJobInspection(r JobInspectionReader) *Service {
	s.jobInspection = r
	return s
}

// WithLiveAttempts wires the existing volatile runtime projection into the
// admin read model. It does not create or persist a second tracker.
func (s *Service) WithLiveAttempts(r LiveAttemptReader) *Service {
	s.liveAttempts = r
	return s
}

func (s *Service) ListAudit(ctx context.Context, resourceID string, limit int) ([]audittrail.Event, error) {
	if s.audit == nil {
		return []audittrail.Event{}, nil
	}
	return s.audit.ListAuditEvents(ctx, resourceID, limit)
}
