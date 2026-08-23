package creatorflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	assetbridge "velox-server/internal/assets"
	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/taskgraph"
	"velox-shared/contract/assembly"
	"velox-shared/contract/domain"
	"velox-shared/publication"
)

// CanonicalJobSubmission is the application boundary shared by the M2M
// intake and the InstaEdit control-plane intake. HTTP handlers only adapt
// their wire/authentication concerns into this value; persistence and
// idempotency remain owned by the resolver below.
//
// IntakeSource is the bounded, operator-facing discriminator that tells
// which producer surface routed this submission (canonical, creator,
// instaedit, batch, …). It drives the `pipeline.intake_source_accepted_total`
// telemetry so alias usage can be measured before any endpoint is
// deprecated/removed. See IntakeSourceCanonical and friends below.
type CanonicalJobSubmission struct {
	ContractVersion  string
	WorkspaceID      int64
	ExternalClientID string
	IntakeSource     string
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	Payload          map[string]interface{}
	DeliveryPlan     map[string]interface{}
	PublicationSpecs []publication.Spec
	Assembly         *assembly.AssemblyJobV1
}

// Canonical intake-source vocabulary. Each value is a bounded label on
// `pipeline.intake_source_accepted_total`; do NOT add free-form strings
// (job ids, client ids) here — they belong in structured logs.
const (
	// IntakeSourceCanonical is POST /api/v1/jobs (and /api/v1/jobs/batch
	// items, which route through the same single-job path).
	IntakeSourceCanonical = "canonical"
	// IntakeSourceCreator is POST /api/v1/creator/jobs (creator push).
	IntakeSourceCreator = "creator"
	// IntakeSourceInstaedit is POST /api/v1/instaedit/jobs (BFF adapter).
	IntakeSourceInstaedit = "instaedit"
	// IntakeSourceBatch is POST /api/v1/jobs/batch (batch envelope).
	IntakeSourceBatch = "batch"
	// IntakeSourceScriptGenerate is POST /api/v1/script/generate-with-images
	// and POST /api/v1/script/generate (script ingress, direct enqueue).
	IntakeSourceScriptGenerate = "script_generate"
	// IntakeSourceScriptKind is POST /api/v1/script/jobs/:kind.
	IntakeSourceScriptKind = "script_kind"
	// IntakeSourcePipelineRun is POST /api/v1/pipeline-runs (durable run).
	IntakeSourcePipelineRun = "pipeline_run"
	// IntakeSourceCalendar is POST /api/v1/calendar/events/:id/enqueue.
	IntakeSourceCalendar = "calendar"
)

// IntakeSourceRecorder records an accepted submission by intake source.
// The production implementation lives in velox-server/internal/metrics
// (IntakeSourceSink); defining the consumer-owned interface here avoids
// an import edge on the metrics package and lets tests inject a recorder.
type IntakeSourceRecorder interface {
	IncAccepted(source string)
}

// CanonicalJobSubmitter is the single production Job+Task submission path.
// The resolver performs the durable idempotency check and the atomic
// forwarding/job/task write; the submitter adds the shared validation and
// the intake-source telemetry so every producer converges on one path.
type CanonicalJobSubmitter struct {
	resolver *Resolver
	intake   IntakeSourceRecorder
}

// NewCanonicalJobSubmitter constructs the canonical submitter. A nil
// resolver yields a nil submitter (callers must nil-check before Submit).
func NewCanonicalJobSubmitter(resolver *Resolver) *CanonicalJobSubmitter {
	if resolver == nil {
		return nil
	}
	return &CanonicalJobSubmitter{resolver: resolver}
}

// WithIntakeSourceRecorder wires the intake-source telemetry sink. Nil is
// a noop (the submitter then records nothing). The composition root passes
// velmetrics.NewIntakeSourceSink().
func (s *CanonicalJobSubmitter) WithIntakeSourceRecorder(r IntakeSourceRecorder) *CanonicalJobSubmitter {
	if s == nil {
		return s
	}
	s.intake = r
	return s
}

func (s *CanonicalJobSubmitter) Submit(ctx context.Context, req CanonicalJobSubmission) (*ResolveOutput, error) {
	if s == nil || s.resolver == nil {
		return nil, fmt.Errorf("job submission service is not configured")
	}
	if req.ContractVersion != "" && req.ContractVersion != "velox.job.v1" {
		return nil, domain.NewInvalidPayload("contract_version", "unsupported", "unsupported contract_version")
	}
	if strings.TrimSpace(req.SourceProvider) == "" {
		return nil, domain.NewInvalidPayload("source_provider", "required", "source_provider is required")
	}
	if strings.TrimSpace(req.SourceJobID) == "" {
		return nil, domain.NewInvalidPayload("idempotency_key", "required", "idempotency_key is required")
	}
	if req.Payload == nil {
		return nil, domain.NewInvalidPayload("payload", "required", "payload is required")
	}
	// These fields are generated at ingress by older adapters. They are
	// execution metadata, not request content; retaining them in the
	// resolver hash would turn an identical retry into a false idempotency
	// conflict because timestamps/UUIDs change between attempts.
	identityHash := sha256.Sum256([]byte(req.SourceProvider + ":" + req.SourceJobID + ":" + req.TargetExecutorID))
	stableIdentity := "submission_" + hex.EncodeToString(identityHash[:8])
	req.Payload["job_id"] = stableIdentity
	req.Payload["job_run_id"] = "run_" + stableIdentity
	req.Payload["correlation_id"] = "corr_" + stableIdentity
	// Fixed timestamps keep the payload hash a function of the canonical
	// request, never of the wall clock at which a retry arrived.
	req.Payload["created_at"] = "1970-01-01T00:00:00Z"
	req.Payload["updated_at"] = "1970-01-01T00:00:00Z"
	if req.DeliveryPlan != nil {
		req.Payload["delivery_plan"] = req.DeliveryPlan["delivery_plan"]
		if req.DeliveryPlan["delivery_plan"] == nil {
			req.Payload["delivery_plan"] = req.DeliveryPlan
		}
	}
	out, err := s.resolver.Resolve(ctx, ResolveRequest{
		WorkspaceID:      req.WorkspaceID,
		ExternalClientID: req.ExternalClientID,
		SourceProvider:   req.SourceProvider,
		SourceJobID:      req.SourceJobID,
		TargetExecutorID: req.TargetExecutorID,
		Payload:          req.Payload,
		DeliveryPlan:     req.DeliveryPlan,
		PublicationSpecs: req.PublicationSpecs,
		Assembly:         req.Assembly,
	})
	if err != nil {
		phase := "unknown"
		if enqueuePhase, ok := enqueue.EnqueuePhaseOf(err); ok {
			phase = string(enqueuePhase)
		}
		errHash := sha256.Sum256([]byte(err.Error()))
		assetCode := ""
		assetField := ""
		assetSource := ""
		if assetErr, ok := assetbridge.AsAcquisitionError(err); ok {
			assetCode = assetErr.ErrorCode
			assetField = assetErr.Field
			assetSource = assetErr.SourceType
		}
		log.Printf("[CREATORFLOW] canonical submission rejected phase=%s error_type=%T error_hash=%x asset_error_code=%s asset_field=%s asset_source=%s error_summary=%q", phase, err, errHash[:4], assetCode, assetField, assetSource, sanitizedErrorSummary(err))
	}
	// Record the intake source ONLY on an accepted submission (the same
	// semantics as the older creator-intake sink: accepted payloads, not
	// attempts). A missing source defaults to "canonical" so a producer
	// that forgets to stamp its identity is still measurable instead of
	// silently creating an unnamed series.
	if err == nil && out != nil {
		source := strings.TrimSpace(req.IntakeSource)
		if source == "" {
			source = IntakeSourceCanonical
		}
		if s.intake != nil {
			s.intake.IncAccepted(source)
		}
	}
	return out, err
}

// SubmitScratch drives the from-scratch job-creation path (script ingress,
// calendar enqueue) through the SAME enqueuer that backs the forwarding
// resolver. Unlike Submit — which forwards a completed pipeline result and
// therefore requires ShouldForwardPipelineResult — SubmitScratch enqueues a
// fresh payload with the shared validate → normalize → persist → schedule
// pipeline. Intake-source telemetry is recorded on an accepted enqueue, with
// the same canonical default for a missing source.
func (s *CanonicalJobSubmitter) SubmitScratch(ctx context.Context, req CanonicalJobSubmission, requirements costmodel.JobRequirements) (map[string]interface{}, error) {
	if s == nil || s.resolver == nil {
		return nil, fmt.Errorf("job submission service is not configured")
	}
	if req.Payload == nil {
		return nil, domain.NewInvalidPayload("payload", "required", "payload is required")
	}
	enq := s.resolver.Enqueuer()
	if enq == nil {
		return nil, fmt.Errorf("job submission service is not configured")
	}
	out, err := enq.Enqueue(ctx, req.Payload, requirements)
	if err == nil && out != nil {
		source := strings.TrimSpace(req.IntakeSource)
		if source == "" {
			source = IntakeSourceCanonical
		}
		if s.intake != nil {
			s.intake.IncAccepted(source)
		}
	}
	return out, err
}

// SubmitRaw persists an already-built Job+Task atomically through the shared
// atomic creator and records the intake source on an accepted creation. It is
// the raw counterpart of SubmitScratch for surfaces whose payload carries
// domain-specific metadata (e.g. calendar_event_id) that the canonical
// enqueue normalization would drop. The caller owns Job+TaskSpec construction
// and field preservation; the submitter owns the atomic write + intake
// telemetry.
func (s *CanonicalJobSubmitter) SubmitRaw(ctx context.Context, source string, job *jobs.Job, spec *taskgraph.TaskSpec, priority int) error {
	if s == nil || s.resolver == nil {
		return fmt.Errorf("job submission service is not configured")
	}
	enq := s.resolver.Enqueuer()
	if enq == nil || enq.Creator == nil {
		return fmt.Errorf("job submission service is not configured")
	}
	if err := enq.Creator.CreateJobWithTask(ctx, job, spec, priority); err != nil {
		return err
	}
	src := strings.TrimSpace(source)
	if src == "" {
		src = IntakeSourceCanonical
	}
	if s.intake != nil {
		s.intake.IncAccepted(src)
	}
	return nil
}

// sanitizedErrorSummary gives operators the actionable shape of an enqueue
// failure while keeping request references and local paths out of logs.
func sanitizedErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	words := strings.Fields(err.Error())
	for i, word := range words {
		if strings.Contains(word, "://") || strings.HasPrefix(word, "/") {
			words[i] = "<redacted-reference>"
		}
	}
	return strings.Join(words, " ")
}
