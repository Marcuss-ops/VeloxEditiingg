// Package worker — durable TaskResult reporting subsystem.
//
// task_result_reporter.go owns the reporting facade: it coordinates outcome
// classification, canonical result construction, report hashing, and the
// publication subsystem. The Worker composes this subsystem behind the small
// TaskResultReporter interface.
package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"

	"google.golang.org/protobuf/encoding/protojson"
)

const (
	taskResultRetryInitial = 2 * time.Second
	taskResultRetryMax     = 2 * time.Minute
	taskResultReplayBatch  = 32
	taskResultAckWait      = 30 * time.Second
	taskResultAckCacheTTL  = 2 * time.Minute
)

// TaskResultReporter is the reporting subsystem seam. The Worker holds only
// this interface; the composition root (New) builds the concrete
// implementation and wires its dependencies.
type TaskResultReporter interface {
	// Submit builds, hashes, and publishes the TaskResult. It returns the
	// WorkerToMasterEnvelope.sent_at timestamp (the real transport boundary
	// when the envelope was serialized) so the caller can stamp
	// result.sent with the exact wire timestamp rather than the wall clock
	// after Submit() returns.
	Submit(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) time.Time
	HandleAck(ack *pb.TaskResultAck)
	StartReplayLoop(ctx context.Context)
}

// artifactProtocolLogger is the callback seam for the structured
// artifact-publication protocol log.
type artifactProtocolLogger func(event string, pte *PendingTaskExecution, startedAt time.Time, commitID, artifactID, uploadID string, fields map[string]interface{})

// taskResultReporter implements TaskResultReporter. All dependencies are
// explicit constructor inputs (never *Worker).
type taskResultReporter struct {
	spool     *spool.Store
	transport func() controltransport.ControlTransport
	workerID  string
	protocol  string
	outputDir string

	storageResolver *storage.Resolver
	logger          *logger.Logger
	onTerminal      func()
	logArtifact     artifactProtocolLogger

	acksMu   sync.RWMutex
	acks     map[string]chan *pb.TaskResultAck
	ackCache map[string]taskResultAckCacheEntry

	wg       *sync.WaitGroup
	stopChan <-chan struct{}
}

func newTaskResultReporter(cfg taskResultReporterConfig) *taskResultReporter {
	return &taskResultReporter{
		spool:           cfg.spool,
		transport:       cfg.transport,
		workerID:        cfg.workerID,
		protocol:        cfg.protocol,
		outputDir:       cfg.outputDir,
		storageResolver: cfg.storageResolver,
		logger:          cfg.logger,
		onTerminal:      cfg.onTerminal,
		logArtifact:     cfg.logArtifact,
		acks:            make(map[string]chan *pb.TaskResultAck),
		ackCache:        make(map[string]taskResultAckCacheEntry),
		wg:              cfg.wg,
		stopChan:        cfg.stopChan,
	}
}

type taskResultReporterConfig struct {
	spool     *spool.Store
	transport func() controltransport.ControlTransport
	workerID  string
	protocol  string
	outputDir string

	storageResolver *storage.Resolver
	logger          *logger.Logger
	onTerminal      func()
	logArtifact     artifactProtocolLogger
	wg              *sync.WaitGroup
	stopChan        <-chan struct{}
}

// Submit coordinates classification, construction, hashing, and publication.
// Terminal cleanup remains gated on HandleAck in the publication module.
// Returns the WorkerToMasterEnvelope.sent_at timestamp so the caller can
// stamp result.sent with the exact wire boundary.
func (r *taskResultReporter) Submit(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) time.Time {
	resultStartedAt := time.Now()
	status, errorCode, errorDetail := classifyTaskResultOutcome(report, execErr)
	tr := buildTaskResult(r, pte, taskID, attemptID, report, status, errorCode, errorDetail)

	// Hash the canonical protojson representation with ReportHash empty, then
	// stamp the hash onto the wire message for idempotency and conflict checks.
	tr.ReportHash = ""
	reportJSON, err := protojson.Marshal(tr)
	if err != nil {
		r.logger.Error("[TASK] Failed to marshal TaskResult to protojson for %s: %v", taskID, err)
	} else {
		tr.ReportHash = fmt.Sprintf("%x", sha256.Sum256(reportJSON))
	}

	sentAt := r.publishTaskResult(ctx, pte, taskID, attemptID, report, tr, status, resultStartedAt)
	return sentAt
}
