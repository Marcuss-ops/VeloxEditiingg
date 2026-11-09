// Package jobdoctor produces a read-only, evidence-backed diagnosis for one
// job. It answers the operator question "why is this job not advancing?"
// without opening a shell on the Master.
//
// The doctor never writes. Every conclusion carries the evidence that
// supports it; when the available tables cannot distinguish the cause, the
// report says INSUFFICIENT_EVIDENCE instead of inventing a diagnosis.
package jobdoctor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"velox-shared/taskcontract"
)

// ErrJobNotFound is returned when the requested job does not exist.
var ErrJobNotFound = errors.New("jobdoctor: job not found")

// Verdict is the top-level state of the job.
type Verdict string

const (
	VerdictAdvancing Verdict = "ADVANCING"
	VerdictStalled   Verdict = "STALLED"
	VerdictBlocked   Verdict = "BLOCKED"
	VerdictComplete  Verdict = "COMPLETE"
	VerdictUnknown   Verdict = "UNKNOWN"
)

// CauseCode is a stable machine-readable explanation. It is intentionally
// closed: new codes are added here, never invented as free-form log strings.
type CauseCode string

const (
	CausePreparationGatePending        CauseCode = "PREPARATION_GATE_PENDING"
	CauseRuntimeAssetsPending          CauseCode = "RUNTIME_ASSETS_PENDING"
	CauseReservationExpired            CauseCode = "RESERVATION_EXPIRED"
	CauseWorkerNotReady                CauseCode = "WORKER_NOT_READY"
	CauseCapacityExhausted             CauseCode = "CAPACITY_EXHAUSTED"
	CauseFenceConflict                 CauseCode = "FENCE_CONFLICT"
	CauseAttemptSucceededWithoutCommit CauseCode = "ATTEMPT_SUCCEEDED_WITHOUT_COMMIT"
	CauseNoProgress                    CauseCode = "NO_PROGRESS"
	CauseNoReadyTasks                  CauseCode = "NO_READY_TASKS"
	CauseInsufficientEvidence          CauseCode = "INSUFFICIENT_EVIDENCE"
)

// Thresholds controls stall detection.
type Thresholds struct {
	NoProgressAfter time.Duration
}

// DefaultThresholds matches the live job stall watchdog default.
func DefaultThresholds() Thresholds {
	return Thresholds{NoProgressAfter: 30 * time.Second}
}

// Evidence is one observed fact supporting the diagnosis.
type Evidence struct {
	Source string `json:"source"`
	Detail string `json:"detail"`
}

// Diagnosis is the stable machine-readable report.
type Diagnosis struct {
	SchemaVersion int     `json:"schema_version"`
	GeneratedAt   string  `json:"generated_at"`
	JobID         string  `json:"job_id"`
	JobStatus     string  `json:"job_status"`
	Verdict       Verdict `json:"verdict"`
	Summary       string  `json:"summary"`

	TaskCounts   map[string]int `json:"task_counts"`
	Tasks        int            `json:"tasks"`
	ReadyTasks   int            `json:"ready_tasks"`
	RunningTasks int            `json:"running_tasks"`

	WorkersTotal     int  `json:"workers_total"`
	WorkersAvailable int  `json:"workers_available"`
	FreeSlots        int  `json:"free_slots"`
	SlotsKnown       bool `json:"slots_known"`

	LastProgressAt string `json:"last_progress_at,omitempty"`
	ProgressAgeMS  int64  `json:"progress_age_ms"`

	Cause       CauseCode `json:"cause,omitempty"`
	CauseDetail string    `json:"cause_detail,omitempty"`

	Evidence []Evidence `json:"evidence,omitempty"`
	Warnings []string   `json:"warnings,omitempty"`

	InsufficientEvidence bool `json:"insufficient_evidence,omitempty"`
}

type taskSpecInfo struct {
	RuntimeAssetsPending bool
	FinalizeStatus       string
	PrepareTaskIDs       int
	RuntimeAssets        int
	PrepareWindow        bool
}

type attemptInfo struct {
	AttemptID string
	Status    string
	WorkerID  string
	LeaseID   string
	UpdatedAt string
}

type reservationInfo struct {
	WorkerID      string
	ReservationID string
	State         string
	ExpiresAt     string
}

type taskInfo struct {
	TaskID          string
	ExecutorID      string
	ExecutorVersion int
	Status          string
	WorkerID        string
	LeaseID         string
	Revision        int
	AttemptCount    int
	CreatedAt       string
	UpdatedAt       string
	Spec            *taskSpecInfo
	Attempts        []attemptInfo
	CommitStatuses  map[string]struct{}
	Reservation     *reservationInfo
}

// Diagnose reads one job and returns a diagnosis. It only issues SELECTs and
// tolerates older databases that lack optional projection tables.
func Diagnose(ctx context.Context, db *sql.DB, jobID string, now time.Time, thresholds Thresholds) (Diagnosis, error) {
	if db == nil {
		return Diagnosis{}, errors.New("jobdoctor: database is required")
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return Diagnosis{}, errors.New("jobdoctor: job id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if thresholds.NoProgressAfter <= 0 {
		thresholds = DefaultThresholds()
	}

	diagnosis := Diagnosis{
		SchemaVersion: 1,
		GeneratedAt:   now.UTC().Format(time.RFC3339Nano),
		JobID:         jobID,
		TaskCounts:    make(map[string]int),
	}

	var jobStatus, lastError string
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(status,''), COALESCE(last_error,'') FROM jobs WHERE job_id=?`, jobID,
	).Scan(&jobStatus, &lastError); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Diagnosis{}, ErrJobNotFound
		}
		return Diagnosis{}, fmt.Errorf("jobdoctor: read job: %w", err)
	}
	diagnosis.JobStatus = jobStatus

	var warnings []string
	tasks, ordered, err := loadTasks(ctx, db, jobID, &warnings)
	if err != nil {
		return Diagnosis{}, err
	}
	diagnosis.Tasks = len(ordered)
	var latestProgress time.Time
	for _, task := range ordered {
		diagnosis.TaskCounts[task.Status]++
		switch task.Status {
		case "READY":
			diagnosis.ReadyTasks++
		case "LEASED", "RUNNING":
			diagnosis.RunningTasks++
		}
		latestProgress = latestTimestamp(latestProgress, task.CreatedAt, task.UpdatedAt)
	}

	loadTaskSpecs(ctx, db, jobID, tasks, &warnings)
	loadAttempts(ctx, db, jobID, tasks, &warnings)
	loadCommits(ctx, db, jobID, tasks, &warnings)
	loadReservations(ctx, db, jobID, tasks, &warnings)
	workersTotal, workersAvailable, freeSlots, slotsKnown, workersKnown := loadWorkers(ctx, db, &warnings)
	diagnosis.WorkersTotal = workersTotal
	diagnosis.WorkersAvailable = workersAvailable
	diagnosis.FreeSlots = freeSlots
	diagnosis.SlotsKnown = slotsKnown
	for _, task := range ordered {
		for _, attempt := range task.Attempts {
			latestProgress = latestTimestamp(latestProgress, attempt.UpdatedAt)
		}
	}
	if runtimeProgress := loadRuntimeProgress(ctx, db, jobID, &warnings); runtimeProgress.After(latestProgress) {
		latestProgress = runtimeProgress
	}

	if !latestProgress.IsZero() {
		diagnosis.LastProgressAt = latestProgress.UTC().Format(time.RFC3339Nano)
		diagnosis.ProgressAgeMS = now.Sub(latestProgress).Milliseconds()
		if diagnosis.ProgressAgeMS < 0 {
			diagnosis.ProgressAgeMS = 0
		}
	}

	// Terminal job states are authoritative; do not infer a stall from rows
	// that the state machine has already sealed.
	switch strings.ToUpper(jobStatus) {
	case "SUCCEEDED":
		diagnosis.Verdict = VerdictComplete
		diagnosis.Summary = fmt.Sprintf("job %s is SUCCEEDED (%d task(s))", jobID, len(ordered))
		addEvidence(&diagnosis, "jobs", "terminal state SUCCEEDED")
		diagnosis.Warnings = sortedStrings(warnings)
		return diagnosis, nil
	case "FAILED", "CANCELLED", "TIMED_OUT":
		diagnosis.Verdict = VerdictBlocked
		diagnosis.CauseDetail = fmt.Sprintf("job is terminal with status %s", jobStatus)
		if strings.TrimSpace(lastError) != "" {
			diagnosis.CauseDetail += ": " + strings.TrimSpace(lastError)
		}
		diagnosis.Summary = fmt.Sprintf("job %s is %s", jobID, jobStatus)
		addEvidence(&diagnosis, "jobs", diagnosis.CauseDetail)
		diagnosis.Warnings = sortedStrings(warnings)
		return diagnosis, nil
	}

	if len(ordered) == 0 {
		diagnosis.Verdict = VerdictUnknown
		diagnosis.Cause = CauseInsufficientEvidence
		diagnosis.CauseDetail = "job has no task rows; the DAG may not have been materialized yet"
		diagnosis.InsufficientEvidence = true
		diagnosis.Summary = fmt.Sprintf("job %s has no tasks to diagnose", jobID)
		diagnosis.Warnings = sortedStrings(warnings)
		return diagnosis, nil
	}

	prepPending := make([]string, 0)
	expiredReservations := make([]string, 0)
	succeededOpen := make([]string, 0)
	commitPending := make([]string, 0)
	fenceConflicts := make([]string, 0)
	hasReservation := false
	for _, task := range ordered {
		if task.Reservation != nil {
			hasReservation = true
			if isExpiredReservation(task.Reservation, now) {
				expiredReservations = append(expiredReservations, task.TaskID)
			}
		}
		if task.Status == "READY" && task.Spec != nil && (task.Spec.RuntimeAssetsPending || isPreparationFinalizeStatus(task.Spec.FinalizeStatus)) {
			prepPending = append(prepPending, task.TaskID)
		}
		if task.Status == "READY" || task.Status == "LEASED" || task.Status == "RUNNING" {
			if succeeded := succeededAttempt(task); succeeded != "" {
				succeededOpen = append(succeededOpen, task.TaskID)
			}
			if task.Status == "LEASED" || task.Status == "RUNNING" {
				if !hasActiveAttempt(task) {
					fenceConflicts = append(fenceConflicts, task.TaskID)
				}
			}
		}
		if task.Status != "SUCCEEDED" && requiresArtifactCommit(task) && hasSucceededAttemptWithoutCommit(task) {
			commitPending = append(commitPending, task.TaskID)
		}
	}

	switch {
	case len(fenceConflicts) > 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseFenceConflict
		diagnosis.CauseDetail = fmt.Sprintf("%d LEASED/RUNNING task(s) have no active attempt", len(fenceConflicts))
		addEvidence(&diagnosis, "task_attempts", "fence conflict tasks: "+strings.Join(fenceConflicts, ","))
	case len(commitPending) > 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseAttemptSucceededWithoutCommit
		diagnosis.CauseDetail = fmt.Sprintf("%d task(s) have a successful attempt but no committed output", len(commitPending))
		addEvidence(&diagnosis, "attempt_commits", "tasks: "+strings.Join(commitPending, ","))
	case len(succeededOpen) > 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseAttemptSucceededWithoutCommit
		diagnosis.CauseDetail = fmt.Sprintf("%d non-terminal task(s) already have a SUCCEEDED attempt", len(succeededOpen))
		addEvidence(&diagnosis, "task_attempts", "tasks: "+strings.Join(succeededOpen, ","))
	case len(expiredReservations) > 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseReservationExpired
		diagnosis.CauseDetail = fmt.Sprintf("%d reservation(s) are expired but their tasks are still READY", len(expiredReservations))
		addEvidence(&diagnosis, "future_task_reservations", "tasks: "+strings.Join(expiredReservations, ","))
	case len(prepPending) > 0:
		diagnosis.Verdict = VerdictStalled
		if hasReservation {
			diagnosis.Cause = CausePreparationGatePending
			diagnosis.CauseDetail = "READY task(s) still declare pending runtime assets or a preparation finalize stage"
			addEvidence(&diagnosis, "task_specs", "tasks: "+strings.Join(prepPending, ","))
		} else {
			diagnosis.Cause = CauseRuntimeAssetsPending
			diagnosis.CauseDetail = "READY task(s) declare pending runtime assets and have no preparation reservation"
			addEvidence(&diagnosis, "task_specs", "tasks: "+strings.Join(prepPending, ","))
		}
	case diagnosis.ReadyTasks > 0 && !workersKnown:
		diagnosis.Verdict = VerdictUnknown
		diagnosis.Cause = CauseInsufficientEvidence
		diagnosis.CauseDetail = "READY tasks exist but the workers projection is unavailable"
		diagnosis.InsufficientEvidence = true
	case diagnosis.ReadyTasks > 0 && workersAvailable == 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseWorkerNotReady
		diagnosis.CauseDetail = fmt.Sprintf("%d READY task(s) but 0 of %d worker(s) are available", diagnosis.ReadyTasks, workersTotal)
		addEvidence(&diagnosis, "workers", fmt.Sprintf("total=%d available=0", workersTotal))
	case diagnosis.ReadyTasks > 0 && slotsKnown && freeSlots == 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseCapacityExhausted
		diagnosis.CauseDetail = fmt.Sprintf("%d READY task(s) but every available worker slot is occupied", diagnosis.ReadyTasks)
		addEvidence(&diagnosis, "workers", fmt.Sprintf("free_slots=0 available=%d", workersAvailable))
	case !latestProgress.IsZero() && now.Sub(latestProgress) > thresholds.NoProgressAfter && (diagnosis.ReadyTasks > 0 || diagnosis.RunningTasks > 0):
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseNoProgress
		diagnosis.CauseDetail = fmt.Sprintf("no task progress for %s (threshold %s)", now.Sub(latestProgress).Round(time.Second), thresholds.NoProgressAfter)
		addEvidence(&diagnosis, "tasks", "last progress at "+latestProgress.UTC().Format(time.RFC3339Nano))
	case diagnosis.ReadyTasks == 0 && diagnosis.RunningTasks == 0:
		diagnosis.Verdict = VerdictStalled
		diagnosis.Cause = CauseNoReadyTasks
		diagnosis.CauseDetail = "the job has no READY or RUNNING tasks and is not terminal"
		addEvidence(&diagnosis, "tasks", "no executable tasks")
	default:
		diagnosis.Verdict = VerdictAdvancing
	}

	if diagnosis.ReadyTasks > 0 && workersKnown {
		addEvidence(&diagnosis, "workers", fmt.Sprintf("total=%d available=%d free_slots=%d slots_known=%t", workersTotal, workersAvailable, freeSlots, slotsKnown))
	}
	if !latestProgress.IsZero() {
		addEvidence(&diagnosis, "progress", fmt.Sprintf("last progress %s ago", now.Sub(latestProgress).Round(time.Second)))
	} else {
		warnings = append(warnings, "no parseable task/attempt progress timestamp; stall age is unknown")
	}
	diagnosis.Summary = summaryFor(diagnosis)
	diagnosis.Warnings = sortedStrings(warnings)
	if diagnosis.Verdict == VerdictStalled && len(diagnosis.Evidence) == 0 {
		diagnosis.InsufficientEvidence = true
		diagnosis.Cause = CauseInsufficientEvidence
	}
	return diagnosis, nil
}

func addEvidence(diagnosis *Diagnosis, source, detail string) {
	diagnosis.Evidence = append(diagnosis.Evidence, Evidence{Source: source, Detail: detail})
}

func summaryFor(diagnosis Diagnosis) string {
	return fmt.Sprintf("job %s is %s: %d task(s), %d READY, %d RUNNING, %d/%d worker(s) available",
		diagnosis.JobID, diagnosis.Verdict, diagnosis.Tasks, diagnosis.ReadyTasks,
		diagnosis.RunningTasks, diagnosis.WorkersAvailable, diagnosis.WorkersTotal)
}

func loadTasks(ctx context.Context, db *sql.DB, jobID string, warnings *[]string) (map[string]*taskInfo, []*taskInfo, error) {
	if !tableExists(ctx, db, "tasks") {
		*warnings = append(*warnings, "table tasks is missing; task state is unknown")
		return map[string]*taskInfo{}, nil, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT task_id, COALESCE(executor_id,''), COALESCE(executor_version,0),
		       COALESCE(status,''), COALESCE(worker_id,''), COALESCE(lease_id,''),
		       COALESCE(revision,0), COALESCE(attempt_count,0),
		       COALESCE(created_at,''), COALESCE(updated_at,'')
		FROM tasks WHERE job_id=? ORDER BY created_at, task_id`, jobID)
	if err != nil {
		return nil, nil, fmt.Errorf("jobdoctor: list tasks: %w", err)
	}
	defer rows.Close()
	byID := make(map[string]*taskInfo)
	var ordered []*taskInfo
	for rows.Next() {
		task := &taskInfo{}
		if err := rows.Scan(&task.TaskID, &task.ExecutorID, &task.ExecutorVersion, &task.Status,
			&task.WorkerID, &task.LeaseID, &task.Revision, &task.AttemptCount, &task.CreatedAt, &task.UpdatedAt); err != nil {
			return nil, nil, fmt.Errorf("jobdoctor: scan task: %w", err)
		}
		byID[task.TaskID] = task
		ordered = append(ordered, task)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("jobdoctor: read tasks: %w", err)
	}
	return byID, ordered, nil
}

func loadTaskSpecs(ctx context.Context, db *sql.DB, jobID string, tasks map[string]*taskInfo, warnings *[]string) {
	if !tableExists(ctx, db, "task_specs") {
		*warnings = append(*warnings, "table task_specs is missing; runtime preparation state is unknown")
		return
	}
	rows, err := db.QueryContext(ctx, `
		SELECT ts.task_id, ts.payload_json
		FROM task_specs ts JOIN tasks t ON t.task_id=ts.task_id
		WHERE t.job_id=?`, jobID)
	if err != nil {
		*warnings = append(*warnings, "task_specs query failed: "+err.Error())
		return
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, raw string
		if err := rows.Scan(&taskID, &raw); err != nil {
			*warnings = append(*warnings, "task_specs scan failed: "+err.Error())
			return
		}
		task := tasks[taskID]
		if task == nil || strings.TrimSpace(raw) == "" {
			continue
		}
		var payload map[string]interface{}
		if json.Unmarshal([]byte(raw), &payload) != nil {
			continue
		}
		info := &taskSpecInfo{}
		info.RuntimeAssetsPending = boolValue(payload["runtime_assets_pending"])
		info.FinalizeStatus, _ = payload["runtime_finalize_status"].(string)
		info.PrepareTaskIDs = len(interfaceSlice(payload["runtime_prepare_task_ids"]))
		info.RuntimeAssets = len(interfaceSlice(payload["runtime_assets"]))
		_, info.PrepareWindow = payload["video_prepare_window"]
		task.Spec = info
	}
	if err := rows.Err(); err != nil {
		*warnings = append(*warnings, "task_specs rows failed: "+err.Error())
	}
}

func loadAttempts(ctx context.Context, db *sql.DB, jobID string, tasks map[string]*taskInfo, warnings *[]string) {
	if !tableExists(ctx, db, "task_attempts") {
		*warnings = append(*warnings, "table task_attempts is missing; attempt state is unknown")
		return
	}
	rows, err := db.QueryContext(ctx, `
		SELECT a.task_id, COALESCE(a.id,''), COALESCE(a.status,''),
		       COALESCE(a.worker_id,''), COALESCE(a.lease_id,''), COALESCE(a.updated_at,'')
		FROM task_attempts a JOIN tasks t ON t.task_id=a.task_id
		WHERE t.job_id=? ORDER BY a.task_id, a.attempt_number`, jobID)
	if err != nil {
		*warnings = append(*warnings, "task_attempts query failed: "+err.Error())
		return
	}
	defer rows.Close()
	for rows.Next() {
		var taskID string
		var attempt attemptInfo
		if err := rows.Scan(&taskID, &attempt.AttemptID, &attempt.Status, &attempt.WorkerID, &attempt.LeaseID, &attempt.UpdatedAt); err != nil {
			*warnings = append(*warnings, "task_attempts scan failed: "+err.Error())
			return
		}
		if task := tasks[taskID]; task != nil {
			task.Attempts = append(task.Attempts, attempt)
		}
	}
	if err := rows.Err(); err != nil {
		*warnings = append(*warnings, "task_attempts rows failed: "+err.Error())
	}
}

func loadCommits(ctx context.Context, db *sql.DB, jobID string, tasks map[string]*taskInfo, warnings *[]string) {
	if !tableExists(ctx, db, "attempt_commits") {
		*warnings = append(*warnings, "table attempt_commits is missing; commit state is unknown")
		return
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.task_id, COALESCE(c.status,'')
		FROM attempt_commits c JOIN tasks t ON t.task_id=c.task_id
		WHERE t.job_id=?`, jobID)
	if err != nil {
		*warnings = append(*warnings, "attempt_commits query failed: "+err.Error())
		return
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, status string
		if err := rows.Scan(&taskID, &status); err != nil {
			*warnings = append(*warnings, "attempt_commits scan failed: "+err.Error())
			return
		}
		if task := tasks[taskID]; task != nil {
			if task.CommitStatuses == nil {
				task.CommitStatuses = make(map[string]struct{})
			}
			task.CommitStatuses[strings.ToUpper(status)] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		*warnings = append(*warnings, "attempt_commits rows failed: "+err.Error())
	}
}

func loadReservations(ctx context.Context, db *sql.DB, jobID string, tasks map[string]*taskInfo, warnings *[]string) {
	columns := tableColumns(ctx, db, "future_task_reservations")
	if len(columns) == 0 {
		*warnings = append(*warnings, "table future_task_reservations is missing; reservation state is unknown")
		return
	}
	selected := []string{"task_id", "worker_id", "reservation_id", "expires_at"}
	if columns["state"] {
		selected = append(selected, "state")
	}
	rows, err := db.QueryContext(ctx,
		"SELECT "+strings.Join(selected, ",")+" FROM future_task_reservations WHERE job_id=?", jobID)
	if err != nil {
		*warnings = append(*warnings, "future_task_reservations query failed: "+err.Error())
		return
	}
	defer rows.Close()
	for rows.Next() {
		dest := make([]interface{}, len(selected))
		values := make([]sql.NullString, len(selected))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			*warnings = append(*warnings, "future_task_reservations scan failed: "+err.Error())
			return
		}
		reservation := &reservationInfo{WorkerID: values[1].String, ReservationID: values[2].String, ExpiresAt: values[3].String}
		if len(values) > 4 {
			reservation.State = values[4].String
		}
		if task := tasks[values[0].String]; task != nil {
			task.Reservation = reservation
		}
	}
	if err := rows.Err(); err != nil {
		*warnings = append(*warnings, "future_task_reservations rows failed: "+err.Error())
	}
}

func loadWorkers(ctx context.Context, db *sql.DB, warnings *[]string) (total, available, freeSlots int, slotsKnown, known bool) {
	columns := tableColumns(ctx, db, "workers")
	if len(columns) == 0 {
		*warnings = append(*warnings, "table workers is missing; worker availability is unknown")
		return 0, 0, 0, false, false
	}
	selected := []string{"worker_id"}
	for _, column := range []string{"status", "schedulable", "drain", "connection_status", "session_active", "active_task_count", "task_slots", "last_heartbeat", "last_heartbeat_at"} {
		if columns[column] {
			selected = append(selected, column)
		}
	}
	rows, err := db.QueryContext(ctx, "SELECT "+strings.Join(selected, ",")+" FROM workers")
	if err != nil {
		*warnings = append(*warnings, "workers query failed: "+err.Error())
		return 0, 0, 0, false, false
	}
	defer rows.Close()
	for rows.Next() {
		dest := make([]interface{}, len(selected))
		values := make([]sql.NullString, len(selected))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			*warnings = append(*warnings, "workers scan failed: "+err.Error())
			return 0, 0, 0, false, false
		}
		record := make(map[string]string, len(selected))
		for i, column := range selected {
			record[column] = values[i].String
		}
		total++
		if workerAvailable(record) {
			available++
			if slots, err := strconv.Atoi(record["task_slots"]); err == nil && slots > 0 {
				active, _ := strconv.Atoi(record["active_task_count"])
				if active < 0 {
					active = 0
				}
				if active > slots {
					active = slots
				}
				freeSlots += slots - active
				slotsKnown = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		*warnings = append(*warnings, "workers rows failed: "+err.Error())
		return total, available, freeSlots, slotsKnown, true
	}
	return total, available, freeSlots, slotsKnown, true
}

func loadRuntimeProgress(ctx context.Context, db *sql.DB, jobID string, warnings *[]string) time.Time {
	columns := tableColumns(ctx, db, "worker_task_runtime")
	if len(columns) == 0 {
		return time.Time{}
	}
	var latest time.Time
	query := "SELECT COALESCE(updated_at,'') FROM worker_task_runtime WHERE job_id=?"
	rows, err := db.QueryContext(ctx, query, jobID)
	if err != nil {
		*warnings = append(*warnings, "worker_task_runtime query failed: "+err.Error())
		return latest
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			*warnings = append(*warnings, "worker_task_runtime scan failed: "+err.Error())
			return latest
		}
		latest = latestTimestamp(latest, raw)
	}
	return latest
}

func workerAvailable(record map[string]string) bool {
	status := strings.ToUpper(strings.TrimSpace(record["status"]))
	connection := strings.ToUpper(strings.TrimSpace(record["connection_status"]))
	schedulable := true
	if raw := strings.TrimSpace(record["schedulable"]); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			schedulable = parsed != 0
		}
	}
	drain := strings.TrimSpace(record["drain"]) == "1" || strings.EqualFold(strings.TrimSpace(record["drain"]), "true")
	connected := record["session_active"] == "1" || connection == "CONNECTED" ||
		status == "CONNECTED" || status == "READY" || status == "HEALTHY" || status == "ACTIVE"
	return connected && schedulable && !drain
}

func requiresArtifactCommit(task *taskInfo) bool {
	taskContract, ok := taskcontract.Default().Lookup(task.ExecutorID, task.ExecutorVersion)
	if !ok {
		return false
	}
	return taskContract.CompletionKind() == taskcontract.CompletionArtifactCommit
}

func hasSucceededAttemptWithoutCommit(task *taskInfo) bool {
	if succeededAttempt(task) == "" {
		return false
	}
	if task.CommitStatuses == nil {
		return false
	}
	_, committed := task.CommitStatuses["COMMITTED"]
	return !committed
}

func succeededAttempt(task *taskInfo) string {
	for _, attempt := range task.Attempts {
		if strings.EqualFold(strings.TrimSpace(attempt.Status), "SUCCEEDED") {
			return attempt.AttemptID
		}
	}
	return ""
}

func hasActiveAttempt(task *taskInfo) bool {
	for _, attempt := range task.Attempts {
		switch strings.ToUpper(strings.TrimSpace(attempt.Status)) {
		case "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT":
			continue
		default:
			return true
		}
	}
	return false
}

func isPreparationFinalizeStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "QUEUED", "RUNNING", "WAITING_PREPARATION":
		return true
	default:
		return false
	}
}

func isExpiredReservation(reservation *reservationInfo, now time.Time) bool {
	if reservation == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(reservation.State), "EXPIRED") {
		return true
	}
	expiresAt, ok := parseTimestamp(reservation.ExpiresAt)
	return ok && now.After(expiresAt)
}

func boolValue(value interface{}) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case float64:
		return typed != 0
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true") || strings.TrimSpace(typed) == "1"
	default:
		return false
	}
}

func interfaceSlice(value interface{}) []interface{} {
	items, _ := value.([]interface{})
	return items
}

func latestTimestamp(current time.Time, values ...string) time.Time {
	for _, raw := range values {
		if parsed, ok := parseTimestamp(raw); ok && parsed.After(current) {
			current = parsed
		}
	}
	return current
}

func parseTimestamp(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func sortedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func tableExists(ctx context.Context, db *sql.DB, table string) bool {
	var found string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&found)
	return err == nil && found == table
}

func tableColumns(ctx context.Context, db *sql.DB, table string) map[string]bool {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil
		}
		columns[name] = true
	}
	return columns
}
