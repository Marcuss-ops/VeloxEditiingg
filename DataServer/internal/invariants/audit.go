// Package invariants provides non-mutating operational audits.
package invariants

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"velox-server/internal/statemachine"

	_ "github.com/mattn/go-sqlite3"
)

// Finding is one invariant violation. Resource identifiers are included to
// make the report actionable, but the auditor never changes the row.
type Finding struct {
	Invariant    string `json:"invariant"`
	Domain       string `json:"domain"`
	ResourceType string `json:"resource_type"`
	ResourceID   string `json:"resource_id"`
	Observed     string `json:"observed"`
	Expected     string `json:"expected"`
	Detail       string `json:"detail"`
}

// Report is the stable machine-readable output of audit-invariants.
type Report struct {
	GeneratedAt string                        `json:"generated_at"`
	Mode        string                        `json:"mode"`
	Database    string                        `json:"database"`
	Rules       []statemachine.TransitionRule `json:"transition_rules"`
	Invariants  []statemachine.Invariant      `json:"invariants"`
	Findings    []Finding                     `json:"findings"`
	OK          bool                          `json:"ok"`
}

// OpenReadOnly opens a SQLite database with SQLite's read-only URI.
// No migration or post-migration adjustment is run, and callers cannot write
// through the returned handle.
func OpenReadOnly(path string) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("invariants: database path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("invariants: normalize database path: %w", err)
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs), RawQuery: "mode=ro"}).String() + "&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("invariants: open read-only database: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("invariants: ping read-only database: %w", err)
	}
	return db, nil
}

// Audit executes only SELECT statements against db. It is safe to run against
// a live database or a filesystem snapshot; callers commonly use OpenReadOnly.
func Audit(ctx context.Context, db *sql.DB, path string, now time.Time) (Report, error) {
	if db == nil {
		return Report{}, errors.New("invariants: nil database")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	registry := statemachine.DefaultRegistry()
	report := Report{
		GeneratedAt: now.UTC().Format(time.RFC3339Nano),
		Mode:        "read-only",
		Database:    path,
		Rules:       registry.Rules(),
		Invariants:  registry.Invariants(),
		OK:          true,
	}
	for _, check := range []func(context.Context, *sql.DB, *[]Finding) error{
		checkUnknownStatuses, checkTaskAttemptPair, checkJobTaskConvergence,
		checkReadyArtifacts, checkCompletedDeliveries, checkWorkerSessions,
		checkCompletedUploads,
	} {
		if err := check(ctx, db, &report.Findings); err != nil {
			return Report{}, err
		}
	}
	sort.Slice(report.Findings, func(i, j int) bool {
		if report.Findings[i].Invariant != report.Findings[j].Invariant {
			return report.Findings[i].Invariant < report.Findings[j].Invariant
		}
		return report.Findings[i].ResourceID < report.Findings[j].ResourceID
	})
	report.OK = len(report.Findings) == 0
	return report, nil
}

func add(out *[]Finding, finding Finding) { *out = append(*out, finding) }

func checkUnknownStatuses(ctx context.Context, db *sql.DB, out *[]Finding) error {
	registry := statemachine.DefaultRegistry()
	checks := []struct {
		table, domain string
		allowed       []string
	}{
		{"jobs", string(statemachine.DomainJob), registry.States(statemachine.DomainJob)},
		{"tasks", string(statemachine.DomainTask), registry.States(statemachine.DomainTask)},
		{"artifacts", string(statemachine.DomainArtifact), registry.States(statemachine.DomainArtifact)},
		{"artifact_uploads", string(statemachine.DomainArtifactUpload), registry.States(statemachine.DomainArtifactUpload)},
		{"job_deliveries", string(statemachine.DomainDelivery), registry.States(statemachine.DomainDelivery)},
		{"worker_sessions", string(statemachine.DomainWorkerSession), registry.States(statemachine.DomainWorkerSession)},
	}
	for _, check := range checks {
		allowed := make([]string, len(check.allowed))
		for i := range check.allowed {
			allowed[i] = "?"
		}
		query := fmt.Sprintf("SELECT %s, status FROM %s WHERE UPPER(COALESCE(status,'')) NOT IN (%s)", idColumn(check.table), check.table, strings.Join(allowed, ","))
		args := make([]interface{}, len(check.allowed))
		for i, state := range check.allowed {
			args[i] = state
		}
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("audit unknown %s statuses: %w", check.domain, err)
		}
		for rows.Next() {
			var id, status string
			if err := rows.Scan(&id, &status); err != nil {
				rows.Close()
				return err
			}
			add(out, Finding{Invariant: "known_states", Domain: check.domain, ResourceType: check.table, ResourceID: id, Observed: status, Expected: strings.Join(check.allowed, "|"), Detail: "row contains a state absent from the transition registry"})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	return nil
}

func idColumn(table string) string {
	switch table {
	case "jobs":
		return "job_id"
	case "tasks":
		return "task_id"
	case "artifacts":
		return "id"
	case "artifact_uploads":
		return "upload_id"
	case "job_deliveries":
		return "delivery_id"
	case "worker_sessions":
		return "session_id"
	default:
		return "rowid"
	}
}

func checkTaskAttemptPair(ctx context.Context, db *sql.DB, out *[]Finding) error {
	rows, err := db.QueryContext(ctx, `
		SELECT t.task_id, t.status,
		       CASE WHEN EXISTS (
			SELECT 1 FROM task_attempts a WHERE a.task_id=t.task_id
			  AND a.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')

		       ) THEN 1 ELSE 0 END
		FROM tasks t
		WHERE (t.status IN ('LEASED','RUNNING') AND NOT EXISTS (
			SELECT 1 FROM task_attempts a WHERE a.task_id=t.task_id
			  AND a.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')

		))
		OR (t.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT') AND EXISTS (
			SELECT 1 FROM task_attempts a WHERE a.task_id=t.task_id
			  AND a.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		))
		OR (t.status='SUCCEEDED' AND NOT EXISTS (
			SELECT 1 FROM task_attempts a WHERE a.task_id=t.task_id AND a.status='SUCCEEDED'
		))
	`)
	if err != nil {
		return fmt.Errorf("audit task/attempt invariant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, status string
		var active int
		if err := rows.Scan(&id, &status, &active); err != nil {
			return err
		}
		add(out, Finding{Invariant: statemachine.InvariantTaskAttemptPair, Domain: "task", ResourceType: "task", ResourceID: id, Observed: fmt.Sprintf("status=%s active_attempt=%t", status, active == 1), Expected: "task and attempt lifecycle states converge", Detail: "task/attempt pair violates the canonical execution invariant"})
	}
	return rows.Err()
}

func checkJobTaskConvergence(ctx context.Context, db *sql.DB, out *[]Finding) error {
	rows, err := db.QueryContext(ctx, `
		SELECT j.job_id, j.status, COALESCE(t.status,'<missing>')
		FROM jobs j LEFT JOIN tasks t ON t.job_id=j.job_id
		WHERE (j.status='SUCCEEDED' AND (t.task_id IS NULL OR t.status<>'SUCCEEDED'))
		   OR (j.status IN ('FAILED','CANCELLED') AND t.status IN ('PENDING','READY','LEASED','RUNNING'))
	`)
	if err != nil {
		return fmt.Errorf("audit job/task convergence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var jobID, jobStatus, taskStatus string
		if err := rows.Scan(&jobID, &jobStatus, &taskStatus); err != nil {
			return err
		}
		add(out, Finding{Invariant: statemachine.InvariantJobTaskConvergence, Domain: "job", ResourceType: "job", ResourceID: jobID, Observed: fmt.Sprintf("job=%s task=%s", jobStatus, taskStatus), Expected: "terminal aggregate agrees with task state", Detail: "job/task roll-up is not converged"})
	}
	return rows.Err()
}

func checkReadyArtifacts(ctx context.Context, db *sql.DB, out *[]Finding) error {
	rows, err := db.QueryContext(ctx, `SELECT id, COALESCE(storage_key,''), COALESCE(local_path,'') FROM artifacts WHERE status='READY' AND COALESCE(storage_key,'')='' AND COALESCE(local_path,'')=''`)
	if err != nil {
		return fmt.Errorf("audit ready artifact invariant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, key, path string
		if err := rows.Scan(&id, &key, &path); err != nil {
			return err
		}
		add(out, Finding{Invariant: statemachine.InvariantArtifactReadyBlob, Domain: "artifact", ResourceType: "artifact", ResourceID: id, Observed: "storage_key/local_path empty", Expected: "durable blob reference", Detail: "READY artifact has no retrievable blob"})
	}
	return rows.Err()
}

func checkCompletedDeliveries(ctx context.Context, db *sql.DB, out *[]Finding) error {
	rows, err := db.QueryContext(ctx, `SELECT delivery_id, COALESCE(remote_id,'') FROM job_deliveries WHERE status='SUCCEEDED' AND COALESCE(remote_id,'')=''`)
	if err != nil {
		return fmt.Errorf("audit delivery invariant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, remoteID string
		if err := rows.Scan(&id, &remoteID); err != nil {
			return err
		}
		add(out, Finding{Invariant: statemachine.InvariantDeliveryRemoteID, Domain: "delivery", ResourceType: "job_delivery", ResourceID: id, Observed: remoteID, Expected: "remote object identifier", Detail: "SUCCEEDED delivery has no provider remote_id"})
	}
	return rows.Err()
}

func checkWorkerSessions(ctx context.Context, db *sql.DB, out *[]Finding) error {
	rows, err := db.QueryContext(ctx, `SELECT worker_id, session_type, COUNT(*) FROM worker_sessions WHERE status='ACTIVE' AND revoked=0 GROUP BY worker_id, session_type HAVING COUNT(*) > 1`)
	if err != nil {
		return fmt.Errorf("audit worker session invariant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var worker, sessionType string
		var count int
		if err := rows.Scan(&worker, &sessionType, &count); err != nil {
			return err
		}
		add(out, Finding{Invariant: statemachine.InvariantWorkerSingleSession, Domain: "worker_session", ResourceType: "worker", ResourceID: worker, Observed: fmt.Sprintf("session_type=%s active=%d", sessionType, count), Expected: "at most one active session per type", Detail: "worker session anti-collision invariant is violated"})
	}
	return rows.Err()
}

func checkCompletedUploads(ctx context.Context, db *sql.DB, out *[]Finding) error {
	rows, err := db.QueryContext(ctx, `SELECT upload_id, COALESCE(completed_at,'') FROM artifact_uploads WHERE status='COMPLETED' AND COALESCE(completed_at,'')=''`)
	if err != nil {
		return fmt.Errorf("audit upload invariant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, completed string
		if err := rows.Scan(&id, &completed); err != nil {
			return err
		}
		add(out, Finding{Invariant: statemachine.InvariantUploadTerminalFields, Domain: "artifact_upload", ResourceType: "artifact_upload", ResourceID: id, Observed: completed, Expected: "completed_at populated", Detail: "COMPLETED upload has no completion timestamp"})
	}
	return rows.Err()
}
