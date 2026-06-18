// Package migrations provides read-only audit helpers used by the
// `velox-server migrate legacy-status` preflight command (PR7).
//
// Every function in this package is READ-ONLY: it MUST NOT mutate the
// database. The PR7 spec mandates that the legacy purge refuses to drop
// any table until LegacyStatusCounts reports EraseSafe() == true.
//
// The package is intentionally minimal — only count queries and schema
// introspection — so it stays unit-testable against an in-memory SQLite
// instance without pulling the full *store.SQLiteStore dependency.
package migrations

import (
	"context"
	"database/sql"
	"fmt"
)

// LegacyStatusCounts is the snapshot of legacy data that still lives in
// canonical tables. A non-zero count for any field blocks the legacy
// drop; PR7 says the drop MUST hard-fail rather than silently lose data.
//
// Counts are intentionally duplicated (table-level + column-level for
// the same predicate) so an operator can see whether stale values live
// in a stored row, in a legacy column, or both.
type LegacyStatusCounts struct {
	// jobs table — row-level state of legacy machines
	JobsProcessing  int64 `json:"jobs_processing_count"`
	JobsCompleted   int64 `json:"jobs_completed_count"`
	JobsAwaitingArt int64 `json:"jobs_awaiting_artifact_count"`
	JobsRenderFin   int64 `json:"jobs_render_finished_count"`

	// jobs table — embedded flat fields still authoritative
	JobsWithMasterPathEmbed   int64 `json:"jobs_with_master_video_path_embedded"`
	JobsWithDriveURLEmbed     int64 `json:"jobs_with_drive_url_embedded"`
	JobsWithYouTubeEmbed      int64 `json:"jobs_with_youtube_url_embedded"`
	JobsWithArtifactIDEmbed   int64 `json:"jobs_with_artifact_id_embedded"`
	JobsWithOutputSHA256Embed int64 `json:"jobs_with_output_sha256_embedded"`
	JobsWithIdempotencyEmbed  int64 `json:"jobs_with_idempotency_key_embedded"`
	JobsWithVideoUploadedCol  int64 `json:"jobs_with_video_uploaded_column_set"`
	JobsWithRawJSONNonEmpty   int64 `json:"jobs_with_raw_json_non_empty"`

	// job_deliveries table — legacy status strings (PROCESSING/COMPLETED)
	JobDeliveriesLegacyStatus int64 `json:"job_deliveries_legacy_status_count"`

	// workflow_runs — raw_json authoritative / non-empty
	WorkflowRunsCount             int64 `json:"workflow_runs_count"`
	WorkflowRunsRawJSONNonEmpty   int64 `json:"workflow_runs_raw_json_non_empty_count"`

	// schema introspection
	JobsHasLegacyColumns       bool     `json:"jobs_has_legacy_columns"`
	JobsLegacyColumnsPresent   []string `json:"jobs_legacy_columns_present,omitempty"`

	// filesystem hints (set by caller, not by SQL)
	DataDirJSONFiles         int `json:"data_dir_json_files,omitempty"`
	LegacyAllowlistNonEmpty  int `json:"legacy_allowlist_non_empty,omitempty"`
}

// EraseSafe reports whether ALL blocking counts are zero AND the jobs
// table no longer carries any of the legacy columns. PR7 specifies the
// legacy drop MUST refuse unless EraseSafe == true.
func (c LegacyStatusCounts) EraseSafe() bool {
	if c.JobsProcessing > 0 || c.JobsCompleted > 0 ||
		c.JobsAwaitingArt > 0 || c.JobsRenderFin > 0 {
		return false
	}
	if c.JobsWithMasterPathEmbed > 0 || c.JobsWithDriveURLEmbed > 0 ||
		c.JobsWithYouTubeEmbed > 0 || c.JobsWithArtifactIDEmbed > 0 ||
		c.JobsWithOutputSHA256Embed > 0 || c.JobsWithIdempotencyEmbed > 0 ||
		c.JobsWithVideoUploadedCol > 0 || c.JobsWithRawJSONNonEmpty > 0 {
		return false
	}
	if c.JobDeliveriesLegacyStatus > 0 {
		return false
	}
	if c.JobsHasLegacyColumns {
		return false
	}
	return true
}

// BlockingReasons returns a short human-readable list of why EraseSafe
// is false. Returns nil if EraseSafe is true.
func (c LegacyStatusCounts) BlockingReasons() []string {
	if c.EraseSafe() {
		return nil
	}
	var reasons []string
	if c.JobsProcessing > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs in PROCESSING status: %d", c.JobsProcessing))
	}
	if c.JobsCompleted > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs in COMPLETED status: %d", c.JobsCompleted))
	}
	if c.JobsAwaitingArt > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs in AWAITING_ARTIFACT status: %d", c.JobsAwaitingArt))
	}
	if c.JobsRenderFin > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs in RENDER_FINISHED status: %d", c.JobsRenderFin))
	}
	if c.JobsWithMasterPathEmbed > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with master_video_path populated: %d", c.JobsWithMasterPathEmbed))
	}
	if c.JobsWithDriveURLEmbed > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with drive_url populated: %d", c.JobsWithDriveURLEmbed))
	}
	if c.JobsWithYouTubeEmbed > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with youtube_url populated: %d", c.JobsWithYouTubeEmbed))
	}
	if c.JobsWithArtifactIDEmbed > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with artifact_id populated: %d", c.JobsWithArtifactIDEmbed))
	}
	if c.JobsWithOutputSHA256Embed > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with output_sha256 populated: %d", c.JobsWithOutputSHA256Embed))
	}
	if c.JobsWithIdempotencyEmbed > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with idempotency_key populated: %d", c.JobsWithIdempotencyEmbed))
	}
	if c.JobsWithVideoUploadedCol > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with video_uploaded=1: %d", c.JobsWithVideoUploadedCol))
	}
	if c.JobsWithRawJSONNonEmpty > 0 {
		reasons = append(reasons, fmt.Sprintf("jobs with non-empty raw_json: %d", c.JobsWithRawJSONNonEmpty))
	}
	if c.JobDeliveriesLegacyStatus > 0 {
		reasons = append(reasons, fmt.Sprintf("job_deliveries rows in legacy status: %d", c.JobDeliveriesLegacyStatus))
	}
	if c.JobsHasLegacyColumns {
		reasons = append(reasons, fmt.Sprintf("jobs table still carries legacy columns: %v", c.JobsLegacyColumnsPresent))
	}
	return reasons
}

// legacyColumnNames are the columns that PR7 says must be dropped from
// the jobs table as part of the rebuild migration. They correspond to
// fields whose values are now owned by artifacts + job_deliveries.
var legacyColumnNames = []string{
	"master_video_path",
	"drive_url",
	"youtube_url",
	"output_video_id",
	"last_upload_result",
	"last_upload_attempt_at",
	"last_drive_upload_result",
	"artifact_id",
	"output_sha256",
	"upload_idempotency_key",
	"video_uploaded",
}

// CountLegacyStatus reads the database and returns a snapshot of where
// legacy data still lives. Pure read-only — never mutates rows.
//
// Robust to partial migrations: if a table does not exist yet (e.g.
// migration hasn't been applied), the corresponding counts return 0
// instead of erroring. This lets the preflight run against both pre-
// and post-migration DBs.
func CountLegacyStatus(ctx context.Context, db *sql.DB) (LegacyStatusCounts, error) {
	if db == nil {
		return LegacyStatusCounts{}, fmt.Errorf("legacy-status: nil db handle")
	}
	var c LegacyStatusCounts

	// Row-level state of legacy machines in jobs.
	c.JobsProcessing = countIfTable(ctx, db, "jobs",
		"UPPER(COALESCE(status, '')) = 'PROCESSING'")
	c.JobsCompleted = countIfTable(ctx, db, "jobs",
		"UPPER(COALESCE(status, '')) = 'COMPLETED'")
	c.JobsAwaitingArt = countIfTable(ctx, db, "jobs",
		"UPPER(COALESCE(status, '')) = 'AWAITING_ARTIFACT'")
	c.JobsRenderFin = countIfTable(ctx, db, "jobs",
		"UPPER(COALESCE(status, '')) = 'RENDER_FINISHED'")

	// Embedded flat fields (row-level counts via legacy columns where
	// present, otherwise 0 if 028_legacy_drop or future migration has
	// dropped them).
	c.JobsWithMasterPathEmbed = countIfColumn(ctx, db, "jobs",
		"master_video_path",
		"COALESCE(master_video_path, '') <> ''")
	c.JobsWithDriveURLEmbed = countIfColumn(ctx, db, "jobs",
		"drive_url",
		"COALESCE(drive_url, '') <> ''")
	c.JobsWithYouTubeEmbed = countIfColumn(ctx, db, "jobs",
		"youtube_url",
		"COALESCE(youtube_url, '') <> ''")
	c.JobsWithArtifactIDEmbed = countIfColumn(ctx, db, "jobs",
		"artifact_id",
		"COALESCE(artifact_id, '') <> ''")
	c.JobsWithOutputSHA256Embed = countIfColumn(ctx, db, "jobs",
		"output_sha256",
		"COALESCE(output_sha256, '') <> ''")
	c.JobsWithIdempotencyEmbed = countIfColumn(ctx, db, "jobs",
		"upload_idempotency_key",
		"COALESCE(upload_idempotency_key, '') <> ''")
	c.JobsWithVideoUploadedCol = countIfColumn(ctx, db, "jobs",
		"video_uploaded",
		"video_uploaded = 1 OR video_uploaded = '1' OR video_uploaded = 'true'")
	c.JobsWithRawJSONNonEmpty = countIfTable(ctx, db, "jobs",
		"COALESCE(raw_json, '') <> '' AND COALESCE(raw_json, '{}') <> '{}'")

	// job_deliveries — only flag legacy status strings; newer statuses
	// (PENDING, RUNNING, etc.) are not counted.
	c.JobDeliveriesLegacyStatus = countIfTable(ctx, db, "job_deliveries",
		"UPPER(COALESCE(status, '')) IN "+
			"('PROCESSING','COMPLETED','AWAITING_ARTIFACT','RENDER_FINISHED')")

	// Workflow runs — raw_json must be empty/auxiliary once v2 is authoritative.
	c.WorkflowRunsCount = countIfTable(ctx, db, "workflow_runs", "1=1")
	c.WorkflowRunsRawJSONNonEmpty = countIfTable(ctx, db, "workflow_runs",
		"COALESCE(raw_json, '') <> '' AND COALESCE(raw_json, '{}') <> '{}'")

	// Schema introspection.
	c.JobsLegacyColumnsPresent = presentLegacyColumns(ctx, db, "jobs")
	c.JobsHasLegacyColumns = len(c.JobsLegacyColumnsPresent) > 0

	return c, nil
}

// countIfTable returns COUNT(*) for table filtered by predicate, or 0 if
// the table does not exist. Safe against partial schema state.
func countIfTable(ctx context.Context, db *sql.DB, table, predicate string) int64 {
	if !tableExists(ctx, db, table) {
		return 0
	}
	q := "SELECT COUNT(*) FROM " + quoteIdent(table) + " WHERE " + predicate
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0
	}
	return n
}

// countIfColumn is countIfTable guarded by column existence. Used for
// columns that newer migrations may have already dropped.
func countIfColumn(ctx context.Context, db *sql.DB, table, column, predicate string) int64 {
	if !tableExists(ctx, db, table) {
		return 0
	}
	if !columnExists(ctx, db, table, column) {
		return 0
	}
	q := "SELECT COUNT(*) FROM " + quoteIdent(table) + " WHERE " + predicate
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0
	}
	return n
}

// tableExists checks sqlite_master for the named table.
func tableExists(ctx context.Context, db *sql.DB, name string) bool {
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`,
		name,
	).Scan(&n)
	return err == nil && n > 0
}

// columnExists checks PRAGMA table_info for the named column.
func columnExists(ctx context.Context, db *sql.DB, table, column string) bool {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table)))
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

// presentLegacyColumns returns the subset of legacyColumnNames still
// present on the named table. Used by the schema-rebuild decision.
func presentLegacyColumns(ctx context.Context, db *sql.DB, table string) []string {
	if !tableExists(ctx, db, table) {
		return nil
	}
	var present []string
	for _, c := range legacyColumnNames {
		if columnExists(ctx, db, table, c) {
			present = append(present, c)
		}
	}
	return present
}

// quoteIdent double-quotes a SQLite identifier, escaping embedded
// double-quotes. Safe for use only with package-controlled identifiers;
// user-supplied table/column names MUST be validated upstream.
func quoteIdent(name string) string {
	out := make([]byte, 0, len(name)+2)
	out = append(out, '"')
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			out = append(out, '"', '"')
			continue
		}
		out = append(out, name[i])
	}
	out = append(out, '"')
	return string(out)
}
