// Package completion / ingest.go
//
// ingest.go: the ingest cluster of the Coordinator interface.
//
// This file owns two phase-methods (DeclareOutputs, RecordUploadProgress)
// and four internal helpers (commitGraceDefault, newUUIDLowerHex,
// generateDeterministicCommitToken, validateManifest) that are deliberately
// OUT OF UNITOFWORK SCOPE per docs/completion-protocol.md Fase 2.3 — the
// HMAC + INSERT-OR-IGNORE dance on attempt_commits / task_output_declarations
// is tightly coupled to the FenceTuple.Read gate and stays raw SQL here.
// Future refactor candidates can fold the SQL into the UnitOfWork repos
// once the HMAC key plumbing has a clearer seam.
//
// The phase-methods in this file:
//
//   - DeclareOutputs:        upsert attempt_commits + per-decl declarations;
//     generate a fresh commit_id (or reuse canonical
//     one on replay); derive the deterministic
//     commit_token via HMAC-SHA256 over the canonical
//     (commit_id, FenceTuple) — Verdetto P0 #6.
//
//   - RecordUploadProgress:  heartbeat path; MONOTONIC-progress guarantee
//     via MAX() on uploaded_bytes + updated_at
//     (Verdetto P0, Blocco 3).
//
// Helpers (only consumed by the methods above):
//   - commitGraceDefault:        canonical deadline-extension window
//   - newUUIDLowerHex:           crypto/rand-backed 16-byte hex ID mint
//   - generateDeterministicCommitToken: HMAC-SHA256 over (commit_id, fence)
//   - validateManifest:          OutputManifest field-shape guard
//
// See coordinator.go for the orchestrator + the remaining phase-methods
// (CompleteUpload in validate.go, CommitAttempt in persist.go,
// ReconcileAttempt in notify.go).
package completion

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ────────────────────────────────────────────────────────────────────────
// DeclareOutputs — OUT OF UNITOFWORK SCOPE (HMAC + INSERT-OR-IGNORE
// dance tied to FenceTuple.Read). See package-level doc.
// ────────────────────────────────────────────────────────────────────────

// DeclareOutputs upserts an AttemptCommit row + per-decl declaration
// rows in one BEGIN IMMEDIATE transaction. The FenceTuple is validated
// at entry; a malformed tuple is rejected with ErrFenceMismatch
// without touching the database.
//
// Idempotency:
//
//   - attempt_commits has UNIQUE(task_id, attempt_id). A replay of
//     DeclareOutputs with the same (task_id, attempt_id) is a SQL
//     no-op on that row (INSERT-OR-IGNORE swallows the conflict).
//   - task_output_declarations has UNIQUE(task_id, attempt_id,
//     output_kind, logical_name). The loop's INSERT-OR-IGNORE makes
//     each declaration upsert individually.
//   - A previous AttemptCommit with status=DECLARED|UPLOADING|... is
//     left untouched; the master is allowed to enrich it via
//     RecordUploadProgress without re-Declaring.
//
// On replay the Reply has FRESH commit_token + commit_token_hash
// (regenerated per-call). The worker should ignore the new token if it
// already holds the first one.
func (c *coordinator) DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error) {
	if err := cmd.Fence.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if len(cmd.OutputManifests) == 0 {
		return nil, fmt.Errorf("completion.DeclareOutputs: at least one OutputManifest required (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	// Mint a fresh candidate commit_id from crypto/rand. The canonical
	// commit_id (i.e. the value persisted in attempt_commits) is decided
	// AFTER FenceTuple.ReadOrMissing runs: if a row already exists, we
	// reuse its commit_id; otherwise we INSERT with this candidate.
	// Entropy failure is fail-closed (Verdetto P0 #7): a deterministic
	// fallback would collide commits across the cluster and break
	// UNIQUE(task_id, attempt_id) dedup.
	commitID, err := newUUIDLowerHex()
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: mint commit_id: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	deadline := time.Now().UTC().Add(commitGraceDefault).Format(time.RFC3339Nano)

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	state, err := cmd.Fence.ReadOrMissing(ctx, tx)
	if err != nil {
		return nil, err
	}
	if state != nil {
		// Replay path: reuse the canonical commit_id from the existing
		// attempt_commits row. The local candidate commitID is discarded.
		commitID = state.CommitID
	}

	// Derive commit_token + commit_token_hash AFTER canonicalization so
	// a replay produces the SAME token against the canonical commitID.
	// The previous Blocco 2 implementation derived the token BEFORE
	// ReadOrMissing (against the local candidate) which broke this
	// invariant; the regression-guard
	// TestCoordinator_DeclareOutputs_ReplayYieldsIdenticalToken caught
	// it. Sequencing here is the contract.
	token, tokenHash, err := generateDeterministicCommitToken(c, commitID, cmd.Fence)
	if err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: derive commit_token: %w", err)
	}

	if state == nil {
		// First-call path: INSERT with commit_id + token_hash.
		res, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO attempt_commits (
				commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
				task_revision, status, required_output_count,
				commit_token_hash, commit_deadline_at, last_progress_at,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, 'DECLARED', ?, ?, ?, ?, ?, ?)`,
			commitID,
			cmd.Fence.TaskID, cmd.Fence.AttemptID, cmd.JobID, cmd.Fence.WorkerID, cmd.Fence.LeaseID,
			cmd.Fence.Revision,
			len(cmd.OutputManifests),
			tokenHash, deadline, now,
			now, now,
		)
		if err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: insert attempt_commits: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected == 0 {
			// Race: another writer inserted between our
			// ReadOrMissing and our INSERT. Re-read the canonical
			// commit_id from the row the other writer inserted and
			// re-derive the determinism-bearing token so the
			// returned UploadPlan stays consistent.
			var existingCommitID string
			if err := tx.QueryRowContext(ctx,
				`SELECT commit_id FROM attempt_commits
				 WHERE task_id = ? AND attempt_id = ?`,
				cmd.Fence.TaskID, cmd.Fence.AttemptID,
			).Scan(&existingCommitID); err != nil {
				return nil, fmt.Errorf("completion.DeclareOutputs: select existing commit_id after race: %w", err)
			}
			commitID = existingCommitID
			token, tokenHash, err = generateDeterministicCommitToken(c, commitID, cmd.Fence)
			if err != nil {
				return nil, fmt.Errorf("completion.DeclareOutputs: re-derive commit_token after race: %w", err)
			}
			_ = tokenHash // canonical row already carries its own commit_token_hash
		}
	}

	for _, m := range cmd.OutputManifests {
		if err := validateManifest(&m); err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: invalid manifest: %w", err)
		}
		declarationID, derr := newUUIDLowerHex()
		if derr != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: mint declaration_id: %w", derr)
		}
		_, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO task_output_declarations (
				declaration_id, commit_id, task_id, attempt_id,
				output_kind, logical_name, mime_type,
				expected_size_bytes, expected_sha256,
				worker_spool_key, status,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DECLARED', ?, ?)`,
			declarationID,
			commitID, cmd.Fence.TaskID, cmd.Fence.AttemptID,
			m.OutputKind, m.LogicalName, m.MimeType,
			m.SizeBytes, m.SHA256,
			m.WorkerSpoolKey,
			now, now,
		)
		if err != nil {
			return nil, fmt.Errorf("completion.DeclareOutputs: insert declaration %s: %w", m.LogicalName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("completion.DeclareOutputs: commit: %w", err)
	}
	committed = true

	return &UploadPlan{
		CommitID:    commitID,
		CommitToken: token,
		Targets:     nil,
	}, nil
}

// ────────────────────────────────────────────────────────────────────────
// RecordUploadProgress — OUT OF UNITOFWORK SCOPE (per-declaration
// uploaded_bytes UPDATE on task_output_declarations is held by the
// heartbeat update path, not by the commit protocol). See package-
// level doc.
// ────────────────────────────────────────────────────────────────────────

// RecordUploadProgress bumps the last_progress_at and commit_deadline_at
// on the canonical attempt_commits row CAS-gated on the FenceTuple
// AND status. uploaded_bytes on the corresponding task_output_declar
// ations row(s) is incremented monotonically.
//
// The MAX(uploaded_bytes, ?) inline aggregate that enforces the
// monotonic-progress guarantee lives in this function's tx.ExecContext
// SQL below (no `store.UpdateProgress` helper exists — pace the Blocco
// 3 doc-vs-impl audit, the aggregate was always inline; do NOT
// reintroduce a removed helper). updated_at follows the same
// MAX-rule so a stale heartbeat cannot roll back the reconciler's
// last_progress_at timestamp.
//
// Idempotency:
//
//   - last_progress_at = MAX(last_progress_at, new_value): a worker
//     that re-sends an older heartbeat cannot regress the timestamp.
//   - commit_deadline_at is bumped forward by the same amount as the
//     progress delta, ensuring the lease-renewal deadline always
//     extends past the worker's progress.
//
// A stale fence (worker on a reaped lease) returns ErrFenceMismatch
// or ErrTransitionConflict depending on which gate failed. The
// supervisor's reconcile path will resurrect a stale DECLARED row
// (Fase 4.1); RecordUploadProgress itself does NOT mutate terminal
// rows.
func (c *coordinator) RecordUploadProgress(ctx context.Context, cmd RecordUploadProgressCommand) error {
	if err := cmd.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	if cmd.UploadID == "" {
		return fmt.Errorf("completion.RecordUploadProgress: UploadID empty (task_id=%s attempt_id=%s)", cmd.Fence.TaskID, cmd.Fence.AttemptID)
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	state, err := cmd.Fence.Read(ctx, tx)
	if err != nil {
		return err
	}

	repos := c.uowFactory.WithTx(tx)
	n, err := repos.AttemptCommits().UpdateProgress(ctx, state.CommitID, nowStr, now.Add(commitGraceDefault).Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: update attempt_commits: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: status=%q (cannot progress past terminal/rejected state)", ErrTransitionConflict, state.Status)
	}

	if cmd.UploadedBytes > 0 {
		// Verdetto P0 (Blocco 3): monotonic-progress guarantee.
		// uploaded_bytes and updated_at use MAX() so a worker
		// that re-sends an older heartbeat (e.g. a chunk upload
		// whose TCP segment was reordered, or a heartbeat that
		// arrived after a newer one) cannot regress the canonical
		// progress. The persisted value is the MAX of the
		// current and incoming values; updated_at follows the
		// same rule so a stale heartbeat doesn't roll back the
		// last_progress_at timestamp the reconciler uses to
		// detect wedged workers.
		_, err = tx.ExecContext(ctx,
			`UPDATE task_output_declarations
			    SET uploaded_bytes = MAX(uploaded_bytes, ?), updated_at = MAX(updated_at, ?)
			  WHERE commit_id IN (
			      SELECT commit_id FROM attempt_commits
			      WHERE task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?
			  )
			  AND upload_id = ?`,
			cmd.UploadedBytes, nowStr,
			cmd.Fence.TaskID, cmd.Fence.AttemptID, cmd.Fence.WorkerID, cmd.Fence.LeaseID,
			cmd.UploadID,
		)
		if err != nil {
			return fmt.Errorf("completion.RecordUploadProgress: update task_output_declarations: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("completion.RecordUploadProgress: commit: %w", err)
	}
	committed = true
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────

// commitGraceDefault is the canonical extension window for the
// commit_deadline_at stamp at DeclareOutputs / RecordUploadProgress
// time. Two minutes keeps a healthy worker's heartbeat inside the
// window while a slow chunk upload still gets a chance to bump it
// forward before the reaper fires.
const commitGraceDefault = 2 * time.Minute

// newUUIDLowerHex returns a 32-hex-char sequence (16 bytes of entropy).
// SQLite's PRIMARY KEY is TEXT and we use the string everywhere on
// the wire and in logs; full UUIDs are 36 chars (with hyphens) — we
// keep the lower-hex form to halve the log noise.
//
// NOTE: This is NOT a UUIDv4 because we do not permute the version
// bits. It is a 16-byte hex string with the same collision property.
//
// Verdetto P0 #7: entropy failure is fail-closed. A previous
// `byte(i+1)` fallback was deterministic and would have collided
// commits across the cluster, breaking UNIQUE(task_id, attempt_id)
// dedup at scale. The error propagates through DeclareOutputs into
// the tx rollback path, surfacing in /ready and the supervisor.
func newUUIDLowerHex() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("completion: entropy failure for UUID (crypto/rand): %w", err)
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, by := range b {
		out[i*2] = hexdigits[by>>4]
		out[i*2+1] = hexdigits[by&0x0f]
	}
	return string(out), nil
}

// generateDeterministicCommitToken derives a replay-safe bearer
// token from the canonical (commit_id, fence) and the master-side
// HMAC key. Two calls with the same inputs return the same token.
//
// Token shape: HMAC-SHA256(key, "v1|<commitID>|<taskID>|<attemptID>|<workerID>|<leaseID>|<revision>").
// The token is the hex-encoded HMAC digest (32 bytes -> 64 hex
// chars). The hash persisted on attempt_commits.commit_token_hash
// is the SHA256 hex of the decoded token bytes.
func generateDeterministicCommitToken(c *coordinator, commitID string, fence FenceTuple) (token, hash string, err error) {
	if len(c.hmacKey) < 32 {
		return "", "", fmt.Errorf("completion: commit HMAC key not configured (must be >= 32 bytes)")
	}
	mac := hmac.New(sha256.New, c.hmacKey)
	if _, ferr := fmt.Fprintf(mac, "v1|%s|%s|%s|%s|%s|%d",
		commitID, fence.TaskID, fence.AttemptID, fence.WorkerID, fence.LeaseID, fence.Revision,
	); ferr != nil {
		return "", "", fmt.Errorf("completion: derive commit_token hmac write: %w", ferr)
	}
	sum := mac.Sum(nil)
	token = hex.EncodeToString(sum)
	hashSum := sha256.Sum256(sum)
	return token, hex.EncodeToString(hashSum[:]), nil
}

// validateManifest enforces the basic invariants on a per-file
// declaration. The check is intentionally minimal (the worker is the
// source of truth for mime / size / sha and the master re-verifies in
// CompleteUpload at Fase 2.5); this guard exists only to surface
// blatant caller mistakes loudly.
func validateManifest(m *OutputManifest) error {
	if strings.TrimSpace(m.OutputKind) == "" {
		return fmt.Errorf("manifest: OutputKind empty")
	}
	if strings.TrimSpace(m.LogicalName) == "" {
		return fmt.Errorf("manifest: LogicalName empty")
	}
	if strings.TrimSpace(m.MimeType) == "" {
		return fmt.Errorf("manifest: MimeType empty")
	}
	if m.SizeBytes <= 0 {
		return fmt.Errorf("manifest: SizeBytes must be > 0 (got %d)", m.SizeBytes)
	}
	if len(m.SHA256) != 64 {
		return fmt.Errorf("manifest: SHA256 must be 64 hex chars (got %d chars)", len(m.SHA256))
	}
	for _, c := range m.SHA256 {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return fmt.Errorf("manifest: SHA256 must be lowercase hex")
		}
	}
	return nil
}
