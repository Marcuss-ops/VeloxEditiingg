package completion

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ListUploadBindings returns the canonical declaration/session joins for a
// commit. Ordering is stable by declaration insertion order and callers that
// need a manifest match should use OutputKind + LogicalName.
func (c *coordinator) ListUploadBindings(ctx context.Context, commitID string) ([]UploadBinding, error) {
	if strings.TrimSpace(commitID) == "" {
		return nil, fmt.Errorf("completion.ListUploadBindings: commitID empty")
	}
	rows, err := c.db.QueryContext(ctx, `
		SELECT d.declaration_id, d.commit_id, COALESCE(d.upload_id,''),
		       COALESCE(d.artifact_id,''), d.task_id, d.attempt_id,
		       ac.worker_id, ac.lease_id, ac.task_revision,
		       d.output_kind, d.logical_name
		  FROM task_output_declarations d
		  JOIN attempt_commits ac ON ac.commit_id = d.commit_id
		 WHERE d.commit_id = ?
		 ORDER BY d.rowid`, commitID)
	if err != nil {
		return nil, fmt.Errorf("completion.ListUploadBindings: query: %w", err)
	}
	defer rows.Close()
	var out []UploadBinding
	for rows.Next() {
		var b UploadBinding
		if err := rows.Scan(&b.DeclarationID, &b.CommitID, &b.UploadID,
			&b.ArtifactID, &b.TaskID, &b.AttemptID, &b.WorkerID, &b.LeaseID,
			&b.Revision, &b.OutputKind, &b.LogicalName); err != nil {
			return nil, fmt.Errorf("completion.ListUploadBindings: scan: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("completion.ListUploadBindings: rows: %w", err)
	}
	return out, nil
}

// GetUploadBinding resolves a worker upload ID back to its fenced commit.
func (c *coordinator) GetUploadBinding(ctx context.Context, uploadID string) (*UploadBinding, error) {
	if strings.TrimSpace(uploadID) == "" {
		return nil, fmt.Errorf("completion.GetUploadBinding: uploadID empty")
	}
	var b UploadBinding
	err := c.db.QueryRowContext(ctx, `
		SELECT d.declaration_id, d.commit_id, d.upload_id,
		       COALESCE(d.artifact_id,''), d.task_id, d.attempt_id,
		       ac.worker_id, ac.lease_id, ac.task_revision,
		       d.output_kind, d.logical_name
		  FROM task_output_declarations d
		  JOIN attempt_commits ac ON ac.commit_id = d.commit_id
		 WHERE d.upload_id = ?`, uploadID).Scan(
		&b.DeclarationID, &b.CommitID, &b.UploadID, &b.ArtifactID,
		&b.TaskID, &b.AttemptID, &b.WorkerID, &b.LeaseID, &b.Revision,
		&b.OutputKind, &b.LogicalName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("completion.GetUploadBinding: upload %q not found", uploadID)
		}
		return nil, fmt.Errorf("completion.GetUploadBinding: query: %w", err)
	}
	return &b, nil
}

// BindUpload atomically associates one declaration with one artifact upload
// session. Replays are accepted only when they present the same binding.
func (c *coordinator) BindUpload(ctx context.Context, declarationID, uploadID, artifactID string) error {
	if declarationID == "" || uploadID == "" || artifactID == "" {
		return fmt.Errorf("completion.BindUpload: declaration, upload and artifact IDs are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := c.db.ExecContext(ctx, `
		UPDATE task_output_declarations
		   SET upload_id = ?, artifact_id = ?, updated_at = ?
		 WHERE declaration_id = ?
		   AND (upload_id IS NULL OR upload_id = ?)
		   AND (artifact_id IS NULL OR artifact_id = ?)`,
		uploadID, artifactID, now, declarationID, uploadID, artifactID)
	if err != nil {
		return fmt.Errorf("completion.BindUpload: update: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("completion.BindUpload: declaration %q binding conflict or missing", declarationID)
	}
	return nil
}

// VerifyUploadToken checks the opaque HMAC-derived commit token without
// persisting or logging its plaintext. The upload URL alone is insufficient
// to authorize a write.
func (c *coordinator) VerifyUploadToken(ctx context.Context, uploadID, token string) error {
	b, err := c.GetUploadBinding(ctx, uploadID)
	if err != nil {
		return err
	}
	derived, _, err := generateDeterministicCommitToken(c, b.CommitID, FenceTuple{
		TaskID: b.TaskID, AttemptID: b.AttemptID, WorkerID: b.WorkerID,
		LeaseID: b.LeaseID, Revision: b.Revision,
	})
	if err != nil {
		return err
	}
	providedBytes, err := hex.DecodeString(token)
	if err != nil {
		return fmt.Errorf("completion.VerifyUploadToken: invalid token encoding")
	}
	providedHash := sha256.Sum256(providedBytes)
	// The DB stores SHA256(token), not the token itself. Read that hash from
	// the commit row and compare in constant time through the encoded bytes.
	var stored string
	if err := c.db.QueryRowContext(ctx,
		`SELECT commit_token_hash FROM attempt_commits WHERE commit_id = ?`, b.CommitID).Scan(&stored); err != nil {
		return fmt.Errorf("completion.VerifyUploadToken: load token hash: %w", err)
	}
	derivedBytes, err := hex.DecodeString(derived)
	if err != nil {
		return fmt.Errorf("completion.VerifyUploadToken: derived token encoding: %w", err)
	}
	derivedHash := sha256.Sum256(derivedBytes)
	if subtleConstantTimeHexEqual(stored, hex.EncodeToString(providedHash[:])) == false ||
		!subtleConstantTimeHexEqual(stored, hex.EncodeToString(derivedHash[:])) {
		return fmt.Errorf("completion.VerifyUploadToken: invalid token")
	}
	return nil
}

func subtleConstantTimeHexEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

var _ UploadProtocolStore = (*coordinator)(nil)
