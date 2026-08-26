package store

// reconciliation_audit.go — shared append-only audit helpers for the
// reconciliation reconcilers (stale-execution, awaiting-artifact,
// delivery-pending). The stale-execution reconciler was extracted into the
// internal/stalereconcile leaf, which carries its own private copy of these
// helpers; this store copy remains for the reconcilers that still live in the
// god-package.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"

	"velox-server/internal/audittrail"
	"velox-server/internal/stalereconcile"
)

// appendReconcileAuditTx appends the deterministic append-only audit row for a
// finding inside an open transaction.
func appendReconcileAuditTx(ctx context.Context, tx *sql.Tx, f stalereconcile.StaleExecutionFinding, actor string, now time.Time) error {
	metadata, err := json.Marshal(map[string]any{"category": f.Category, "reason": f.Reason, "old_status": f.OldStatus, "proposed_status": f.ProposedStatus, "observed_at": f.ObservedAt.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	h := sha256.Sum256([]byte(string(f.Category) + ":" + f.ResourceType + ":" + f.ResourceID + ":" + f.OldStatus))
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO audit_events (id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json) VALUES (?, ?, 'operator', ?, 'STALE_EXECUTION_RECONCILED', ?, ?, '', '', ?, ?, ?)`, "reconcile-"+hex.EncodeToString(h[:]), now.UTC().Format(time.RFC3339Nano), actor, f.ResourceType, f.ResourceID, hashText(f.OldStatus), hashText(f.ProposedStatus), audittrail.RedactMetadata(string(metadata)))
	return err
}

func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
