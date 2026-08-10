package store

import (
	"context"
	"fmt"
)

func (s *SQLiteCompletionStore) ScanCompletionCandidates(ctx context.Context, now, deadlineCutoff, progressCutoff, outboxCutoff string, limit int) ([]CompletionReconcileCandidate, int64, error) {
	q := `SELECT commit_id,case_label FROM (
SELECT commit_id,'deadline_expired' AS case_label FROM attempt_commits WHERE status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING') AND commit_deadline_at<?
UNION ALL SELECT ac.commit_id,'orphan_terminal_task' FROM attempt_commits ac JOIN task_attempts ta ON ta.id=ac.attempt_id WHERE ta.status IN ('FAILED','CANCELLED','TIMED_OUT') AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
UNION ALL SELECT ac.commit_id,'stale_fence' FROM attempt_commits ac JOIN tasks t ON t.task_id=ac.task_id WHERE t.lease_id!=ac.lease_id AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
UNION ALL SELECT ac.commit_id,'missing_worker' FROM attempt_commits ac LEFT JOIN workers w ON w.worker_id=ac.worker_id WHERE w.worker_id IS NULL AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
UNION ALL SELECT ac.commit_id,'missing_declarations' FROM attempt_commits ac LEFT JOIN (SELECT commit_id,COUNT(*) n FROM task_output_declarations GROUP BY commit_id) d ON d.commit_id=ac.commit_id WHERE ac.status='UPLOADING' AND COALESCE(d.n,0)=0
UNION ALL SELECT ac.commit_id,'missing_commit' FROM attempt_commits ac WHERE ac.status='RECEIVED' AND ac.required_output_count>0 AND ac.ready_output_count>=ac.required_output_count AND ac.last_progress_at<?
UNION ALL SELECT ac.commit_id,'upload_stuck' FROM attempt_commits ac WHERE ac.status='UPLOADING' AND ac.last_progress_at<?
UNION ALL SELECT ac.commit_id,'fence_expired' FROM attempt_commits ac JOIN tasks t ON t.task_id=ac.task_id WHERE t.lease_id!='' AND t.worker_id='' AND ac.status='DECLARED'
UNION ALL SELECT ac.commit_id,'outbox_pending_too_long' FROM attempt_commits ac JOIN outbox_events oe ON oe.aggregate_type='task' AND oe.aggregate_id=ac.task_id AND oe.event_type='commit_protocol.committed' AND oe.payload_json LIKE '%'||ac.commit_id||'%' WHERE oe.status='PENDING' AND oe.created_at<?
UNION ALL SELECT ac.commit_id,'required_outputs_missing' FROM attempt_commits ac WHERE ac.status='AWAITING_REQUIRED' AND ac.required_output_count>ac.ready_output_count
UNION ALL SELECT ac.commit_id,'job_all_succeeded_no_job_deliveries' FROM attempt_commits ac JOIN tasks t ON t.task_id=ac.task_id WHERE t.status='SUCCEEDED' AND ac.status='COMMITTED' AND NOT EXISTS (SELECT 1 FROM artifacts a JOIN job_deliveries jd ON jd.artifact_id=a.id WHERE a.job_id=ac.job_id)
) ORDER BY commit_id LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, now, deadlineCutoff, progressCutoff, outboxCutoff, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("store: scan completion candidates: %w", err)
	}
	defer rows.Close()
	var out []CompletionReconcileCandidate
	var deadline int64
	for rows.Next() {
		var c CompletionReconcileCandidate
		if err := rows.Scan(&c.CommitID, &c.Case); err != nil {
			return nil, 0, err
		}
		if c.Case == "deadline_expired" {
			deadline++
		}
		out = append(out, c)
	}
	return out, deadline, rows.Err()
}
