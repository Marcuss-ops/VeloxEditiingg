package completion

import (
	"context"
	"fmt"
	"time"
)

func (s *ReconcileSupervisor) scanCandidates(ctx context.Context) ([]ReconcileCandidate, int64, error) {
	now := time.Now().UTC()
	rows, deadline, err := s.Store.ScanCompletionCandidates(ctx,
		now.Format(time.RFC3339Nano),
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		now.Add(-5*time.Hour).Format(time.RFC3339Nano),
		now.Add(-1*time.Hour).Format(time.RFC3339Nano),
		s.Limit)
	if err != nil {
		return nil, 0, fmt.Errorf("reconcile: scan: %w", err)
	}
	out := make([]ReconcileCandidate, 0, len(rows))
	for _, row := range rows {
		if row.CommitID == "" || row.Case == "" {
			continue
		}
		key := row.CommitID + ":" + row.Case
		s.seenMu.Lock()
		if _, ok := s.seenIDs[key]; ok {
			s.seenMu.Unlock()
			continue
		}
		s.seenIDs[key] = now
		s.seenMu.Unlock()
		out = append(out, ReconcileCandidate{CommitID: row.CommitID, Case: ReconcileCase(row.Case)})
	}
	return out, deadline, nil
}
