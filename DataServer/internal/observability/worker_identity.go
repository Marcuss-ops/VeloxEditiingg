package observability

import "strings"

// workerDisplayName reads the operator-facing name from the canonical worker
// registry. WorkerID remains the immutable security/history identity; this
// helper only enriches read models and never rewrites or aliases IDs locally.
func (s *Service) workerDisplayName(workerID string) string {
	if s == nil || s.workers == nil || strings.TrimSpace(workerID) == "" {
		return ""
	}
	worker, err := s.workers.GetWorker(workerID)
	if err != nil || worker == nil {
		return ""
	}
	if name, ok := worker["worker_name"].(string); ok {
		return strings.TrimSpace(name)
	}
	return ""
}
