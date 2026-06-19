package worker

import (
	"strconv"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/pkg/api"
)

// renderJobParams è un alias per contract.RenderJobParams.
// Manteniamo l'alias locale per isolare il package worker dai dettagli del package contract.
type renderJobParams = contract.RenderJobParams

// extractRenderJobParams estrae i parametri di un job dalla mappa generica dei parametri
// in un renderJobParams tipizzato. Delega a contract.ExtractRenderJobParams.
func extractRenderJobParams(params map[string]interface{}) renderJobParams {
	return contract.ExtractRenderJobParams(params)
}

func resolveLeaseID(job *api.Job) string {
	if job == nil {
		return ""
	}
	if trimmed := strings.TrimSpace(job.LeaseID); trimmed != "" {
		return trimmed
	}
	if v, ok := job.Parameters["lease_id"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

func resolveJobAttempt(job *api.Job) int {
	if job == nil {
		return 0
	}
	if job.Attempt > 0 {
		return job.Attempt
	}
	if v, ok := job.Parameters["attempt"]; ok {
		switch t := v.(type) {
		case int:
			return t
		case int64:
			return int(t)
		case float64:
			return int(t)
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
				return n
			}
		}
	}
	return 0
}

func resolveJobCreatedAt(job *api.Job) string {
	if job == nil {
		return ""
	}
	switch v := job.CreatedAt.(type) {
	case string:
		return strings.TrimSpace(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	case int:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	case int32:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	case int64:
		return time.Unix(v, 0).UTC().Format(time.RFC3339)
	case float64:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	case float32:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	case uint:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	case uint32:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	case uint64:
		return time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	default:
		return ""
	}
}
