package worker

import (
	"strconv"
	"strings"

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
