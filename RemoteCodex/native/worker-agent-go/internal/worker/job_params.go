package worker

import "velox-shared/contract"

// renderJobParams è un alias per contract.RenderJobParams.
// Manteniamo l'alias locale per isolare il package worker dai dettagli del package contract.
type renderJobParams = contract.RenderJobParams

// extractRenderJobParams estrae i parametri di un job dalla mappa generica dei parametri
// in un renderJobParams tipizzato. Delega a contract.ExtractRenderJobParams.
func extractRenderJobParams(params map[string]interface{}) renderJobParams {
	return contract.ExtractRenderJobParams(params)
}
