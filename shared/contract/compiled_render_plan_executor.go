package contract

import (
	"encoding/json"
	"strings"
)

const (
	VideoAssemblyCopyExecutorID  = "video.assemble.copy.v1"
	VideoAssemblySliceExecutorID = "video.assemble.slice.v1"
)

// RequiredVideoAssemblyExecutor routes every new compiled video plan through
// the single packet-copy assembler. Runtime bindings and FINALIZE resolve
// stock/source assets before execution; legacy slice tasks remain readable by
// workers but are no longer selected for new plans.
func RequiredVideoAssemblyExecutor(payload map[string]interface{}) string {
	planJSON, _ := payload[PayloadKeyCompiledRenderPlanJSON].(string)
	var plan CompiledRenderPlanV2
	if strings.TrimSpace(planJSON) == "" || json.Unmarshal([]byte(planJSON), &plan) != nil {
		return ""
	}
	_ = plan // Decoding above is the payload validity check used by routing.
	return VideoAssemblyCopyExecutorID
}
