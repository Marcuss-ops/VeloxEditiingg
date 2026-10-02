package pipeline

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/metrics"
	"velox-shared/contract"
)

// maxSceneKindWarningIDs bounds how many scene identifiers travel in the
// accept envelope. The full set stays in structured logs; the envelope
// carries enough for the generator to locate the first offenders.
const maxSceneKindWarningIDs = 10

// sceneKindWarningCode is the stable machine-readable code echoed in the
// accept envelope and used as the metric reason label.
const sceneKindWarningCode = contract.SceneKindClipAnomalyReasonKindClipWithoutClipAsset

// sceneKindWarningsForPayload scans a raw intake payload for soft-deprecated
// scene declarations (kind="clip" without a clip asset) and returns the
// accept-envelope warnings. Empty intake shapes yield nil: the happy path
// stays warning-free and byte-identical to before.
func sceneKindWarningsForPayload(payload map[string]interface{}) []gin.H {
	anomalies := contract.FindKindClipAnomaliesInPayload(payload)
	if len(anomalies) == 0 {
		return nil
	}
	ids := make([]string, 0, len(anomalies))
	for _, anomaly := range anomalies {
		if id := strings.TrimSpace(anomaly.SceneID); id != "" {
			ids = append(ids, id)
		} else {
			ids = append(ids, fmt.Sprintf("index:%d", anomaly.Index))
		}
	}
	shown := ids
	omitted := 0
	if len(shown) > maxSceneKindWarningIDs {
		omitted = len(shown) - maxSceneKindWarningIDs
		shown = shown[:maxSceneKindWarningIDs]
	}
	detail := fmt.Sprintf(
		"%d scene(s) declared kind=\"clip\" without a clip asset (worker compiles them as mute stock backgrounds; testimony audio is lost). "+
			"Use kind=\"stock\" for backgrounds, or add clip:{url,...} for testimony. "+
			"Hard rejection lands after %s.",
		len(anomalies), contract.SunsetSceneKindClipEnforcement)
	if omitted > 0 {
		detail += fmt.Sprintf(" Showing first %d scene(s), %d omitted.", len(shown), omitted)
	}
	return []gin.H{{
		"code":       sceneKindWarningCode,
		"scene_ids":  shown,
		"sceneTotal": len(anomalies),
		"detail":     detail,
		"sunset":     contract.SunsetSceneKindClipEnforcement,
	}}
}

// warningPathForIntakeSource maps the canonical intake source onto the
// bounded warning-metric path vocabulary (creator_push is reported by
// its own handler; the jobs core reports api_v1_jobs for single submits
// and batch for batch items so operators can tell the surfaces apart).
func warningPathForIntakeSource(source string) string {
	if source == creatorflow.IntakeSourceBatch {
		return creatorflow.IntakeSourceBatch
	}
	return "api_v1_jobs"
}

// reportSceneKindWarnings records the soft-deprecation telemetry for one
// accepted payload: a bounded metric increment plus a structured WARN log
// line carrying the source-job hash and the affected scene identifiers.
// High-cardinality identifiers stay in logs, never in metric labels.
func reportSceneKindWarnings(path, sourceJobID string, warnings []gin.H) {
	if len(warnings) == 0 {
		return
	}
	metrics.RecordIntakeSceneWarning(path, sceneKindWarningCode)
	sceneTotal := 0
	if total, ok := warnings[0]["sceneTotal"].(int); ok {
		sceneTotal = total
	}
	pipelineLog(
		"WARN scene-kind soft-deprecation path=%s source_job_hash=%s reason=%s scenes=%d ids=%v sunset=%s",
		path,
		logHashShort(sourceJobID),
		sceneKindWarningCode,
		sceneTotal,
		warnings[0]["scene_ids"],
		contract.SunsetSceneKindClipEnforcement,
	)
}
