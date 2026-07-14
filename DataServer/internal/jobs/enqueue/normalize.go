// Package enqueue — scene-video payload normalization cluster (R2-A extract).
//
// Pure, stateless functions that canonicalize a scene-video payload before
// it is compiled into a Job+TaskSpec. No DB, no Enqueuer state, no
// goroutines. The companion orchestrator enqueue.go owns the executor
// boundary and the atomic-creator call; asset resolution (voiceover /
// scene-image rewrite) lives in R2-B.
package enqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"velox-server/internal/routing"
	"velox-shared/contract"
	"velox-shared/payload"
)

// validatePlanPayload enforces the precondition invariants on an
// already-resolved plan (no DB hit). Used by enforceDeliveryPlanPrecondition;
// *Tx callers (AtomicForwardAndEnqueue) get the analogous gate via
// store.validateDeliveryDestinationTx inside CreateJobWithTaskTx.
//
// Invariants: plan must be non-nil and carry >=1 destination; every
// destination's retry_budget > 0. On success, writes the MAX
// retry_budget into job.MaxRetries.
func validatePlanPayload(plan *ResolvedPlan, job *jobs.Job) error {
	if plan == nil || len(plan.Destinations) == 0 {
		return &validationError{field: "delivery_plan", message: "no explicit delivery plan; create job_delivery_plans rows for this job before enqueueing"}
	}
	maxRetry := 0
	for i, d := range plan.Destinations {
		if d.RetryBudget <= 0 {
			return &validationError{field: fmt.Sprintf("delivery_plan[%d].retry_budget", i), message: "must be > 0"}
		}
		if d.RetryBudget > maxRetry {
			maxRetry = d.RetryBudget
		}
	}
	if job != nil {
		job.MaxRetries = maxRetry
	}
	return nil
}

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {

func normalizeScenes(payloadMap map[string]interface{}) ([]map[string]interface{}, string, error) {

func normalizeSceneArray(value interface{}) []map[string]interface{} {

func normalizeVoiceoverList(payloadMap map[string]interface{}) []string {

func sceneCountFromPayload(payloadMap map[string]interface{}) int {

func voiceoverCountFromPayload(payloadMap map[string]interface{}) int {

func hasClipTimelinePayload(payloadMap map[string]interface{}) bool {

func copyTimelinePayloadFields(out, src map[string]interface{}) {

func syncAudioURLFromVoiceover(payloadMap map[string]interface{}) {

func resolveInternalExecutorID(payloadMap map[string]interface{}) string {

// resolveRequiredCapabilities returns the capability strings a task requires
// based on its executor. These are stored in task_requirements and consumed
// by the placement matcher's capability gate (matcher.go Select).
//
// For now the mapping is executor-driven:
//   - scene.composite.* → artifact.commit.v1
//   - All other executors → nil (no extra capabilities yet)
func resolveRequiredCapabilities(executorID string) []string {
	if strings.HasPrefix(executorID, "scene.composite") {
		return []string{"artifact.commit.v1"}
	}
	return nil
}

func sceneVideoFingerprint(parts ...interface{}) string {

// extractPlanMaxRetry computes the maximum retry_budget across the
// payload's delivery_plan entries. The single writer of job.MaxRetries
// on the insert path.
func extractPlanMaxRetry(payload map[string]interface{}) int {

