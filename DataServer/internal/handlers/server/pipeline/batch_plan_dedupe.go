// Package pipeline / batch_plan_dedupe.go — in-batch plan dedupe for
// POST /api/v1/jobs/batch.
//
// The batch surface already isolates items (per-item idempotency keys, per-item
// enqueue transaction, per-item outcome). What it did NOT do is collapse
// identical work: a producer submitting 100 variants of the same template
// where 37 items are byte-identical would enqueue 37 identical renders.
//
// This file adds an in-batch plan identity. After NormalizeCanonicalRecipe has
// normalized an item into its canonical recipe shape, the fingerprint is
// SHA-256 over a deterministic serialization of every render-relevant field
// EXCEPT:
//
//   - idempotency_key  (per-item identity, never render-relevant)
//   - video_name       (display only)
//   - publications / delivery_plan / publishing_target — deliberately
//     excluded: two items that differ only by destination are two different
//     PUBLICATIONS of the same video, and the second must reuse the first
//     render instead of re-rendering it.
//   - placement_pin_worker_id (scheduling intent, not render intent)
//   - assembly (control-plane metadata that never enters the renderer payload)
//
// On a fingerprint match with an already-accepted item of the same batch, the
// duplicate item is NOT re-enqueued: it reuses the accepted item's job_id as a
// re-publication of that job (same artifact, different publication intents).
// This makes the batch surface a template+variants contract: pay one render,
// publish N times.
//
// Wire contract for consumers (SubmitJobBatchItemResult gains `deduped_of`):
//
//	{
//	  "index": 4,
//	  "idempotency_key": "variant-4",
//	  "job_id": "<job of items.0>",
//	  "status": "dedup",
//	  "deduped_of": 0
//	}
//
// The `status: "dedup"` outcome is distinct from accepted/conflict so
// producers can distinguish a fresh enqueue from a reuse. job_id is always
// populated on dedup items, matching the anchor item's job.
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// BatchItemFingerprint renders the dedupe fingerprint of one normalized item.
//
// Included: job_type, template id/version, output geometry/format, the
// canonical recipe body (spec, scenes, layers, overlays, visual
// replacements, audio, script, copy_only), and the manifest ref identity
// (same manifest = same plan).
func BatchItemFingerprint(item SubmitJobRequest) (string, error) {
	identity := struct {
		JobType         string                 `json:"job_type"`
		TemplateID      string                 `json:"template_id"`
		TemplateVersion int                    `json:"template_version"`
		Output          *SubmitOutput          `json:"output,omitempty"`
		ScriptText      string                 `json:"script_text"`
		AudioURL        string                 `json:"audio_url"`
		CopyOnly        bool                   `json:"copy_only"`
		CompiledPlan    string                 `json:"compiled_render_plan_sha256,omitempty"`
		Manifest        *SubmitManifestRef     `json:"manifest_ref,omitempty"`
		Spec            map[string]interface{} `json:"spec,omitempty"`
		Scenes          []SubmitScene          `json:"scenes"`
		Layers          []SubmitLayer          `json:"layers,omitempty"`
		Overlays        []SubmitOverlay        `json:"overlays,omitempty"`
		Replacements    []SubmitVisualReplacement `json:"visual_replacements,omitempty"`
	}{
		JobType:         item.JobType,
		TemplateID:      item.TemplateID,
		TemplateVersion: item.TemplateVersion,
		Output:          item.Output,
		ScriptText:      item.ScriptText,
		AudioURL:        item.AudioURL,
		CopyOnly:        item.CopyOnly,
		CompiledPlan:    item.CompiledRenderPlanSHA256,
		Manifest:        item.ManifestRef,
		Spec:            item.Spec,
		Scenes:          item.Scenes,
		Layers:          item.Layers,
		Overlays:        item.Overlays,
		Replacements:    item.VisualReplacements,
	}
	// json.Marshal on a struct with fixed field order is deterministic. Two
	// items are fingerprint-equal iff this identity is deep-equal.
	data, err := json.Marshal(&identity)
	if err != nil {
		return "", fmt.Errorf("batch fingerprint marshal: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// batchDedupeWindow is the per-request dedupe state. It is created by the
// batch handler and never shared across requests. The batch loop is the only
// writer; the window is a passive ledger of accepted anchors plus counters.
type batchDedupeWindow struct {
	// anchorByFingerprint maps fingerprint → index of the first accepted item
	// with that fingerprint.
	anchorByFingerprint map[string]int
	// jobByIndex maps item index → job_id for accepted items.
	jobByIndex map[int]string
	// fingerprintByIndex maps item index → fingerprint for accepted items.
	fingerprintByIndex map[int]string
	// counts summarize the batch for the response envelope.
	accepted int
	deduped  int
	rejected int
	conflict int
	failed   int
}

func newBatchDedupeWindow() *batchDedupeWindow {
	return &batchDedupeWindow{
		anchorByFingerprint: map[string]int{},
		jobByIndex:          map[int]string{},
		fingerprintByIndex:  map[int]string{},
	}
}

// recordAccepted registers a fresh enqueue at index with the given
// fingerprint. The first accepted item with a given fingerprint is the dedupe
// anchor; later identical items dedupe onto it.
func (w *batchDedupeWindow) recordAccepted(index int, fingerprint, jobID string) {
	w.fingerprintByIndex[index] = fingerprint
	w.jobByIndex[index] = jobID
	if _, exists := w.anchorByFingerprint[fingerprint]; !exists {
		w.anchorByFingerprint[fingerprint] = index
	}
	w.accepted++
}

// recordDedup registers one item that reused an earlier item's job.
func (w *batchDedupeWindow) recordDedup() { w.deduped++ }

// recordRejected registers one item rejected for its own validation errors.
func (w *batchDedupeWindow) recordRejected() { w.rejected++ }

// recordConflict registers one cross-item conflict (duplicate key in-batch).
func (w *batchDedupeWindow) recordConflict() { w.conflict++ }

// recordFailed registers one item failed for infrastructure reasons.
func (w *batchDedupeWindow) recordFailed() { w.failed++ }

// lookup returns (anchorIndex, anchorJobID, true) when fingerprint matches an
// already-accepted item, else (0, "", false).
func (w *batchDedupeWindow) lookup(fingerprint string) (int, string, bool) {
	index, ok := w.anchorByFingerprint[fingerprint]
	if !ok {
		return 0, "", false
	}
	return index, w.jobByIndex[index], true
}
