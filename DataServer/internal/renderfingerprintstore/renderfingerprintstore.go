// Package renderfingerprintstore is the SQLite persistence for render
// fingerprints. It was split out of the internal/store god-package: the pure
// domain types live in internal/renderfingerprint and this package owns only
// the SQLite INSERT. It depends on storecore (the shared DB primitive), never
// on internal/store.
package renderfingerprintstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/renderfingerprint"
	"velox-server/internal/storecore"
)

// SaveRenderFingerprint persists the render identity in the caller-owned
// transaction. It is idempotent for a repeated report; the same fingerprint
// may intentionally be attached to multiple historical attempts.
func SaveRenderFingerprint(ctx context.Context, tx storecore.DBTX, attemptID, taskID, jobID string, fp *renderfingerprint.Fingerprint, now time.Time) error {
	if fp == nil {
		return nil
	}
	if tx == nil || attemptID == "" || taskID == "" || jobID == "" {
		return fmt.Errorf("render fingerprint: transaction, attempt_id, task_id and job_id are required")
	}
	assets, err := json.Marshal(fp.AssetHashes)
	if err != nil {
		return fmt.Errorf("render fingerprint: asset hashes: %w", err)
	}
	fonts, err := json.Marshal(fp.FontHashes)
	if err != nil {
		return fmt.Errorf("render fingerprint: font hashes: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO task_attempt_render_fingerprints (
			attempt_id, task_id, job_id, render_fingerprint,
			render_plan_hash, canonical_payload_hash, input_manifest_hash,
			asset_hashes_json, font_hashes_json, template_version,
			engine_version, ffmpeg_version, worker_version, docker_image_digest,
			config_hash, encoder_config_hash, random_seed, locale, timezone, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(attempt_id) DO UPDATE SET
			render_fingerprint=excluded.render_fingerprint,
			render_plan_hash=excluded.render_plan_hash,
			canonical_payload_hash=excluded.canonical_payload_hash,
			input_manifest_hash=excluded.input_manifest_hash,
			asset_hashes_json=excluded.asset_hashes_json,
			font_hashes_json=excluded.font_hashes_json,
			template_version=excluded.template_version,
			engine_version=excluded.engine_version,
			ffmpeg_version=excluded.ffmpeg_version,
			worker_version=excluded.worker_version,
			docker_image_digest=excluded.docker_image_digest,
			config_hash=excluded.config_hash,
			encoder_config_hash=excluded.encoder_config_hash,
			random_seed=excluded.random_seed,
			locale=excluded.locale,
			timezone=excluded.timezone`,
		attemptID, taskID, jobID, fp.Value,
		fp.RenderPlanHash, fp.CanonicalPayloadHash, fp.InputManifestHash,
		assets, fonts, fp.TemplateVersion, fp.EngineVersion, fp.FFmpegVersion,
		fp.WorkerVersion, fp.DockerImageDigest, fp.ConfigHash, fp.EncoderConfigHash,
		fp.RandomSeed, fp.Locale, fp.Timezone, now.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("render fingerprint: persist: %w", err)
	}
	return nil
}
