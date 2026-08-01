// Package renderfingerprint defines the deterministic identity of a render.
//
// The fingerprint deliberately contains inputs that are often stored in
// different parts of the system (plan, assets, fonts and runtime versions) so
// an operator can compare two attempts without reconstructing their identity
// from ad-hoc log fields.
package renderfingerprint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Input is the complete render identity. AssetHashes and FontHashes preserve
// semantic order; callers must provide them in the order consumed by the
// render plan.
type Input struct {
	RenderPlan        any
	CanonicalPayload  any
	InputManifest     any
	AssetHashes       []string
	FontHashes        []string
	TemplateVersion   string
	EngineVersion     string
	FFmpegVersion     string
	WorkerVersion     string
	DockerImageDigest string
	ConfigHash        string
	EncoderConfigHash string
	RandomSeed        int64
	Locale            string
	Timezone          string
}

// Fingerprint is the persisted, explainable render identity.
type Fingerprint struct {
	Value                string   `json:"render_fingerprint"`
	RenderPlanHash       string   `json:"render_plan_hash"`
	CanonicalPayloadHash string   `json:"canonical_payload_hash"`
	InputManifestHash    string   `json:"input_manifest_hash"`
	AssetHashes          []string `json:"asset_hashes"`
	FontHashes           []string `json:"font_hashes"`
	TemplateVersion      string   `json:"template_version,omitempty"`
	EngineVersion        string   `json:"engine_version,omitempty"`
	FFmpegVersion        string   `json:"ffmpeg_version,omitempty"`
	WorkerVersion        string   `json:"worker_version,omitempty"`
	DockerImageDigest    string   `json:"docker_image_digest,omitempty"`
	ConfigHash           string   `json:"config_hash,omitempty"`
	EncoderConfigHash    string   `json:"encoder_config_hash,omitempty"`
	RandomSeed           int64    `json:"random_seed"`
	Locale               string   `json:"locale,omitempty"`
	Timezone             string   `json:"timezone,omitempty"`
}

// Build canonicalizes JSON object key order before hashing it. encoding/json
// sorts map keys, while arrays retain their input order.
func Build(in Input) (Fingerprint, error) {
	plan, err := canonicalJSON(in.RenderPlan)
	if err != nil {
		return Fingerprint{}, fmt.Errorf("render fingerprint: render plan: %w", err)
	}
	payload, err := canonicalJSON(in.CanonicalPayload)
	if err != nil {
		return Fingerprint{}, fmt.Errorf("render fingerprint: canonical payload: %w", err)
	}
	manifest, err := canonicalJSON(in.InputManifest)
	if err != nil {
		return Fingerprint{}, fmt.Errorf("render fingerprint: input manifest: %w", err)
	}

	f := Fingerprint{
		RenderPlanHash:       digest(plan),
		CanonicalPayloadHash: digest(payload),
		InputManifestHash:    digest(manifest),
		AssetHashes:          append([]string(nil), in.AssetHashes...),
		FontHashes:           append([]string(nil), in.FontHashes...),
		TemplateVersion:      in.TemplateVersion,
		EngineVersion:        in.EngineVersion,
		FFmpegVersion:        in.FFmpegVersion,
		WorkerVersion:        in.WorkerVersion,
		DockerImageDigest:    in.DockerImageDigest,
		ConfigHash:           in.ConfigHash,
		EncoderConfigHash:    in.EncoderConfigHash,
		RandomSeed:           in.RandomSeed,
		Locale:               in.Locale,
		Timezone:             in.Timezone,
	}
	identity, err := json.Marshal(struct {
		RenderPlanHash       string   `json:"render_plan_hash"`
		CanonicalPayloadHash string   `json:"canonical_payload_hash"`
		InputManifestHash    string   `json:"input_manifest_hash"`
		AssetHashes          []string `json:"asset_hashes"`
		FontHashes           []string `json:"font_hashes"`
		TemplateVersion      string   `json:"template_version"`
		EngineVersion        string   `json:"engine_version"`
		FFmpegVersion        string   `json:"ffmpeg_version"`
		WorkerVersion        string   `json:"worker_version"`
		DockerImageDigest    string   `json:"docker_image_digest"`
		ConfigHash           string   `json:"config_hash"`
		EncoderConfigHash    string   `json:"encoder_config_hash"`
		RandomSeed           int64    `json:"random_seed"`
		Locale               string   `json:"locale"`
		Timezone             string   `json:"timezone"`
	}{f.RenderPlanHash, f.CanonicalPayloadHash, f.InputManifestHash, f.AssetHashes,
		f.FontHashes, f.TemplateVersion, f.EngineVersion, f.FFmpegVersion,
		f.WorkerVersion, f.DockerImageDigest, f.ConfigHash, f.EncoderConfigHash,
		f.RandomSeed, f.Locale, f.Timezone})
	if err != nil {
		return Fingerprint{}, fmt.Errorf("render fingerprint: identity: %w", err)
	}
	f.Value = digest(identity)
	return f, nil
}

func canonicalJSON(value any) ([]byte, error) {
	if value == nil {
		return []byte("null"), nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(b, &normalized); err != nil {
		return nil, err
	}
	b, err = json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(b), nil
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
