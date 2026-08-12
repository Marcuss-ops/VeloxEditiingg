package workercache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-shared/assetref"
)

const derivedAssetKeyNamespace = "derived:v1:"

// NormalizationProfile is the complete media contract used when a source
// asset is converted into a reusable worker-cache asset. Every field that
// can change the resulting bytes belongs in this value; omitting one would
// allow an incompatible normalized asset to be reused as a cache hit.
//
// The JSON representation is also the canonical serialization used by
// DerivedAssetKey. Keep field additions intentional: adding a field changes
// the identity of all profiles that populate it, which is the safe behavior.
type NormalizationProfile struct {
	NormalizerVersion int    `json:"normalizer_version"`
	Codec             string `json:"codec"`
	Width             int    `json:"width"`
	Height            int    `json:"height"`
	FPSNum            int    `json:"fps_num"`
	FPSDen            int    `json:"fps_den"`
	PixelFormat       string `json:"pixel_format"`
	ColorProperties   string `json:"color_properties,omitempty"`
	Profile           string `json:"profile,omitempty"`
	Level             string `json:"level,omitempty"`
	GOPPolicy         string `json:"gop_policy,omitempty"`
	EncoderSettings   string `json:"encoder_settings,omitempty"`
}

// Validate rejects incomplete profiles before they can create cache keys.
// A derived asset with an unspecified media contract is not safely reusable.
func (p NormalizationProfile) Validate() error {
	if p.NormalizerVersion <= 0 {
		return errors.New("workercache: normalizer_version must be positive")
	}
	if strings.TrimSpace(p.Codec) == "" {
		return errors.New("workercache: normalization codec is required")
	}
	if p.Width <= 0 || p.Height <= 0 {
		return errors.New("workercache: normalization dimensions must be positive")
	}
	if p.FPSNum <= 0 || p.FPSDen <= 0 {
		return errors.New("workercache: normalization frame rate must be positive")
	}
	if strings.TrimSpace(p.PixelFormat) == "" {
		return errors.New("workercache: normalization pixel format is required")
	}
	return nil
}

// CanonicalJSON returns the stable, schema-owned profile encoding. Struct
// field order is deterministic in encoding/json; maps are deliberately not
// used here so the same profile has the same bytes across workers.
func (p NormalizationProfile) CanonicalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// DerivedAssetKey deterministically names the normalized output for one
// verified source and one complete normalization profile. It is a logical
// AssetKey in the existing cached_assets table; no second cache/index exists.
func DerivedAssetKey(sourceHash assetref.ContentHash, profile NormalizationProfile) (assetref.AssetKey, error) {
	source, err := assetref.ParseContentHash(string(sourceHash))
	if err != nil {
		return "", fmt.Errorf("workercache: invalid source content hash: %w", err)
	}
	profileJSON, err := profile.CanonicalJSON()
	if err != nil {
		return "", err
	}
	// Explicit framing prevents concatenation ambiguity if the profile schema
	// is extended in the future.
	payload := []byte(derivedAssetKeyNamespace + string(source) + "\n" + string(profileJSON))
	digest := sha256.Sum256(payload)
	return assetref.AssetKey(derivedAssetKeyNamespace + hex.EncodeToString(digest[:])), nil
}

// FindDerived looks up a reusable normalized asset in the canonical cache.
// Incomplete rows are never returned as hits: a crash during normalization
// must not make a partial file usable by a render.
func (c *Cache) FindDerived(ctx context.Context, sourceHash assetref.ContentHash, profile NormalizationProfile) (Entry, bool, error) {
	key, err := DerivedAssetKey(sourceHash, profile)
	if err != nil {
		return Entry{}, false, err
	}
	entry, found, err := c.Find(ctx, string(key))
	if err != nil || !found || !entry.DownloadComplete {
		return entry, false, err
	}
	return entry, true, nil
}

// StoreDerived registers a verified normalized file in the same durable
// cache used for downloaded Drive assets. The caller must atomically promote
// the file and verify outputHash before calling this method.
func (c *Cache) StoreDerived(ctx context.Context, sourceHash assetref.ContentHash, profile NormalizationProfile, entry Entry) (assetref.AssetKey, error) {
	key, err := DerivedAssetKey(sourceHash, profile)
	if err != nil {
		return "", err
	}
	if entry.AssetKey != "" && entry.AssetKey != key {
		return "", fmt.Errorf("workercache: derived entry asset_key %q does not match canonical key %q", entry.AssetKey, key)
	}
	if !entry.DownloadComplete {
		return "", errors.New("workercache: derived entry must be complete")
	}
	entry.AssetKey = key
	if err := c.Store(ctx, entry); err != nil {
		return "", err
	}
	return key, nil
}
