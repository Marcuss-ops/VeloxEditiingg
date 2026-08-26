package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/inputsecurity"
)

const maxRenderManifestBytes int64 = 4 << 20

// ResolveRenderManifestRef downloads, verifies, validates, and substitutes a
// velox.render-manifest.v1 document into a SubmitJobRequest. The returned
// request is the canonical inline form consumed by the existing SubmitJob
// validation, SSRF, quota, resolver, and enqueue path.
func (h *Handlers) ResolveRenderManifestRef(ctx context.Context, req SubmitJobRequest) (SubmitJobRequest, *SubmitJobValidationError) {
	if req.ManifestRef == nil {
		return req, nil
	}
	body, vErr := h.fetchRenderManifest(ctx, req.ManifestRef.URL)
	if vErr != nil {
		return req, vErr
	}
	rawHash := sha256.Sum256(body)
	rawHex := hex.EncodeToString(rawHash[:])
	if rawHex != req.ManifestRef.SHA256 {
		return req, manifestValidationError(gin.H{
			"path":     "manifest_ref.sha256",
			"issue":    "mismatch",
			"observed": rawHex,
			"expected": req.ManifestRef.SHA256,
		})
	}

	manifest, parseErr := parseRenderManifestRefJSON(body)
	if parseErr != nil {
		return req, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "invalid_json",
		})
	}
	resolved, details := renderManifestToSubmitRequest(req, manifest)
	if len(details) > 0 {
		return req, manifestValidationError(details...)
	}
	resolved.ResolvedManifest = cloneJSONMap(manifest)
	resolved.ResolvedManifestRef = map[string]interface{}{
		"schema_version": req.ManifestRef.SchemaVersion,
		"url":            strings.TrimSpace(req.ManifestRef.URL),
		"sha256":         req.ManifestRef.SHA256,
	}
	resolved.ResolvedManifestSHA256 = req.ManifestRef.SHA256
	resolved.ManifestRef = nil
	return resolved, nil
}

func (h *Handlers) fetchRenderManifest(ctx context.Context, rawURL string) ([]byte, *SubmitJobValidationError) {
	rawURL = strings.TrimSpace(rawURL)
	if strings.HasPrefix(strings.ToLower(rawURL), "velox-asset://") {
		return nil, manifestValidationError(gin.H{
			"path":     "manifest_ref.url",
			"issue":    "unsupported_scheme",
			"observed": rawURL,
			"allowed":  []string{"https://", "http://"},
		})
	}
	testPrivateNetworkPolicy := h != nil && h.inputPolicy != nil && h.inputPolicy.AllowPrivateNetworks
	if !testPrivateNetworkPolicy {
		if err := ValidateExternalURL(rawURL, h.configuredAllowedDomains(), h.configuredAllowLoopbackHTTP()); err != nil {
			if se, ok := err.(*SSRFValidationError); ok {
				return nil, &SubmitJobValidationError{
					Code:    "ssrf_rejected",
					Reason:  se.Reason,
					Message: "manifest_ref.url failed the egress policy",
					Details: []gin.H{{
						"path":   "manifest_ref.url",
						"url":    se.URL,
						"reason": se.Reason,
					}},
				}
			}
		}
	}
	if _, err := url.Parse(rawURL); err != nil {
		return nil, manifestValidationError(gin.H{
			"path":  "manifest_ref.url",
			"issue": "malformed",
		})
	}
	policy := inputsecurity.DefaultPolicy()
	policy.MaxBytes = maxRenderManifestBytes
	if h != nil && h.cfg != nil && h.cfg.Runtime.DataDir != "" {
		policy.TempDir = h.cfg.Runtime.DataDir
		policy.QuarantineDir = h.cfg.Runtime.DataDir + "/quarantine/input-security"
	}
	if h != nil && h.inputPolicy != nil {
		policy = *h.inputPolicy
		if policy.MaxBytes <= 0 {
			policy.MaxBytes = maxRenderManifestBytes
		}
	}
	fetched, err := inputsecurity.NewFetcher(policy).Fetch(ctx, rawURL, inputsecurity.KindManifest)
	if err != nil {
		return nil, manifestValidationError(gin.H{
			"path":       "manifest_ref.url",
			"issue":      "fetch_failed",
			"error_code": string(inputsecurity.CodeOf(err)),
		})
	}
	defer os.Remove(fetched.Path)
	body, err := os.ReadFile(fetched.Path)
	if err != nil {
		return nil, manifestValidationError(gin.H{
			"path":       "manifest_ref.url",
			"issue":      "fetch_failed",
			"error_code": string(inputsecurity.ErrReadFailed),
		})
	}
	if int64(len(body)) > maxRenderManifestBytes {
		return nil, manifestValidationError(gin.H{
			"path":     "manifest_ref.url",
			"issue":    "max_bytes",
			"max":      maxRenderManifestBytes,
			"observed": len(body),
		})
	}
	return body, nil
}

func (h *Handlers) configuredAllowedDomains() []string {
	if h == nil || h.cfg == nil {
		return nil
	}
	return h.cfg.AllowedExternalDomains
}

func (h *Handlers) configuredAllowLoopbackHTTP() bool {
	return h != nil && h.cfg != nil && h.cfg.Runtime.AllowLoopbackAdminAuthDev
}
