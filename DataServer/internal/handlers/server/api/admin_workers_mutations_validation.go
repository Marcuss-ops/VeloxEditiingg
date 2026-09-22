package api

import (
	"errors"
	"io"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/deploy"
	"velox-server/internal/fleet"
)

// MutationRequest is the JSON body shape for drain/resume/quarantine
// POSTs. Reason is the operator's intent text rendered verbatim in
// the audit dashboard's audit log; falls back to a constant when
// the operator omits the field (the admin auth context does not yet
// carry an operator identity in Step 6).
//
// Body is OPTIONAL — handlers tolerate an absent body, an empty
// body, and a body without the `reason` field. The
// TrimWhitespace+empty-fallback chain ensures the schema's
// `length(reason) > 0` CHECK never trips on handler output.
type MutationRequest struct {
	Reason           string `json:"reason"`
	TargetDigest     string `json:"target_digest"`
	AudioMixStrategy string `json:"audio_mix_strategy"`
	AudioMixProfile  *int   `json:"audio_mix_profile"`
	// FMP4StreamProfile is the canonical rollout knob for the fMP4
	// streaming-profile admission gate. It must move through this audited
	// operation; direct edits to /etc/velox-worker/worker.env are forbidden.
	FMP4StreamProfile *int `json:"fmp4_stream_profile"`
	// AssetDownloadConcurrency and PrefetchMaxConcurrent are bounded worker
	// runtime knobs. They share the audited worker-config operation so an
	// operator cannot bypass the root-owned helper over HTTP.
	AssetDownloadConcurrency *int `json:"asset_download_concurrency"`
	PrefetchMaxConcurrent    *int `json:"prefetch_max_concurrent"`
}

// validateAdminTargetDigest is the API boundary for worker updates. Reuse
// deploy.ValidateImageRef so the HTTP path, UpdateExecutor, and worker-side
// prepare-host validation accept exactly the same immutable GHCR reference.
// Keep this validation before worker lookup, registry mutation, and operation
// publication so rejected requests have no observable side effects.
func validateAdminTargetDigest(ref string) error {
	return deploy.ValidateImageRef(ref)
}

// invalidMutationJSON and invalidMutationDigest keep HTTP mapping in the
// orchestrator while keeping request decoding and policy at this boundary.
type invalidMutationJSON struct{}

func (invalidMutationJSON) Error() string { return "invalid JSON request body" }

type invalidMutationDigest struct{}

func (invalidMutationDigest) Error() string {
	return "target_digest is required and must be a pinned ghcr.io image digest"
}

// bindMutationRequest parses the optional mutation body and applies the
// same defaults and update-only digest policy used by the HTTP handler.
func bindMutationRequest(c *gin.Context, kind string) (MutationRequest, error) {
	var req MutationRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		return MutationRequest{}, invalidMutationJSON{}
	}
	req.Reason = strings.TrimSpace(req.Reason)
	req.TargetDigest = strings.TrimSpace(req.TargetDigest)
	req.AudioMixStrategy = strings.TrimSpace(req.AudioMixStrategy)
	if req.Reason == "" {
		req.Reason = "triggered via admin API"
	}
	if kind == fleet.OperationKindUpdate {
		if err := validateAdminTargetDigest(req.TargetDigest); err != nil {
			return MutationRequest{}, invalidMutationDigest{}
		}
	}
	if kind == fleet.OperationKindRestart {
		if req.AudioMixStrategy != "" && req.AudioMixStrategy != "legacy" && req.AudioMixStrategy != "optimized" && req.AudioMixStrategy != "auto" {
			return MutationRequest{}, errors.New("audio_mix_strategy must be legacy, optimized, or auto")
		}
		if req.AudioMixProfile != nil && *req.AudioMixProfile != 0 && *req.AudioMixProfile != 1 {
			return MutationRequest{}, errors.New("audio_mix_profile must be 0 or 1")
		}
		if req.FMP4StreamProfile != nil && *req.FMP4StreamProfile != 0 && *req.FMP4StreamProfile != 1 {
			return MutationRequest{}, errors.New("fmp4_stream_profile must be 0 or 1")
		}
		if req.AssetDownloadConcurrency != nil && (*req.AssetDownloadConcurrency < 1 || *req.AssetDownloadConcurrency > 128) {
			return MutationRequest{}, errors.New("asset_download_concurrency must be in [1,128]")
		}
		if req.PrefetchMaxConcurrent != nil && (*req.PrefetchMaxConcurrent < 1 || *req.PrefetchMaxConcurrent > 127) {
			return MutationRequest{}, errors.New("prefetch_max_concurrent must be in [1,127]")
		}
		if req.AssetDownloadConcurrency != nil && req.PrefetchMaxConcurrent != nil && *req.PrefetchMaxConcurrent >= *req.AssetDownloadConcurrency {
			return MutationRequest{}, errors.New("prefetch_max_concurrent must be less than asset_download_concurrency")
		}
		if req.AudioMixStrategy == "" && req.AudioMixProfile == nil && req.FMP4StreamProfile == nil && req.AssetDownloadConcurrency == nil && req.PrefetchMaxConcurrent == nil {
			return MutationRequest{}, errors.New("worker config requires audio_mix_strategy, audio_mix_profile, fmp4_stream_profile, asset_download_concurrency, or prefetch_max_concurrent")
		}
	}
	return req, nil
}
