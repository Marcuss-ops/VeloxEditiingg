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
		if req.AudioMixStrategy == "" && req.AudioMixProfile == nil {
			return MutationRequest{}, errors.New("worker config requires audio_mix_strategy or audio_mix_profile")
		}
	}
	return req, nil
}
