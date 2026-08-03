package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/credentials"
	"velox-server/internal/publicationcap"
)

// ValidateJob handles POST /api/v1/jobs/validate. It executes all intake
// checks without creating a job, rendering, downloading or publishing.
func (h *Handlers) ValidateJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := decodeStrictJSON(c.Request.Body, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error_code": "INVALID_JSON", "error": err.Error()})
			return
		}
		if vErr, bad := ValidateIdempotencyKey(req.IdempotencyKey); bad {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": vErr.Code, "details": gin.H{"path": "idempotency_key", "reason": vErr.Reason}})
			return
		}
		if err := NormalizeCanonicalRecipe(&req); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": "UNSUPPORTED_RECIPE", "error": err.Error()})
			return
		}
		if vErr, bad := ValidateSubmitJobRequest(req); bad {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": vErr.Code, "message": vErr.Message, "details": vErr.Details})
			return
		}
		if err := h.validatePreflightPublication(req); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": "PUBLICATION_NOT_REALIZABLE", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "valid": true, "payload_valid": true, "destination_compatible": true, "credential_valid": true, "artifact_identifiable": len(req.Publications) == 0 || publicationsHaveOutputRefs(req.Publications), "publication_realizable": true})
	}
}

// EstimateJob returns a deterministic resource estimate without starting work.
func (h *Handlers) EstimateJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := decodeStrictJSON(c.Request.Body, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error_code": "INVALID_JSON", "error": err.Error()})
			return
		}
		if err := NormalizeCanonicalRecipe(&req); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": "UNSUPPORTED_RECIPE", "error": err.Error()})
			return
		}
		if vErr, bad := ValidateSubmitJobRequest(req); bad {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": vErr.Code, "details": vErr.Details})
			return
		}
		var duration float64
		for _, scene := range req.Scenes {
			duration += scene.DurationSeconds
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "estimated_output_duration_seconds": duration, "estimated_render_seconds": duration * 2.5, "estimated_cpu_seconds": duration * 8, "estimated_download_bytes": int64(len(req.Scenes)) * 10 * 1024 * 1024, "estimated_temp_bytes": int64(len(req.Scenes)) * 50 * 1024 * 1024, "estimated_output_bytes": int64(duration * 2 * 1024 * 1024), "warnings": []string{}, "generated_at": time.Now().UTC()})
	}
}

// PreviewPublication resolves the user-visible publication snapshot without
// performing any provider side effect.
func (h *Handlers) PreviewPublication() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := decodeStrictJSON(c.Request.Body, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error_code": "INVALID_JSON", "error": err.Error()})
			return
		}
		if err := NormalizeCanonicalRecipe(&req); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": "UNSUPPORTED_RECIPE", "error": err.Error()})
			return
		}
		if vErr, bad := ValidateSubmitJobRequest(req); bad {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error_code": vErr.Code, "details": vErr.Details})
			return
		}
		items := make([]gin.H, 0, len(req.Publications))
		for _, pub := range req.Publications {
			items = append(items, gin.H{"publication_id": pub.PublicationID, "title": pub.Metadata.Title, "description": pub.Metadata.Description, "language": pub.Language, "publish_at": pub.Metadata.PublishAt, "destinations": pub.Destinations, "artifact": pub.OutputRef, "credential_ref": "opaque_ref_required_at_publication"})
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "preview": items, "upload_performed": false})
	}
}

func publicationsHaveOutputRefs(publications []SubmitPublication) bool {
	for _, pub := range publications {
		if pub.OutputRef.VariantID == "" && pub.OutputRef.ArtifactRole == "" {
			return false
		}
	}
	return true
}

func validatePublicationCapabilities(req SubmitJobRequest) error {
	registry := publicationcap.DefaultRegistry()
	for _, pub := range req.Publications {
		for _, destination := range pub.Destinations {
			provider := "youtube"
			if value, ok := destination.ProviderOptions["provider"].(string); ok && value != "" {
				provider = value
			}
			hasLocalizations := len(pub.Localizations) > 0
			if err := registry.Validate(provider, publicationcap.Metadata{Title: pub.Metadata.Title, Description: pub.Metadata.Description, Tags: pub.Metadata.Tags, HasSchedule: pub.Metadata.PublishAt != "", HasLocalizations: hasLocalizations, HasThumbnail: false, HasCaptions: false}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handlers) validatePreflightPublication(req SubmitJobRequest) error {
	if err := validatePublicationCapabilities(req); err != nil {
		return err
	}
	for _, pub := range req.Publications {
		for _, destination := range pub.Destinations {
			ref := strings.TrimSpace(destination.CredentialRef)
			if ref == "" {
				return fmt.Errorf("credential_ref_required: publication %s destination %s", pub.PublicationID, destination.DestinationID)
			}
			if !credentials.ValidReference(ref) {
				return fmt.Errorf("credential_ref_invalid: %s", ref)
			}
			if h.credentialVault == nil {
				return fmt.Errorf("credential_vault_unavailable")
			}
			if err := h.credentialVault.ValidateReference(context.Background(), ref); err != nil {
				return fmt.Errorf("credential_invalid: %w", err)
			}
		}
	}
	return nil
}
