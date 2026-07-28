package enqueue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"velox-server/internal/socialclient"

	"velox-shared/contract/deliveryplan"
)

// delivery_plan_validator.go — Step 4/8 canonical-purity preflight.
//
// Gates every Job behind an explicit delivery_plan whose per-entry
// retry_budget is >= 0. Without this gate, FinalizeVerified
// discovers the missing plan AFTER the render has burned its
// budget — see the diagnostic "Validate delivery plan at enqueue or
// pre-render".
//
// Starting with the shared/contract/deliveryplan extraction,
// this file owns ONLY the enqueue-layer pre-flight loop:
//
//  1. Call deliveryplan.Parse(payload) — shape rules + duplicate
//     detection + per-entry constraints live there.
//  2. For each resulting shared.Entry, run the social_repo
//     pre-flight via the DestinationValidator interface (no-op by
//     default for legacy/dev mode). The HARD/SOFT classification
//     of socialclient sentinels (ErrPermanent/ErrAuth HARD,
//     ErrTransient/ErrRateLimit/ErrNotConfigured SOFT) stays
//     here because the socialclient boundary is enqueue-only.
//
// Canonical rename note (YouTube → Delivery, PR-15.8):
//
//	YouTubeGroup       → DestinationGroupID   (was: youtube_group_id)
//	YouTubeChannelID   → ExternalDestinationID (was: youtube_channel)
//	YouTubeVideoID     → RemoteMediaID        (was: youtube_video_id)
//	YouTubeURL         → RemoteURL            (was: youtube_published_url)
//	YouTubeStatus      → DeliveryStatus       (was: youtube_publish_status)
//
// The five YouTube-prefixed fields are absent from active Go
// runtime code at this revision. Velox does NOT SELECT
// youtube_channels, youtube_oauth_tokens, or youtube_groups
// anywhere (those tables are dropped); destination validation is
// delegated to the external Social API at
// POST /internal/v1/destinations/:id/validate (see the optional
// pre-flight loop below).

// =============================================================================
// Typed validation errors — TYPE ALIAS to the shared package.
// =============================================================================
//
// The private *validationError type alias'd below points at the
// shared *deliveryplan.ValidationError, so existing tests using
// `var verr *validationError; errors.As(err, &verr)` continue to
// work without any test-side import change. The alias is the same
// pattern store.DeliveryPlanValidationError uses. The two aliases
// point at the SAME canonical type, so a single validator emit can
// be detected by both layers through errors.As.
//
// P0 history: the typed alias is the durable fix for the
// plaintext-error-classification regression (a 422 invalid_payload
// that fell through to 500 resolver_failure because the
// in-handler validator emitted fmt.Errorf with a substring, not
// a typed shape). Before the alias, the enqueue layer used a
// private *validationError that cross-package callers could not
// detect. After the alias, errors.As reaches the canonical typed
// surface from any layer.
//
// IMPORTANT: after the alias, the struct's exported field names
// (FieldPath / Msg / Wrapped — capitalised) are the canonical
// constructor inputs. The historical lower-case field-name
// literals (`&validationError{field: ..., message: ..., wrapped: ...}`)
// were rewritten to call the canonical constructors
// (deliveryplan.NewValidationError / NewValidationErrorWrapped).
// FORBIDDEN: any future literal against the alias struct that
// uses the lowercase field names — they were never exported on
// the alias and the Go compiler will reject them.

// validationError is the alias to the canonical typed validator
// rejection emitted by deliveryplan.Parse. The exported alias
// preserves the historical (private) identifier so existing tests
// keeping `var verr *validationError` continue to compile and pass.
type validationError = deliveryplan.ValidationError

// =============================================================================
// Destination pre-flight
// =============================================================================

// DestinationValidator is the minimal contract the enqueue-layer
// validator needs from the Social API boundary. Production wires
// *socialclient.Client here; tests can wire a hand-rolled stub.
//
// The contract is intentionally narrow: one method, ctx-aware,
// single-attempt. The validator applies the hard/soft
// classification policy ON TOP of this sentinel so the
// socialclient stays unaware of enqueue semantics.
type DestinationValidator interface {
	ValidateDestination(ctx context.Context, socialDestID string) error
}

// noopDestinationValidator is the default validator when no
// *socialclient.Client has been wired in (legacy consumers, dev
// mode without a Social API configured). It short-circuits the
// per-entry pre-flight loop and skips any Social API call so the
// existing happy-path unit tests still pass without DI plumbing.
type noopDestinationValidator struct{}

func (noopDestinationValidator) ValidateDestination(ctx context.Context, socialDestID string) error {
	return nil
}

// =============================================================================
// Public entry points
// =============================================================================

// validateDeliveryPlanRequires is the canonical-purity preflight.
// Must be called from PrepareJobAndTask before the Job+TaskSpec is
// handed to the atomic creator; on error, the Job is NOT queued.
//
// The optional `validator` parameter performs a per-entry
// pre-flight against the external Social API
// (POST /internal/v1/destinations/:id/validate). Plug it in via
// Enqueuer.WithSocialValidator at the composition root; pass
// `nil` (or the bundled `noopDestinationValidator{}`) for the
// legacy paths that bypass the social_repo boundary (Drive-only,
// pre-rollout dev mode).
//
// Sentinel handling on the per-entry pre-flight loop:
//
//	nil                       → OK, proceed.
//	ErrPermanent / ErrAuth    → HARD fail: bad / unauthorized
//	                            destination, enqueue is rejected
//	                            with a wrapped *validationError.
//	ErrTransient / ErrRateLimit / ErrNotConfigured
//	                          → SOFT warn: log and continue; the
//	                            runner's per-destination
//	                            retry_budget will re-resolve at
//	                            FinalizeVerified.
func validateDeliveryPlanRequires(ctx context.Context, payloadMap map[string]interface{}, validator DestinationValidator) error {
	// Parse owns the shape rules + duplicate detection + per-entry
	// validation (retry_budget<0, priority<0, dup, disabled,
	// missing destination_id, wrong root type, etc.). Every emit
	// from deliveryplan.Parse is *deliveryplan.ValidationError, which
	// IS *validationError via the type alias declared above; the
	// typed surface is identical, so a verbatim pass-through
	// preserves the field path + envelope unchanged.
	entries, err := deliveryplan.Parse(payloadMap)
	if err != nil {
		return err
	}

	if validator == nil {
		validator = noopDestinationValidator{}
	}

	for i, e := range entries {
		socialDestID := strings.TrimSpace(e.ExternalDestinationID)
		if socialDestID == "" {
			// Empty external_destination_id means "no Social
			// API routing for this entry" (legacy Drive-only
			// destinations) and the loop skips the validation
			// entirely. Both canonical
			// (`external_destination_id`) and legacy
			// (`social_destination_id`) JSON keys funnel into
			// the same canonical slot via deliveryplan.Parse's
			// shapeFromMap, so the back-compat read is
			// transparent here.
			continue
		}
		if perr := validator.ValidateDestination(ctx, socialDestID); perr != nil {
			switch {
			case errors.Is(perr, socialclient.ErrPermanent),
				errors.Is(perr, socialclient.ErrAuth):
				// HARD fail: bad / unauthorized destination.
				// enqueue refused to keep the job from becoming
				// visibly un-routable. The wrapped sentinel
				// travels through errors.Is at the HTTP envelope
				// layer (NewValidationErrorWrapped produces a
				// *ValidationError whose Unwrap() returns perr).
				return deliveryplan.NewValidationErrorWrapped(
					deliveryplan.FieldPath(i, "external_destination_id"),
					fmt.Sprintf("social destination %q rejected by social_repo (%v); enqueue refused to keep the job from becoming visibly un-routable",
						socialDestID, perr),
					perr,
				)
			default:
				// Soft: ErrTransient / ErrRateLimit /
				// ErrNotConfigured. Log a warning and
				// continue. The DeliveryRunner's retry_budget
				// at finalize is the recovery path.
				log.Printf("[PREFLIGHT][enqueue] external_destination_id=%q for destination_id=%q skipped: %v (soft: enqueue continues; runner will re-attempt at finalize)",
					socialDestID, e.DestinationID, perr)
			}
		}
	}
	return nil
}

// validateDeliveryPlanShapeOnly is the pure payload-shape validator
// preserved for the canonical-purity gate on paths where the
// Social API boundary is intentionally NOT exercised (dev mode
// without a configured social_repo, legacy consumers, fuzz
// harnesses). It is the same loop validateDeliveryPlanRequires
// runs minus the per-entry pre-flight. Callers that want both
// shape + pre-flight must route through
// validateDeliveryPlanRequires.
func validateDeliveryPlanShapeOnly(payloadMap map[string]interface{}) error {
	return validateDeliveryPlanRequires(context.Background(), payloadMap, nil)
}

// =============================================================================
// Cross-package re-exports
// =============================================================================

// ValidationErrorField returns the structured field path of the
// canonical *deliveryplan.ValidationError wrapped inside err, or
// "" if err is not a validationError. Re-exported at the enqueue
// layer for cross-package callers that historically referenced
// the helper as `enqueue.ValidationErrorField(...)` (the
// integration_test package + integration_test golden assertions).
// The implementation delegates to the canonical shared package;
// the re-export is the durable public surface so the historical
// import path keeps resolving without changes at any call site.
//
// Typical usage:
//
//	if got := enqueue.ValidationErrorField(err); got != "delivery_plan.0.social_destination_id" {
//	    // fail the assertion
//	}
//
// Returns "" (not error) on a non-validationError input so callers
// can use it in expression position without short-circuiting
// their flow.
func ValidationErrorField(err error) string {
	return deliveryplan.ValidationField(err)
}
