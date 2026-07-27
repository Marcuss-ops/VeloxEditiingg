package creatorflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

// TestWriteResolverError is the table-driven specification for
// WriteResolverError. Each row asserts (status, error code,
// details shape) for the canonical mapping canon:
//
//   errors.Is(err, ErrResolverNotComplete)               → 422 payload_incomplete
//   errors.Is(err, ErrIdempotencyKeyReused)             → 409 + path-from-typed-or-default
//   validationFieldExtractor(err) != ""                  → 422 invalid_payload + typed path
//   strings.Contains(strings.ToLower(err.Error()), "required") → 422 invalid_payload
//   default                                              → 500 resolver_failure
//
// The two paths the helper family supports (POST /api/v1/creator/jobs
// pushes the typed validationError wrapped over the inner "payload";
// POST /api/v1/jobs surfaces the un-typed error) both round-trip
// through the helper with no callsite-provided path annotation.
//
// The "ValidationErrorField != ''" branch is covered by stashing a
// fake extractor via the `validationFieldExtractor` package-private
// indirection — enqueue's *validationError type is unexported and
// can't be constructed from outside the enqueue package without
// adding a dedicated constructor, which would force the test to
// pull the enqueue dep into the surface API of this package.
// The function-variable indirection keeps the test signal-local.
//
// `useNilErr` / `useNilContext` / `useFakeField` are the three test-rig
// toggles. The rig asserts the noop branches stay defensive (nil
// inputs never write a body, even when gin.Context itself is malformed).
func TestWriteResolverError(t *testing.T) {
	cases := []struct {
		name             string
		err              error
		useNilErr        bool
		useNilContext    bool
		useFakeField     bool
		fakeField        string // override validationFieldExtractor
		wantStatus       int
		wantCode         string
		wantDetailsPath  string
		wantDetailsIssue string
		wantHasDetails   bool
		wantMsgContains  string
	}{
		{
			name:       "ErrResolverNotComplete -> 422 payload_incomplete, no details",
			err:        ErrResolverNotComplete,
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "payload_incomplete",
		},
		{
			// SubmitJob path: an un-typed 409 (no wrapped
			// validationError) surfaces path "idempotency_key"
			// via the helper's documented fallback. Pinned by
			// the openapi.yaml ErrorEnvelope cross-check.
			name:             "ErrIdempotencyKeyReused un-typed -> 409 + hash_mismatch, default path \"idempotency_key\"",
			err:              ErrIdempotencyKeyReused,
			wantStatus:       http.StatusConflict,
			wantCode:         "idempotency_key_reused",
			wantDetailsPath:  "idempotency_key",
			wantDetailsIssue: "hash_mismatch",
			wantHasDetails:   true,
		},
		{
			// CreatorPush path: a 409 raised over a typed
			// validationError with field "payload" (the inner
			// envelope's hash-handle) flows the typed path
			// through verbatim, without third-arg annotation.
			name:             "ErrIdempotencyKeyReused wrapped over typed validationError -> 409 + path from Field()",
			err:              ErrIdempotencyKeyReused,
			useFakeField:     true,
			fakeField:        "payload",
			wantStatus:       http.StatusConflict,
			wantCode:         "idempotency_key_reused",
			wantDetailsPath:  "payload",
			wantDetailsIssue: "hash_mismatch",
			wantHasDetails:   true,
		},
		{
			// Enqueue-layer typed rejection covering the
			// destination_id / delivery_plan invalid classes.
			// Each enqueue.package validationError exposes
			// .Field() so this row asserts the helper reads it
			// via validationFieldExtractor (err) and surfaces
			// details.path verbatim without callsite annotation.
			name:             "ValidationErrorField returns path -> 422 invalid_payload + field path (enqueue typed rejection)",
			err:              fmt.Errorf("enqueue wrapper carrying a validationError inside"),
			useFakeField:     true,
			fakeField:        "delivery_plan[0].external_destination_id",
			wantStatus:       http.StatusUnprocessableEntity,
			wantCode:         "invalid_payload",
			wantDetailsPath:  "delivery_plan[0].external_destination_id",
			wantDetailsIssue: "invalid",
			wantHasDetails:   true,
		},
		{
			// Typed validationError surfacing as
			// "scenes[3].destination_id missing" (a hypothetical
			// from the canonical creator frontend after the
			// validatePlanPayload rewrite). The Field() path is
			// the only reliable way to surface this without
			// parsing err.Error(). Without this row, a regression
			// to err.Error() matching would silently re-introduce
			// the 500 downgrade.
			name:             "scenes[i].destination_id missing (typed) -> 422 invalid_payload + scenes[i].destination_id",
			err:              fmt.Errorf("enqueue wrapper"),
			useFakeField:     true,
			fakeField:        "scenes[3].destination_id",
			wantStatus:       http.StatusUnprocessableEntity,
			wantCode:         "invalid_payload",
			wantDetailsPath:  "scenes[3].destination_id",
			wantDetailsIssue: "invalid",
			wantHasDetails:   true,
		},
		{
			// Un-typed resolver-internal validation. Captures
			// "source_provider and source_job_id are required"
			// — the canonical upstream check before the helper
			// was wired. Without this row, a regression to the
			// default branch would silently emit 500.
			name:            "untyped 'are required' -> 422 invalid_payload, no details",
			err:             fmt.Errorf("creatorflow: Resolve: source_provider and source_job_id are required"),
			wantStatus:      http.StatusUnprocessableEntity,
			wantCode:        "invalid_payload",
			wantMsgContains: "are required",
		},
		{
			// Un-typed resolver-internal validation. Captures
			// "payload is required" — the canonical inner
			// envelope check the original CreatorPush handler
			// emitted with normalizeCreatorPushRequest's first
			// line. Without this row, a regression to the
			// default branch would silently emit 500.
			name:            "untyped 'payload is required' -> 422 invalid_payload, no details",
			err:             fmt.Errorf("payload is required"),
			wantStatus:      http.StatusUnprocessableEntity,
			wantCode:        "invalid_payload",
			wantMsgContains: "is required",
		},
		{
			// Wrapped resolver failure (sql: connection done).
			// Without this row, an err that wraps an internal
			// failure would silently leak "%w sql: connection
			// done" into the message body without envelope
			// shaping.
			name:       "wrapped hard fail (sql: connection done) -> 500 resolver_failure, no details",
			err:        fmt.Errorf("creatorflow: Resolve atomic: %w", errors.New("sql: connection done")),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "resolver_failure",
		},
		{
			// Raw resolver failure (network baseline timeout).
			// Pinned by the openapi.yaml ErrorEnvelope +
			// 500 status code cross-check.
			name:       "raw hard fail (network timeout) -> 500 resolver_failure, no details",
			err:        errors.New("network baseline timeout"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "resolver_failure",
		},
		{
			name:      "nil err -> noop (no response written)",
			useNilErr: true,
		},
		{
			// Defensive: gin.Context is a heap-allocated pointer;
			// in some test rigs (intentionally malformed mounts)
			// it can be nil. The helper must NEVER panic and
			// NEVER write a body on this path.
			name:          "nil gin.Context (defensive) -> noop, no panic",
			err:           ErrResolverNotComplete,
			useNilContext: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()

			errArg := tc.err
			if tc.useNilErr {
				errArg = nil
			}

			var c *gin.Context
			if !tc.useNilContext {
				c, _ = gin.CreateTestContext(rec)
			}

			if tc.useFakeField {
				saved := validationFieldExtractor
				t.Cleanup(func() { validationFieldExtractor = saved })
				validationFieldExtractor = func(err error) string {
					return tc.fakeField
				}
			}

			WriteResolverError(c, errArg)

			// No-op branch: err == nil.
			if errArg == nil {
				if rec.Body.Len() != 0 {
					t.Fatalf("nil err should not write body, got %q", rec.Body.String())
				}
				return
			}

			// No-op branch: nil gin.Context.
			if c == nil {
				if rec.Body.Len() != 0 {
					t.Fatalf("nil c should not write body via WriteResolverError, got %q", rec.Body.String())
				}
				return
			}

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", rec.Code, tc.wantStatus, rec.Body.String())
			}

			var body struct {
				OK      bool   `json:"ok"`
				Error   string `json:"error"`
				Message string `json:"message"`
				Details []struct {
					Path  string `json:"path"`
					Issue string `json:"issue"`
				} `json:"details"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v (raw=%q)", err, rec.Body.String())
			}
			if body.OK != false {
				t.Fatalf("ok = %v, want false", body.OK)
			}
			if body.Error != tc.wantCode {
				t.Fatalf("error = %q, want %q", body.Error, tc.wantCode)
			}
			if tc.wantMsgContains != "" && !strings.Contains(body.Message, tc.wantMsgContains) {
				t.Fatalf("message %q does not contain %q", body.Message, tc.wantMsgContains)
			}
			if tc.wantHasDetails {
				if len(body.Details) != 1 {
					t.Fatalf("details len = %d, want 1 (raw=%s)", len(body.Details), rec.Body.String())
				}
				if body.Details[0].Path != tc.wantDetailsPath {
					t.Fatalf("details[0].path = %q, want %q", body.Details[0].Path, tc.wantDetailsPath)
				}
				if body.Details[0].Issue != tc.wantDetailsIssue {
					t.Fatalf("details[0].issue = %q, want %q", body.Details[0].Issue, tc.wantDetailsIssue)
				}
			} else if len(body.Details) > 0 {
				t.Fatalf("details should be empty, got %+v", body.Details)
			}
		})
	}
}
