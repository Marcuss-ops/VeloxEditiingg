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
// details shape). The "ValidationErrorField != ''" branch is
// covered by stashing a fake extractor via the
// `validationFieldExtractor` package-private indirection —
// enqueue's *validationError type is unexported and can't be
// constructed from outside the enqueue package without adding a
// dedicated constructor.
//
// Two boolean flags steer the test rig:
//
//   - useNilErr: pass nil for the err argument; assert NO response
//     written (the helper must noop).
//   - useNilContext: leave `c` as the zero value (`*gin.Context =
//     nil`); assert the helper stays defensive and never panics
//     even when gin.Context is malformed.
//
// `useFakeField` (function-variable-style override) feeds a synthetic
// ValidationErrorField result for the typed-path branch.
func TestWriteResolverError(t *testing.T) {
	cases := []struct {
		name             string
		err              error
		idemField        string
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
			idemField:  "payload",
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "payload_incomplete",
		},
		{
			name:             "ErrIdempotencyKeyReused (payload) -> 409 + hash_mismatch",
			err:              ErrIdempotencyKeyReused,
			idemField:        "payload",
			wantStatus:       http.StatusConflict,
			wantCode:         "idempotency_key_reused",
			wantDetailsPath:  "payload",
			wantDetailsIssue: "hash_mismatch",
			wantHasDetails:   true,
		},
		{
			name:             "ErrIdempotencyKeyReused (idempotency_key) -> 409 + hash_mismatch",
			err:              ErrIdempotencyKeyReused,
			idemField:        "idempotency_key",
			wantStatus:       http.StatusConflict,
			wantCode:         "idempotency_key_reused",
			wantDetailsPath:  "idempotency_key",
			wantDetailsIssue: "hash_mismatch",
			wantHasDetails:   true,
		},
		{
			name:             "ValidationErrorField returns path -> 422 invalid_payload + field path",
			err:              fmt.Errorf("enqueue wrapper carrying a validationError inside"),
			idemField:        "payload",
			useFakeField:     true,
			fakeField:        "delivery_plan[0].external_destination_id",
			wantStatus:       http.StatusUnprocessableEntity,
			wantCode:         "invalid_payload",
			wantDetailsPath:  "delivery_plan[0].external_destination_id",
			wantDetailsIssue: "invalid",
			wantHasDetails:   true,
		},
		{
			name:            "untyped 'are required' -> 422 invalid_payload, no details",
			err:             fmt.Errorf("creatorflow: Resolve: source_provider and source_job_id are required"),
			idemField:       "payload",
			wantStatus:      http.StatusUnprocessableEntity,
			wantCode:        "invalid_payload",
			wantMsgContains: "are required",
		},
		{
			name:            "untyped 'payload is required' -> 422 invalid_payload, no details",
			err:             fmt.Errorf("payload is required"),
			idemField:       "payload",
			wantStatus:      http.StatusUnprocessableEntity,
			wantCode:        "invalid_payload",
			wantMsgContains: "is required",
		},
		{
			name:       "wrapped hard fail -> 500 resolver_failure, no details",
			err:        fmt.Errorf("creatorflow: Resolve atomic: %w", errors.New("sql: connection done")),
			idemField:  "payload",
			wantStatus: http.StatusInternalServerError,
			wantCode:   "resolver_failure",
		},
		{
			name:       "raw hard fail -> 500 resolver_failure, no details",
			err:        errors.New("network baseline timeout"),
			idemField:  "payload",
			wantStatus: http.StatusInternalServerError,
			wantCode:   "resolver_failure",
		},
		{
			name:      "nil err -> noop (no response written)",
			useNilErr: true,
			idemField: "payload",
		},
		{
			name:          "nil gin.Context (defensive) -> noop, no panic",
			err:           ErrResolverNotComplete,
			useNilContext: true,
			idemField:     "payload",
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

			WriteResolverError(c, errArg, tc.idemField)

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
