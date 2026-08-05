package deliveries

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"

	"velox-server/internal/credentials"
	"velox-shared/contract/domain"
)

// TestCredentialErrorCodeTypedClassification pins the typed error-code
// projection used for BLOCKED_AUTH rows. Every classification must come
// from errors.Is against the credentials-vault / deliveries sentinels or
// from the canonical DomainError FailureCode; the historical
// strings.Contains(err.Error(), ...) marker table was the plaintext
// classifier this test replaces. A regression that re-introduces Error()
// text matching would fail the "unclassified auth" row: an error whose
// MESSAGE contains every marker but whose chain carries no typed sentinel
// must stay CREDENTIAL_AUTH.
func TestCredentialErrorCodeTypedClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "CREDENTIAL_AUTH"},
		{"vault revoked", credentials.ErrRevoked, "CREDENTIAL_REVOKED"},
		{"vault expired wrapped", fmt.Errorf("outer: %w", credentials.ErrExpired), "CREDENTIAL_EXPIRED"},
		{"vault scope denied", credentials.ErrScope, "CREDENTIAL_SCOPE_DENIED"},
		{"vault not found", credentials.ErrNotFound, "CREDENTIAL_NOT_FOUND"},
		{"vault key unavailable", credentials.ErrKeyUnavailable, "CREDENTIAL_VAULT_UNAVAILABLE"},
		{"credential ref required", fmt.Errorf("%w: %w", ErrProviderAuth, ErrCredentialRefRequired), "CREDENTIAL_REF_REQUIRED"},
		{"credential vault unavailable", fmt.Errorf("%w: %w", ErrProviderAuth, ErrCredentialVaultUnavailable), "CREDENTIAL_VAULT_UNAVAILABLE"},
		{"vault revoked through issue wrapper", fmt.Errorf("%w: issue credential lease: %w", ErrProviderAuth, credentials.ErrRevoked), "CREDENTIAL_REVOKED"},
		{
			// Canonical DomainError FailureCode takes precedence over the
			// typed sentinels (domain.FailureCodeOf is checked first).
			name: "domain failure code wins",
			err: domain.NewClassified(
				domain.CodeDeliveryDestinationRejected, "delivery_plan.0.external_destination_id",
				"unavailable", "social destination unavailable", nil, false,
				422, codes.InvalidArgument,
				domain.FailureDestinationUnavailable, domain.MetricDestinationUnavailable,
				domain.ComponentEnqueue, domain.PhaseValidation,
			),
			want: "DELIVERY_TARGET_UNAVAILABLE",
		},
		{
			// Message contains every marker substring but the chain is
			// untyped: the classifier MUST NOT match on text.
			name: "unclassified auth message mentioning markers stays generic",
			err:  fmt.Errorf("%w: credential revoked while vault unavailable (scope) — credential_ref is required", ErrProviderAuth),
			want: "CREDENTIAL_AUTH",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := credentialErrorCode(tc.err); got != tc.want {
				t.Fatalf("credentialErrorCode(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
