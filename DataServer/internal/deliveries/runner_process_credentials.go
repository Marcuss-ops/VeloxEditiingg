package deliveries

// runner_process_credentials.go: credential resolution + short-lived
// lease issuance + credential error classification for the DeliveryRunner.
// Split out of runner_process.go; processLease stays in that file and the
// lease-renewal loop lives in runner_process_lease.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"velox-server/internal/credentials"
	"velox-server/internal/deliverystore"
	"velox-shared/contract/domain"
)

// resolveCredentialReference keeps credential material out of the job
// payload. The BFF carries only the opaque external destination ID; the
// destination catalog is therefore the authoritative fallback when a
// delivery plan has no per-publication credential_ref override. An explicit
// metadata value still wins, and an invalid explicit value remains a hard
// error rather than silently falling back to a different credential.
func resolveCredentialReference(metadataJSON, configurationJSON string) (string, error) {
	ref, err := credentials.ReferenceFromJSON(metadataJSON)
	if err != nil || ref != "" {
		return ref, err
	}
	return credentials.ReferenceFromJSON(configurationJSON)
}

func (r *DeliveryRunner) issueCredentialLease(ctx context.Context, provider Provider, destination *Destination, lease deliverystore.DeliveryLease) (*credentials.AccessLease, error) {
	credentialProvider, needsCredential := provider.(CredentialLeaseProvider)
	if !needsCredential {
		return nil, nil
	}
	if destination == nil || destination.CredentialRef == "" {
		return nil, fmt.Errorf("%w: %w", ErrProviderAuth, ErrCredentialRefRequired)
	}
	if r.vault == nil {
		return nil, fmt.Errorf("%w: %w", ErrProviderAuth, ErrCredentialVaultUnavailable)
	}
	scopes := []string{"publish"}
	if scoped, ok := credentialProvider.(CredentialScopeProvider); ok {
		scopes = scoped.RequiredCredentialScopes()
	}
	publicationID := lease.DeliveryID
	var metadata map[string]any
	if err := json.Unmarshal([]byte(destination.DeliveryMetadataJSON), &metadata); err == nil {
		if value, ok := metadata["publication_id"].(string); ok && value != "" {
			publicationID = value
		}
	}
	accessLease, err := r.vault.IssueAccessLease(ctx, destination.CredentialRef, r.identity, publicationID, scopes)
	if err != nil {
		// %w keeps the vault's typed sentinel (ErrRevoked / ErrExpired /
		// ErrScope / ErrNotFound / ErrKeyUnavailable) reachable through
		// errors.Is so classification never parses Error() text.
		return nil, fmt.Errorf("%w: issue credential lease: %w", ErrProviderAuth, err)
	}
	return accessLease, nil
}

// credentialErrorCode derives the machine-readable BLOCKED_AUTH code from
// the typed error chain only: the credentials vault sentinels and the
// deliveries credential sentinels. No Error() text is inspected; unclassified
// auth failures fall back to the generic CREDENTIAL_AUTH code.
func credentialErrorCode(err error) string {
	if err == nil {
		return "CREDENTIAL_AUTH"
	}
	if code := domain.FailureCodeOf(err); code != "" {
		return code
	}
	switch {
	case errors.Is(err, credentials.ErrRevoked):
		return "CREDENTIAL_REVOKED"
	case errors.Is(err, credentials.ErrExpired):
		return "CREDENTIAL_EXPIRED"
	case errors.Is(err, credentials.ErrScope):
		return "CREDENTIAL_SCOPE_DENIED"
	case errors.Is(err, credentials.ErrNotFound):
		return "CREDENTIAL_NOT_FOUND"
	case errors.Is(err, credentials.ErrKeyUnavailable),
		errors.Is(err, ErrCredentialVaultUnavailable):
		return "CREDENTIAL_VAULT_UNAVAILABLE"
	case errors.Is(err, ErrCredentialRefRequired):
		return "CREDENTIAL_REF_REQUIRED"
	default:
		return "CREDENTIAL_AUTH"
	}
}
