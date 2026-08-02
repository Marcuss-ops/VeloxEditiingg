package deliveries

import "testing"

func TestResolveCredentialReference_UsesDestinationCatalogFallback(t *testing.T) {
	const ref = "cred_0123456789abcdef0123456789abcdef0123"
	got, err := resolveCredentialReference(`{"publication_id":"pub-1"}`, `{"credential_ref":"`+ref+`"}`)
	if err != nil {
		t.Fatalf("resolveCredentialReference: %v", err)
	}
	if got != ref {
		t.Fatalf("credential ref = %q, want catalog ref", got)
	}
}

func TestResolveCredentialReference_ExplicitInvalidValueDoesNotFallback(t *testing.T) {
	_, err := resolveCredentialReference(`{"credential_ref":"not-a-ref"}`, `{"credential_ref":"cred_0123456789abcdef0123456789abcdef0123"}`)
	if err == nil {
		t.Fatal("invalid explicit credential_ref unexpectedly fell back")
	}
}
