package credentials

import (
	"errors"
	"testing"
)

func TestReferenceFromJSONAcceptsOnlyOpaqueReference(t *testing.T) {
	ref := "cred_0123456789abcdef0123456789abcdef0123"
	got, err := ReferenceFromJSON(`{"credential_ref":"` + ref + `"}`)
	if err != nil || got != ref {
		t.Fatalf("ReferenceFromJSON = %q, %v; want %q", got, err, ref)
	}
	for _, raw := range []string{
		`{"credential_ref":"access-token"}`,
		`{"credential_ref":"Bearer secret"}`,
		`{"credential_ref":123}`,
		`{"credential_ref":"cred_bad"}`,
		`{"credential_ref":"cred_0123456789abcdef0123456789abcdef012345"}`,
	} {
		if _, err := ReferenceFromJSON(raw); !errors.Is(err, ErrInvalidReference) {
			t.Fatalf("ReferenceFromJSON(%s) error = %v; want ErrInvalidReference", raw, err)
		}
	}
	if got, err := ReferenceFromJSON(`{"title":"safe"}`); err != nil || got != "" {
		t.Fatalf("metadata without reference = %q, %v; want empty", got, err)
	}
}
