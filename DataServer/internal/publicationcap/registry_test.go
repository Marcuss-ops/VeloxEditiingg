package publicationcap

import "testing"

func TestRegistryRejectsUnsupportedAndInvalidMetadata(t *testing.T) {
	r := DefaultRegistry()
	if err := r.Validate("youtube", Metadata{HasLocalizations: true, HasCaptions: true}); err != nil {
		t.Fatal(err)
	}
	if err := r.Validate("google_drive", Metadata{HasLocalizations: true}); err == nil {
		t.Fatal("unsupported localization was accepted")
	}
	if err := r.Validate("youtube", Metadata{Title: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}); err == nil {
		t.Fatal("oversized title was accepted")
	}
}
