package instaeditauth

import (
	"strings"
	"testing"
)

func TestCanonicalScopeVocabularyContainsOnlyNonEditorFixtureScopes(t *testing.T) {
	want := []string{
		"jobs.read",
		"jobs.write",
		"workers.read",
		"assets.read",
		"assets.write",
	}

	got := []string{
		ScopeJobsRead,
		ScopeJobsWrite,
		ScopeWorkersRead,
		ScopeAssetsRead,
		ScopeAssetsWrite,
	}
	if len(got) != len(want) {
		t.Fatalf("canonical scope count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("canonical scope[%d]: got %q, want %q", i, got[i], want[i])
		}
	}

	if len(allScopesSuperset) != len(want) {
		t.Fatalf("allScopesSuperset count: got %d, want %d", len(allScopesSuperset), len(want))
	}
	for _, scope := range allScopesSuperset {
		found := false
		for _, allowed := range want {
			if scope == allowed {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("allScopesSuperset contains non-canonical scope %q", scope)
		}
	}
}

func TestCanonicalScopeVocabularyRejectsRetiredEditorAndYouTubeScopes(t *testing.T) {
	retired := []string{
		strings.Join([]string{"editor", "project", "read"}, "."),
		strings.Join([]string{"editor", "project", "write"}, "."),
		strings.Join([]string{"editor", "asset", "upload"}, "."),
		strings.Join([]string{"youtube", "session", "publish"}, "."),
	}
	for _, scope := range retired {
		if (&Claims{Scopes: allScopesSuperset}).HasScope(scope) {
			t.Errorf("retired scope %q must not be granted by allScopesSuperset", scope)
		}
	}
}
