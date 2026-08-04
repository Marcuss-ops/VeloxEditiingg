package instaeditauth

import (
	"strings"
	"testing"
)

func TestCanonicalScopeVocabularyContainsOnlyJobWorkerAssetScopes(t *testing.T) {
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

	if len(AllScopesSuperset) != len(want) {
		t.Fatalf("AllScopesSuperset count: got %d, want %d", len(AllScopesSuperset), len(want))
	}
	for _, scope := range AllScopesSuperset {
		found := false
		for _, allowed := range want {
			if scope == allowed {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("AllScopesSuperset contains non-canonical scope %q", scope)
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
		if (&Claims{Scopes: AllScopesSuperset}).HasScope(scope) {
			t.Errorf("retired scope %q must not be granted by AllScopesSuperset", scope)
		}
	}
}
