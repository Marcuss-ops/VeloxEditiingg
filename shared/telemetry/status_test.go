package telemetry

import "testing"

// TestCanonicalStatusVocabulary pins the closed status vocabulary: exactly
// the two shared values, fail-closed on empty/unknown.
func TestCanonicalStatusVocabulary(t *testing.T) {
	if StatusOK != "ok" || StatusFailed != "failed" {
		t.Fatalf("status constants = %q/%q, want ok/failed", StatusOK, StatusFailed)
	}
	for _, s := range []string{"ok", "failed"} {
		if !IsCanonicalStatus(s) {
			t.Errorf("IsCanonicalStatus(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "completed", "started", "progress", "invented"} {
		if IsCanonicalStatus(s) {
			t.Errorf("IsCanonicalStatus(%q) = true, want false", s)
		}
	}
}

// TestCanonicalEventTypeVocabulary pins the closed event_type vocabulary:
// exactly the four shared values, fail-closed on unknown non-empty values.
func TestCanonicalEventTypeVocabulary(t *testing.T) {
	want := []string{"completed", "failed", "started", "progress"}
	got := []string{EventTypeCompleted, EventTypeFailed, EventTypeStarted, EventTypeProgress}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event_type constants = %v, want %v", got, want)
		}
	}
	for _, s := range want {
		if !IsCanonicalEventType(s) {
			t.Errorf("IsCanonicalEventType(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "invented", "ok", "queued"} {
		if IsCanonicalEventType(s) {
			t.Errorf("IsCanonicalEventType(%q) = true, want false", s)
		}
	}
}

// TestStatusAndEventTypeVocabulariesMatchCatalog pins the bijection between
// the Go constants and the language-neutral schema vocabularies. The
// parse-time validateSchemaVocabularies panics on drift in production; this
// test documents the same contract in CI.
func TestStatusAndEventTypeVocabulariesMatchCatalog(t *testing.T) {
	doc := mustLanguageNeutralCatalog()
	if !sameStringSet(doc.Schema.Statuses, []string{StatusOK, StatusFailed}) {
		t.Fatalf("schema.statuses %v diverge from constants %v", doc.Schema.Statuses, []string{StatusOK, StatusFailed})
	}
	if !sameStringSet(doc.Schema.EventTypes, []string{EventTypeCompleted, EventTypeFailed, EventTypeStarted, EventTypeProgress}) {
		t.Fatalf("schema.event_types %v diverge from constants %v", doc.Schema.EventTypes, []string{EventTypeCompleted, EventTypeFailed, EventTypeStarted, EventTypeProgress})
	}
}
