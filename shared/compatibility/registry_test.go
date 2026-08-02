package compatibility

import (
	"reflect"
	"sync"
	"testing"
)

func TestRegistryVoiceoverAliases(t *testing.T) {
	entry, ok := Lookup(VoiceoverPathsKey)
	if !ok {
		t.Fatal("voiceover alias registry entry is missing")
	}
	if entry.CanonicalKey != VoiceoverPathsKey {
		t.Fatalf("canonical key = %q, want %q", entry.CanonicalKey, VoiceoverPathsKey)
	}
	for _, want := range []string{
		"voiceover_path", "voiceover", "unified_voiceover_link", "voiceovers",
		"voiceovers_urls", "audio_url", "audio_path", "source_url",
		"source_media", "source_media_url", "audio_source",
	} {
		if !contains(entry.Aliases, want) {
			t.Errorf("registry aliases do not contain %q: %v", want, entry.Aliases)
		}
	}
	if entry.Sunset == "" {
		t.Fatal("registry entry must define sunset metadata")
	}
}

func TestReadStringListReportsLegacyAliases(t *testing.T) {
	var mu sync.Mutex
	var events [][2]string
	SetAliasReadObserver(func(alias, canonical string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, [2]string{alias, canonical})
	})
	t.Cleanup(func() { SetAliasReadObserver(nil) })

	got := ReadStringList(map[string]interface{}{
		"voiceover_paths": []interface{}{"canonical.mp3"},
		"voiceover_path":  "legacy.mp3",
		"audio_path":      "legacy.mp3",
	}, VoiceoverPathsKey)
	want := []string{"canonical.mp3", "legacy.mp3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadStringList = %v, want %v", got, want)
	}
	if len(events) != 2 {
		t.Fatalf("observer events = %d, want 2: %v", len(events), events)
	}
	for _, event := range events {
		if event[1] != VoiceoverPathsKey {
			t.Errorf("event canonical = %q, want %q", event[1], VoiceoverPathsKey)
		}
	}
}

func TestReadStringListDoesNotReportCanonicalKey(t *testing.T) {
	called := false
	SetAliasReadObserver(func(string, string) { called = true })
	t.Cleanup(func() { SetAliasReadObserver(nil) })

	got := ReadStringList(map[string]interface{}{
		VoiceoverPathsKey: "canonical.mp3\nsecond.mp3",
	}, VoiceoverPathsKey)
	if !reflect.DeepEqual(got, []string{"canonical.mp3", "second.mp3"}) {
		t.Fatalf("ReadStringList = %v", got)
	}
	if called {
		t.Fatal("canonical reads must not emit alias telemetry")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
