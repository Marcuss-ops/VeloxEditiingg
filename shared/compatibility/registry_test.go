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
	if entry.Owner == "" || len(entry.Consumers) == 0 || entry.RemovalDate == "" || entry.MinimumVersion == "" {
		t.Fatalf("incomplete lifecycle metadata: %#v", entry)
	}
}

func TestReadStringListReportsLegacyAliasesAndCounters(t *testing.T) {
	SetMode(ModeCompat)
	t.Cleanup(func() { SetMode(ModeCompat); SetAliasReadObserver(nil) })
	var mu sync.Mutex
	var events [][2]string
	SetAliasReadObserver(func(alias, canonical string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, [2]string{alias, canonical})
	})

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
	entry, _ := Lookup(VoiceoverPathsKey)
	if entry.ReadCounter < 2 {
		t.Fatalf("read counter = %d, want at least 2", entry.ReadCounter)
	}
}

func TestStrictModeRejectsLegacyAliasesAndCountsRejection(t *testing.T) {
	SetMode(ModeStrict)
	t.Cleanup(func() { SetMode(ModeCompat); SetAliasRejectedObserver(nil) })
	var rejected [][2]string
	SetAliasRejectedObserver(func(alias, canonical string) {
		rejected = append(rejected, [2]string{alias, canonical})
	})
	source := map[string]interface{}{"voiceover_path": "legacy.mp3"}
	if err := ValidateNoLegacyAliases(source); err == nil {
		t.Fatal("strict validation accepted legacy alias")
	} else {
		if _, ok := err.(*AliasRejectionError); !ok {
			t.Fatalf("error type = %T, want *AliasRejectionError", err)
		}
	}
	if got := ReadStringList(source, VoiceoverPathsKey); got != nil {
		t.Fatalf("strict ReadStringList = %v, want nil", got)
	}
	if len(rejected) != 2 {
		t.Fatalf("rejection events = %d, want 2 (validation + read): %v", len(rejected), rejected)
	}
	entry, _ := Lookup(VoiceoverPathsKey)
	if entry.RejectionCounter < 2 {
		t.Fatalf("rejection counter = %d, want at least 2", entry.RejectionCounter)
	}
}

func TestReadStringListDoesNotReportCanonicalKey(t *testing.T) {
	SetMode(ModeCompat)
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
