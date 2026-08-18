package enqueue

import (
	"reflect"
	"testing"
)

func TestExtractVoiceoverPathsCharacterization(t *testing.T) {
	payload := map[string]interface{}{
		"voiceover_path": "  /tmp/voiceover.wav  ",
		"voiceover_paths": []interface{}{
			"/tmp/voiceover.wav",
			"https://cdn.example/voiceover.mp3",
			"",
		},
		"voiceover": map[string]interface{}{
			"url": "https://cdn.example/voiceover.mp3",
		},
		"voiceover_info": map[string]interface{}{
			"local_path": "/tmp/second.wav",
		},
	}

	got := extractVoiceoverPaths(payload)
	want := []string{"/tmp/voiceover.wav", "https://cdn.example/voiceover.mp3", "/tmp/second.wav"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractVoiceoverPaths() = %#v, want %#v", got, want)
	}
}

func TestExtractVoiceoverPathsCharacterizationAcceptsStringList(t *testing.T) {
	got := extractVoiceoverPaths(map[string]interface{}{
		"voiceover_paths": []string{" a.wav ", "b.wav", "a.wav"},
	})
	want := []string{"a.wav", "b.wav"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractVoiceoverPaths() = %#v, want %#v", got, want)
	}
}
