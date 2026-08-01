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

func TestHasAudioTracksCharacterization(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]interface{}
		want    bool
	}{
		{
			name: "interface slice with source alias",
			payload: map[string]interface{}{
				"audio_tracks": []interface{}{
					map[string]interface{}{"source": "music.wav"},
				},
			},
			want: true,
		},
		{
			name: "typed slice with url alias",
			payload: map[string]interface{}{
				"audio_tracks": []map[string]interface{}{
					{"url": "https://cdn.example/music.mp3"},
				},
			},
			want: true,
		},
		{
			name: "empty source is not a track",
			payload: map[string]interface{}{
				"audio_tracks": []interface{}{
					map[string]interface{}{"role": "music"},
				},
			},
			want: false,
		},
		{
			name:    "missing tracks",
			payload: map[string]interface{}{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasAudioTracks(tt.payload); got != tt.want {
				t.Fatalf("hasAudioTracks() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeAudioAndSubtitleTracksHaveSameShapeBehavior(t *testing.T) {
	tests := []struct {
		name string
		raw  interface{}
	}{
		{
			name: "interface slice",
			raw: []interface{}{
				map[string]interface{}{"source_url": "music.wav"},
				map[string]interface{}{},
				"not a track",
			},
		},
		{
			name: "typed map slice",
			raw: []map[string]interface{}{
				{"source_url": "voice.wav"},
				{},
			},
		},
		{
			name: "unsupported shape",
			raw:  "not a list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			audio := normalizeAudioTracks(tt.raw)
			subtitles := normalizeSubtitleTracks(tt.raw)
			if !reflect.DeepEqual(audio, subtitles) {
				t.Fatalf("audio tracks = %#v, subtitle tracks = %#v", audio, subtitles)
			}
		})
	}
}
