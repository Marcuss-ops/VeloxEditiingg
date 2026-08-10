package pipeline

import "testing"

func TestSubmitRequestToRawPayloadExplicitAudioTracksOverrideLegacyAlias(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "audio-precedence-1",
		Spec: map[string]interface{}{
			"voiceover_paths": []interface{}{"legacy.mp3"},
		},
		AudioTracks: []SubmitAudioTrack{{
			SourceURL: "velox-asset://audio/explicit.mp3",
			Role:      "background_music",
		}},
	}

	payload := submitRequestToRawPayload(&req)
	tracks, ok := payload["audio_tracks"].([]interface{})
	if !ok || len(tracks) != 1 {
		t.Fatalf("audio_tracks = %#v, want one explicit track", payload["audio_tracks"])
	}
	track := tracks[0].(map[string]interface{})
	if track["source_url"] != "velox-asset://audio/explicit.mp3" {
		t.Fatalf("explicit track = %#v, legacy alias leaked or explicit track changed", track)
	}
	if track["role"] != "background_music" {
		t.Fatalf("explicit track role = %v, want background_music", track["role"])
	}
}
