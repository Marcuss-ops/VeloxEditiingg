package inputsecurity

import "testing"

func TestMIMECompatibleForKindAcceptsAudioMP4Container(t *testing.T) {
	tests := []struct {
		name     string
		kind     Kind
		declared string
		detected string
		want     bool
	}{
		{"audio mp4", KindAudio, "audio/mp4", "video/mp4", true},
		{"voiceover m4a", KindVoiceover, "audio/m4a", "video/mp4", true},
		{"voiceover x m4a", KindVoiceover, "audio/x-m4a", "video/mp4", true},
		{"clip rejects audio declaration", KindClip, "audio/mp4", "video/mp4", false},
		{"audio rejects unrelated declaration", KindAudio, "audio/ogg", "video/mp4", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mimeCompatibleForKind(tt.kind, tt.declared, tt.detected); got != tt.want {
				t.Fatalf("mimeCompatibleForKind(%v, %q, %q) = %v, want %v", tt.kind, tt.declared, tt.detected, got, tt.want)
			}
		})
	}
}

func TestSniffMIMERecognizesMPEGFrameWithoutID3Tag(t *testing.T) {
	data := []byte{0xff, 0xfb, 0x90, 0x64, 0x00, 0x00, 0x00, 0x00}
	if got := sniffMIME(data, "voiceover.mp3"); got != "audio/mpeg" {
		t.Fatalf("sniffMIME() = %q, want audio/mpeg", got)
	}
}
