package inputsecurity

import "testing"

func TestSniffMIMERecognizesMPEGFrameWithoutID3Tag(t *testing.T) {
	data := []byte{0xff, 0xfb, 0x90, 0x64, 0x00, 0x00, 0x00, 0x00}
	if got := sniffMIME(data, "voiceover.mp3"); got != "audio/mpeg" {
		t.Fatalf("sniffMIME() = %q, want audio/mpeg", got)
	}
}
