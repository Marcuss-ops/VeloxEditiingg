package inputsecurity

import "testing"

func TestSniffMIMERecognizesMagicBytes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"png", []byte("\x89PNG\r\n\x1a\n0000"), "image/png"},
		{"jpeg", []byte{0xff, 0xd8, 0xff, 0x00}, "image/jpeg"},
		{"gif87a", []byte("GIF87a"), "image/gif"},
		{"gif89a", []byte("GIF89a"), "image/gif"},
		{"webp", []byte("RIFF....WEBP"), "image/webp"},
		{"mp4", []byte("\x00\x00\x00\x00ftypisom"), "video/mp4"},
		{"mp3 id3", []byte("ID3\x04\x00"), "audio/mpeg"},
		{"mp3 bare frame", []byte{0xff, 0xfb, 0x90, 0x64}, "audio/mpeg"},
		{"wav", []byte("RIFF....WAVE"), "audio/wav"},
		{"ogg", []byte("OggS"), "audio/ogg"},
		{"flac", []byte("fLaC"), "audio/flac"},
		{"woff", []byte("wOFF"), "font/woff"},
		{"woff2", []byte("wOF2"), "font/woff"},
		{"ttf otto", []byte("OTTO"), "font/ttf"},
		{"ttf binary", []byte{0x00, 0x01, 0x00, 0x00}, "font/ttf"},
		{"json", []byte(`{"a":1}`), "application/json"},
		{"plain text", []byte("hello world"), "text/plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sniffMIME(tt.data, "input.bin"); got != tt.want {
				t.Fatalf("sniffMIME(%q) = %q, want %q", tt.data, got, tt.want)
			}
		})
	}
}

// TestSniffMIMETablePreservesOrder locks the precedence of the flattened
// table: a RIFF container with WEBP at offset 8 must resolve to image/webp
// before the WAV sniffer, and a RIFF+WAVE container must still resolve to
// audio/wav.
func TestSniffMIMETablePreservesOrder(t *testing.T) {
	webp := []byte("RIFF\x24\x00\x00\x00WEBP")
	if got := sniffMIME(webp, "x.webp"); got != "image/webp" {
		t.Fatalf("webp = %q, want image/webp", got)
	}
	wav := []byte("RIFF\x24\x00\x00\x00WAVE")
	if got := sniffMIME(wav, "x.wav"); got != "audio/wav" {
		t.Fatalf("wav = %q, want audio/wav", got)
	}
}
