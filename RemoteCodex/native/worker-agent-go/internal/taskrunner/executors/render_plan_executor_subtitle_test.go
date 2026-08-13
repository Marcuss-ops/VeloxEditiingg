package executors

import (
	"strings"
	"testing"
)

// render_plan_executor_subtitle_test.go pins the hand-rolled ASS writers
// (writeASSTime / writeASSText) that replaced the per-event fmt.Sprintf
// allocations. The output format is an external contract (the `.ass`
// subtitle container the worker ships), so a silent drift in timestamp
// layout or newline escaping would corrupt every subtitle event.

func TestWriteASSTime(t *testing.T) {
	cases := []struct {
		name    string
		seconds float64
		want    string
	}{
		{name: "zero", seconds: 0, want: "0:00:00.00"},
		{name: "centiseconds only", seconds: 0.5, want: "0:00:00.50"},
		{name: "sub-minute", seconds: 59.99, want: "0:00:59.99"},
		{name: "minute boundary", seconds: 60.0, want: "0:01:00.00"},
		{name: "minutes and seconds", seconds: 65.5, want: "0:01:05.50"},
		{name: "hour boundary", seconds: 3600.0, want: "1:00:00.00"},
		{name: "hour with minutes", seconds: 3661.25, want: "1:01:01.25"},
		{name: "multi-hour no padding", seconds: 2*3600 + 5, want: "2:00:05.00"},
		{name: "round half up", seconds: 0.005, want: "0:00:00.01"},
		{name: "negative clamps to zero", seconds: -5, want: "0:00:00.00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			writeASSTime(&b, tc.seconds)
			if got := b.String(); got != tc.want {
				t.Fatalf("writeASSTime(%v) = %q, want %q", tc.seconds, got, tc.want)
			}
		})
	}
}

func TestWriteASSText(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{name: "plain text unchanged", text: "hello world", want: "hello world"},
		{name: "empty unchanged", text: "", want: ""},
		{name: "newline escaped", text: "line one\nline two", want: `line one\Nline two`},
		{name: "multiple newlines", text: "a\nb\nc", want: `a\Nb\Nc`},
		{name: "no newline skips replace", text: "already\\Nhere", want: "already\\Nhere"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			writeASSText(&b, tc.text)
			if got := b.String(); got != tc.want {
				t.Fatalf("writeASSText(%q) = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}

// TestWriteASSFullDialogueLine assembles one full Dialogue line exactly as
// the subtitle executor does, pinning the column layout end to end.
func TestWriteASSFullDialogueLine(t *testing.T) {
	var b strings.Builder
	b.WriteString("Dialogue: 0,")
	writeASSTime(&b, 61.5)
	b.WriteByte(',')
	writeASSTime(&b, 65.0)
	b.WriteString(",Default,")
	writeASSText(&b, "hello\nworld")
	b.WriteByte('\n')

	want := "Dialogue: 0,0:01:01.50,0:01:05.00,Default,hello\\Nworld\n"
	if got := b.String(); got != want {
		t.Fatalf("full dialogue line = %q, want %q", got, want)
	}
}
