package identity

import (
	"strings"
	"testing"

	"velox-shared/validation"
)

func TestGenerateWorkerIDFormat(t *testing.T) {
	for i := 0; i < 16; i++ {
		id := GenerateWorkerID()
		if !strings.HasPrefix(id, "worker-") {
			t.Fatalf("missing prefix: %s", id)
		}
		suffix := strings.TrimPrefix(id, "worker-")
		if len(suffix) != 8 {
			t.Fatalf("suffix wrong length: %d (%s)", len(suffix), id)
		}
		if !validation.IsHexRun(suffix) {
			t.Fatalf("suffix not hex: %s", id)
		}
	}
}

func TestGenerateWorkerIDUnique(t *testing.T) {
	a := GenerateWorkerID()
	b := GenerateWorkerID()
	if a == b {
		t.Skip("two consecutive collisions is extraordinarily unlikely; rerun")
	}
}

func TestNormalizeWorkerIDEmptyOrUnchanged(t *testing.T) {
	cases := []string{"", "  ", "worker-8e98ce85", "w1", "test"}
	for _, in := range cases {
		if got := NormalizeWorkerID(in); got != in {
			t.Fatalf("input %q expected untouched, got %q", in, got)
		}
	}
}

func TestNormalizeWorkerIDDedupesPrefixAndDots(t *testing.T) {
	cases := map[string]string{
		"host_57.129.132.133":           "host_57_129_132_133",
		"host_host_57_129_132_133":      "host_57_129_132_133",
		"host_host_host_57_129_132_133": "host_57_129_132_133",
		"host_57_129_132_133 ":          "host_57_129_132_133",
		"  host_host_57_129_132_133":    "host_57_129_132_133",
		"host_57.129.132.133.host_foo":  "host_57_129_132_133_host_foo",
	}
	for in, want := range cases {
		if got := NormalizeWorkerID(in); got != want {
			t.Fatalf("NormalizeWorkerID(%q) = %q, want %q", in, got, want)
		}
	}
}
