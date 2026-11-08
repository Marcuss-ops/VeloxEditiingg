package contract

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFMP4Evidence writes a benchmark artifact and points the gate at it.
func writeFMP4Evidence(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(FMP4GateEvidenceEnv, path)
	return path
}

func validFMP4Evidence() string {
	return `{"version":2,"jobs":100,"certified_jobs":100,"jobs_with_progressive_overlap":100,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250,"recorded_at":"2026-11-08T00:00:00Z"}`
}

func TestFMP4GateFailsClosed(t *testing.T) {
	t.Run("unset evidence path", func(t *testing.T) {
		t.Setenv(FMP4GateEvidenceEnv, "")
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened without an evidence path")
		}
	})

	t.Run("missing evidence file", func(t *testing.T) {
		t.Setenv(FMP4GateEvidenceEnv, filepath.Join(t.TempDir(), "absent.json"))
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with a missing evidence file")
		}
	})

	t.Run("empty evidence file", func(t *testing.T) {
		writeFMP4Evidence(t, "empty.json", "")
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with an empty evidence file")
		}
	})

	t.Run("malformed evidence", func(t *testing.T) {
		writeFMP4Evidence(t, "malformed.json", "{not json")
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with malformed evidence")
		}
	})

	t.Run("wrong schema version", func(t *testing.T) {
		writeFMP4Evidence(t, "v1.json", `{"version":1,"jobs":100,"certified_jobs":100,"jobs_with_progressive_overlap":100,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250,"recorded_at":"2026-11-08T00:00:00Z"}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with an unknown evidence schema")
		}
	})

	t.Run("under-sampled run", func(t *testing.T) {
		writeFMP4Evidence(t, "99.json", `{"version":2,"jobs":99,"certified_jobs":99,"jobs_with_progressive_overlap":99,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250,"recorded_at":"2026-11-08T00:00:00Z"}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with fewer than the required jobs")
		}
	})

	t.Run("uncertified jobs", func(t *testing.T) {
		writeFMP4Evidence(t, "uncertified.json", `{"version":2,"jobs":100,"certified_jobs":99,"jobs_with_progressive_overlap":100,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250,"recorded_at":"2026-11-08T00:00:00Z"}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with uncertified jobs")
		}
	})

	t.Run("certified exceeds jobs", func(t *testing.T) {
		writeFMP4Evidence(t, "impossible.json", `{"version":2,"jobs":100,"certified_jobs":101,"jobs_with_progressive_overlap":100,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250,"recorded_at":"2026-11-08T00:00:00Z"}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with certified_jobs > jobs")
		}
	})

	t.Run("missing recorded_at", func(t *testing.T) {
		writeFMP4Evidence(t, "undated.json", `{"version":2,"jobs":100,"certified_jobs":100,"jobs_with_progressive_overlap":100,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened without a recorded_at timestamp")
		}
	})

	t.Run("no progressive overlap", func(t *testing.T) {
		writeFMP4Evidence(t, "no-overlap.json", `{"version":2,"jobs":100,"certified_jobs":100,"jobs_with_progressive_overlap":99,"progressive_overlap_bytes_before_render_end":0,"p95_progressive_overlap_ms":0,"recorded_at":"2026-11-08T00:00:00Z"}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened without measured progressive overlap")
		}
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		writeFMP4Evidence(t, "bad-time.json", `{"version":2,"jobs":100,"certified_jobs":100,"jobs_with_progressive_overlap":100,"progressive_overlap_bytes_before_render_end":1024,"p95_progressive_overlap_ms":250,"recorded_at":"yesterday"}`)
		if FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate opened with an invalid recorded_at timestamp")
		}
	})

	t.Run("certified 100-job run", func(t *testing.T) {
		writeFMP4Evidence(t, "certified.json", validFMP4Evidence())
		if !FMP4StreamProfileGateSatisfied() {
			t.Fatal("gate did not open for a certified 100-job run")
		}
	})
}

func TestFMP4ProfileOptInNeverBypassesTheGate(t *testing.T) {
	writeFMP4Evidence(t, "certified.json", validFMP4Evidence())

	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	if !FMP4StreamProfileEnabled() {
		t.Fatal("opt-in with certified evidence should enable the profile")
	}

	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "0")
	if FMP4StreamProfileEnabled() {
		t.Fatal("explicit opt-out must keep the profile disabled")
	}

	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	t.Setenv(FMP4GateEvidenceEnv, filepath.Join(t.TempDir(), "absent.json"))
	if FMP4StreamProfileEnabled() {
		t.Fatal("opt-in without evidence must not bypass the gate")
	}

	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "0")
	writeFMP4Evidence(t, "certified-2.json", validFMP4Evidence())
	if FMP4StreamProfileEnabled() {
		t.Fatal("certified evidence must not enable a profile the operator left off")
	}
}

func TestFMP4AdmissionFailsClosedUntilTheGateOpens(t *testing.T) {
	t.Setenv(FMP4GateEvidenceEnv, "")
	if _, err := KnownCanonicalVideoProfileV1(CanonicalVideoProfileFMP4StreamV1); err == nil {
		t.Fatal("fMP4 profile was admitted while the gate was closed")
	}

	writeFMP4Evidence(t, "certified.json", validFMP4Evidence())
	if _, err := KnownCanonicalVideoProfileV1(CanonicalVideoProfileFMP4StreamV1); err == nil {
		t.Fatal("fMP4 profile was admitted without the operator opt-in")
	}

	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	profile, err := KnownCanonicalVideoProfileV1(CanonicalVideoProfileFMP4StreamV1)
	if err != nil {
		t.Fatalf("fMP4 profile was not admitted after gate + opt-in: %v", err)
	}
	if profile.ContainerLayout != ContainerLayoutFragmented {
		t.Fatalf("container layout = %q, want %q", profile.ContainerLayout, ContainerLayoutFragmented)
	}
}
