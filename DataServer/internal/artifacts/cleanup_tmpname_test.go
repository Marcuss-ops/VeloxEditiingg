// Package artifacts / cleanup_tmpname_test.go
//
// Unit tests for isBlobstoreTempName — the suffix-precise temp-file matcher
// used by walkFinalDir to skip leftover PromoteToCanonical temp files.
//
// Regression guard #1: the original matcher used strings.Contains(name, ".tmp"),
// which silently excluded any legitimate artifact whose name merely CONTAINED
// ".tmp" (e.g. render.tmp-cut.mp4) from the DB-diff set — leaking it from
// rule-2 orphan sweeps forever.
//
// Regression guard #2 (format pin): Go's os.CreateTemp renders its random
// suffix in DECIMAL (variable length 1..10 for uint32 values on Go >= 1.20;
// runtime_rand → strconv.Itoa, empirically verified against go1.26.0). The
// "video.mp4.tmp.1298262028" cases below are REAL CreateTemp outputs captured
// from this toolchain, so a future Go release that changes the suffix format
// fails here loudly instead of silently missing every temp file in FinalDir.
package artifacts

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestIsBlobstoreTempName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		// Real os.CreateTemp outputs (pattern "*.tmp.*", decimal suffix).
		{"artifact.mp4.tmp.1298262028", true},
		{"artifact.mp4.tmp.9324842", true},
		{"artifact.mp4.tmp.1639697517", true},
		{"video.mp4.tmp.0123456789", true},

		// Legitimate artifacts that merely CONTAIN ".tmp" — must NOT be
		// skipped (this is the regression the suffix matcher fixes).
		{"render.tmp-cut.mp4", false},
		{"my.tmpfolder_asset.mp4", false},
		{"artifact.tmp.backup.mp4", false},

		// Not temp-shaped at all.
		{"plain-artifact.mp4", false},
		{"", false},
		{".tmp", false},

		// ".tmp." present but suffix is not a decimal number.
		{"video.mp4.tmp.abcdef", false},
		{"video.mp4.tmp.", false},
		{"video.mp4.tmp.12x34", false},
	}

	for _, tc := range cases {
		if got := isBlobstoreTempName(tc.name); got != tc.want {
			t.Errorf("isBlobstoreTempName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestIsBlobstoreTempName_MatchesRealCreateTempOutput pins the matcher
// against the RUNNING toolchain's actual CreateTemp output rather than a
// hardcoded sample: generate 200 temp files with the exact pattern the
// blobstore uses (filepath.Base(finalPath)+".tmp.*") and assert every one is
// recognized. If a future Go changes the suffix format, this test fails
// before production sweeps misclassify temp files as canonical artifacts.
func TestIsBlobstoreTempName_MatchesRealCreateTempOutput(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 200; i++ {
		f, err := os.CreateTemp(dir, "artifact_01_final.mp4.tmp.*")
		if err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
		name := filepath.Base(f.Name())
		_ = f.Close()
		_ = os.Remove(f.Name())

		if !isBlobstoreTempName(name) {
			t.Fatalf("real CreateTemp output %q not recognized as temp name — Go changed the suffix format?", name)
		}
		// Sanity: the suffix really is decimal, as the doc comment claims.
		idx := strings.LastIndex(name, ".tmp.")
		if _, perr := strconv.ParseUint(name[idx+len(".tmp."):], 10, 64); perr != nil {
			t.Fatalf("CreateTemp suffix %q is not decimal: %v", name, perr)
		}
	}
}
