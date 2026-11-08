package assets

import "testing"

const testSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestParsePreparedWindowReference(t *testing.T) {
	tests := []struct {
		name, ref       string
		start, duration int64
	}{
		{"stock", "velox-stock-window://driveFileId/" + testSHA256 + "/0/5000000/VELOX_ASSEMBLY_READY_V1", 0, 5_000_000},
		{"prepared", "velox-video-window://driveFileId/" + testSHA256 + "/4166667/833333/VELOX_ASSEMBLY_READY_V1/revision-1", 4_166_667, 833_333},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePreparedWindowReference(tt.ref)
			if err != nil {
				t.Fatal(err)
			}
			if got.sourceID != "driveFileId" || got.startUS != tt.start || got.durationUS != tt.duration || got.profile != "VELOX_ASSEMBLY_READY_V1" {
				t.Fatalf("unexpected parsed reference: %+v", got)
			}
		})
	}
}

func TestParsePreparedWindowReferenceRejectsInvalidRanges(t *testing.T) {
	for _, ref := range []string{
		"https://example.com/video.mp4",
		"velox-stock-window://driveFileId/" + testSHA256 + "/-1/5000000/VELOX_ASSEMBLY_READY_V1",
		"velox-stock-window://driveFileId/" + testSHA256 + "/0/0/VELOX_ASSEMBLY_READY_V1",
		"velox-stock-window://driveFileId/not-a-hash/0/5000000/VELOX_ASSEMBLY_READY_V1",
	} {
		if _, err := parsePreparedWindowReference(ref); err == nil {
			t.Errorf("expected invalid reference %q to fail", ref)
		}
	}
}
