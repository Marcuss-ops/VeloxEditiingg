package publisher

import "testing"

func TestArtifactPublishStateCanComplete(t *testing.T) {
	base := ArtifactPublishState{
		EngineFinalized:  true,
		OutputDurable:    true,
		FinalSHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		FinalSizeBytes:   10,
		AllPartsUploaded: true,
		UploadedParts:    1,
		ExpectedParts:    1,
	}
	if !base.CanComplete() {
		t.Fatal("complete prerequisites should pass")
	}
	cases := []struct {
		name   string
		mutate func(*ArtifactPublishState)
	}{
		{"engine not finalized", func(s *ArtifactPublishState) { s.EngineFinalized = false }},
		{"output not durable", func(s *ArtifactPublishState) { s.OutputDurable = false }},
		{"missing sha", func(s *ArtifactPublishState) { s.FinalSHA256 = "" }},
		{"missing size", func(s *ArtifactPublishState) { s.FinalSizeBytes = 0 }},
		{"parts incomplete", func(s *ArtifactPublishState) { s.AllPartsUploaded = false }},
		{"part count incomplete", func(s *ArtifactPublishState) { s.UploadedParts = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if s.CanComplete() {
				t.Fatal("complete must be rejected")
			}
		})
	}
}
