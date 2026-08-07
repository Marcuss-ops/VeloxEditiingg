package pipeline

import (
	"strings"

	"velox-server/internal/store"
)

// selectPrimaryReadyArtifact returns the deterministic public output for a
// completed job. Only READY artifacts are eligible; transient or quarantined
// rows must never leak through the polling projection.
func selectPrimaryReadyArtifact(artifacts []store.Artifact) *store.Artifact {
	var best *store.Artifact
	bestRank := 99
	for i := range artifacts {
		a := &artifacts[i]
		if a.Status != "READY" {
			continue
		}
		rank := 3
		switch {
		case a.Type == "final_video":
			rank = 0
		case strings.HasPrefix(a.Type, "video/") || a.Type == "video":
			rank = 1
		}
		if best == nil || rank < bestRank || (rank == bestRank && (a.CreatedAt > best.CreatedAt || (a.CreatedAt == best.CreatedAt && a.ID < best.ID))) {
			best = a
			bestRank = rank
		}
	}
	return best
}
