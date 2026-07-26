package performance

import "sort"

// PlacementCandidate is a worker-specific estimate after compatibility and
// admission have been checked by the Master.
type PlacementCandidate struct {
	WorkerID      string
	Estimate      Estimate
	FailureRiskMs float64
	Rejected      string
}

// PlacementDecision is deliberately explainable: the selected worker and
// every rejected/competing worker carry their reason or predicted finish.
type PlacementDecision struct {
	SelectedWorker string
	Selected       Estimate
	Candidates     []PlacementCandidate
}

// ChooseWorker selects the minimum predicted finish time with uncertainty and
// failure penalties. Rejected candidates never win and remain visible to
// operators.
func ChooseWorker(candidates []PlacementCandidate) PlacementDecision {
	decision := PlacementDecision{Candidates: append([]PlacementCandidate(nil), candidates...)}
	sort.SliceStable(decision.Candidates, func(i, j int) bool {
		return placementScore(decision.Candidates[i]) < placementScore(decision.Candidates[j])
	})
	for _, candidate := range decision.Candidates {
		if candidate.Rejected != "" {
			continue
		}
		decision.SelectedWorker = candidate.WorkerID
		decision.Selected = candidate.Estimate
		break
	}
	return decision
}

func placementScore(c PlacementCandidate) float64 {
	if c.Rejected != "" {
		return 1e30
	}
	uncertainty := (1 - c.Estimate.Confidence) * c.Estimate.FinishMs
	return c.Estimate.FinishMs + c.FailureRiskMs + uncertainty
}
