package pipeline

import (
	"context"

	"velox-server/internal/jobs/enqueue"
)

type noopPlanResolver struct{}

func (noopPlanResolver) ResolvePlan(_ context.Context, _, _ string) (*enqueue.ResolvedPlan, error) {
	return &enqueue.ResolvedPlan{
		JobID: "test-job",
		Destinations: []enqueue.PlanDestination{
			{DestinationID: "destination-main", Priority: 0, RetryBudget: 5},
		},
	}, nil
}
