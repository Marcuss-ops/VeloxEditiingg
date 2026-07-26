package performance

import "testing"

func TestPlanShardsAvoidsTinyTasks(t *testing.T) {
	p := PlanShards(ShardRequest{TotalWorkMs: 100, TemporalClass: FrameLocal, WorkersAvailable: 8, OverheadMsPerShard: 80})
	if p.ShardCount != 1 {
		t.Fatalf("plan=%+v", p)
	}
}

func TestPlanShardsUsesParallelWorkersWhenUseful(t *testing.T) {
	p := PlanShards(ShardRequest{TotalWorkMs: 10000, TemporalClass: FrameLocal, WorkersAvailable: 4, OverheadMsPerShard: 50})
	if p.ShardCount != 4 || p.ParallelEfficiency < .75 {
		t.Fatalf("plan=%+v", p)
	}
}

func TestPlanShardsKeepsGlobalWorkSerial(t *testing.T) {
	p := PlanShards(ShardRequest{TotalWorkMs: 10000, TemporalClass: Global, WorkersAvailable: 8})
	if p.ShardCount != 1 || p.Reason != "temporal_dependency" {
		t.Fatalf("plan=%+v", p)
	}
}
