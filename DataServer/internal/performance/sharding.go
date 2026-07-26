package performance

// TemporalClass describes how safely a workload can be split.
type TemporalClass string

const (
	FrameLocal TemporalClass = "frame_local"
	Windowed   TemporalClass = "windowed"
	Stateful   TemporalClass = "stateful"
	Global     TemporalClass = "global"
)

type ShardRequest struct {
	TotalWorkMs        float64
	TemporalClass      TemporalClass
	WorkersAvailable   int
	OverheadMsPerShard float64
	MinimumEfficiency  float64
	MaxShards          int
}

type ShardPlan struct {
	ShardCount         int
	WorkMsPerShard     float64
	EstimatedOverhead  float64
	ParallelEfficiency float64
	Reason             string
}

// PlanShards returns one shard for global/stateful work and increases shard
// count only while useful compute remains larger than orchestration overhead.
func PlanShards(req ShardRequest) ShardPlan {
	if req.TotalWorkMs <= 0 {
		return ShardPlan{ShardCount: 1, Reason: "no_work"}
	}
	if req.WorkersAvailable < 1 {
		req.WorkersAvailable = 1
	}
	if req.MaxShards < 1 || req.MaxShards > req.WorkersAvailable {
		req.MaxShards = req.WorkersAvailable
	}
	if req.MinimumEfficiency <= 0 || req.MinimumEfficiency >= 1 {
		req.MinimumEfficiency = .75
	}
	if req.TemporalClass == Global || req.TemporalClass == Stateful {
		return ShardPlan{ShardCount: 1, WorkMsPerShard: req.TotalWorkMs, Reason: "temporal_dependency"}
	}
	best := ShardPlan{ShardCount: 1, WorkMsPerShard: req.TotalWorkMs, ParallelEfficiency: 1, Reason: "single_shard_overhead"}
	for shards := 2; shards <= req.MaxShards; shards++ {
		work := req.TotalWorkMs / float64(shards)
		overhead := req.OverheadMsPerShard * float64(shards)
		efficiency := req.TotalWorkMs / (req.TotalWorkMs + overhead)
		if efficiency < req.MinimumEfficiency || work <= req.OverheadMsPerShard {
			break
		}
		best = ShardPlan{ShardCount: shards, WorkMsPerShard: work, EstimatedOverhead: overhead, ParallelEfficiency: efficiency, Reason: "parallelism_pays_overhead"}
	}
	return best
}
