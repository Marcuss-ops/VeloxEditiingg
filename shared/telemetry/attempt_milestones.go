package telemetry

type AttemptMilestone string

const (
	MilestoneAttemptAccepted              AttemptMilestone = "attempt.accepted"
	MilestoneExecutionStarted             AttemptMilestone = "execution.started"
	MilestoneAssetsRequested              AttemptMilestone = "assets.requested"
	MilestoneFirstAssetStarted            AttemptMilestone = "assets.first_started"
	MilestoneAllAssetsReady               AttemptMilestone = "assets.all_ready"
	MilestonePlanStarted                  AttemptMilestone = "plan.started"
	MilestonePlanCompleted                AttemptMilestone = "plan.completed"
	MilestoneRenderStarted                AttemptMilestone = "render.started"
	MilestoneRenderCompleted              AttemptMilestone = "render.completed"
	MilestoneFinalizeStarted              AttemptMilestone = "finalize.started"
	MilestoneFinalizeCompleted            AttemptMilestone = "finalize.completed"
	MilestoneOutputDurable                AttemptMilestone = "output.durable"
	MilestonePublishQueued                AttemptMilestone = "publish.queued"
	MilestonePublishStarted               AttemptMilestone = "publish.started"
	MilestonePublishCompleted             AttemptMilestone = "publish.completed"
	MilestoneResultSending                AttemptMilestone = "result.sending"
	MilestoneResultSent                   AttemptMilestone = "result.sent"
	MilestoneAttemptCompleted             AttemptMilestone = "attempt.completed"
	MilestoneRemoteMaterializationWaitStarted   AttemptMilestone = "assets.remote_wait.started"
	MilestoneRemoteMaterializationWaitCompleted AttemptMilestone = "assets.remote_wait.completed"
)

var canonicalAttemptMilestones = map[AttemptMilestone]struct{}{
	MilestoneAttemptAccepted: {}, MilestoneExecutionStarted: {}, MilestoneAssetsRequested: {},
	MilestoneFirstAssetStarted: {}, MilestoneAllAssetsReady: {}, MilestonePlanStarted: {},
	MilestonePlanCompleted: {}, MilestoneRenderStarted: {}, MilestoneRenderCompleted: {},
	MilestoneFinalizeStarted: {}, MilestoneFinalizeCompleted: {}, MilestoneOutputDurable: {},
	MilestonePublishQueued: {}, MilestonePublishStarted: {}, MilestonePublishCompleted: {},
	MilestoneResultSending: {}, MilestoneResultSent: {}, MilestoneAttemptCompleted: {},
	MilestoneRemoteMaterializationWaitStarted: {}, MilestoneRemoteMaterializationWaitCompleted: {},
}

func IsCanonicalAttemptMilestone(name AttemptMilestone) bool {
	_, ok := canonicalAttemptMilestones[name]
	return ok
}

func CanonicalAttemptMilestones() []AttemptMilestone {
	return []AttemptMilestone{
		MilestoneAttemptAccepted, MilestoneExecutionStarted, MilestoneAssetsRequested,
		MilestoneFirstAssetStarted, MilestoneAllAssetsReady, MilestonePlanStarted,
		MilestonePlanCompleted, MilestoneRenderStarted, MilestoneRenderCompleted,
		MilestoneFinalizeStarted, MilestoneFinalizeCompleted, MilestoneOutputDurable,
		MilestonePublishQueued, MilestonePublishStarted, MilestonePublishCompleted,
		MilestoneResultSending, MilestoneResultSent, MilestoneAttemptCompleted,
	}
}

// AttemptMilestoneSample is the canonical milestone record. The worker
// emits Name/Sequence/ElapsedMS/OccurredAt; the Master enriches the same
// record with MasterReceivedAt/MasterCommittedAt when it folds the
// heartbeat into the live projection (and the durable report carries
// report-level received/persisted timestamps). The two clocks are NEVER
// subtracted from each other: compare deltas (master received deltas vs
// worker elapsed_ms deltas) to separate transport/heartbeat delay from
// real worker runtime.
type AttemptMilestoneSample struct {
	Name       AttemptMilestone `json:"name"`
	Sequence   uint64           `json:"sequence"`
	ElapsedMS  int64            `json:"elapsed_ms"`
	OccurredAt string           `json:"occurred_at"`
	// MasterReceivedAt is the Master-local receive timestamp (RFC3339Nano)
	// stamped when the heartbeat carrying this milestone was folded into
	// worker_task_runtime.canonical_events_json. Populated only on the
	// Master side; the worker never emits it.
	MasterReceivedAt string `json:"master_received_at,omitempty"`
	// MasterCommittedAt is the Master-local write timestamp of the same
	// fold, equal to the heartbeat transaction's commit time.
	MasterCommittedAt string `json:"master_committed_at,omitempty"`
}
