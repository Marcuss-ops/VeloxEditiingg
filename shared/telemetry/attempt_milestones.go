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

type AttemptMilestoneSample struct {
	Name       AttemptMilestone `json:"name"`
	Sequence   uint64           `json:"sequence"`
	ElapsedMS  int64            `json:"elapsed_ms"`
	OccurredAt string           `json:"occurred_at"`
}
