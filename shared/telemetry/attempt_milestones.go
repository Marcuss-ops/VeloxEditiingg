package telemetry

type AttemptMilestone string

const (
	MilestoneAttemptAccepted                    AttemptMilestone = "attempt.accepted"
	MilestoneExecutionStarted                   AttemptMilestone = "execution.started"
	MilestoneAssetsRequested                    AttemptMilestone = "assets.requested"
	MilestoneFirstAssetStarted                  AttemptMilestone = "assets.first_started"
	MilestoneAllAssetsReady                     AttemptMilestone = "assets.all_ready"
	MilestonePlanStarted                        AttemptMilestone = "plan.started"
	MilestonePlanCompleted                      AttemptMilestone = "plan.completed"
	MilestoneRenderStarted                      AttemptMilestone = "render.started"
	MilestoneRenderCompleted                    AttemptMilestone = "render.completed"
	MilestoneFinalizeStarted                    AttemptMilestone = "finalize.started"
	MilestoneFinalizeCompleted                  AttemptMilestone = "finalize.completed"
	MilestoneOutputDurable                      AttemptMilestone = "output.durable"
	MilestonePublishQueued                      AttemptMilestone = "publish.queued"
	MilestonePublishStarted                     AttemptMilestone = "publish.started"
	MilestonePublishCompleted                   AttemptMilestone = "publish.completed"
	MilestonePublishSlotWaitStarted             AttemptMilestone = "publish.slot_wait.started"
	MilestonePublishSlotWaitCompleted           AttemptMilestone = "publish.slot_wait.completed"
	MilestonePublishDeclareStarted              AttemptMilestone = "publish.declare.started"
	MilestonePublishDeclareCompleted            AttemptMilestone = "publish.declare.completed"
	MilestonePublishUploadStarted               AttemptMilestone = "publish.upload.started"
	MilestonePublishUploadCompleted             AttemptMilestone = "publish.upload.completed"
	MilestonePublishRemoteFinalizeStarted       AttemptMilestone = "publish.remote_finalize.started"
	MilestonePublishRemoteFinalizeCompleted     AttemptMilestone = "publish.remote_finalize.completed"
	MilestonePublishCommitWaitStarted           AttemptMilestone = "publish.commit_wait.started"
	MilestonePublishCommitWaitCompleted         AttemptMilestone = "publish.commit_wait.completed"
	MilestonePublishSpoolCommitStarted          AttemptMilestone = "publish.spool_commit.started"
	MilestonePublishSpoolCommitCompleted        AttemptMilestone = "publish.spool_commit.completed"
	MilestoneResultSending                      AttemptMilestone = "result.sending"
	MilestoneResultSent                         AttemptMilestone = "result.sent"
	MilestoneAttemptCompleted                   AttemptMilestone = "attempt.completed"
	MilestoneRemoteMaterializationWaitStarted   AttemptMilestone = "assets.remote_wait.started"
	MilestoneRemoteMaterializationWaitCompleted AttemptMilestone = "assets.remote_wait.completed"
)

var canonicalAttemptMilestones = map[AttemptMilestone]struct{}{
	MilestoneAttemptAccepted: {}, MilestoneExecutionStarted: {}, MilestoneAssetsRequested: {},
	MilestoneFirstAssetStarted: {}, MilestoneAllAssetsReady: {}, MilestonePlanStarted: {},
	MilestonePlanCompleted: {}, MilestoneRenderStarted: {}, MilestoneRenderCompleted: {},
	MilestoneFinalizeStarted: {}, MilestoneFinalizeCompleted: {}, MilestoneOutputDurable: {},
	MilestonePublishQueued: {}, MilestonePublishStarted: {}, MilestonePublishCompleted: {},
	MilestonePublishSlotWaitStarted: {}, MilestonePublishSlotWaitCompleted: {},
	MilestonePublishDeclareStarted: {}, MilestonePublishDeclareCompleted: {},
	MilestonePublishUploadStarted: {}, MilestonePublishUploadCompleted: {},
	MilestonePublishRemoteFinalizeStarted: {}, MilestonePublishRemoteFinalizeCompleted: {},
	MilestonePublishCommitWaitStarted: {}, MilestonePublishCommitWaitCompleted: {},
	MilestonePublishSpoolCommitStarted: {}, MilestonePublishSpoolCommitCompleted: {},
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
		MilestonePublishQueued, MilestonePublishStarted,
		MilestonePublishSlotWaitStarted, MilestonePublishSlotWaitCompleted,
		MilestonePublishDeclareStarted, MilestonePublishDeclareCompleted,
		MilestonePublishUploadStarted, MilestonePublishUploadCompleted,
		MilestonePublishRemoteFinalizeStarted, MilestonePublishRemoteFinalizeCompleted,
		MilestonePublishCommitWaitStarted, MilestonePublishCommitWaitCompleted,
		MilestonePublishSpoolCommitStarted, MilestonePublishSpoolCommitCompleted,
		MilestonePublishCompleted,
		MilestoneResultSending, MilestoneResultSent, MilestoneAttemptCompleted,
	}
}

// AttemptMilestoneSample is the canonical milestone record. The worker
// emits Name/Sequence/ElapsedMS/OccurredAt; the Master enriches the same
// record with MasterReceivedAt/MasterCommittedAt when it folds the heartbeat.
type AttemptMilestoneSample struct {
	Name              AttemptMilestone `json:"name"`
	Sequence          uint64           `json:"sequence"`
	ElapsedMS         int64            `json:"elapsed_ms"`
	OccurredAt        string           `json:"occurred_at"`
	MasterReceivedAt  string           `json:"master_received_at,omitempty"`
	MasterCommittedAt string           `json:"master_committed_at,omitempty"`
}
