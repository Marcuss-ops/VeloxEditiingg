package pipelineruns

import (
	"testing"
)

func TestAllStatuses_Count(t *testing.T) {
	all := AllStatuses()
	if len(all) != 17 {
		t.Fatalf("AllStatuses: want 17, got %d", len(all))
	}
}

func TestStatusValid(t *testing.T) {
	for _, s := range AllStatuses() {
		if !s.Valid() {
			t.Errorf("status %q should be Valid", s)
		}
	}
	if Status("BOGUS").Valid() {
		t.Error("BOGUS should not be Valid")
	}
}

func TestStatusTerminal(t *testing.T) {
	terminal := []Status{StatusCompleted, StatusFailed, StatusCancelled}
	for _, s := range terminal {
		if !s.Terminal() {
			t.Errorf("status %q should be Terminal", s)
		}
	}
	nonTerminal := []Status{
		StatusAccepted, StatusRemoteSubmitting, StatusRemoteQueued,
		StatusRemoteRunning, StatusRemoteCompleted, StatusForwarding,
		StatusWorkerQueued, StatusRendering, StatusArtifactProcessing,
		StatusArtifactReady, StatusDeliveryPending, StatusDelivering,
		StatusScheduled, StatusPublished,
	}
	for _, s := range nonTerminal {
		if s.Terminal() {
			t.Errorf("status %q should NOT be Terminal", s)
		}
	}
}

func TestStageOf(t *testing.T) {
	cases := []struct {
		status Status
		stage  Stage
	}{
		{StatusAccepted, StageRemote},
		{StatusRemoteSubmitting, StageRemote},
		{StatusRemoteQueued, StageRemote},
		{StatusRemoteRunning, StageRemote},
		{StatusRemoteCompleted, StageRemote},
		{StatusForwarding, StageForwarding},
		{StatusWorkerQueued, StageWorker},
		{StatusRendering, StageWorker},
		{StatusArtifactProcessing, StageArtifact},
		{StatusArtifactReady, StageArtifact},
		{StatusDeliveryPending, StageDelivery},
		{StatusDelivering, StageDelivery},
		{StatusScheduled, StageDelivery},
		{StatusPublished, StageDelivery},
		{StatusCompleted, StageTerminal},
		{StatusFailed, StageTerminal},
		{StatusCancelled, StageTerminal},
	}
	for _, tc := range cases {
		if got := tc.status.StageOf(); got != tc.stage {
			t.Errorf("StageOf(%q) = %q, want %q", tc.status, got, tc.stage)
		}
	}
}

func TestDeriveStatus_EmptyState(t *testing.T) {
	if got := DeriveStatus(InternalState{}); got != StatusAccepted {
		t.Errorf("DeriveStatus(empty) = %q, want ACCEPTED", got)
	}
}

func TestDeriveStatus_RemotePhase(t *testing.T) {
	cases := []struct {
		name   string
		remote string
		want   Status
	}{
		{"queued", "queued", StatusRemoteQueued},
		{"running", "running", StatusRemoteRunning},
		{"completed", "completed", StatusRemoteCompleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveStatus(InternalState{RemoteJobStatus: strPtr(tc.remote)})
			if got != tc.want {
				t.Errorf("DeriveStatus remote=%q = %q, want %q", tc.remote, got, tc.want)
			}
		})
	}
}

func TestDeriveStatus_ForwardingPhase(t *testing.T) {
	cases := []struct {
		name   string
		fwd    string
		want   Status
	}{
		{"PENDING", "PENDING", StatusRemoteQueued},
		{"POLLING", "POLLING", StatusRemoteQueued},
		{"RETRY_WAIT", "RETRY_WAIT", StatusRemoteQueued},
		{"FORWARDING", "FORWARDING", StatusForwarding},
		{"READY_TO_FORWARD", "READY_TO_FORWARD", StatusForwarding},
		{"FORWARDED", "FORWARDED", StatusWorkerQueued},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveStatus(InternalState{ForwardingStatus: strPtr(tc.fwd)})
			if got != tc.want {
				t.Errorf("DeriveStatus fwd=%q = %q, want %q", tc.fwd, got, tc.want)
			}
		})
	}
}

func TestDeriveStatus_JobPhase(t *testing.T) {
	fwd := strPtr("FORWARDED")
	cases := []struct {
		name  string
		job   string
		want  Status
	}{
		{"PENDING", "PENDING", StatusWorkerQueued},
		{"LEASED", "LEASED", StatusRendering},
		{"RUNNING", "RUNNING", StatusRendering},
		{"AWAITING_ARTIFACT", "AWAITING_ARTIFACT", StatusArtifactProcessing},
		{"SUCCEEDED", "SUCCEEDED", StatusArtifactProcessing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveStatus(InternalState{
				ForwardingStatus: fwd,
				JobStatus:        strPtr(tc.job),
			})
			if got != tc.want {
				t.Errorf("DeriveStatus job=%q = %q, want %q", tc.job, got, tc.want)
			}
		})
	}
}

func TestDeriveStatus_ArtifactPhase(t *testing.T) {
	jobSucceeded := strPtr("SUCCEEDED")
	cases := []struct {
		name     string
		artifact string
		want     Status
	}{
		{"STAGING", "STAGING", StatusArtifactProcessing},
		{"READY", "READY", StatusArtifactReady},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveStatus(InternalState{
				JobStatus:      jobSucceeded,
				ArtifactStatus: strPtr(tc.artifact),
			})
			if got != tc.want {
				t.Errorf("DeriveStatus artifact=%q = %q, want %q", tc.artifact, got, tc.want)
			}
		})
	}
}

func TestDeriveStatus_DeliveryPhase(t *testing.T) {
	artReady := strPtr("READY")
	cases := []struct {
		name      string
		delivery  string
		scheduled bool
		allDone   bool
		want      Status
	}{
		{"PENDING", "PENDING", false, false, StatusDeliveryPending},
		{"RUNNING", "RUNNING", false, false, StatusDelivering},
		{"RETRY_WAIT", "RETRY_WAIT", false, false, StatusDelivering},
		{"SUCCEEDED_no_schedule", "SUCCEEDED", false, false, StatusPublished},
		{"SUCCEEDED_scheduled", "SUCCEEDED", true, false, StatusScheduled},
		{"SUCCEEDED_all_done", "SUCCEEDED", false, true, StatusCompleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveStatus(InternalState{
				ArtifactStatus:         artReady,
				DeliveryStatus:         strPtr(tc.delivery),
				HasScheduledDelivery:   tc.scheduled,
				AllDeliveriesSucceeded: tc.allDone,
			})
			if got != tc.want {
				t.Errorf("DeriveStatus delivery=%q sched=%v allDone=%v = %q, want %q",
					tc.delivery, tc.scheduled, tc.allDone, got, tc.want)
			}
		})
	}
}

func TestDeriveStatus_FailurePropagation(t *testing.T) {
	cases := []struct {
		name  string
		state InternalState
	}{
		{"forwarding FAILED", InternalState{ForwardingStatus: strPtr("FAILED")}},
		{"forwarding BLOCKED", InternalState{ForwardingStatus: strPtr("BLOCKED")}},
		{"job FAILED", InternalState{JobStatus: strPtr("FAILED")}},
		{"artifact FAILED", InternalState{ArtifactStatus: strPtr("FAILED")}},
		{"artifact QUARANTINED", InternalState{ArtifactStatus: strPtr("QUARANTINED")}},
		{"delivery FAILED", InternalState{DeliveryStatus: strPtr("FAILED")}},
		{"delivery BLOCKED_AUTH", InternalState{DeliveryStatus: strPtr("BLOCKED_AUTH")}},
		{"remote failed", InternalState{RemoteJobStatus: strPtr("failed")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveStatus(tc.state); got != StatusFailed {
				t.Errorf("DeriveStatus(%+v) = %q, want FAILED", tc.state, got)
			}
		})
	}
}

func TestDeriveStatus_CancelledPropagation(t *testing.T) {
	cases := []struct {
		name  string
		state InternalState
	}{
		{"job CANCELLED", InternalState{JobStatus: strPtr("CANCELLED")}},
		{"delivery CANCELLED", InternalState{DeliveryStatus: strPtr("CANCELLED")}},
		{"remote cancelled", InternalState{RemoteJobStatus: strPtr("cancelled")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveStatus(tc.state); got != StatusCancelled {
				t.Errorf("DeriveStatus(%+v) = %q, want CANCELLED", tc.state, got)
			}
		})
	}
}

func TestDeriveStatus_FailureOverridesEverything(t *testing.T) {
	// Even if delivery is SUCCEEDED, a FAILED job means the run FAILED.
	got := DeriveStatus(InternalState{
		JobStatus:      strPtr("FAILED"),
		DeliveryStatus: strPtr("SUCCEEDED"),
	})
	if got != StatusFailed {
		t.Errorf("DeriveStatus(job=FAILED, delivery=SUCCEEDED) = %q, want FAILED", got)
	}
}

func TestStrPtr(t *testing.T) {
	if strPtr("") != nil {
		t.Error("strPtr(\"\") should return nil")
	}
	s := strPtr("PENDING")
	if s == nil || *s != "PENDING" {
		t.Errorf("strPtr(\"PENDING\") = %v, want ptr to PENDING", s)
	}
}
