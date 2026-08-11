package grpcserver

import (
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func lifecycleTask() taskgraph.Task {
	return taskgraph.Task{ID: "task-identity", JobID: "job-identity", AttemptID: "attempt-identity", AttemptNumber: 2, LeaseID: "lease-identity", WorkerID: "worker-identity", Revision: 7, Status: taskgraph.StatusLeased}
}

func lifecycleHandler(t *testing.T) (*Handler, *spoofStubTaskRepo, *workerSession) {
	t.Helper()
	task := lifecycleTask()
	repo := &spoofStubTaskRepo{nowTask: task}
	h := NewHandler(nil, nil, nil, repo, nil, nil, nil, &HandlerConfig{PushMode: true})
	sess := &workerSession{workerID: task.WorkerID, sendCh: make(chan *outboundMessage, 2)}
	sess.pendingTaskOffer = &taskgraph.TaskWithSpec{Task: task}
	h.sessions["session-identity"] = sess
	h.workerSessions[task.WorkerID] = "session-identity"
	return h, repo, sess
}

func lifecycleAccepted(task taskgraph.Task) *pb.TaskAccepted {
	return &pb.TaskAccepted{TaskId: task.ID, JobId: task.JobID, AttemptId: task.AttemptID, LeaseId: task.LeaseID, AttemptNumber: int32(task.AttemptNumber), Revision: int32(task.Revision)}
}

func lifecycleRejected(task taskgraph.Task) *pb.TaskRejected {
	return &pb.TaskRejected{TaskId: task.ID, JobId: task.JobID, AttemptId: task.AttemptID, LeaseId: task.LeaseID, AttemptNumber: int32(task.AttemptNumber), Revision: int32(task.Revision), Reason: "worker_declined"}
}

func lifecycleRenewal(task taskgraph.Task) *pb.TaskLeaseRenewal {
	return &pb.TaskLeaseRenewal{TaskId: task.ID, JobId: task.JobID, AttemptId: task.AttemptID, LeaseId: task.LeaseID, AttemptNumber: int32(task.AttemptNumber), Revision: int32(task.Revision), RequestedExpiry: timestamppb.New(time.Now().UTC().Add(time.Hour))}
}

func TestValidateTaskIdentityRejectsEveryWireFieldMismatch(t *testing.T) {
	task := lifecycleTask()
	master := taskIdentityFromTask(&task)
	cases := []struct {
		name   string
		mutate func(*taskIdentity)
		field  string
	}{{"task_id", func(v *taskIdentity) { v.taskID = "other-task" }, "task_id"}, {"job_id", func(v *taskIdentity) { v.jobID = "other-job" }, "job_id"}, {"attempt_id", func(v *taskIdentity) { v.attemptID = "other-attempt" }, "attempt_id"}, {"lease_id", func(v *taskIdentity) { v.leaseID = "other-lease" }, "lease_id"}, {"attempt_number", func(v *taskIdentity) { v.attemptNumber++ }, "attempt_number"}, {"revision", func(v *taskIdentity) { v.revision++ }, "revision"}, {"worker_id", func(v *taskIdentity) { v.workerID = "other-worker" }, "worker_id"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := master
			tc.mutate(&wire)
			err := validateTaskIdentity(wire, master)
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("validateTaskIdentity error = %v, want %s mismatch", err, tc.field)
			}
		})
	}
}

func assertLifecycleUnchanged(t *testing.T, repo *spoofStubTaskRepo, sess *workerSession, want taskgraph.Task) {
	t.Helper()
	if repo.nowTask.Status != want.Status || repo.nowTask.Revision != want.Revision || repo.nowTask.WorkerID != want.WorkerID || repo.nowTask.LeaseID != want.LeaseID || repo.nowTask.AttemptID != want.AttemptID || repo.nowTask.AttemptNumber != want.AttemptNumber {
		t.Fatalf("task mutated: got=%+v want status=%s revision=%d worker=%s lease=%s attempt=%s/%d", repo.nowTask, want.Status, want.Revision, want.WorkerID, want.LeaseID, want.AttemptID, want.AttemptNumber)
	}
	if sess.pendingTaskOffer == nil || sess.pendingTaskOffer.ID != want.ID || sess.pendingTaskOffer.LeaseID != want.LeaseID || sess.pendingTaskOffer.AttemptID != want.AttemptID || sess.pendingTaskOffer.Revision != want.Revision {
		t.Fatalf("pending offer mutated: got=%+v want task=%s lease=%s attempt=%s revision=%d", sess.pendingTaskOffer, want.ID, want.LeaseID, want.AttemptID, want.Revision)
	}
}

func TestHandleTaskAcceptedRejectsMismatchBeforeMutation(t *testing.T) {
	fields := []struct {
		name   string
		mutate func(*pb.TaskAccepted)
	}{{"task_id", func(v *pb.TaskAccepted) { v.TaskId = "other-task" }}, {"job_id", func(v *pb.TaskAccepted) { v.JobId = "other-job" }}, {"attempt_id", func(v *pb.TaskAccepted) { v.AttemptId = "other-attempt" }}, {"lease_id", func(v *pb.TaskAccepted) { v.LeaseId = "other-lease" }}, {"attempt_number", func(v *pb.TaskAccepted) { v.AttemptNumber++ }}, {"revision", func(v *pb.TaskAccepted) { v.Revision++ }}}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			h, repo, sess := lifecycleHandler(t)
			want := lifecycleTask()
			msg := lifecycleAccepted(want)
			tc.mutate(msg)
			h.handleTaskAccepted("worker-identity", msg, sess)
			if repo.acceptCalls != 0 {
				t.Fatalf("AcceptTaskAtomic calls = %d, want 0", repo.acceptCalls)
			}
			assertLifecycleUnchanged(t, repo, sess, want)
		})
	}
}

func TestHandleTaskAcceptedGrantUsesPostTransitionRevision(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	task := lifecycleTask()
	h.handleTaskAccepted(task.WorkerID, lifecycleAccepted(task), sess)
	if repo.acceptCalls != 1 {
		t.Fatalf("accept calls = %d, want 1", repo.acceptCalls)
	}
	select {
	case out := <-sess.sendCh:
		grant := out.Envelope.GetTaskLeaseGranted()
		if grant == nil {
			t.Fatalf("message type = %T, want TaskLeaseGranted", out.Envelope.Msg)
		}
		if grant.GetRevision() != int32(repo.nowTask.Revision) {
			t.Fatalf("grant revision = %d, want %d", grant.GetRevision(), repo.nowTask.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for TaskLeaseGranted")
	}
	h.handleTaskRenewal(task.WorkerID, lifecycleRenewal(repo.nowTask), sess)
	if repo.renewCalls != 1 || repo.lastRenewRevision != repo.nowTask.Revision {
		t.Fatalf("renew calls=%d revision=%d, want calls=1 revision=%d", repo.renewCalls, repo.lastRenewRevision, repo.nowTask.Revision)
	}
}

func TestHandleTaskRenewalCancelledParentConvergesTask(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	h.jobsRepo = &spoofStubJobsRepo{getJob: &jobs.Job{ID: "job-identity", Status: jobs.StatusCancelled}}

	h.handleTaskRenewal("worker-identity", lifecycleRenewal(repo.nowTask), sess)

	if repo.transitionCalls != 1 {
		t.Fatalf("TransitionTaskToTerminalAtomic calls = %d, want 1", repo.transitionCalls)
	}
	if repo.renewCalls != 0 {
		t.Fatalf("RenewLease calls = %d, want 0 for cancelled parent", repo.renewCalls)
	}
}

func TestHandleTaskAcceptedReplayReissuesGrantWithoutMutation(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	task := lifecycleTask()
	msg := lifecycleAccepted(task)
	h.handleTaskAccepted(task.WorkerID, msg, sess)
	if repo.acceptCalls != 1 {
		t.Fatalf("first accept calls = %d, want 1", repo.acceptCalls)
	}
	select {
	case <-sess.sendCh:
	case <-time.After(time.Second):
		t.Fatal("timeout consuming initial grant")
	}
	firstRevision := repo.nowTask.Revision
	h.handleTaskAccepted(task.WorkerID, msg, sess)
	if repo.acceptCalls != 1 || repo.nowTask.Revision != firstRevision {
		t.Fatalf("replay mutated: calls=%d revision=%d", repo.acceptCalls, repo.nowTask.Revision)
	}
	select {
	case out := <-sess.sendCh:
		grant := out.Envelope.GetTaskLeaseGranted()
		if grant == nil || int(grant.GetRevision()) != firstRevision {
			t.Fatalf("replay grant = %#v, want revision %d", grant, firstRevision)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for replay grant")
	}
}

func TestHandleTaskAcceptedWrongWorkerIsRejectedBeforeMutation(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	want := lifecycleTask()
	h.handleTaskAccepted("worker-other", lifecycleAccepted(want), sess)
	if repo.acceptCalls != 0 {
		t.Fatalf("wrong-worker accept calls = %d, want 0", repo.acceptCalls)
	}
	assertLifecycleUnchanged(t, repo, sess, want)
}

func TestHandleTaskAcceptedTakeoverIsRejectedBeforeMutation(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	wire := lifecycleTask()
	repo.nowTask.WorkerID = "worker-takeover"
	repo.nowTask.LeaseID = "lease-takeover"
	repo.nowTask.Revision = 8
	wantTask := repo.nowTask
	wantOffer := wire
	h.handleTaskAccepted("worker-identity", lifecycleAccepted(wire), sess)
	if repo.acceptCalls != 0 {
		t.Fatalf("takeover accept calls = %d, want 0", repo.acceptCalls)
	}
	if repo.nowTask.Status != wantTask.Status || repo.nowTask.Revision != wantTask.Revision || repo.nowTask.WorkerID != wantTask.WorkerID || repo.nowTask.LeaseID != wantTask.LeaseID || repo.nowTask.AttemptID != wantTask.AttemptID || repo.nowTask.AttemptNumber != wantTask.AttemptNumber {
		t.Fatalf("takeover task mutated: got=%+v want=%+v", repo.nowTask, wantTask)
	}
	if sess.pendingTaskOffer == nil || sess.pendingTaskOffer.ID != wantOffer.ID || sess.pendingTaskOffer.LeaseID != wantOffer.LeaseID || sess.pendingTaskOffer.AttemptID != wantOffer.AttemptID || sess.pendingTaskOffer.Revision != wantOffer.Revision {
		t.Fatalf("takeover pending offer mutated: got=%+v want=%+v", sess.pendingTaskOffer, wantOffer)
	}
}

func TestHandleTaskAcceptedReplayWithNonAdjacentRevisionIsRejected(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	want := lifecycleTask()
	repo.nowTask.Status = taskgraph.StatusRunning
	repo.nowTask.Revision = want.Revision + 2
	h.handleTaskAccepted(want.WorkerID, lifecycleAccepted(want), sess)
	if repo.acceptCalls != 0 {
		t.Fatalf("non-adjacent replay calls = %d, want 0", repo.acceptCalls)
	}
	select {
	case out := <-sess.sendCh:
		t.Fatalf("non-adjacent replay emitted grant: %T", out.Envelope.Msg)
	default:
	}
}

func TestHandleTaskRejectedRejectsMismatchAndReplay(t *testing.T) {
	fields := []struct {
		name   string
		mutate func(*pb.TaskRejected)
	}{{"task_id", func(v *pb.TaskRejected) { v.TaskId = "other-task" }}, {"job_id", func(v *pb.TaskRejected) { v.JobId = "other-job" }}, {"attempt_id", func(v *pb.TaskRejected) { v.AttemptId = "other-attempt" }}, {"lease_id", func(v *pb.TaskRejected) { v.LeaseId = "other-lease" }}, {"attempt_number", func(v *pb.TaskRejected) { v.AttemptNumber++ }}, {"revision", func(v *pb.TaskRejected) { v.Revision++ }}}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			h, repo, sess := lifecycleHandler(t)
			want := lifecycleTask()
			msg := lifecycleRejected(want)
			tc.mutate(msg)
			h.handleTaskRejected("worker-identity", msg, sess)
			if repo.releaseCalls != 0 {
				t.Fatalf("ReleaseLease calls = %d, want 0", repo.releaseCalls)
			}
			assertLifecycleUnchanged(t, repo, sess, want)
		})
	}
	t.Run("happy_path_and_replay_after_reject", func(t *testing.T) {
		h, repo, sess := lifecycleHandler(t)
		task := lifecycleTask()
		msg := lifecycleRejected(task)
		h.handleTaskRejected(task.WorkerID, msg, sess)
		if repo.releaseCalls != 1 {
			t.Fatalf("first reject calls = %d, want 1", repo.releaseCalls)
		}
		if repo.nowTask.Status != taskgraph.StatusReady || sess.pendingTaskOffer != nil {
			t.Fatalf("reject did not release task/offer")
		}
		h.handleTaskRejected(task.WorkerID, msg, sess)
		if repo.releaseCalls != 1 {
			t.Fatalf("replay reject calls = %d, want 1", repo.releaseCalls)
		}
	})
}

func TestHandleTaskRejectedReleaseFailurePreservesOffer(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	want := lifecycleTask()
	repo.releaseErr = errors.New("lease takeover")
	h.handleTaskRejected(want.WorkerID, lifecycleRejected(want), sess)
	if repo.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", repo.releaseCalls)
	}
	assertLifecycleUnchanged(t, repo, sess, want)
}

func TestHandleUnsupportedExecutorReleaseFailurePreservesSessionState(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	repo.releaseErr = errors.New("lease takeover")
	registry, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: "executor.identity", Version: 7})
	if err != nil {
		t.Fatal(err)
	}
	sess.replaceCapabilities(registry, nil)
	wantOffer := sess.pendingTaskOffer
	h.handleTaskRejected(sess.workerID, &pb.TaskRejected{
		TaskId:        repo.nowTask.ID,
		JobId:         repo.nowTask.JobID,
		AttemptId:     repo.nowTask.AttemptID,
		LeaseId:       repo.nowTask.LeaseID,
		AttemptNumber: int32(repo.nowTask.AttemptNumber),
		Revision:      int32(repo.nowTask.Revision),
		Reason:        "unsupported_executor",
	}, sess)
	if repo.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", repo.releaseCalls)
	}
	if sess.pendingTaskOffer != wantOffer {
		t.Fatal("release failure cleared pending offer")
	}
	sess.executorsMu.RLock()
	stillAdvertised := sess.executors.Has("executor.identity", 7)
	sess.executorsMu.RUnlock()
	if !stillAdvertised {
		t.Fatal("release failure invalidated session capability")
	}
}

func TestHandleTaskRejectedWrongWorkerIsRejectedBeforeMutation(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	want := lifecycleTask()
	h.handleTaskRejected("worker-other", lifecycleRejected(want), sess)
	if repo.releaseCalls != 0 {
		t.Fatalf("wrong-worker reject calls = %d, want 0", repo.releaseCalls)
	}
	assertLifecycleUnchanged(t, repo, sess, want)
}

func TestHandleTaskRejectedTakeoverIsRejectedBeforeMutation(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	repo.nowTask.WorkerID = "worker-takeover"
	repo.nowTask.LeaseID = "lease-takeover"
	repo.nowTask.Revision = 8
	h.handleTaskRejected("worker-identity", lifecycleRejected(lifecycleTask()), sess)
	if repo.releaseCalls != 0 {
		t.Fatalf("takeover reject calls = %d, want 0", repo.releaseCalls)
	}
}

func TestHandleTaskRenewalUsesWireRevisionAndRejectsTakeoverReplay(t *testing.T) {
	h, repo, sess := lifecycleHandler(t)
	task := lifecycleTask()
	h.handleTaskRenewal("worker-other", lifecycleRenewal(task), sess)
	if repo.renewCalls != 0 {
		t.Fatalf("wrong-worker renewal calls=%d, want 0", repo.renewCalls)
	}
	h.handleTaskRenewal(task.WorkerID, lifecycleRenewal(task), sess)
	if repo.renewCalls != 1 || repo.lastRenewRevision != task.Revision {
		t.Fatalf("renew calls=%d revision=%d, want calls=1 revision=%d", repo.renewCalls, repo.lastRenewRevision, task.Revision)
	}
	repo.nowTask.WorkerID = "worker-takeover"
	repo.nowTask.LeaseID = "lease-takeover"
	repo.nowTask.Revision = task.Revision + 1
	h.handleTaskRenewal(task.WorkerID, lifecycleRenewal(task), sess)
	if repo.renewCalls != 1 {
		t.Fatalf("takeover renewal calls=%d, want 1", repo.renewCalls)
	}
}

func TestHandleTaskRenewalRejectsEveryWireMismatchBeforeMutation(t *testing.T) {
	fields := []struct {
		name   string
		mutate func(*pb.TaskLeaseRenewal)
	}{{"task_id", func(v *pb.TaskLeaseRenewal) { v.TaskId = "other-task" }}, {"job_id", func(v *pb.TaskLeaseRenewal) { v.JobId = "other-job" }}, {"attempt_id", func(v *pb.TaskLeaseRenewal) { v.AttemptId = "other-attempt" }}, {"lease_id", func(v *pb.TaskLeaseRenewal) { v.LeaseId = "other-lease" }}, {"attempt_number", func(v *pb.TaskLeaseRenewal) { v.AttemptNumber++ }}, {"revision", func(v *pb.TaskLeaseRenewal) { v.Revision++ }}}
	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			h, repo, sess := lifecycleHandler(t)
			want := lifecycleTask()
			msg := lifecycleRenewal(want)
			tc.mutate(msg)
			h.handleTaskRenewal("worker-identity", msg, sess)
			if repo.renewCalls != 0 {
				t.Fatalf("RenewLease calls = %d, want 0", repo.renewCalls)
			}
			assertLifecycleUnchanged(t, repo, sess, want)
		})
	}
}

func TestHandleTaskLifecycleRejectsIncompleteIdentityBeforeMutation(t *testing.T) {
	t.Run("accepted_master", func(t *testing.T) {
		h, repo, sess := lifecycleHandler(t)
		want := lifecycleTask()
		repo.nowTask.AttemptID = ""
		h.handleTaskAccepted("worker-identity", lifecycleAccepted(want), sess)
		if repo.acceptCalls != 0 {
			t.Fatal("accepted incomplete master identity must not mutate")
		}
	})
	t.Run("accepted_wire", func(t *testing.T) {
		h, repo, sess := lifecycleHandler(t)
		want := lifecycleTask()
		msg := lifecycleAccepted(want)
		msg.Revision = 0
		h.handleTaskAccepted("worker-identity", msg, sess)
		if repo.acceptCalls != 0 {
			t.Fatal("accepted incomplete wire identity must not mutate")
		}
		assertLifecycleUnchanged(t, repo, sess, want)
	})
	t.Run("rejected_wire", func(t *testing.T) {
		h, repo, sess := lifecycleHandler(t)
		want := lifecycleTask()
		msg := lifecycleRejected(want)
		msg.AttemptNumber = 0
		h.handleTaskRejected(want.WorkerID, msg, sess)
		if repo.releaseCalls != 0 {
			t.Fatal("rejected incomplete wire identity must not mutate")
		}
		assertLifecycleUnchanged(t, repo, sess, want)
	})
	t.Run("renewal_wire", func(t *testing.T) {
		h, repo, sess := lifecycleHandler(t)
		want := lifecycleTask()
		msg := lifecycleRenewal(want)
		msg.LeaseId = ""
		h.handleTaskRenewal(want.WorkerID, msg, sess)
		if repo.renewCalls != 0 {
			t.Fatal("renewal incomplete wire identity must not mutate")
		}
		assertLifecycleUnchanged(t, repo, sess, want)
	})
	t.Run("replay_master_incomplete", func(t *testing.T) {
		h, repo, sess := lifecycleHandler(t)
		want := lifecycleTask()
		repo.nowTask.Status = taskgraph.StatusRunning
		repo.nowTask.AttemptID = ""
		repo.nowTask.Revision = want.Revision + 1
		h.handleTaskAccepted(want.WorkerID, lifecycleAccepted(want), sess)
		if repo.acceptCalls != 0 {
			t.Fatal("replay with incomplete master identity must not mutate")
		}
		select {
		case out := <-sess.sendCh:
			t.Fatalf("replay with incomplete master identity emitted grant: %T", out.Envelope.Msg)
		default:
		}
	})
}
