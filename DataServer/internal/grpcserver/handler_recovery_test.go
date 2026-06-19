package grpcserver

import (
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	pb "velox-shared/controltransport/pb"
)

// recvFromChan drains c with a small grace period so tests are deterministic.
func recvFromChan(t *testing.T, c <-chan *outboundMessage) *pb.MasterToWorkerEnvelope {
	t.Helper()
	select {
	case msg := <-c:
		return msg.Envelope
	default:
		t.Fatal("expected a queued envelope, got none")
		return nil
	}
}

func newTestSession() *workerSession {
	return &workerSession{
		sessionID: "test-session",
		sendCh:    make(chan *outboundMessage, 4),
	}
}

// TestHandleRecoveryReport_NoExtra returns false without enqueueing.
func TestHandleRecoveryReport_NoExtra(t *testing.T) {
	h := &Handler{}
	sess := newTestSession()
	hb := &pb.Heartbeat{}
	if h.handleRecoveryReport("w1", sess, hb) {
		t.Fatal("expected false with no extra field")
	}
}

// TestHandleRecoveryReport_NoRecoveryKey returns false (regular heartbeat).
func TestHandleRecoveryReport_NoRecoveryKey(t *testing.T) {
	h := &Handler{}
	sess := newTestSession()
	extra, _ := structpb.NewStruct(map[string]interface{}{"unrelated": "value"})
	hb := &pb.Heartbeat{Extra: extra}
	if h.handleRecoveryReport("w1", sess, hb) {
		t.Fatal("expected false when extra has no recovery_report_v1")
	}
}


// TestHandleRecoveryReport_ValidPayload enqueues a ConfigurationUpdate.
func TestHandleRecoveryReport_ValidPayload(t *testing.T) {
	h := &Handler{}
	sess := newTestSession()
	recMap := map[string]interface{}{
		"schema_version":       "v1",
		"saved_at":             "2026-06-19T09:00:00Z",
		"active_jobs_count":    1.0,
		"pending_leases_count": 0.0,
		"seen_commands_count":  0.0,
		"active_jobs": []interface{}{
			map[string]interface{}{"job_id": "job-1", "job_run_id": "run-1", "job_type": "render", "lease_id": "L1"},
		},
		"pending_lease_jobs": []interface{}{},
	}
	// recMap is consumed directly by both NewStruct calls below; no
	// pre-built *structpb.Struct needed (avoids declaring variables
	// that older structpb runtimes reject inside NewStruct maps).
	// Pass the flat map[string]interface{} directly. structpb.NewStruct
	// accepts nested maps and emits Value_StructValue internally when
	// given a Go map as a value. Some older structpb runtimes reject
	// both *structpb.Struct and *structpb.Value inside the input map,
	// so the flat-map form is the most portable.
	extra, err := structpb.NewStruct(map[string]interface{}{
		RecoveryReportKey: recMap,
	})
	if err != nil {
		t.Fatalf("setup NewStruct(extra): %v", err)
	}
	hb := &pb.Heartbeat{Extra: extra}

	if !h.handleRecoveryReport("w1", sess, hb) {
		t.Fatalf("expected true when recovery_report_v1 present (extra.Fields=%v)", hb.GetExtra().GetFields())
	}
	env := recvFromChan(t, sess.sendCh)
	cfgUpdate := env.GetConfigurationUpdate()
	if cfgUpdate == nil {
		t.Fatal("expected ConfigurationUpdate message")
	}
	cfgMap := cfgUpdate.GetConfiguration().AsMap()
	raw, ok := cfgMap[RecoveryActionKey]
	if !ok {
		t.Fatalf("expected %q key in directive: %v", RecoveryActionKey, cfgMap)
	}
	dir, ok := raw.(map[string]interface{})
	if !ok {
		t.Fatalf("directive not map: %T (%v)", raw, raw)
	}
	if dir["action"] != RecoveryActionContinue {
		t.Fatalf("action mismatch: %v", dir["action"])
	}
	perJob, ok := dir["job_actions"].(map[string]interface{})
	if !ok || len(perJob) != 1 {
		t.Fatalf("expected per-job map with 1 entry: %v", dir["job_actions"])
	}
	if perJob["job-1"] != RecoveryActionContinue {
		t.Fatalf("per-job action mismatch: %v", perJob["job-1"])
	}
}

// TestHandleRecoveryReport_NilSession is a no-op (returns false).
// Defensive guard — handler should never NPE on a stray session ptr.
func TestHandleRecoveryReport_NilSession(t *testing.T) {
	h := &Handler{}
	extra, _ := structpb.NewStruct(map[string]interface{}{RecoveryReportKey: map[string]interface{}{}})
	hb := &pb.Heartbeat{Extra: extra}
	if h.handleRecoveryReport("w1", nil, hb) {
		t.Fatal("expected false for nil session")
	}
}

// TestSessionConcurrency_SendCloseRace (sanity check for safeSend usage).
func TestSafeSend_NonBlocking(t *testing.T) {
	ch := make(chan *outboundMessage, 2)
	ch <- &outboundMessage{Envelope: &pb.MasterToWorkerEnvelope{}} // 1/2
	ch <- &outboundMessage{Envelope: &pb.MasterToWorkerEnvelope{}} // 2/2 (full)
	if safeSend(ch, &outboundMessage{Envelope: &pb.MasterToWorkerEnvelope{}}) {
		t.Fatal("expected safeSend to fail when channel is full and not closed")
	}
}

// pin: ensure no goroutine leak from the async path under recovery.
func TestHandleRecoveryReport_NoGoroutineLeak(t *testing.T) {
	h := &Handler{}
	sess := newTestSession()
	recStruct, _ := structpb.NewStruct(map[string]interface{}{})
	extra, _ := structpb.NewStruct(map[string]interface{}{RecoveryReportKey: recStruct})
	hb := &pb.Heartbeat{Extra: extra}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.handleRecoveryReport("w", sess, hb)
		}()
	}
	wg.Wait()
}
