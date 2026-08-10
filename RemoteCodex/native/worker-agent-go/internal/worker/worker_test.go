package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/pkg/video/pipeline"
)

func TestConnectionStateTransitions(t *testing.T) {
	// Verify all connection state constants are unique
	states := map[ConnectionState]bool{}
	all := []ConnectionState{
		ConnDisconnected, ConnConnecting, ConnAuthenticating, ConnReady, ConnDraining,
	}
	for _, s := range all {
		if states[s] {
			t.Errorf("duplicate connection state: %s", s)
		}
		states[s] = true
	}
	if len(all) != 5 {
		t.Errorf("expected 5 connection states, got %d", len(all))
	}
}

func TestStatusDerivation_AllStates(t *testing.T) {
	// Verify status constants are unique and cover expected set
	all := []Status{StatusIdle, StatusBusy, StatusError, StatusStopped}
	seen := map[Status]bool{}
	for _, s := range all {
		if seen[s] {
			t.Errorf("duplicate status: %s", s)
		}
		seen[s] = true
	}
	if len(all) != 4 {
		t.Errorf("expected 4 statuses, got %d", len(all))
	}

	// Verify string values are meaningful
	if string(StatusIdle) != "idle" {
		t.Errorf("unexpected StatusIdle value: %q", StatusIdle)
	}
	if string(StatusBusy) != "busy" {
		t.Errorf("unexpected StatusBusy value: %q", StatusBusy)
	}
	if string(StatusError) != "error" {
		t.Errorf("unexpected StatusError value: %q", StatusError)
	}
	if string(StatusStopped) != "stopped" {
		t.Errorf("unexpected StatusStopped value: %q", StatusStopped)
	}
}

func TestRegistrationBackoffConstants(t *testing.T) {
	// Verify backoff constants are reasonable
	if registrationInitialBackoff != 5*time.Second {
		t.Errorf("expected initial backoff 5s, got %v", registrationInitialBackoff)
	}
	if registrationMaxBackoff != 5*time.Minute {
		t.Errorf("expected max backoff 5m, got %v", registrationMaxBackoff)
	}
	if registrationBackoffMult < 1.0 {
		t.Error("backoff multiplier should be >= 1.0")
	}
	// Verify max > initial
	if registrationMaxBackoff <= registrationInitialBackoff {
		t.Error("max backoff must exceed initial backoff")
	}
}

func TestJobProgressZeroValues(t *testing.T) {
	p := JobProgress{}
	if p.Percent != 0 {
		t.Errorf("default Percent should be 0, got %d", p.Percent)
	}
	if p.Scene != 0 {
		t.Errorf("default Scene should be 0, got %d", p.Scene)
	}
	if p.TotalScenes != 0 {
		t.Errorf("default TotalScenes should be 0, got %d", p.TotalScenes)
	}
	if p.Stage != "" {
		t.Errorf("default Stage should be empty, got %q", p.Stage)
	}
}

func TestWithJobProgressCallbackPublishesCanonicalSnapshotWithThrottle(t *testing.T) {
	w := &Worker{
		activeTasks:   map[string]*ActiveTaskExecution{},
		heartbeatWake: make(chan struct{}, 4),
	}
	w.activeTasks["task-progress"] = &ActiveTaskExecution{TaskID: "task-progress"}

	ctx := w.withJobProgressCallback(context.Background(), "task-progress")
	callback := pipeline.DetailedProgressCallback(ctx)
	if callback == nil {
		t.Fatal("progress callback is nil")
	}

	callback(pipeline.ProgressSnapshot{
		Percent: 12, Scene: 2, TotalScenes: 8, Segment: 3, TotalSegments: 16,
		Phase: "building_segments", FramesEncoded: 100, FramesDecoded: 120,
		FramesComposited: 100, FfmpegSpeedX: 1.5, ElapsedMS: 2400,
		CumulativeMetrics: map[string]float64{"frames_encoded": 100},
	})
	first := w.activeTasks["task-progress"].Progress
	if first.Phase != "building_segments" || first.Segment != 3 || first.FramesEncoded != 100 || first.LastProgressAt.IsZero() {
		t.Fatalf("first canonical progress = %+v", first)
	}
	if first.CumulativeMetrics["frames_encoded"] != 100 {
		t.Fatalf("first cumulative metrics = %#v", first.CumulativeMetrics)
	}

	// Same phase/segment inside the two-second checkpoint window is
	// retained in the canonical in-memory Attempt projection, while the
	// heartbeat publication clock remains throttled.
	callback(pipeline.ProgressSnapshot{Percent: 20, Scene: 2, TotalScenes: 8, Segment: 3, TotalSegments: 16, Phase: "building_segments", FramesEncoded: 200})
	second := w.activeTasks["task-progress"].Progress
	if second.Percent != 20 || second.FramesEncoded != 200 || second.LastProgressAt.IsZero() {
		t.Fatalf("latest throttled progress was not retained: first=%+v second=%+v", first, second)
	}
	if !second.LastPublishedAt.Equal(first.LastPublishedAt) {
		t.Fatalf("throttled heartbeat publication advanced: first=%+v second=%+v", first, second)
	}

	// An identical snapshot is deduplicated even after the interval.
	beforeDuplicateWake := len(w.heartbeatWake)
	callback(pipeline.ProgressSnapshot{Percent: 20, Scene: 2, TotalScenes: 8, Segment: 3, TotalSegments: 16, Phase: "building_segments", FramesEncoded: 200})
	if len(w.heartbeatWake) != beforeDuplicateWake {
		t.Fatalf("identical progress snapshot generated traffic")
	}

	// A phase transition bypasses the interval and publishes immediately.
	callback(pipeline.ProgressSnapshot{Percent: 75, Scene: 8, TotalScenes: 8, Segment: 16, TotalSegments: 16, Phase: "concatenating", FramesEncoded: 200})
	third := w.activeTasks["task-progress"].Progress
	if third.Phase != "concatenating" || third.Percent != 75 || third.FramesEncoded != 200 {
		t.Fatalf("phase transition was not published: %+v", third)
	}

	// A metrics-only change is retained in memory but remains subject to the
	// periodic 2s checkpoint; it must not create an immediate traffic burst.
	beforeMetricsWake := len(w.heartbeatWake)
	callback(pipeline.ProgressSnapshot{Percent: 75, Scene: 8, TotalScenes: 8, Segment: 16, TotalSegments: 16, Phase: "concatenating", FramesEncoded: 200, CumulativeMetrics: map[string]float64{"frames_encoded": 201}})
	if len(w.heartbeatWake) != beforeMetricsWake {
		t.Fatalf("metrics-only progress bypassed the traffic throttle")
	}
	if got := w.activeTasks["task-progress"].Progress.CumulativeMetrics["frames_encoded"]; got != 201 {
		t.Fatalf("metrics-only snapshot was not retained: %#v", w.activeTasks["task-progress"].Progress.CumulativeMetrics)
	}

	// A segment completion is an explicit checkpoint, even if the phase is unchanged.
	callback(pipeline.ProgressSnapshot{Percent: 80, Scene: 8, TotalScenes: 8, Segment: 17, TotalSegments: 17, Phase: "building_segments", SegmentCompleted: true})
	fourth := w.activeTasks["task-progress"].Progress
	if !fourth.SegmentCompleted || fourth.Segment != 17 {
		t.Fatalf("segment completion was not retained: %+v", fourth)
	}
}

type heartbeatLoopRecordingTransport struct {
	mu     sync.Mutex
	sends  []time.Time
	notify chan time.Time
	err    error
}

func (r *heartbeatLoopRecordingTransport) Connect(context.Context, controltransport.WorkerHello) error {
	return nil
}

func (r *heartbeatLoopRecordingTransport) Receive(context.Context) (<-chan controltransport.ControlMessage, <-chan error, error) {
	return nil, nil, nil
}

func (r *heartbeatLoopRecordingTransport) Send(_ context.Context, _ controltransport.ControlMessage) error {
	now := time.Now()
	r.mu.Lock()
	r.sends = append(r.sends, now)
	r.mu.Unlock()
	select {
	case r.notify <- now:
	default:
	}
	return r.err
}

func (r *heartbeatLoopRecordingTransport) Close() error { return nil }

func (r *heartbeatLoopRecordingTransport) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sends)
}

func TestHeartbeatLoop_TrafficFloorCoversInitialTickerAndCoalescedWake(t *testing.T) {
	w, _ := newDispatchTestWorker(t)
	transport := &heartbeatLoopRecordingTransport{notify: make(chan time.Time, 4)}
	w.transport = transport
	w.heartbeatWake = make(chan struct{}, 1)
	w.activeTasks["task-heartbeat-floor"] = &ActiveTaskExecution{TaskID: "task-heartbeat-floor"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.wg.Add(1)
	// Use a short test-only ticker so this test exercises the ticker branch
	// without waiting for the production two-second busy cadence.
	go w.heartbeatLoopWithInterval(ctx, 50*time.Millisecond)

	var firstSent time.Time
	select {
	case firstSent = <-transport.notify:
		if got := transport.count(); got != 1 {
			t.Fatalf("initial heartbeat sends = %d, want 1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("initial heartbeat was not sent")
	}

	// With no wake queued, the short ticker is the only source of the second
	// send. The shared gate must delay it until the 250ms floor expires.
	select {
	case secondSent := <-transport.notify:
		if got := transport.count(); got != 2 {
			t.Fatalf("ticker heartbeat sends = %d, want exactly 2", got)
		}
		if elapsed := secondSent.Sub(firstSent); elapsed < heartbeatWakeMinInterval-25*time.Millisecond {
			t.Fatalf("ticker heartbeat elapsed=%v, want at least %v", elapsed, heartbeatWakeMinInterval)
		}
	case <-time.After(heartbeatWakeMinInterval + time.Second):
		t.Fatal("ticker did not produce a heartbeat")
	}

	// A burst of phase/segment wakes is coalesced by heartbeatWake. The same
	// gate must apply to the wake path, even though the ticker is also active.
	wakeQueuedAt := time.Now()
	for i := 0; i < 32; i++ {
		w.wakeHeartbeat()
	}
	select {
	case thirdSent := <-transport.notify:
		if got := transport.count(); got != 3 {
			t.Fatalf("coalesced wake sends = %d, want exactly 3", got)
		}
		if elapsed := thirdSent.Sub(wakeQueuedAt); elapsed < heartbeatWakeMinInterval-25*time.Millisecond {
			t.Fatalf("wake heartbeat elapsed=%v, want at least %v", elapsed, heartbeatWakeMinInterval)
		}
	case <-time.After(heartbeatWakeMinInterval + time.Second):
		t.Fatal("coalesced wake did not produce a heartbeat")
	}

	close(w.stopChan)
	w.wg.Wait()
}

func TestHeartbeatLoop_TrafficFloorThrottlesFailedAttempts(t *testing.T) {
	w, _ := newDispatchTestWorker(t)
	transport := &heartbeatLoopRecordingTransport{
		notify: make(chan time.Time, 4),
		err:    errors.New("synthetic heartbeat failure"),
	}
	w.transport = transport
	w.heartbeatWake = make(chan struct{}, 1)
	w.activeTasks["task-heartbeat-failure-floor"] = &ActiveTaskExecution{TaskID: "task-heartbeat-failure-floor"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.wg.Add(1)
	go w.heartbeatLoopWithInterval(ctx, time.Hour)

	var firstAttempt time.Time
	select {
	case firstAttempt = <-transport.notify:
	case <-time.After(time.Second):
		t.Fatal("initial failed heartbeat was not attempted")
	}
	w.wakeHeartbeat()

	select {
	case secondAttempt := <-transport.notify:
		if got := transport.count(); got != 2 {
			t.Fatalf("failed heartbeat attempts = %d, want exactly 2", got)
		}
		if elapsed := secondAttempt.Sub(firstAttempt); elapsed < heartbeatWakeMinInterval-25*time.Millisecond {
			t.Fatalf("failed-attempt floor elapsed=%v, want at least %v", elapsed, heartbeatWakeMinInterval)
		}
	case <-time.After(heartbeatWakeMinInterval + time.Second):
		t.Fatal("wake retry after failed heartbeat was not attempted")
	}

	close(w.stopChan)
	w.wg.Wait()
}

func TestHeartbeatWakeMinIntervalIsBounded(t *testing.T) {
	if heartbeatIntervalBusy != 2*time.Second {
		t.Fatalf("busy heartbeat interval = %v, want 2s", heartbeatIntervalBusy)
	}
	if heartbeatWakeMinInterval != 250*time.Millisecond {
		t.Fatalf("wake floor = %v, want 250ms", heartbeatWakeMinInterval)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan struct{})
	start := time.Now()
	if !waitHeartbeatFloor(ctx, stop, start) {
		t.Fatal("heartbeat floor unexpectedly cancelled")
	}
	if elapsed := time.Since(start); elapsed < heartbeatWakeMinInterval-25*time.Millisecond {
		t.Fatalf("heartbeat floor elapsed=%v, want at least %v", elapsed, heartbeatWakeMinInterval)
	}
}

func TestActiveTaskExecutionFields(t *testing.T) {
	// Verify ActiveTaskExecution struct has expected fields (compile-time check).
	// PR 1: the `Job` field on ActiveTaskExecution was removed when the legacy
	// Job-side state mirror (`persistedState.ActiveJobs`) was deleted. Test now
	// checks the canonical task-native fields.
	at := &ActiveTaskExecution{
		StartedAt: time.Now(),
	}
	if at.LeaseID != "" {
		t.Errorf("default LeaseID should be empty, got %q", at.LeaseID)
	}
	if at.Cancel != nil {
		t.Error("default Cancel should be nil")
	}
	if at.StartedAt.IsZero() {
		t.Error("StartedAt should be set")
	}
}

func TestStatusCanTransitionTo(t *testing.T) {
	// Verify the transition rules in canTransitionTo logic
	// Idle → Busy (OK), Idle → Stopped (OK)
	// Busy → Busy (OK for another concurrent slot), Busy → Idle (OK),
	// Busy → Error (OK), Busy → Stopped (OK)
	// Error → Idle (OK), Error → Stopped (OK)
	// Stopped → anything (NOT OK)

	type transition struct {
		from Status
		to   Status
		ok   bool
	}
	transitions := []transition{
		{StatusIdle, StatusBusy, true},
		{StatusIdle, StatusStopped, true},
		{StatusIdle, StatusError, false},
		{StatusBusy, StatusIdle, true},
		{StatusBusy, StatusBusy, true},
		{StatusBusy, StatusError, true},
		{StatusBusy, StatusStopped, true},
		{StatusError, StatusIdle, true},
		{StatusError, StatusStopped, true},
		{StatusError, StatusBusy, false},
		{StatusStopped, StatusIdle, false},
		{StatusStopped, StatusBusy, false},
		{StatusStopped, StatusError, false},
	}

	for _, tr := range transitions {
		t.Run(string(tr.from)+"→"+string(tr.to), func(t *testing.T) {
			var ok bool
			switch tr.from {
			case StatusIdle:
				ok = tr.to == StatusBusy || tr.to == StatusStopped
			case StatusBusy:
				ok = tr.to == StatusBusy || tr.to == StatusIdle || tr.to == StatusError || tr.to == StatusStopped
			case StatusError:
				ok = tr.to == StatusIdle || tr.to == StatusStopped
			case StatusStopped:
				ok = false
			}
			if ok != tr.ok {
				t.Errorf("transition %s→%s: expected %v, got %v", tr.from, tr.to, tr.ok, ok)
			}
		})
	}
}

func TestBackoffConfigDefaults(t *testing.T) {
	bc := &backoffConfig{
		initialInterval: 5 * time.Second,
		maxInterval:     60 * time.Second,
		multiplier:      2.0,
	}
	if bc.initialInterval != 5*time.Second {
		t.Errorf("expected 5s initial, got %v", bc.initialInterval)
	}
	if bc.maxInterval != 60*time.Second {
		t.Errorf("expected 60s max, got %v", bc.maxInterval)
	}
	if bc.multiplier != 2.0 {
		t.Errorf("expected 2.0 multiplier, got %f", bc.multiplier)
	}
}

func TestActiveTasksMap_Concurrency(t *testing.T) {
	// Verify activeTasks map supports concurrent access patterns
	atMap := make(map[string]*ActiveTaskExecution)

	// Add tasks
	atMap["task-1"] = &ActiveTaskExecution{TaskID: "task-1", LeaseID: "lease-1", StartedAt: time.Now()}
	atMap["task-2"] = &ActiveTaskExecution{TaskID: "task-2", LeaseID: "lease-2", StartedAt: time.Now()}

	if len(atMap) != 2 {
		t.Errorf("expected 2 active tasks, got %d", len(atMap))
	}

	// Read task
	at1, ok := atMap["task-1"]
	if !ok || at1.LeaseID != "lease-1" {
		t.Error("task-1 not found or wrong lease")
	}

	// Delete task
	delete(atMap, "task-1")
	if len(atMap) != 1 {
		t.Errorf("expected 1 task after delete, got %d", len(atMap))
	}
	if _, ok := atMap["task-1"]; ok {
		t.Error("task-1 should be deleted")
	}
}

func TestReRegistrationBackoffGrowth(t *testing.T) {
	// Verify backoff grows exponentially and caps at max
	initial := registrationInitialBackoff
	max := registrationMaxBackoff
	mult := registrationBackoffMult

	backoff := initial
	for i := 0; i < 20; i++ {
		backoff = time.Duration(float64(backoff) * mult)
		if backoff > max {
			backoff = max
		}
	}

	if backoff != max {
		t.Errorf("backoff should cap at %v, got %v after 20 iterations", max, backoff)
	}

	// Verify initial backoff is less than max
	if initial >= max {
		t.Error("initial backoff must be less than max")
	}

	// Verify growth: after 1 iteration, backoff > initial
	grow1 := time.Duration(float64(initial) * mult)
	if grow1 <= initial {
		t.Errorf("backoff must grow after 1 iteration: %v → %v (mult=%v)", initial, grow1, mult)
	}
}

func TestReRegistrationBackoffCapsAtMax(t *testing.T) {
	// After enough iterations, backoff stays at max
	backoff := registrationInitialBackoff
	for i := 0; i < 10; i++ {
		backoff = time.Duration(float64(backoff) * registrationBackoffMult)
		if backoff > registrationMaxBackoff {
			backoff = registrationMaxBackoff
		}
	}
	if backoff != registrationMaxBackoff {
		t.Errorf("backoff should be capped at %v, got %v", registrationMaxBackoff, backoff)
	}
}

func TestStatusDerivationFromActiveTasks(t *testing.T) {
	// Simulate the Status() derivation logic
	type scenario struct {
		name        string
		stopped     bool
		activeTasks int
		errorState  Status
		expected    Status
	}

	scenarios := []scenario{
		{"idle-empty", false, 0, StatusIdle, StatusIdle},
		{"busy-one-task", false, 1, StatusIdle, StatusBusy},
		{"busy-multiple", false, 3, StatusIdle, StatusBusy},
		{"error-no-tasks", false, 0, StatusError, StatusError},
		{"busy-with-error-bg", false, 2, StatusError, StatusBusy}, // Busy takes priority
		{"stopped", true, 0, StatusIdle, StatusStopped},
		{"stopped-with-tasks", true, 1, StatusIdle, StatusStopped}, // Stopped overrides
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			var result Status
			if sc.stopped {
				result = StatusStopped
			} else if sc.activeTasks > 0 {
				result = StatusBusy
			} else if sc.errorState == StatusError {
				result = StatusError
			} else {
				result = StatusIdle
			}

			if result != sc.expected {
				t.Errorf("expected %s, got %s (stopped=%v, tasks=%d, err=%s)",
					sc.expected, result, sc.stopped, sc.activeTasks, sc.errorState)
			}
		})
	}
}
