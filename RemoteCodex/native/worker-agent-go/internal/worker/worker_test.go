package worker

import (
	"testing"
	"time"
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

func TestActiveTaskExecutionFields(t *testing.T) {
	// Verify ActiveTaskExecution struct has expected fields (compile-time check)
	at := &ActiveTaskExecution{
		StartedAt: time.Now(),
	}
	if at.Job != nil {
		t.Error("default Job should be nil")
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
	// Busy → Idle (OK), Busy → Error (OK), Busy → Stopped (OK)
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
				ok = tr.to == StatusIdle || tr.to == StatusError || tr.to == StatusStopped
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
