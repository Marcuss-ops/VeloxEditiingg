package supervisor

// supervisor_diagnostics.go: Len / Names / Classes / Missing / States —
// the diagnostics surface of the Supervisor used by the /ready probe.
// Split out of supervisor.go.

import "sort"

// Len returns the number of registered runners.
func (s *Supervisor) Len() int {
	return len(s.runners)
}

// Names returns the name of every registered runner.
func (s *Supervisor) Names() []string {
	names := make([]string, len(s.runners))
	for i, r := range s.runners {
		names[i] = r.Name
	}
	return names
}

// Classes returns the class of every registered runner.
func (s *Supervisor) Classes() []RunnerClass {
	out := make([]RunnerClass, len(s.runners))
	for i, r := range s.runners {
		out[i] = r.Class
	}
	return out
}

// Missing returns the names of every registered runner whose current
// state is not healthy. ClassOneShot runners in STOPPED state are NOT
// flagged (they are expected to exit cleanly). ClassRestartable and
// ClassCritical runners are flagged when their state is BACKING_OFF,
// FAILED, or STOPPED.
//
// This is the gate the /ready check uses to fail-loud on runner
// silent-death (e.g. a critical runner exhausted retries and the master
// is now serving with a dead delivery pipeline).
func (s *Supervisor) Missing() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var missing []string
	for _, r := range s.runners {
		state, ok := s.states[r.Name]
		if !ok {
			// Runner was registered but never started — structural bug.
			missing = append(missing, r.Name)
			continue
		}
		if r.Class == ClassOneShot {
			// One-shot runners are expected to exit cleanly.
			// The !ok branch above handles the never-started case;
			// a stopped OneShot is not flagged as missing.
			continue
		}
		if !state.IsHealthy() {
			missing = append(missing, r.Name)
		}
	}
	sort.Strings(missing)
	return missing
}

// States returns the current state of every registered runner.
// Used by the /ready probe to surface per-runner health details.
func (s *Supervisor) States() map[string]RunnerState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]RunnerState, len(s.runners))
	for _, r := range s.runners {
		if state, ok := s.states[r.Name]; ok {
			out[r.Name] = state
		} else {
			out[r.Name] = ""
		}
	}
	return out
}
