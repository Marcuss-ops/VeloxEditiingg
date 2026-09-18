package taskgraph

import (
	"encoding/json"
	"fmt"
)

// depends_on persistence helpers and DAG verification.
//
// Migration 176 added the depends_on JSON column to tasks; these helpers
// are the single owner of its wire format (a JSON array of task IDs,
// always an array — never null). The graph predicates below are pure
// functions over the task list so they are unit-testable without a
// store, and both TickReadiness (readiness) and the failure-propagation
// sweep reuse the same definitions of "blocked" and "doomed".

// MarshalDependsOn renders the canonical depends_on JSON. Nil slices are
// normalized to [] so no row ever stores the string "null".
func MarshalDependsOn(dependsOn []string) (string, error) {
	if dependsOn == nil {
		dependsOn = []string{}
	}
	data, err := json.Marshal(dependsOn)
	if err != nil {
		return "", fmt.Errorf("marshal depends_on: %w", err)
	}
	return string(data), nil
}

// UnmarshalDependsOn parses a stored depends_on value. Empty strings
// (pre-176 rows mid-migration) parse as an empty edge list. The literal
// "null" is rejected: MarshalDependsOn never emits it, so encountering
// it means the row was written by something other than this code path.
func UnmarshalDependsOn(raw string, into *[]string) error {
	if into == nil {
		return fmt.Errorf("unmarshal depends_on: nil destination")
	}
	if raw == "" {
		*into = nil
		return nil
	}
	if raw == "null" {
		return fmt.Errorf("unmarshal depends_on: null is not a valid edge list")
	}
	var edges []string
	if err := json.Unmarshal([]byte(raw), &edges); err != nil {
		return fmt.Errorf("unmarshal depends_on %q: %w", raw, err)
	}
	*into = edges
	return nil
}

// FindDependencyCycle returns the first dependency cycle found among the
// supplied tasks, or nil when the graph is acyclic.
//
// The edges are the persisted DependsOn lists. Unknown dependency IDs
// (edges pointing at tasks absent from the list) are ignored here: they
// are a dangling-reference problem caught by repository-level
// validation, not a cycle, and AreDependenciesSatisfied already treats
// a missing dependency as permanently unsatisfied (the task stays
// PENDING instead of executing with unknown inputs).
func FindDependencyCycle(tasks []Task) []string {
	edges := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		edges[t.ID] = t.DependsOn
	}

	const (
		white = 0 // unvisited
		grey  = 1 // on the current DFS stack
		black = 2 // fully explored, provably cycle-free
	)
	color := make(map[string]int, len(tasks))
	var stack []string

	var visit func(id string) []string
	visit = func(id string) []string {
		switch color[id] {
		case grey:
			// Found a cycle: reconstruct it from the DFS stack.
			start := len(stack)
			for i, s := range stack {
				if s == id {
					start = i
					break
				}
			}
			cycle := append(append([]string{}, stack[start:]...), id)
			return cycle
		case black:
			return nil
		}
		color[id] = grey
		stack = append(stack, id)
		for _, dep := range edges[id] {
			if cycle := visit(dep); cycle != nil {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return nil
	}

	// Deterministic iteration order: tasks are visited in list order and
	// edges in stored order, so the same graph always yields the same
	// first cycle (matters for tests and for operator-facing error text).
	for _, t := range tasks {
		if color[t.ID] != white {
			continue
		}
		if cycle := visit(t.ID); cycle != nil {
			return cycle
		}
	}
	return nil
}

// ValidateTaskGraph is the fan-out enqueue gate (100-percent-plan/04 §1
// "Validate the complete graph before any Task becomes READY"). It
// rejects a task set that would publish dangling edges or cycles.
func ValidateTaskGraph(tasks []Task) error {
	known := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		known[t.ID] = struct{}{}
	}
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			if _, ok := known[dep]; !ok {
				return fmt.Errorf("taskgraph.ValidateTaskGraph: task %s depends on unknown task %s", t.ID, dep)
			}
		}
	}
	if cycle := FindDependencyCycle(tasks); cycle != nil {
		return fmt.Errorf("taskgraph.ValidateTaskGraph: dependency cycle: %v", cycle)
	}
	return nil
}

// DoomedTaskIDs returns the IDs of every non-terminal task transitively
// downstream of a failed dependency (failure propagation, Track 4 §2
// "Define failure propagation from Task to dependent Tasks").
//
// Semantics: when a dependency reaches a terminal state that is NOT
// SUCCEEDED (FAILED, CANCELLED, TIMED_OUT), every task that depends on
// it — directly or transitively — can never become READY (its readiness
// predicate requires ALL dependencies SUCCEEDED). Marking them CANCELLED
// converts an unbounded zombie wait into a bounded, visible outcome; the
// parent Job's terminal roll-up then observes "all tasks terminal" and
// completes deterministically.
//
// The result excludes tasks already terminal (they need no transition),
// and iteration is deterministic (list order) so concurrent sweeps
// produce identical candidate sets. The caller owns the CAS transitions;
// a concurrent status change is handled by the repository CAS, not here.
func DoomedTaskIDs(tasks []Task) []string {
	return doomedTaskIDs(tasks)
}

// doomedTaskIDs is the pure core of DoomedTaskIDs.
func doomedTaskIDs(tasks []Task) []string {
	// status of every task by ID.
	status := make(map[string]Status, len(tasks))
	// reverse edges: dependency → tasks that depend on it.
	downstream := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		status[t.ID] = t.Status
		for _, dep := range t.DependsOn {
			downstream[dep] = append(downstream[dep], t.ID)
		}
	}

	failed := make(map[string]struct{})
	queue := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if t.Status == StatusFailed || t.Status == StatusCancelled || t.Status == StatusTimedOut {
			failed[t.ID] = struct{}{}
			queue = append(queue, t.ID)
		}
	}

	doomed := map[string]struct{}{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, child := range downstream[id] {
			if _, already := doomed[child]; already {
				continue
			}
			if childStatus := status[child]; childStatus.IsTerminal() {
				// Already terminal (e.g. cancelled by a concurrent sweep):
				// do not re-mark, but still propagate through it — its own
				// dependents are equally doomed.
			} else {
				doomed[child] = struct{}{}
			}
			if _, isFailed := failed[child]; !isFailed {
				failed[child] = struct{}{}
				queue = append(queue, child)
			}
		}
	}

	// Deterministic output order.
	result := make([]string, 0, len(doomed))
	for _, t := range tasks {
		if _, ok := doomed[t.ID]; ok {
			result = append(result, t.ID)
		}
	}
	return result
}
