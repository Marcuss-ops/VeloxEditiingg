package completion

import (
	"fmt"

	"velox-shared/identity"
)

// FenceTuple is the canonical worker-bound lease identity used by the
// artifact-commit protocol. WorkerID is the typed canonical identity
// (velox-shared/identity): only a WorkerID may fence a lease.
type FenceTuple struct {
	TaskID    string
	AttemptID string
	WorkerID  identity.WorkerID
	LeaseID   string
	Revision  int
}

func (f FenceTuple) Validate() error {
	if f.TaskID == "" {
		return fmt.Errorf("completion.FenceTuple: TaskID empty: %+v", f)
	}
	if f.AttemptID == "" {
		return fmt.Errorf("completion.FenceTuple: AttemptID empty: %+v", f)
	}
	if f.WorkerID.IsEmpty() {
		return fmt.Errorf("completion.FenceTuple: WorkerID empty: %+v", f)
	}
	if f.LeaseID == "" {
		return fmt.Errorf("completion.FenceTuple: LeaseID empty: %+v", f)
	}
	if f.Revision < 0 {
		return fmt.Errorf("completion.FenceTuple: Revision < 0: %+v", f)
	}
	return nil
}

func (f FenceTuple) Equal(g FenceTuple) bool { return f == g }

// SQLWhere/SQLArgs are retained as pure predicate data for tests and
// diagnostics; production persistence uses repository.CompletionFence directly.
func (f FenceTuple) SQLWhere() string {
	return "task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ? AND task_revision = ?"
}

func (f FenceTuple) SQLArgs() []any {
	return []any{f.TaskID, f.AttemptID, f.WorkerID.String(), f.LeaseID, f.Revision}
}
