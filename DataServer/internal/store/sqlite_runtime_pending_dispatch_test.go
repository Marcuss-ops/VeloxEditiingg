package store

import (
	"context"
	"testing"
	"time"

	"velox-shared/dispatchable"
)

func TestDispatchableQueriesHideRuntimeAssetsPending(t *testing.T) {
	_, db := openCandidatesTestDB(t)
	t0 := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	seedCandidateTask(t, db, "T-visible", "J-visible", 1, "READY", false, "", "scene.composite.v1", 1, t0)
	seedCandidateTask(t, db, "T-pending", "J-pending", 2, "READY", false, "", "scene.composite.v1", 1, t0.Add(time.Second))
	if _, err := db.Exec(`INSERT INTO task_specs(task_id,payload_json) VALUES(?,?)`, "T-pending", `{"runtime_assets_pending":true}`); err != nil {
		t.Fatal(err)
	}
	for name, list := range map[string]func(context.Context, dispatchable.Querier, int) ([]dispatchable.Job, error){
		"first page": dispatchable.ListNextDispatchableJobs,
		"paged": func(ctx context.Context, q dispatchable.Querier, n int) ([]dispatchable.Job, error) {
			return dispatchable.ListNextDispatchableJobsPage(ctx, q, n, 0)
		},
	} {
		t.Run(name, func(t *testing.T) {
			jobs, err := list(context.Background(), db, 10)
			if err != nil {
				t.Fatalf("list dispatchable: %v", err)
			}
			if len(jobs) != 1 || jobs[0].TaskID != "T-visible" {
				t.Fatalf("jobs=%+v, want only T-visible", jobs)
			}
		})
	}
}
