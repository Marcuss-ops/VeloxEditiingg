package costmodel

import "testing"

func TestResourceSnapshotFromMapsPrefersMetrics(t *testing.T) {
	r := ResourceSnapshotFromMaps(map[string]interface{}{"cpu_cores": float64(8)}, map[string]interface{}{
		"cpu_cores": float64(16), "active_tasks": float64(2), "task_slots": float64(4),
	})
	if r.CPUCores != 16 || r.ActiveTasks != 2 || r.TaskSlots != 4 {
		t.Fatalf("snapshot=%+v", r)
	}
}
