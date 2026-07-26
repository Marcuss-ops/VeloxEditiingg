package costmodel

import "testing"

func TestAdmitRejectsOversubscription(t *testing.T) {
	r := ResourceSnapshot{CPUCores: 16, CPUThreadsInUse: 10, MemoryBytes: 32 << 30, MemoryUsedBytes: 4 << 30, DiskFreeBytes: 100 << 30, TaskSlots: 4, ActiveTasks: 1}
	d := Admit(r, JobRequirements{CPUThreads: 6}, DefaultAdmissionPolicy())
	if d.Admitted || d.Reason != "cpu_budget_exhausted" {
		t.Fatalf("decision=%+v", d)
	}
}

func TestAdmitRejectsPressureBeforeCapacity(t *testing.T) {
	r := ResourceSnapshot{CPUCores: 8, MemoryBytes: 8 << 30, MemoryUsedBytes: 8 << 30, DiskFreeBytes: 100 << 30, TaskSlots: 1, ActiveTasks: 1}
	d := Admit(r, JobRequirements{}, DefaultAdmissionPolicy())
	if d.Admitted || d.Reason != "memory_pressure" {
		t.Fatalf("decision=%+v", d)
	}
}
