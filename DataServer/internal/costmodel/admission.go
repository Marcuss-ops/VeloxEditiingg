package costmodel

// AdmissionDecision is the explainable result of reserving resources for one
// task. It does not mutate registry state; the canonical task claim remains
// the single atomic writer.
type AdmissionDecision struct {
	Admitted             bool
	Reason               string
	CPUThreads           int
	MemoryBytes          int64
	TempBytes            int64
	AvailableCPUThreads  int
	AvailableMemoryBytes int64
	AvailableTempBytes   int64
}

// Admit checks task reservations against a worker snapshot and policy.
// Zero reservations are accepted for legacy tasks, while reported pressure
// still prevents new work from being admitted.
func Admit(r ResourceSnapshot, req JobRequirements, p AdmissionPolicy) AdmissionDecision {
	if p == (AdmissionPolicy{}) {
		p = DefaultAdmissionPolicy()
	}
	availableCPU := r.CPUCores - p.ReservedCPUCores - r.CPUThreadsInUse
	availableMemory := r.MemoryBytes - r.MemoryUsedBytes
	availableTemp := r.DiskFreeBytes - r.TempReservedBytes
	decision := AdmissionDecision{
		Admitted: true, CPUThreads: req.CPUThreads, MemoryBytes: req.MemoryBytes,
		TempBytes: req.TempBytes, AvailableCPUThreads: availableCPU,
		AvailableMemoryBytes: availableMemory, AvailableTempBytes: availableTemp,
	}
	pressure := DerivePressure(r, p)
	if pressure.Memory || pressure.Disk || pressure.Swap || pressure.IOWait {
		decision.Admitted = false
		switch {
		case pressure.Memory:
			decision.Reason = "memory_pressure"
		case pressure.Disk:
			decision.Reason = "disk_pressure"
		case pressure.Swap:
			decision.Reason = "swap_pressure"
		default:
			decision.Reason = "io_wait_pressure"
		}
		return decision
	}
	if r.TaskSlots > 0 && r.ActiveTasks >= r.TaskSlots {
		decision.Admitted = false
		decision.Reason = "capacity_full"
		return decision
	}
	if req.CPUThreads > 0 && availableCPU < req.CPUThreads {
		decision.Admitted = false
		decision.Reason = "cpu_budget_exhausted"
		return decision
	}
	if req.MemoryBytes > 0 && availableMemory-req.MemoryBytes < p.MinFreeMemoryBytes {
		decision.Admitted = false
		decision.Reason = "memory_budget_exhausted"
		return decision
	}
	if req.TempBytes > 0 && availableTemp-req.TempBytes < p.MinFreeDiskBytes {
		decision.Admitted = false
		decision.Reason = "temp_budget_exhausted"
	}
	return decision
}
