package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/worker/concurrency"
)

// simulateTaskPipeline mimics the worker's executeTask admission flow:
// 1. Admission gate (CanAdmit)
// 2. Concurrency slot (Acquire)
// 3. Work (simulated by sleep)
// 4. Release concurrency slot
// 5. Record admission result (hysteresis)
func simulateTaskPipeline(rac *ResourceAdmissionController, cl *concurrency.ConcurrencyLimiter, kind ResourceKind, workMS int) (admitted bool, slotAcquired bool, err error) {
	// Step 1: Admission gate
	decision := rac.CanAdmit(ResourceClaim{Kind: kind})
	if decision != Admit {
		rac.RecordAdmissionResult(ResourceClaim{Kind: kind}, false)
		return false, false, nil
	}

	// Step 2: Concurrency slot
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := cl.Acquire(ctx, "test-job", 0); err != nil {
		return true, false, err
	}

	// Step 3: Work
	time.Sleep(time.Duration(workMS) * time.Millisecond)

	// Step 4: Release
	cl.Release()

	// Step 5: Record result
	rac.RecordAdmissionResult(ResourceClaim{Kind: kind}, true)
	return true, true, nil
}

// TestIntegration_AdmissionPlusConcurrency_BelowThresholds verifies that
// when both admission and concurrency are available, tasks proceed normally.
func TestIntegration_AdmissionPlusConcurrency_BelowThresholds(t *testing.T) {
	// 70 GiB total, 50 GiB RSS = 71.4% — below all throttle thresholds.
	rac := NewResourceAdmissionController(fixedSampler(50<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(2)

	var completed atomic.Int32
	var wg sync.WaitGroup
	const tasks = 4

	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			admitted, slotAcquired, err := simulateTaskPipeline(rac, cl, ResourceRender, 5)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !admitted {
				t.Error("expected admission below threshold")
				return
			}
			if !slotAcquired {
				t.Error("expected concurrency slot acquisition")
				return
			}
			completed.Add(1)
		}()
	}
	wg.Wait()

	if got := completed.Load(); got != tasks {
		t.Fatalf("completed = %d, want %d", got, tasks)
	}
	stats := cl.Stats()
	if stats.TotalJobs != tasks {
		t.Fatalf("concurrency total = %d, want %d", stats.TotalJobs, tasks)
	}
	if rac.AdmissionRejections() != 0 {
		t.Fatalf("admission rejections = %d, want 0", rac.AdmissionRejections())
	}
}

// TestIntegration_AdmissionRejectionPreventsSlotWaste verifies that when
// admission rejects, no concurrency slot is consumed.
func TestIntegration_AdmissionRejectionPreventsSlotWaste(t *testing.T) {
	// Start low: 70 GiB total, 50 GiB RSS = 71.4% — below all thresholds.
	rss := int64(50 << 30)
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(1)

	// First task: admitted, takes the single slot.
	admitted, slotAcquired, err := simulateTaskPipeline(rac, cl, ResourceRender, 5)
	if err != nil {
		t.Fatalf("task 1: unexpected error: %v", err)
	}
	if !admitted || !slotAcquired {
		t.Fatalf("task 1: admitted=%v slot=%v, want both true", admitted, slotAcquired)
	}

	// RSS rises above 93% — admission now rejects.
	rss = 66 << 30 // 94.3%

	// Second task: admission rejects — concurrency slot should NOT be consumed.
	admitted, slotAcquired, err = simulateTaskPipeline(rac, cl, ResourceRender, 5)
	if err != nil {
		t.Fatalf("task 2: unexpected error: %v", err)
	}
	if admitted {
		t.Error("task 2: expected admission rejection at 94.3% RSS")
	}
	if slotAcquired {
		t.Error("task 2: expected no concurrency slot when admission rejects")
	}

	// Concurrency should have exactly 1 total job (only the first task).
	stats := cl.Stats()
	if stats.TotalJobs != 1 {
		t.Fatalf("concurrency total = %d, want 1 (admission rejection must not consume slot)", stats.TotalJobs)
	}
	if stats.ActiveJobs != 0 {
		t.Fatalf("concurrency active = %d, want 0 (task 1 already completed)", stats.ActiveJobs)
	}
	if rac.AdmissionRejections() != 1 {
		t.Fatalf("admission rejections = %d, want 1", rac.AdmissionRejections())
	}
}

// TestIntegration_ConcurrencyFullAdmissionOk verifies that when concurrency
// is full but admission allows, tasks wait in the concurrency queue.
func TestIntegration_ConcurrencyFullAdmissionOk(t *testing.T) {
	// 70 GiB total, 50 GiB RSS = 71.4% — below all thresholds.
	rac := NewResourceAdmissionController(fixedSampler(50<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(1) // single slot

	// First task: occupies the slot for 50ms.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	if err := cl.Acquire(ctx1, "job-1", 0); err != nil {
		t.Fatalf("task 1 acquire: %v", err)
	}

	// Second task: admission allows but concurrency is full → waits.
	var slot2Acquired atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		decision := rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
		if decision != Admit {
			t.Error("expected admission at 71.4% RSS")
			return
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel2()
		if err := cl.Acquire(ctx2, "job-2", 0); err != nil {
			t.Errorf("task 2 acquire: %v", err)
			return
		}
		slot2Acquired.Store(true)
		cl.Release()
	}()

	// Give the waiter time to block on concurrency.
	time.Sleep(10 * time.Millisecond)

	// Release the first slot — waiter should unblock.
	cl.Release()
	wg.Wait()

	if !slot2Acquired.Load() {
		t.Fatal("expected second task to acquire slot after first released")
	}
	if rac.AdmissionRejections() != 0 {
		t.Fatalf("admission rejections = %d, want 0", rac.AdmissionRejections())
	}
}

// TestIntegration_HysteresisRecoveryUnblocksConcurrency verifies that after
// admission rejects due to high RSS, recovery below the hysteresis threshold
// allows tasks to proceed again.
func TestIntegration_HysteresisRecoveryUnblocksConcurrency(t *testing.T) {
	total := int64(70 << 30)
	rss := int64(66 << 30) // 94.3% — above render throttle
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))
	cl := concurrency.NewConcurrencyLimiter(2)

	// Phase 1: RSS high → admission rejects.
	decision := rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if decision != RejectMemory {
		t.Fatalf("phase 1: expected RejectMemory, got %v", decision)
	}
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, false)
	if !rac.IsThrottled(ResourceRender) {
		t.Fatal("phase 1: expected render throttled")
	}

	// Phase 2: RSS drops to 60 GiB (85.7%) — above recovery (83%), still throttled.
	rss = 60 << 30
	decision = rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if decision != RejectMemory {
		t.Fatalf("phase 2: expected RejectMemory at 85.7%%, got %v", decision)
	}
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, true)
	if !rac.IsThrottled(ResourceRender) {
		t.Fatal("phase 2: expected render still throttled at 85.7%")
	}

	// Phase 3: RSS drops to 57 GiB (81.4%) — below recovery (83%).
	// RecordAdmissionResult checks recovery threshold and deactivates throttle.
	rss = 57 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, true)
	if rac.IsThrottled(ResourceRender) {
		t.Fatal("phase 3: expected render recovered at 81.4%")
	}
	// Now CanAdmit should pass since throttle is cleared.
	decision = rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if decision != Admit {
		t.Fatalf("phase 3: expected Admit after recovery at 81.4%%, got %v", decision)
	}

	// Tasks now proceed through both admission and concurrency.
	var completed atomic.Int32
	var wg sync.WaitGroup
	const tasks = 3
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			admitted, slotAcquired, err := simulateTaskPipeline(rac, cl, ResourceRender, 2)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !admitted || !slotAcquired {
				t.Errorf("after recovery: admitted=%v slot=%v, want both true", admitted, slotAcquired)
				return
			}
			completed.Add(1)
		}()
	}
	wg.Wait()

	if got := completed.Load(); got != tasks {
		t.Fatalf("after recovery: completed = %d, want %d", got, tasks)
	}
}

// TestIntegration_StopPlusAdmission verifies that when the admission
// controller is stopped, all tasks are rejected regardless of concurrency.
func TestIntegration_StopPlusAdmission(t *testing.T) {
	rac := NewResourceAdmissionController(fixedSampler(50<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(2)

	// Stop the admission controller.
	rac.Stop()

	// Task should be rejected by admission.
	decision := rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if decision != RejectStopped {
		t.Fatalf("expected RejectStopped, got %v", decision)
	}

	// Concurrency should still be available (unused).
	stats := cl.Stats()
	if stats.MaxActiveJobs != 2 {
		t.Fatalf("concurrency max = %d, want 2", stats.MaxActiveJobs)
	}
	if stats.ActiveJobs != 0 {
		t.Fatalf("concurrency active = %d, want 0", stats.ActiveJobs)
	}
}

// TestIntegration_PrefetchGateWhileRenderAdmitted verifies that prefetch
// can be rejected (80% threshold) while render is still admitted (93%).
func TestIntegration_PrefetchGateWhileRenderAdmitted(t *testing.T) {
	// 70 GiB total, 59 GiB RSS = 84.3% — above prefetch (80%), below render (93%).
	rac := NewResourceAdmissionController(fixedSampler(59<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(2)

	// Render: admitted.
	renderDecision := rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if renderDecision != Admit {
		t.Fatalf("render: expected Admit at 84.3%%, got %v", renderDecision)
	}

	// Prefetch: rejected.
	prefetchDecision := rac.CanAdmit(ResourceClaim{Kind: ResourcePrefetch})
	if prefetchDecision != RejectMemory {
		t.Fatalf("prefetch: expected RejectMemory at 84.3%%, got %v", prefetchDecision)
	}

	// Render task proceeds through the full pipeline.
	admitted, slotAcquired, err := simulateTaskPipeline(rac, cl, ResourceRender, 5)
	if err != nil {
		t.Fatalf("render task: %v", err)
	}
	if !admitted || !slotAcquired {
		t.Fatalf("render task: admitted=%v slot=%v, want both true", admitted, slotAcquired)
	}

	// Prefetch task is rejected — no concurrency slot consumed.
	admitted, slotAcquired, err = simulateTaskPipeline(rac, cl, ResourcePrefetch, 5)
	if err != nil {
		t.Fatalf("prefetch task: %v", err)
	}
	if admitted {
		t.Error("prefetch task: expected admission rejection")
	}
	if slotAcquired {
		t.Error("prefetch task: expected no concurrency slot")
	}

	stats := cl.Stats()
	if stats.TotalJobs != 1 {
		t.Fatalf("concurrency total = %d, want 1 (only render task)", stats.TotalJobs)
	}
}

// TestIntegration_ConcurrentAdmissionAndConcurrency verifies that multiple
// goroutines competing for both admission and concurrency slots produce
// correct counters.
func TestIntegration_ConcurrentAdmissionAndConcurrency(t *testing.T) {
	// 70 GiB total, 50 GiB RSS = 71.4% — below all thresholds.
	rac := NewResourceAdmissionController(fixedSampler(50<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(3)

	var admittedCount, rejectedCount atomic.Int32
	var wg sync.WaitGroup
	const goroutines = 20

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			admitted, _, _ := simulateTaskPipeline(rac, cl, ResourceRender, 1)
			if admitted {
				admittedCount.Add(1)
			} else {
				rejectedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	total := admittedCount.Load() + rejectedCount.Load()
	if total != goroutines {
		t.Fatalf("total = %d, want %d", total, goroutines)
	}
	// All should be admitted since RSS is below all thresholds.
	if rejectedCount.Load() != 0 {
		t.Fatalf("rejected = %d, want 0 (RSS below all thresholds)", rejectedCount.Load())
	}
	if admittedCount.Load() != goroutines {
		t.Fatalf("admitted = %d, want %d", admittedCount.Load(), goroutines)
	}
	// Concurrency total must match admitted count.
	stats := cl.Stats()
	if stats.TotalJobs != int64(goroutines) {
		t.Fatalf("concurrency total = %d, want %d", stats.TotalJobs, goroutines)
	}
}

// TestIntegration_AdmissionRejectionDoesNotLeakConcurrency verifies that
// when admission rejects AND concurrency is simultaneously full, no slot
// is leaked and the concurrency counter stays correct.
func TestIntegration_AdmissionRejectionDoesNotLeakConcurrency(t *testing.T) {
	// 70 GiB total, 66 GiB RSS = 94.3% — above render throttle.
	rac := NewResourceAdmissionController(fixedSampler(66<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(1)

	// Manually occupy the slot.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := cl.Acquire(ctx, "holder", 0); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Attempt admission-rejected tasks — should not affect concurrency.
	const rejectedTasks = 5
	for i := 0; i < rejectedTasks; i++ {
		admitted, slotAcquired, err := simulateTaskPipeline(rac, cl, ResourceRender, 1)
		if err != nil {
			t.Fatalf("task %d: %v", i, err)
		}
		if admitted {
			t.Errorf("task %d: expected admission rejection", i)
		}
		if slotAcquired {
			t.Errorf("task %d: expected no concurrency slot", i)
		}
	}

	// Release the held slot.
	cl.Release()

	// Concurrency should show exactly 1 total job (the holder).
	stats := cl.Stats()
	if stats.TotalJobs != 1 {
		t.Fatalf("concurrency total = %d, want 1 (rejected tasks must not consume slots)", stats.TotalJobs)
	}
	if stats.ActiveJobs != 0 {
		t.Fatalf("concurrency active = %d, want 0", stats.ActiveJobs)
	}
	if rac.AdmissionRejections() != rejectedTasks {
		t.Fatalf("admission rejections = %d, want %d", rac.AdmissionRejections(), rejectedTasks)
	}
}

// TestIntegration_PrefetchAndPublishIndependentThrottling verifies that
// prefetch and publish throttle states are independent — one being throttled
// does not affect the other.
func TestIntegration_PrefetchAndPublishIndependentThrottling(t *testing.T) {
	// 70 GiB total, 62 GiB RSS = 88.6% — above prefetch (80%) and publish (88%),
	// but below render (93%).
	rac := NewResourceAdmissionController(fixedSampler(62<<30), fixedTotalRAM(70<<30))
	cl := concurrency.NewConcurrencyLimiter(2)

	// Record prefetch rejection → prefetch throttled.
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, false)
	if !rac.IsThrottled(ResourcePrefetch) {
		t.Fatal("expected prefetch throttled")
	}

	// Publish is NOT yet throttled (hasn't been recorded).
	if rac.IsThrottled(ResourcePublish) {
		t.Fatal("publish should not be throttled yet (no RecordAdmissionResult)")
	}

	// Record publish rejection → publish throttled.
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePublish}, false)
	if !rac.IsThrottled(ResourcePublish) {
		t.Fatal("expected publish throttled")
	}

	// Prefetch still throttled.
	if !rac.IsThrottled(ResourcePrefetch) {
		t.Fatal("prefetch should still be throttled")
	}

	// Render is NOT throttled (below 93%).
	if rac.IsThrottled(ResourceRender) {
		t.Fatal("render should not be throttled at 88.6%")
	}

	// Render task proceeds.
	admitted, slotAcquired, err := simulateTaskPipeline(rac, cl, ResourceRender, 2)
	if err != nil {
		t.Fatalf("render task: %v", err)
	}
	if !admitted || !slotAcquired {
		t.Fatalf("render task: admitted=%v slot=%v, want both true", admitted, slotAcquired)
	}
}
