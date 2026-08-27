package worker

import (
	"sync"
	"testing"
)

// fixedSampler returns a closure that always returns the given RSS value.
func fixedSampler(rss int64) RSSSampler {
	return func() int64 { return rss }
}

// fixedTotalRAM returns a closure that always returns the given total RAM.
func fixedTotalRAM(total int64) TotalRAMBytesFunc {
	return func() int64 { return total }
}

func TestResourceAdmissionController_RenderAdmittedBelowThreshold(t *testing.T) {
	// 70 GiB total, 60 GiB RSS = 85.7% — below 93% render throttle.
	rac := NewResourceAdmissionController(fixedSampler(60<<30), fixedTotalRAM(70<<30))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourceRender, RAMBytes: 1 << 30})
	if dec != Admit {
		t.Fatalf("expected Admit, got %v", dec)
	}
}

func TestResourceAdmissionController_RenderRejectedAboveThreshold(t *testing.T) {
	// 70 GiB total, 66 GiB RSS = 94.3% — above 93% render throttle.
	rac := NewResourceAdmissionController(fixedSampler(66<<30), fixedTotalRAM(70<<30))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourceRender, RAMBytes: 1 << 30})
	if dec != RejectMemory {
		t.Fatalf("expected RejectMemory, got %v", dec)
	}
}

func TestResourceAdmissionController_PrefetchRejectedAboveThreshold(t *testing.T) {
	// 70 GiB total, 57 GiB RSS = 81.4% — above 80% prefetch throttle.
	rac := NewResourceAdmissionController(fixedSampler(57<<30), fixedTotalRAM(70<<30))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourcePrefetch, RAMBytes: 1 << 30})
	if dec != RejectMemory {
		t.Fatalf("expected RejectMemory, got %v", dec)
	}
}

func TestResourceAdmissionController_PrefetchAdmittedBelowThreshold(t *testing.T) {
	// 70 GiB total, 55 GiB RSS = 78.6% — below 80% prefetch throttle.
	rac := NewResourceAdmissionController(fixedSampler(55<<30), fixedTotalRAM(70<<30))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourcePrefetch, RAMBytes: 1 << 30})
	if dec != Admit {
		t.Fatalf("expected Admit, got %v", dec)
	}
}

func TestResourceAdmissionController_PublishRejectedAboveThreshold(t *testing.T) {
	// 70 GiB total, 62 GiB RSS = 88.6% — above 88% publish throttle.
	rac := NewResourceAdmissionController(fixedSampler(62<<30), fixedTotalRAM(70<<30))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourcePublish, RAMBytes: 1 << 30})
	if dec != RejectMemory {
		t.Fatalf("expected RejectMemory, got %v", dec)
	}
}

func TestResourceAdmissionController_PublishAdmittedBelowThreshold(t *testing.T) {
	// 70 GiB total, 61 GiB RSS = 87.1% — below 88% publish throttle.
	rac := NewResourceAdmissionController(fixedSampler(61<<30), fixedTotalRAM(70<<30))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourcePublish, RAMBytes: 1 << 30})
	if dec != Admit {
		t.Fatalf("expected Admit, got %v", dec)
	}
}

func TestResourceAdmissionController_RejectedStopIfStopped(t *testing.T) {
	rac := NewResourceAdmissionController(fixedSampler(0), fixedTotalRAM(70<<30))
	rac.Stop()
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if dec != RejectStopped {
		t.Fatalf("expected RejectStopped, got %v", dec)
	}
}

func TestResourceAdmissionController_AdmitWithZeroTotalRAM(t *testing.T) {
	// Unknown total RAM — fail open.
	rac := NewResourceAdmissionController(fixedSampler(99<<30), fixedTotalRAM(0))
	dec := rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if dec != Admit {
		t.Fatalf("expected Admit (fail open on unknown total RAM), got %v", dec)
	}
}

func TestResourceAdmissionController_HysteresisRenderRecovery(t *testing.T) {
	total := int64(70 << 30)

	// Phase 1: RSS = 66 GiB (94.3%) → above 93% throttle
	rss := int64(66 << 30)
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	// Admitted (CanAdmit is side-effect-free).
	if dec := rac.CanAdmit(ResourceClaim{Kind: ResourceRender}); dec != RejectMemory {
		t.Fatalf("phase 1: expected RejectMemory, got %v", dec)
	}

	// Record result → backpressure event emitted, throttle activated.
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, false)
	if !rac.IsThrottled(ResourceRender) {
		t.Fatal("expected render throttled after high RSS")
	}
	if rac.BackpressureEvents() != 1 {
		t.Fatalf("expected 1 backpressure event, got %d", rac.BackpressureEvents())
	}
	if rac.AdmissionRejections() != 1 {
		t.Fatalf("expected 1 admission rejection, got %d", rac.AdmissionRejections())
	}

	// Phase 2: RSS drops to 60 GiB (85.7%) — above recovery threshold (83%)
	rss = 60 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, true)
	if !rac.IsThrottled(ResourceRender) {
		t.Fatal("expected render still throttled at 85.7% (recovery threshold is 83%)")
	}

	// Phase 3: RSS drops to 57 GiB (81.4%) — below recovery threshold (83%)
	rss = 57 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, true)
	if rac.IsThrottled(ResourceRender) {
		t.Fatal("expected render recovered at 81.4% (below 83% recovery threshold)")
	}

	// No new backpressure event on recovery.
	if rac.BackpressureEvents() != 1 {
		t.Fatalf("expected still 1 backpressure event after recovery, got %d", rac.BackpressureEvents())
	}
}

func TestResourceAdmissionController_HysteresisPrefetchRecovery(t *testing.T) {
	total := int64(70 << 30)

	// Phase 1: RSS = 57 GiB (81.4%) → above 80% throttle
	rss := int64(57 << 30)
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, false)
	if !rac.IsThrottled(ResourcePrefetch) {
		t.Fatal("expected prefetch throttled")
	}

	// Phase 2: RSS drops to 51 GiB (72.9%) — above recovery threshold (70%)
	rss = 51 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, true)
	if !rac.IsThrottled(ResourcePrefetch) {
		t.Fatal("expected prefetch still throttled at 72.9% (recovery threshold is 70%)")
	}

	// Phase 3: RSS drops to 48 GiB (68.6%) — below recovery threshold (70%)
	rss = 48 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, true)
	if rac.IsThrottled(ResourcePrefetch) {
		t.Fatal("expected prefetch recovered at 68.6% (below 70% recovery threshold)")
	}
}

func TestResourceAdmissionController_HysteresisPublishRecovery(t *testing.T) {
	total := int64(70 << 30)

	// Phase 1: RSS = 62 GiB (88.6%) → above 88% throttle
	rss := int64(62 << 30)
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePublish}, false)
	if !rac.IsThrottled(ResourcePublish) {
		t.Fatal("expected publish throttled")
	}

	// Phase 2: RSS drops to 56 GiB (80%) — above recovery threshold (78%)
	rss = 56 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePublish}, true)
	if !rac.IsThrottled(ResourcePublish) {
		t.Fatal("expected publish still throttled at 80% (recovery threshold is 78%)")
	}

	// Phase 3: RSS drops to 53 GiB (75.7%) — below recovery threshold (78%)
	rss = 53 << 30
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePublish}, true)
	if rac.IsThrottled(ResourcePublish) {
		t.Fatal("expected publish recovered at 75.7% (below 78% recovery threshold)")
	}
}

func TestResourceAdmissionController_BackpressureNotDoubleCounted(t *testing.T) {
	total := int64(70 << 30)
	rss := int64(66 << 30)
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	// Multiple rejections should only emit one backpressure event.
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, false)
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, false)
	rac.RecordAdmissionResult(ResourceClaim{Kind: ResourcePrefetch}, false)

	if rac.BackpressureEvents() != 1 {
		t.Fatalf("expected 1 backpressure event (not triple-counted), got %d", rac.BackpressureEvents())
	}
	if rac.AdmissionRejections() != 3 {
		t.Fatalf("expected 3 admission rejections, got %d", rac.AdmissionRejections())
	}
}

func TestResourceAdmissionController_PeakRSSTracking(t *testing.T) {
	total := int64(70 << 30)
	rss := int64(50 << 30)
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	// First check sets peak.
	rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if peak := rac.PeakRSSBytes(); peak != 50<<30 {
		t.Fatalf("expected peak 50 GiB, got %d", peak)
	}

	// Higher RSS updates peak.
	rss = 60 << 30
	rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if peak := rac.PeakRSSBytes(); peak != 60<<30 {
		t.Fatalf("expected peak 60 GiB, got %d", peak)
	}

	// Lower RSS does NOT reduce peak.
	rss = 45 << 30
	rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
	if peak := rac.PeakRSSBytes(); peak != 60<<30 {
		t.Fatalf("expected peak still 60 GiB (high-water mark), got %d", peak)
	}
}

func TestResourceAdmissionController_ConcurrentAdmissionChecks(t *testing.T) {
	total := int64(70 << 30)
	rss := int64(55 << 30) // 78.6% — below all throttle thresholds
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	var wg sync.WaitGroup
	const goroutines = 100
	results := make([]AdmissionDecision, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = rac.CanAdmit(ResourceClaim{Kind: ResourceRender})
		}(i)
	}
	wg.Wait()

	for i, dec := range results {
		if dec != Admit {
			t.Fatalf("goroutine %d: expected Admit, got %v", i, dec)
		}
	}
}

func TestResourceAdmissionController_ConcurrentThrottleTransitions(t *testing.T) {
	total := int64(70 << 30)
	rss := int64(66 << 30) // 94.3% — above all throttle thresholds
	rac := NewResourceAdmissionController(func() int64 { return rss }, fixedTotalRAM(total))

	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rac.RecordAdmissionResult(ResourceClaim{Kind: ResourceRender}, false)
		}()
	}
	wg.Wait()

	// Must emit exactly 1 backpressure event despite concurrent calls.
	if events := rac.BackpressureEvents(); events != 1 {
		t.Fatalf("expected 1 backpressure event, got %d", events)
	}
	if rejections := rac.AdmissionRejections(); rejections != goroutines {
		t.Fatalf("expected %d admission rejections, got %d", goroutines, rejections)
	}
}

func TestResourceAdmissionController_StopCh(t *testing.T) {
	rac := NewResourceAdmissionController(fixedSampler(0), fixedTotalRAM(70<<30))

	select {
	case <-rac.StopCh():
		t.Fatal("StopCh should not be closed before Stop()")
	default:
	}

	rac.Stop()

	select {
	case <-rac.StopCh():
		// Expected.
	default:
		t.Fatal("StopCh should be closed after Stop()")
	}
}

func TestResourceAdmissionController_RSSPressurePercent(t *testing.T) {
	rac := NewResourceAdmissionController(fixedSampler(63<<30), fixedTotalRAM(70<<30))
	pct := rac.RSSPressurePercent()
	if pct < 89.9 || pct > 90.1 {
		t.Fatalf("expected ~90%%, got %.1f%%", pct)
	}
}

func TestResourceAdmissionController_RSSPressurePercentUnknownTotal(t *testing.T) {
	rac := NewResourceAdmissionController(fixedSampler(63<<30), fixedTotalRAM(0))
	pct := rac.RSSPressurePercent()
	if pct != -1 {
		t.Fatalf("expected -1 for unknown total RAM, got %.1f", pct)
	}
}

func TestResourceKind_String(t *testing.T) {
	tests := []struct {
		kind ResourceKind
		want string
	}{
		{ResourceRender, "RENDER"},
		{ResourcePrefetch, "PREFETCH"},
		{ResourcePublish, "PUBLISH"},
		{ResourceKind(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("ResourceKind(%d).String() = %q, want %q", tt.kind, got, tt.want)
		}
	}
}

func TestAdmissionDecision_String(t *testing.T) {
	tests := []struct {
		dec  AdmissionDecision
		want string
	}{
		{Admit, "ADMIT"},
		{RejectMemory, "REJECT_MEMORY"},
		{RejectStopped, "REJECT_STOPPED"},
		{AdmissionDecision(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.dec.String(); got != tt.want {
			t.Errorf("AdmissionDecision(%d).String() = %q, want %q", tt.dec, got, tt.want)
		}
	}
}
