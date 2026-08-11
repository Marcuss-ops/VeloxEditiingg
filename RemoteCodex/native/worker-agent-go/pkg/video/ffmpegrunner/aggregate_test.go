package ffmpegrunner

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestAggregate_TotalsAndOperationBreakdown(t *testing.T) {
	profiles := []FFmpegResult{
		{Operation: OperationCompose, ProcessSpawnMS: 10, FirstOutputMS: 40, ProcessingMS: 500, ExitWaitMS: 2, ProcessWallMS: 545, UserCPUMs: 300, SystemCPUMs: 20, PeakRSSBytes: 1 << 20, ReadBytes: 100, WriteBytes: 200},
		{Operation: OperationCompose, ProcessSpawnMS: 12, FirstOutputMS: 45, ProcessingMS: 480, ExitWaitMS: 3, ProcessWallMS: 535, UserCPUMs: 290, SystemCPUMs: 15, PeakRSSBytes: 2 << 20, ReadBytes: 110, WriteBytes: 210},
		{Operation: OperationEncode, ProcessSpawnMS: 8, FirstOutputMS: 60, ProcessingMS: 900, ExitWaitMS: 4, ProcessWallMS: 970, UserCPUMs: 700, SystemCPUMs: 50, PeakRSSBytes: 3 << 20, ReadBytes: 500, WriteBytes: 400},
	}
	got := Aggregate(profiles)
	if got.ProcessCount != 3 {
		t.Errorf("ProcessCount = %d, want 3", got.ProcessCount)
	}
	if got.TotalSpawnMS != 30 || got.TotalFirstOutputMS != 145 || got.TotalProcessingMS != 1880 {
		t.Errorf("phase totals = spawn %d first_output %d processing %d, want 30/145/1880", got.TotalSpawnMS, got.TotalFirstOutputMS, got.TotalProcessingMS)
	}
	if got.TotalExitWaitMS != 9 || got.TotalWallMS != 2050 {
		t.Errorf("totals = exit_wait %d wall %d, want 9/2050", got.TotalExitWaitMS, got.TotalWallMS)
	}
	if got.TotalUserCPUMs != 1290 || got.TotalSystemCPUMs != 85 {
		t.Errorf("cpu totals = user %d sys %d, want 1290/85", got.TotalUserCPUMs, got.TotalSystemCPUMs)
	}
	if got.PeakRSSBytes != 3<<20 {
		t.Errorf("PeakRSSBytes = %d, want max 3<<20", got.PeakRSSBytes)
	}
	if got.TotalReadBytes != 710 || got.TotalWriteBytes != 810 {
		t.Errorf("io totals = read %d write %d, want 710/810", got.TotalReadBytes, got.TotalWriteBytes)
	}
	compose := got.Operations[string(OperationCompose)]
	if compose.ProcessCount != 2 || compose.TotalProcessingMS != 980 {
		t.Errorf("compose breakdown = count %d processing %d, want 2/980", compose.ProcessCount, compose.TotalProcessingMS)
	}
	encode := got.Operations[string(OperationEncode)]
	if encode.ProcessCount != 1 || encode.TotalProcessingMS != 900 {
		t.Errorf("encode breakdown = count %d processing %d, want 1/900", encode.ProcessCount, encode.TotalProcessingMS)
	}
}

func TestAggregate_Empty(t *testing.T) {
	got := Aggregate(nil)
	if got.ProcessCount != 0 || len(got.Operations) != 0 {
		t.Errorf("Aggregate(nil) = %+v, want zero aggregate", got)
	}
}

func TestAggregate_UnknownOperationBucketsToUnknown(t *testing.T) {
	got := Aggregate([]FFmpegResult{{ProcessWallMS: 10}})
	if got.ProcessCount != 1 {
		t.Fatalf("ProcessCount = %d, want 1", got.ProcessCount)
	}
	if _, ok := got.Operations["unknown"]; !ok {
		t.Errorf("Operations = %v, want an 'unknown' bucket", got.Operations)
	}
}

func TestProfileAggregate_JSONSafeFlatShape(t *testing.T) {
	got := Aggregate([]FFmpegResult{
		{Operation: OperationCompose, ProcessSpawnMS: 10, FirstOutputMS: 40, ProcessingMS: 500, ExitWaitMS: 2, ProcessWallMS: 545},
	})
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal aggregate: %v", err)
	}
	var object map[string]interface{}
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatalf("unmarshal aggregate: %v", err)
	}
	// Embedded totals must flatten, not nest under profile_totals.
	if _, ok := object["total_spawn_ms"]; !ok {
		t.Errorf("flat shape missing total_spawn_ms: %s", data)
	}
	if _, ok := object["process_count"]; !ok {
		t.Errorf("flat shape missing process_count: %s", data)
	}
	if _, ok := object["operations"]; !ok {
		t.Errorf("flat shape missing operations breakdown: %s", data)
	}
}

func TestAggregator_AccumulatesAndResets(t *testing.T) {
	agg := NewAggregator()
	if agg.ProcessCount() != 0 {
		t.Fatalf("fresh ProcessCount = %d, want 0", agg.ProcessCount())
	}
	agg.Add(FFmpegResult{Operation: OperationCompose, ProcessWallMS: 100})
	agg.Add(FFmpegResult{Operation: OperationCompose, ProcessWallMS: 200})
	if agg.ProcessCount() != 2 {
		t.Errorf("ProcessCount = %d, want 2", agg.ProcessCount())
	}
	if got := agg.Aggregate(); got.TotalWallMS != 300 || got.ProcessCount != 2 {
		t.Errorf("Aggregate = %+v, want wall 300 / count 2", got)
	}
	agg.Reset()
	if agg.ProcessCount() != 0 {
		t.Errorf("after Reset ProcessCount = %d, want 0", agg.ProcessCount())
	}
}

func TestAggregator_ConcurrentAdd(t *testing.T) {
	agg := NewAggregator()
	const goroutines = 8
	const perGoroutine = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				agg.Add(FFmpegResult{Operation: OperationCompose, ProcessWallMS: 1})
			}
		}()
	}
	wg.Wait()
	if got := agg.ProcessCount(); got != goroutines*perGoroutine {
		t.Errorf("ProcessCount = %d, want %d (concurrent adds must not lose entries)", got, goroutines*perGoroutine)
	}
	if got := agg.Aggregate(); got.TotalWallMS != int64(goroutines*perGoroutine) {
		t.Errorf("TotalWallMS = %d, want %d", got.TotalWallMS, goroutines*perGoroutine)
	}
}

func TestAggregator_NilSafe(t *testing.T) {
	var agg *Aggregator
	agg.Add(FFmpegResult{})
	if agg.ProcessCount() != 0 {
		t.Errorf("nil ProcessCount = %d, want 0", agg.ProcessCount())
	}
	if got := agg.Aggregate(); got.ProcessCount != 0 {
		t.Errorf("nil Aggregate = %+v, want zero", got)
	}
}
