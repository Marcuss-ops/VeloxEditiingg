package grpcserver

import "testing"

func TestPrefetchSkipCounter_Empty(t *testing.T) {
	c := newPrefetchSkipCounter()
	if c.total() != 0 {
		t.Errorf("total() = %d, want 0", c.total())
	}
	if s := c.summary(); s != "{}" {
		t.Errorf("summary() = %q, want {}", s)
	}
}

func TestPrefetchSkipCounter_SingleReason(t *testing.T) {
	c := newPrefetchSkipCounter()
	c.add(SkipReservedByOther)
	c.add(SkipReservedByOther)
	if c.total() != 2 {
		t.Errorf("total() = %d, want 2", c.total())
	}
	want := `{"reserved_by_other":2}`
	if s := c.summary(); s != want {
		t.Errorf("summary() = %q, want %q", s, want)
	}
}

func TestPrefetchSkipCounter_MultipleReasons_SortedKeys(t *testing.T) {
	c := newPrefetchSkipCounter()
	c.add(SkipReservationConflict)
	c.add(SkipDifferentWarmWorker)
	c.add(SkipPayloadUnavailable)
	c.add(SkipDifferentWarmWorker)
	if c.total() != 4 {
		t.Errorf("total() = %d, want 4", c.total())
	}
	want := `{"different_warm_worker":2,"payload_unavailable":1,"reservation_conflict":1}`
	if s := c.summary(); s != want {
		t.Errorf("summary() = %q, want %q", s, want)
	}
}
