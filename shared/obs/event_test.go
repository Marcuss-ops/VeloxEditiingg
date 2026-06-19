package obs

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEventStringJSON(t *testing.T) {
	e := NewEvent("TEST_EVENT").WithField("foo", "bar").WithError(nil)
	s := e.String()
	if !strings.Contains(s, `"event":"TEST_EVENT"`) {
		t.Fatalf("missing event field: %s", s)
	}
	if !strings.Contains(s, `"foo":"bar"`) {
		t.Fatalf("missing foo field: %s", s)
	}
	if strings.Contains(s, `"error"`) {
		t.Fatalf("nil error should not be serialized: %s", s)
	}
}

func TestEventWithErrorOmitsEmpty(t *testing.T) {
	e := NewEvent("X").WithError(nil)
	if _, ok := e.Fields["error"]; ok {
		t.Fatalf("nil error must not populate Fields[error]")
	}
}

func TestEventWithDuration(t *testing.T) {
	e := NewEvent("X").WithDuration(1500 * time.Millisecond)
	if v, _ := e.Fields["duration_ms"].(int64); v != 1500 {
		t.Fatalf("expected 1500ms, got %v", e.Fields["duration_ms"])
	}
}

func TestRateLimiterMilestones(t *testing.T) {
	rl := NewRateLimiter()
	expectedTrue := map[int]bool{1: true, 5: true, 10: true, 50: true, 100: true, 500: true, 1000: true, 2000: true, 3000: true}
	for i := 1; i <= 3000; i++ {
		emit, count := rl.ShouldLog("k")
		if count != i {
			t.Fatalf("counter desync at i=%d, got count=%d", i, count)
		}
		if expectedTrue[i] && !emit {
			t.Fatalf("expected emit at milestone %d", i)
		}
	}
}

func TestRateLimiterConcurrent(t *testing.T) {
	rl := NewRateLimiter()
	const goroutines = 50
	const perGoroutine = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_, c := rl.ShouldLog("kk")
				if c < 1 || c > goroutines*perGoroutine {
					t.Errorf("out-of-range count: %d", c)
				}
			}
		}()
	}
	wg.Wait()
	_, final := rl.ShouldLog("kk")
	if final != goroutines*perGoroutine+1 {
		t.Fatalf("expected total %d, got %d", goroutines*perGoroutine+1, final)
	}
}

func TestGlobalRateLimiterReset(t *testing.T) {
	rl := GlobalRateLimiter()
	_, n1 := rl.ShouldLog("test-reset-key")
	if n1 != 1 {
		t.Fatalf("expected 1, got %d", n1)
	}
	rl.Reset("test-reset-key")
	_, n2 := rl.ShouldLog("test-reset-key")
	if n2 != 1 {
		t.Fatalf("after reset expected 1, got %d", n2)
	}
}

// TestAPITransportEventCodes locks in that the cross-component EventCode
// constants exposed by obs have the well-known string values and no
// duplicates. Future typos (e.g. "API_RETRy") would be caught here.
func TestAPITransportEventCodes(t *testing.T) {
	cases := []struct {
		code EventCode
		want string
	}{
		{EventAPIRetry, "API_RETRY"},
		{EventAPISuccess, "API_SUCCESS"},
		{EventAPIError, "API_ERROR"},
	}
	seen := map[EventCode]bool{}
	for _, c := range cases {
		if c.code == "" {
			t.Fatalf("empty event code")
		}
		if string(c.code) != c.want {
			t.Errorf("expected %q, got %q", c.want, string(c.code))
		}
		if seen[c.code] {
			t.Errorf("duplicate event code: %s", c.code)
		}
		seen[c.code] = true
	}
}
