package remoteengine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRetrySchedule(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 5 * time.Second},
		{2, 15 * time.Second},
		{3, 30 * time.Second},
		{4, 60 * time.Second},
		{5, 5 * time.Minute},
		{10, 5 * time.Minute},
		{100, 5 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(string(rune('0'+tt.attempt)), func(t *testing.T) {
			if got := RetrySchedule(tt.attempt); got != tt.want {
				t.Fatalf("attempt %d: got %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}

	t.Run("negative attempt is treated as 0", func(t *testing.T) {
		if got := RetrySchedule(-1); got != 1*time.Second {
			t.Fatalf("negative: got %v, want 1s", got)
		}
	})
}

// ── AddJitter ────────────────────────────────────────────────────────────────

func TestAddJitter(t *testing.T) {
	base := 10 * time.Second

	for seed := int64(0); seed < 100; seed++ {
		jittered := AddJitter(base, seed)
		min := time.Duration(float64(base) * 0.8)
		max := time.Duration(float64(base) * 1.2)
		if jittered < min || jittered > max {
			t.Fatalf("seed %d: jittered %v outside [%v, %v]", seed, jittered, min, max)
		}
	}

	t.Run("zero duration returns zero", func(t *testing.T) {
		if d := AddJitter(0, 42); d != 0 {
			t.Fatalf("zero: got %v, want 0", d)
		}
	})

	t.Run("deterministic for same seed", func(t *testing.T) {
		d1 := AddJitter(base, 42)
		d2 := AddJitter(base, 42)
		if d1 != d2 {
			t.Fatalf("same seed should be deterministic: %v vs %v", d1, d2)
		}
	})
}

// ── truncateBody ─────────────────────────────────────────────────────────────

func TestTruncateBody(t *testing.T) {
	t.Run("short body unchanged", func(t *testing.T) {
		body := "hello"
		if got := truncateBody(body, 256); got != body {
			t.Fatalf("got %q, want %q", got, body)
		}
	})

	t.Run("long body truncated with ellipsis", func(t *testing.T) {
		body := strings.Repeat("x", 500)
		got := truncateBody(body, 256)
		// 256 bytes of "x" + 3 bytes for UTF-8 ellipsis (…) = 259 bytes total.
		if len(got) != 259 {
			t.Fatalf("length: got %d, want 259", len(got))
		}
		if !strings.HasSuffix(got, "…") {
			t.Fatalf("should end with ellipsis: %q", got)
		}
	})

	t.Run("multi-byte characters not split", func(t *testing.T) {
		// Each character is 3 bytes in UTF-8.
		body := strings.Repeat("€", 100)
		got := truncateBody(body, 50)
		runes := []rune(got)
		if len(runes) != 51 { // 50 € + 1 …
			t.Fatalf("rune count: got %d, want 51", len(runes))
		}
	})
}

// ── RetryPolicy ──────────────────────────────────────────────────────────────

func TestDefaultRetryPolicy(t *testing.T) {
	// Retries=5 means 1 initial attempt + 5 retries = 6 total attempts.
	p := DefaultRetryPolicy(5)
	if p.MaxAttempts != 6 {
		t.Fatalf("MaxAttempts: got %d, want 6", p.MaxAttempts)
	}
	if p.MaxMalformedAttempts != DefaultMalformedRetryLimit {
		t.Fatalf("MaxMalformedAttempts: got %d, want %d", p.MaxMalformedAttempts, DefaultMalformedRetryLimit)
	}

	// When maxRetries + 1 is less than the malformed limit, the malformed
	// limit is clamped to MaxAttempts so we never exceed the overall cap.
	p2 := DefaultRetryPolicy(1)
	if p2.MaxMalformedAttempts != 2 {
		t.Fatalf("clamped MaxMalformedAttempts: got %d, want 2", p2.MaxMalformedAttempts)
	}

	// Zero is an explicit no-retry budget (one total attempt).
	p3 := DefaultRetryPolicy(0)
	if p3.MaxAttempts != 1 {
		t.Fatalf("zero MaxAttempts: got %d, want 1", p3.MaxAttempts)
	}
}

func TestRetryPolicy_ShouldStop_Permanent(t *testing.T) {
	policy := DefaultRetryPolicy(5)

	tests := []RemoteErrorClass{
		RemoteErrorValidation,
		RemoteErrorAuthentication,
		RemoteErrorPermanent,
	}
	for _, class := range tests {
		t.Run(string(class), func(t *testing.T) {
			err := &RemoteError{Class: class, Message: "test"}
			got, stop := policy.ShouldStop(err, 0)
			if !stop {
				t.Fatal("ShouldStop should return true for permanent errors")
			}
			if got != err {
				t.Fatal("ShouldStop should return the same error for permanent")
			}
		})
	}
}

func TestRetryPolicy_ShouldStop_Transient(t *testing.T) {
	policy := DefaultRetryPolicy(5)
	err := &RemoteError{Class: RemoteErrorTransient, Message: "timeout"}
	got, stop := policy.ShouldStop(err, 0)
	if stop {
		t.Fatal("TRANSIENT should NOT stop")
	}
	if got != err {
		t.Fatal("ShouldStop should return the same error for transient")
	}
}

func TestRetryPolicy_ShouldStop_Malformed_LimitedRetry(t *testing.T) {
	policy := DefaultRetryPolicy(10) // MaxMalformedAttempts = 2

	// Simulate a realistic error from ClassifyDecodeError which wraps
	// ErrMalformedResponse in the Cause chain.
	err := &RemoteError{
		Class:   RemoteErrorMalformed,
		Code:    "DECODE",
		Message: "bad json",
		Cause:   fmt.Errorf("%w: %s", ErrMalformedResponse, "unexpected end of JSON"),
	}

	// Below the limit: keep retrying.
	_, stop := policy.ShouldStop(err, 0)
	if stop {
		t.Fatal("malformed attempt 0 should NOT stop")
	}
	_, stop = policy.ShouldStop(err, 1)
	if stop {
		t.Fatal("malformed attempt 1 should NOT stop")
	}

	// At the limit: promote to PERMANENT and stop.
	got, stop := policy.ShouldStop(err, 2)
	if !stop {
		t.Fatal("malformed attempt 2 should STOP")
	}

	var re *RemoteError
	if !errors.As(got, &re) {
		t.Fatalf("promoted error should be *RemoteError, got %T", got)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("promoted class: got %s, want PERMANENT", re.Class)
	}
	if re.Code != "DECODE_RETRY_EXCEEDED" {
		t.Fatalf("promoted code: got %s, want DECODE_RETRY_EXCEEDED", re.Code)
	}
	// The promoted error should wrap ErrMalformedRetryExceeded.
	if !errors.Is(got, ErrMalformedRetryExceeded) {
		t.Fatal("promoted error should wrap ErrMalformedRetryExceeded")
	}
	// The original cause should still be discoverable.
	if !errors.Is(got, ErrMalformedResponse) {
		t.Fatal("promoted error should still wrap ErrMalformedResponse")
	}
}

func TestRetryPolicy_ShouldStop_NilError(t *testing.T) {
	policy := DefaultRetryPolicy(3)
	got, stop := policy.ShouldStop(nil, 0)
	if stop {
		t.Fatal("nil error should NOT stop")
	}
	if got != nil {
		t.Fatalf("nil error should return nil, got %v", got)
	}
}
