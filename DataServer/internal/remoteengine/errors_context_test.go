package remoteengine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWithRetry_TransientExactAttempts(t *testing.T) {
	// Retries=3 means 1 initial attempt + 3 retries = 4 total attempts.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 3})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_503",
			Message: "server busy",
		}
	})

	if callCount != 4 {
		t.Fatalf("callCount: got %d, want 4", callCount)
	}

	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorTransient {
		t.Fatalf("class: got %s, want TRANSIENT", re.Class)
	}
}

func TestWithRetry_RetriesEqualsOneMeansTwoAttempts(t *testing.T) {
	// Retries=1 means 1 initial attempt + 1 retry = 2 total attempts.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 1})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_503",
			Message: "server busy",
		}
	})

	if callCount != 2 {
		t.Fatalf("callCount: got %d, want 2", callCount)
	}
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
}

func TestWithRetry_UntypedErrorStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return errors.New("untyped failure")
	})

	if callCount != 1 {
		t.Fatalf("untyped error should only call fn once, got %d", callCount)
	}
	if err == nil || err.Error() != "untyped failure" {
		t.Fatalf("expected untyped error, got %v", err)
	}
}

func TestWithRetry_ContextCanceledStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	callCount := 0
	err := client.withRetry(ctx, func(attempt int) error {
		callCount++
		return nil
	})

	if callCount != 0 {
		t.Fatalf("canceled context should not call fn, got %d", callCount)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWithRetry_ContextDeadlineExceededStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return context.DeadlineExceeded
	})

	if callCount != 1 {
		t.Fatalf("context.DeadlineExceeded should only call fn once, got %d", callCount)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestWithRetry_MaxDuration(t *testing.T) {
	// Backoff schedule starts at 1s; with a 100ms context timeout the
	// retry loop should return before the first backoff completes.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	callCount := 0
	start := time.Now()
	err := client.withRetry(ctx, func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_503",
			Message: "server busy",
		}
	})
	elapsed := time.Since(start)

	if callCount < 1 {
		t.Fatal("expected at least one attempt")
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("expected to stop near context timeout, took %v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// ── Integration: 401 Authentication ──────────────────────────────────────────

func TestClassifyHTTPResponse_401_Authentication(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"invalid token"}`))
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)

	re := classifyHTTPResponse(resp, body[:n], nil)
	if re.Class != RemoteErrorAuthentication {
		t.Fatalf("class: got %s, want AUTHENTICATION", re.Class)
	}
	if re.IsRetryable() {
		t.Fatal("AUTHENTICATION should NOT be retryable")
	}
	if !re.IsPermanent() {
		t.Fatal("AUTHENTICATION should be permanent")
	}
}
