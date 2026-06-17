package deprecation_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/deprecation"
)

// (atomic.Int64 imported above is used by errCount on the test side; the
// Registry's counters live under its own sync.RWMutex.)


// TestTrackAtomicityUnderConcurrency drives a single Track-wrapped endpoint
// with N concurrent goroutines and asserts that every increment lands
// (Hits == N). Designed to be run under `go test -race ./...`: the -race
// detector flags any unsynchronized access on Registry counters.
//
// We exercise:
//   - the Registry.mu write-lock path inside Track (hits++ / errors++ /
//     firstHitNS / lastHitNS / lastClient updates),
//   - the RWMutex.RLock path inside Snapshot (read-side counter copy),
//   - gin's ServeHTTP which is safe for concurrent use.
//
// Each goroutine creates its own *httptest.ResponseRecorder and *http.Request
// so only the Registry is contended — exactly what production code looks like.
func TestTrackAtomicityUnderConcurrency(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const N = 100
	now := time.Now().UTC()
	reg := deprecation.New(now, now.Add(72*time.Hour))
	reg.Register("POST", "/api/legacy/concurrency-stress", "/api/v2/concurrency-stress")

	r := gin.New()
	r.POST("/api/legacy/concurrency-stress",
		reg.Track("POST", "/api/legacy/concurrency-stress"),
		func(c *gin.Context) {
			c.String(http.StatusOK, "ok")
		})

	var (
		wg       sync.WaitGroup
		errCount atomic.Int64
	)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/api/legacy/concurrency-stress", nil)
			// Distinct RemoteAddr per goroutine so LastClient races contend too.
			req.RemoteAddr = fmt.Sprintf("10.0.0.%d:54321", (idx%253)+1)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if got := errCount.Load(); got != 0 {
		t.Fatalf("%d/%d goroutines got non-200 status", got, N)
	}

	snap := reg.Snapshot()
	if len(snap.Stats) != 1 {
		t.Fatalf("stats entries = %d, want 1", len(snap.Stats))
	}

	stat := snap.Stats[0]
	if stat.Hits != int64(N) {
		t.Errorf("Hits = %d, want %d (lost increments under concurrency — RWMutex contract broken)",
			stat.Hits, N)
	}
	if stat.Errors != 0 {
		t.Errorf("Errors = %d, want 0 (all responses were 200 OK)", stat.Errors)
	}
	if stat.FirstHitAt == "" {
		t.Errorf("FirstHitAt must be set after %d concurrent hits", N)
	}
	if stat.LastHitAt == "" {
		t.Errorf("LastHitAt must be set after %d concurrent hits", N)
	}
}
