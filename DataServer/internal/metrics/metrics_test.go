package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"velox-server/internal/taskattempts"
)

// ─── pre-existing tests ──────────────────────────────────────────────────

// TestNewCounterFamily_RejectsUnsafeLabel: the load-bearing guard
// rail — unsafe label keys (job_id, task_id, artifact_id, hash,
// sha256, etc.) MUST panic at registration time.
func TestNewCounterFamily_RejectsUnsafeLabel(t *testing.T) {
	for _, key := range []string{"job_id", "task_id", "artifact_id", "sha256", "video_title"} {
		func() {
			defer func() {
				rec := recover()
				if rec == nil {
					t.Errorf("expected panic on unsafe label %q", key)
				}
			}()
			_ = NewCounterFamily("velox_test_"+key, "x", []string{key})
		}()
	}
}

func TestRegistry_TextExposition(t *testing.T) {
	r := NewRegistry()
	cf := NewCounterFamily("velox_test_counter_total", "test", []string{"executor_id", "phase"})
	cf.Inc([]string{"scene.composite.v1", "render"}, 5)
	cf.Inc([]string{"scene.composite.v1", "encode"}, 1)
	r.Register(cf)

	gf := NewGaugeFamily("velox_test_gauge", "test", []string{"executor_id"})
	gf.GaugeSet([]string{"scene.composite.v1"}, 42)
	r.Register(gf)

	hf := NewHistogramFamily("velox_test_hist_seconds", "test", []string{"phase"}, []float64{0.5, 1, 5, 10})
	hf.Observe([]string{"render"}, 0.3)
	hf.Observe([]string{"render"}, 0.8)
	hf.Observe([]string{"render"}, 6)
	r.Register(hf)

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"# HELP velox_test_counter_total test",
		"# TYPE velox_test_counter_total counter",
		`velox_test_counter_total{executor_id="scene.composite.v1",phase="render"} 5`,
		`velox_test_counter_total{executor_id="scene.composite.v1",phase="encode"} 1`,
		"# HELP velox_test_gauge test",
		"# TYPE velox_test_gauge gauge",
		`velox_test_gauge{executor_id="scene.composite.v1"} 42`,
		"# HELP velox_test_hist_seconds test",
		"# TYPE velox_test_hist_seconds histogram",
		`velox_test_hist_seconds_bucket{phase="render",le="0.5"} 1`,
		`velox_test_hist_seconds_bucket{phase="render",le="1"} 2`,
		`velox_test_hist_seconds_bucket{phase="render",le="5"} 2`,
		`velox_test_hist_seconds_bucket{phase="render",le="10"} 3`,
		`velox_test_hist_seconds_bucket{phase="render",le="+Inf"} 3`,
		`velox_test_hist_seconds_count{phase="render"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRegistry_HTTPHandler(t *testing.T) {
	r := NewRegistry()
	cf := NewCounterFamily("velox_test_http_total", "x", nil)
	cf.Inc(nil, 7)
	r.Register(cf)

	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q (expected text/plain...)", ct)
	}
}

func TestRegistry_HTTPHandler_RejectsNonGET(t *testing.T) {
	r := NewRegistry()
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// TestHistogram_BucketsStrictlyIncreasing: catalog invariant.
func TestHistogram_BucketsStrictlyIncreasing(t *testing.T) {
	for _, bad := range [][]float64{{1, 1, 1}, {5, 4, 3}, {0.1, 0.5, 0.2}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("expected panic on bad buckets %v", bad)
				}
			}()
			_ = NewHistogramFamily("velox_test_bad", "x", nil, bad)
		}()
	}
}

// TestInc_WrongLabelLen_Panics: catalog invariant.
func TestInc_WrongLabelLen_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic on wrong label length")
		}
	}()
	cf := NewCounterFamily("velox_test_xx", "x", []string{"a", "b"})
	cf.Inc([]string{"only-one"}, 1)
}

// ─── F: compute-outcome refactor (spec §14) ──────────────────────────────

// TestComputeSeconds_MultipleOutcomesDistinct: the load-bearing test
// from spec §14. Drive all 4 terminal outcomes through
// RecordAttemptOutcome and assert:
//
//  1. exactly one child per (outcome) under velox_compute_seconds_total
//     — no cross-population; useful/failed/cancelled/stale are distinct
//     series under the SAME family;
//  2. each child carries the cumulative CPUTimeMS value we passed;
//  3. the sibling family velox_compute_failure_reasons_total has
//     exactly one child per FAILED attempt (keyed by errCode) and
//     zero children for the other 3 outcomes.
func TestComputeSeconds_MultipleOutcomesDistinct(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)

	// 4 terminals, each carrying a distinct CPUTimeMS so any series
	// mix-up is observable in the values themselves.
	c.RecordAttemptOutcome(taskattempts.AttemptStatusSucceeded, "", 120_000)         // ms
	c.RecordAttemptOutcome(taskattempts.AttemptStatusFailed, "RENDER_ERROR", 30_000) // ms
	c.RecordAttemptOutcome(taskattempts.AttemptStatusCancelled, "", 5_000)           // ms
	c.RecordAttemptOutcome(taskattempts.AttemptStatusTimedOut, "", 60_000)           // ms
	// Bonus: a second FAILED with a DIFFERENT reason code — the
	// sibling family must produce a 2nd distinct child.
	c.RecordAttemptOutcome(taskattempts.AttemptStatusFailed, "DISK_FULL", 12_000)

	cases := []struct {
		outcome string
		want    uint64
	}{
		{"useful", 120_000},
		{"failed", 30_000 + 12_000},
		{"cancelled", 5_000},
		{"stale", 60_000},
	}
	for _, tc := range cases {
		got := loadOutcomeSeconds(t, reg, tc.outcome)
		if got != tc.want {
			t.Errorf("compute_seconds{outcome=%q} = %d, want %d", tc.outcome, got, tc.want)
		}
	}

	// Failure reasons: 2 distinct reasons emitted.
	if got := loadFailureReasonCount(t, reg, "RENDER_ERROR"); got != 1 {
		t.Errorf("compute_failure_reasons{reason=RENDER_ERROR} = %d, want 1", got)
	}
	if got := loadFailureReasonCount(t, reg, "DISK_FULL"); got != 1 {
		t.Errorf("compute_failure_reasons{reason=DISK_FULL} = %d, want 1", got)
	}
}

// TestComputeSeconds_NonTerminal_NoEmit: PENDING / RUNNING attempts
// must NOT populate any outcome series — supervisor polls should
// never double-count via partial writes.
func TestComputeSeconds_NonTerminal_NoEmit(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	c.RecordAttemptOutcome(taskattempts.AttemptStatusPending, "", 999_999)
	c.RecordAttemptOutcome(taskattempts.AttemptStatusRunning, "", 999_999)

	out := dumpFamily(t, reg, "velox_compute_seconds_total")
	// No child rows should be emitted — empty children omit the
	// body entirely in our exposition format.
	if strings.Contains(out, "outcome=\"useful\"") ||
		strings.Contains(out, "outcome=\"failed\"") ||
		strings.Contains(out, "outcome=\"cancelled\"") ||
		strings.Contains(out, "outcome=\"stale\"") {
		t.Errorf("non-terminal statuses must not emit; got:\n%s", out)
	}
}

// TestComputeSeconds_FailedReasonEmptyMappedToUnknown: when status is
// FAILED but errCode is empty (legacy caller or task-attempt row
// missing ErrorCode), RecordAttemptOutcome must still bump the
// sibling family — using the key "unknown" so dashboards always have
// a stable sentinel.
func TestComputeSeconds_FailedReasonEmptyMappedToUnknown(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	c.RecordAttemptOutcome(taskattempts.AttemptStatusFailed, "", 50_000)
	if got := loadFailureReasonCount(t, reg, "unknown"); got != 1 {
		t.Errorf("compute_failure_reasons{reason=unknown} = %d, want 1", got)
	}
	if got := loadOutcomeSeconds(t, reg, "failed"); got != 50_000 {
		t.Errorf("compute_seconds{outcome=failed} = %d, want 50000", got)
	}
}

// TestComputeSeconds_ZeroCPU_NoEmitOnUseful: zero CPU is legal
// (worker crashed before reporting any compute); we don't bump the
// series with 0 to keep dashboards clean of "real 0" data points.
// Failure reasons are still bumped (count-of-failures is
// independent of duration).
//
// The OrZero-valued helper returns 0 for both "absent child" AND
// "child present with value 0". To prevent a future regression where
// a bug emits `Inc(0)` per call (instead of skipping on zero CPU),
// we ALSO assert the exposition contains no row for `outcome=useful`
// or `outcome=failed` — the strict absence check.
func TestComputeSeconds_ZeroCPU_NoEmitOnUseful(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	c.RecordAttemptOutcome(taskattempts.AttemptStatusSucceeded, "", 0)
	if got := loadOutcomeSecondsOrZero(t, reg, "useful"); got != 0 {
		t.Errorf("zero-CPU useful bump should stay 0; got %d", got)
	}
	c.RecordAttemptOutcome(taskattempts.AttemptStatusFailed, "OOM", 0)
	if got := loadOutcomeSecondsOrZero(t, reg, "failed"); got != 0 {
		t.Errorf("zero-CPU failed bump should stay 0 on seconds family; got %d", got)
	}
	if got := loadFailureReasonCount(t, reg, "OOM"); got != 1 {
		t.Errorf("zero-CPU failed must still bump reasons family; got %d", got)
	}
	// Strict absence assertion: a regression that bumps Inc(0) per
	// call would emit `outcome="useful" 0` and sneak past the OrZero
	// guard. Catch it here.
	out := dumpFamily(t, reg, "velox_compute_seconds_total")
	for _, banned := range []string{`outcome="useful"`, `outcome="failed"`} {
		if strings.Contains(out, banned) {
			t.Errorf("zero-CPU RecordAttemptOutcome leaked %s into exposition; regression:\n%s", banned, out)
		}
	}
}

// TestComputeSeconds_TextExposition_OneFamily: the unified wire
// shape — exactly one HELP/TYPE pair for the seconds family plus one
// HELP/TYPE pair for the reasons family; no legacy
// `_total_failed`/`_total_cancelled`/`_total_stale` rows.
func TestComputeSeconds_TextExposition_OneFamily(t *testing.T) {
	reg := NewRegistry()
	c := NewCollector(reg)
	c.RecordAttemptOutcome(taskattempts.AttemptStatusSucceeded, "", 1000)
	c.RecordAttemptOutcome(taskattempts.AttemptStatusFailed, "OOM", 2000)

	out := dumpFamily(t, reg, "velox_compute_seconds_total")
	for _, want := range []string{
		"# TYPE velox_compute_seconds_total counter",
		`velox_compute_seconds_total{outcome="useful"} 1000`,
		`velox_compute_seconds_total{outcome="failed"} 2000`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	for _, banned := range []string{
		"velox_compute_seconds_total_failed",
		"velox_compute_seconds_total_cancelled",
		"velox_compute_seconds_total_stale",
		"_total_useful",
	} {
		if strings.Contains(out, banned) {
			t.Errorf("legacy split-family name %q should be removed (spec §14 single family); found in:\n%s", banned, out)
		}
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────

// loadOutcomeSeconds reads the compute_seconds_total family and
// returns the cumulative value for the requested outcome child.
// Fails the test if the family or child is missing.
func loadOutcomeSeconds(t *testing.T, reg *Registry, outcome string) uint64 {
	t.Helper()
	v, ok := lookupChildValue(t, reg, "velox_compute_seconds_total",
		`velox_compute_seconds_total{outcome="`+outcome+`"} `)
	if !ok {
		t.Fatalf("compute_seconds{outcome=%q} missing", outcome)
	}
	return v
}

// loadOutcomeSecondsOrZero returns the cumulative value for the
// outcome child, OR 0 if the child is absent (zero-CPU emit is a
// valid no-op per RecordAttemptOutcome semantics).
func loadOutcomeSecondsOrZero(t *testing.T, reg *Registry, outcome string) uint64 {
	t.Helper()
	v, _ := lookupChildValue(t, reg, "velox_compute_seconds_total",
		`velox_compute_seconds_total{outcome="`+outcome+`"} `)
	return v
}

// loadFailureReasonCountOrZero is the same shape for the sibling
// reasons family.
func loadFailureReasonCountOrZero(t *testing.T, reg *Registry, reason string) uint64 {
	t.Helper()
	v, _ := lookupChildValue(t, reg, "velox_compute_failure_reasons_total",
		`velox_compute_failure_reasons_total{reason="`+reason+`"} `)
	return v
}

// loadFailureReasonCount is the strict variant (Fatalf on missing).
func loadFailureReasonCount(t *testing.T, reg *Registry, reason string) uint64 {
	t.Helper()
	v, ok := lookupChildValue(t, reg, "velox_compute_failure_reasons_total",
		`velox_compute_failure_reasons_total{reason="`+reason+`"} `)
	if !ok {
		t.Fatalf("compute_failure_reasons{reason=%q} missing", reason)
	}
	return v
}

// lookupChildValue walks out for the family registration line, then
// finds the named needle; returns the trailing numeric value plus
// presence bool. Single source of truth for child lookup so the
// strict / lenient helpers stay in lockstep.
func lookupChildValue(t *testing.T, reg *Registry, familyName, needle string) (uint64, bool) {
	t.Helper()
	out := dumpFamily(t, reg, familyName)
	idx := strings.Index(out, needle)
	if idx < 0 {
		return 0, false
	}
	tail := out[idx+len(needle):]
	nl := strings.IndexByte(tail, '\n')
	if nl < 0 {
		nl = len(tail)
	}
	valStr := strings.TrimSpace(tail[:nl])
	var n uint64
	if _, err := stdParseUint(valStr, &n); err != nil {
		t.Fatalf("parse child value %q for %q: %v", valStr, familyName, err)
	}
	return n, true
}

func dumpFamily(t *testing.T, reg *Registry, name string) string {
	t.Helper()
	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "# TYPE "+name+" ") {
		t.Fatalf("family %q not registered in registry output:\n%s", name, out)
	}
	return out
}

// stdParseUint is a tiny local uint parser to avoid importing strconv
// here just for test helpers; intentionally tolerant of trailing
// spaces and trailing newlines.
func stdParseUint(s string, out *uint64) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	var n uint64 = 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, &parseIntError{s: s}
		}
		n = n*10 + uint64(c-'0')
	}
	*out = n
	return len(s), nil
}

type parseIntError struct{ s string }

func (e *parseIntError) Error() string { return "parseUint: not a uint: " + e.s }
