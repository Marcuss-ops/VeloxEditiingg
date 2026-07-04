package supervisor

// supervisor_policy_test.go
//
// Tests for the supervisor-policy helpers: effectiveMaxRetries
// (per-class MaxRetries mapping) + shouldExitAfterFailure (the
// canonical exit-after-failure rule matrix).
import "testing"

// TestEffectiveMaxRetries maps RunnerClass × raw MaxRetries through
// the canonical effectiveMaxRetries helper.
func TestEffectiveMaxRetries(t *testing.T) {
	cases := []struct {
		class RunnerClass
		n     int
		want  int
	}{
		{ClassOneShot, 5, 0},
		{ClassOneShot, 0, 0},
		{ClassRestartable, 5, 5},
		{ClassRestartable, 0, 0},
		{ClassRestartable, -1, 0},
		{ClassCritical, 0, -1},
		{ClassCritical, 7, 7},
		{ClassCritical, -1, -1},
		{RunnerClass(99), 5, 5}, // unknown class falls through
	}
	for _, tc := range cases {
		got := effectiveMaxRetries(tc.class, tc.n)
		if got != tc.want {
			t.Errorf("effectiveMaxRetries(%s, %d) = %d, want %d", tc.class, tc.n, got, tc.want)
		}
	}
}

// TestShouldExitAfterFailure pins the exit-after-failure rule matrix.
// ClassRestartable with MaxRetries=0 must exit on first error (the bug
// fix from the old `if maxR > 0 && attempt > maxR` short-circuit).
func TestShouldExitAfterFailure(t *testing.T) {
	cases := []struct {
		name       string
		class      RunnerClass
		maxRetries int
		attempt    int
		want       bool
	}{
		// ClassOneShot: always exit.
		{"OneShot/maxRetries=5/attempt=1", ClassOneShot, 5, 1, true},
		{"OneShot/maxRetries=0/attempt=1", ClassOneShot, 0, 1, true},
		{"OneShot/maxRetries=5/attempt=10", ClassOneShot, 5, 10, true},

		// ClassRestartable: zero/negative → exit on first error.
		{"Restartable/maxRetries=0/attempt=1", ClassRestartable, 0, 1, true},
		{"Restartable/maxRetries=0/attempt=5", ClassRestartable, 0, 5, true},
		{"Restartable/maxRetries=-1/attempt=1", ClassRestartable, -1, 1, true},
		// ClassRestartable with positive budget: exit when attempt exceeds it.
		{"Restartable/maxRetries=3/attempt=1", ClassRestartable, 3, 1, false},
		{"Restartable/maxRetries=3/attempt=3", ClassRestartable, 3, 3, false},
		{"Restartable/maxRetries=3/attempt=4", ClassRestartable, 3, 4, true},
		{"Restartable/maxRetries=3/attempt=10", ClassRestartable, 3, 10, true},

		// ClassCritical: zero/negative → never exit (ctx cancellation only).
		{"Critical/maxRetries=0/attempt=1", ClassCritical, 0, 1, false},
		{"Critical/maxRetries=0/attempt=1000", ClassCritical, 0, 1000, false},
		{"Critical/maxRetries=-1/attempt=1", ClassCritical, -1, 1, false},
		// ClassCritical with positive budget: exit when attempt exceeds it.
		{"Critical/maxRetries=2/attempt=1", ClassCritical, 2, 1, false},
		{"Critical/maxRetries=2/attempt=2", ClassCritical, 2, 2, false},
		{"Critical/maxRetries=2/attempt=3", ClassCritical, 2, 3, true},

		// Defensive: unknown class exits immediately.
		{"Unknown/maxRetries=5/attempt=1", RunnerClass(99), 5, 1, true},
		{"Unknown/maxRetries=0/attempt=1", RunnerClass(99), 0, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldExitAfterFailure(tc.class, tc.maxRetries, tc.attempt)
			if got != tc.want {
				t.Errorf("shouldExitAfterFailure(%s, maxRetries=%d, attempt=%d) = %v, want %v",
					tc.class, tc.maxRetries, tc.attempt, got, tc.want)
			}
		})
	}
}
