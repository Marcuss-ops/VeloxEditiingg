package workerauthz

import "testing"

func TestAuthorizerDecisionMatrix(t *testing.T) {
	tests := []struct {
		name     string
		allowed  []string
		insecure bool
		worker   string
		want     bool
	}{
		{name: "exact match", allowed: []string{"worker-1"}, worker: "worker-1", want: true},
		{name: "unknown worker", allowed: []string{"worker-1"}, worker: "worker-2", want: false},
		{name: "trimmed both sides", allowed: []string{" worker-1 "}, worker: " worker-1 ", want: true},
		{name: "empty worker", allowed: []string{"worker-1"}, worker: "  ", want: false},
		{name: "empty production", worker: "worker-1", want: false},
		{name: "empty development", insecure: true, worker: "worker-1", want: true},
		{name: "wildcard production", allowed: []string{"*"}, worker: "worker-1", want: false},
		{name: "wildcard development", allowed: []string{"*"}, insecure: true, worker: "worker-1", want: true},
		{name: "wildcard with explicit IDs", allowed: []string{"*", "worker-1"}, worker: "worker-1", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.allowed, tt.insecure).IsAllowed(tt.worker); got != tt.want {
				t.Fatalf("IsAllowed(%q) = %v, want %v", tt.worker, got, tt.want)
			}
		})
	}
}
