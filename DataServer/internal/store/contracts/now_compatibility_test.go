package contracts

import (
	"testing"
	"time"
)

func TestNanoNowCompatibilityUsesRealNano(t *testing.T) {
	original := realNano
	t.Cleanup(func() { realNano = original })

	const want int64 = 123456789
	realNano = func() int64 { return want }
	if got := nanoNow(); got != want {
		t.Fatalf("nanoNow() = %d, want %d", got, want)
	}
}

func TestRealTimeSourcesAreInitializedAndRecent(t *testing.T) {
	before := time.Now().UnixNano()
	nano := realNano()
	timestamp := realTimeNow()
	after := time.Now().UnixNano()

	if nano < before || nano > after {
		t.Fatalf("realNano() = %d, want value in [%d, %d]", nano, before, after)
	}
	if timestamp < before || timestamp > after {
		t.Fatalf("realTimeNow() = %d, want value in [%d, %d]", timestamp, before, after)
	}
}

func TestRealNanoAndRealTimeNowRemainCallableIndirections(t *testing.T) {
	if realNano == nil {
		t.Fatal("realNano must be initialized")
	}
	if realTimeNow == nil {
		t.Fatal("realTimeNow must be initialized")
	}
}
