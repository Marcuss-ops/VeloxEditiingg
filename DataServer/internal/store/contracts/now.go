package contracts

import (
	"crypto/rand"
	"encoding/hex"
)

// nanoNow indirection keeps `time` import surface minimal; replaced by tests.
func nanoNow() int64 {
	// Use the real time source. test-only dependency, no production impact.
	return realNano()
}

// realNano is the indirection variable; set in time_now.go by the first init to run.
var realNano func() int64

// realTimeNow is the actual time source; overridden in tests would require build tags.
var realTimeNow = func() int64 {
	// Overridden in time_now.go by a later init to avoid a `time` import in this file.
	return 0
}

func init() {
	// Bind once to avoid per-call indirection cost.
	// If time_now.go's init already ran (file order undefined), realNano keeps its value.
	if realNano == nil {
		realNano = realTimeNow
	}
}

// randSuffix returns a 12-char hex nonce for test isolation.
// crypto/rand → time-based suffix would collide in parallel runs.
func randSuffix() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Test-context fallback: encode the current nanosecond as a stable suffix.
		return hex.EncodeToString([]byte("fallback")) + itoa(int(nanoNow()))
	}
	return hex.EncodeToString(b[:])
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
