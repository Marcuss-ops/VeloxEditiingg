package contracts

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// nanoNow is kept as a small indirection so package tests can replace the
// source locally without relying on file-order-dependent init functions.
func nanoNow() int64 {
	return realNano()
}

// realTimeNow is the production time source. It remains a variable so the
// package-level compatibility seam used by tests is unchanged.
var realTimeNow = func() int64 {
	return time.Now().UnixNano()
}

// realNano is the source used by nanoNow. It is initialized directly from the
// production source, so it is never transiently nil and does not depend on
// init ordering across files.
var realNano = realTimeNow

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
