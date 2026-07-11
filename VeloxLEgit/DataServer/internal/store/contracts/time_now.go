package contracts

import "time"

func init() {
	realTimeNow = func() int64 { return time.Now().UnixNano() }
	// Re-bind realNano so nanoNow() sees the real source regardless of init order.
	realNano = realTimeNow
}
