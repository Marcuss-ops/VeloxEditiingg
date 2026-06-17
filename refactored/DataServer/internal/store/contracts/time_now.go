package contracts

import "time"

// realTimeNow is the wrapped time source; see contracts/now.go.
func realTimeNow() int64 { return time.Now().UnixNano() }
