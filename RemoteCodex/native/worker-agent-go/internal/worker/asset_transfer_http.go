package worker

import (
	"net/http"
	"time"
)

var assetTransport = &http.Transport{
	MaxIdleConns:        128,
	MaxIdleConnsPerHost: 64,
	IdleConnTimeout:     90 * time.Second,
	ForceAttemptHTTP2:   true,
}
