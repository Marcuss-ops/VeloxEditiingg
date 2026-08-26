package worker

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"velox-worker-agent/internal/telemetry"
)

var assetTransport = &http.Transport{
	MaxIdleConns:        128,
	MaxIdleConnsPerHost: 64,
	IdleConnTimeout:     90 * time.Second,
	ForceAttemptHTTP2:   true,
}

var assetHTTPMetricsMu sync.Mutex
var assetHTTPMetrics struct {
	requests int64
	reused   int64
	newConns int64
	dnsMs    int64
	tcpMs    int64
	tlsMs    int64
	ttfbMs   int64
	http2    int64
}

func recordAssetHTTPTrace(reused bool, dns, tcp, tls, ttfb time.Duration, isHTTP2 bool) {
	assetHTTPMetricsMu.Lock()
	assetHTTPMetrics.requests++
	if reused {
		assetHTTPMetrics.reused++
	} else {
		assetHTTPMetrics.newConns++
	}
	assetHTTPMetrics.dnsMs += dns.Milliseconds()
	assetHTTPMetrics.tcpMs += tcp.Milliseconds()
	assetHTTPMetrics.tlsMs += tls.Milliseconds()
	assetHTTPMetrics.ttfbMs += ttfb.Milliseconds()
	if isHTTP2 {
		assetHTTPMetrics.http2++
	}
	assetHTTPMetricsMu.Unlock()
}

func snapshotAssetHTTPMetrics() telemetry.RawExecutionMetrics {
	assetHTTPMetricsMu.Lock()
	m := assetHTTPMetrics
	assetHTTPMetricsMu.Unlock()
	var out telemetry.RawExecutionMetrics
	out.PopulateHTTPMetrics(m.requests, m.reused, m.newConns, m.dnsMs, m.tcpMs, m.tlsMs, m.ttfbMs, m.http2)
	return out
}

func newAssetHTTPTrace() (*httptrace.ClientTrace, func() (dns, tcp, tls, ttfb time.Duration, reused, isHTTP2 bool)) {
	var dnsStart, tcpStart, tlsStart, reqStart time.Time
	var dnsDur, tcpDur, tlsDur, ttfbDur time.Duration
	var reused, isHTTP2 bool
	trace := &httptrace.ClientTrace{
		DNSStart:          func(info httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone:           func(info httptrace.DNSDoneInfo) { dnsDur = time.Since(dnsStart) },
		ConnectStart:      func(network, addr string) { tcpStart = time.Now() },
		ConnectDone:       func(network, addr string, err error) { tcpDur = time.Since(tcpStart) },
		TLSHandshakeStart: func() { tlsStart = time.Now() },
		TLSHandshakeDone:  func(s tls.ConnectionState, err error) { tlsDur = time.Since(tlsStart) },
		GotConn: func(info httptrace.GotConnInfo) {
			reused = info.Reused
			isHTTP2 = info.Conn != nil
			if !reused {
				reqStart = time.Now()
			}
		},
		GotFirstResponseByte: func() { ttfbDur = time.Since(reqStart) },
	}
	snap := func() (time.Duration, time.Duration, time.Duration, time.Duration, bool, bool) {
		return dnsDur, tcpDur, tlsDur, ttfbDur, reused, isHTTP2
	}
	return trace, snap
}
