package worker

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"velox-worker-agent/internal/telemetry"
)

var assetTransport = &http.Transport{
	MaxIdleConns:        128,
	MaxIdleConnsPerHost: 64,
	IdleConnTimeout:     90 * time.Second,
	ForceAttemptHTTP2:   true,
}

// A3-2 audit fix: the process-wide HTTP transfer metrics are atomic counters
// instead of a struct guarded by one global mutex. Parallel asset downloads
// previously serialized on assetHTTPMetricsMu for every DNS/TCP/TLS/TTFB
// trace event, so contention grew with download parallelism — the exact
// concurrency the transfer pipeline is designed for. Counters are monotonic
// sums (same semantics as before), so the lock-free read in
// snapshotAssetHTTPMetrics yields a per-counter-consistent snapshot instead
// of a strictly point-in-time one; every consumer treats the snapshot as
// aggregate telemetry, so that is sufficient.
var assetHTTPMetrics struct {
	requests atomic.Int64
	reused   atomic.Int64
	newConns atomic.Int64
	dnsMs    atomic.Int64
	tcpMs    atomic.Int64
	tlsMs    atomic.Int64
	ttfbMs   atomic.Int64
	http2    atomic.Int64
}

func recordAssetHTTPTrace(reused bool, dns, tcp, tls, ttfb time.Duration, isHTTP2 bool) {
	assetHTTPMetrics.requests.Add(1)
	if reused {
		assetHTTPMetrics.reused.Add(1)
	} else {
		assetHTTPMetrics.newConns.Add(1)
	}
	assetHTTPMetrics.dnsMs.Add(dns.Milliseconds())
	assetHTTPMetrics.tcpMs.Add(tcp.Milliseconds())
	assetHTTPMetrics.tlsMs.Add(tls.Milliseconds())
	assetHTTPMetrics.ttfbMs.Add(ttfb.Milliseconds())
	if isHTTP2 {
		assetHTTPMetrics.http2.Add(1)
	}
}

func snapshotAssetHTTPMetrics() telemetry.RawExecutionMetrics {
	var out telemetry.RawExecutionMetrics
	out.PopulateHTTPMetrics(
		assetHTTPMetrics.requests.Load(),
		assetHTTPMetrics.reused.Load(),
		assetHTTPMetrics.newConns.Load(),
		assetHTTPMetrics.dnsMs.Load(),
		assetHTTPMetrics.tcpMs.Load(),
		assetHTTPMetrics.tlsMs.Load(),
		assetHTTPMetrics.ttfbMs.Load(),
		assetHTTPMetrics.http2.Load(),
	)
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
