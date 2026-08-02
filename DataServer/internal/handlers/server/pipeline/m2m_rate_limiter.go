// Package pipeline: m2m_rate_limiter.go — the in-memory per-client
// token-bucket rate limiter used by the M2M auth middleware. Split out
// of m2m_auth.go; the middleware itself lives in m2m_auth.go and the
// audit + quota helpers in m2m_audit.go.
package pipeline

import (
	"strings"
	"sync"
	"time"

	"database/sql"
)

// sqlNullString is a tiny helper used by the audit helpers below
// to lift a string into sql.NullString without repeating the
// Valid=len>0 ceremony at every call site.
func sqlNullString(s string) sql.NullString {
	s = strings.TrimSpace(s)
	return sql.NullString{String: s, Valid: s != ""}
}

// =====================================================================
// Per-client rate limiter (in-memory token bucket).
// =====================================================================
//
// In-memory by design: the cluster-wide counter via SQLite would
// serialize every request on a single DB connection (the writes
// would dominate submission latency). The restart-loss window is
// acceptable because the start of the master drains load and a
// fresh token bucket is more lenient than the prior steady-state —
// the NewResolver path's identity-dedup invariants protect against
// the burstier post-restart window producing duplicate jobs.
//
// One bucket per (client_id) → keyed bucket map. Buckets are
// created lazily on first request and held indefinitely; entries
// are NOT cleared for inactive clients (the GC is "operator calls
// DisableM2MAPIKey", at which point the bucket is unreachable
// anyway).

type m2mRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*m2mBucket
}

type m2mBucket struct {
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func newM2MRateLimiter() *m2mRateLimiter {
	return &m2mRateLimiter{
		buckets: make(map[string]*m2mBucket),
	}
}

// take returns true if a token was successfully taken; false if
// the bucket was empty (caller should reject with 429). The bucket
// state is mutated atomically.
//
// Standard token-bucket semantics: capacity is the MAXIMUM tokens
// the bucket can hold at any time. On lazy init the bucket is
// created FULLY loaded then ONE token is immediately consumed for
// the in-progress request. Without the drain-on-init, capacity=N
// effectively grants N+1 requests before exhaustion, which is the
// universal bug in naive implementations.
func (l *m2mRateLimiter) take(clientID string, capacity, refillRate float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	bucket, ok := l.buckets[clientID]
	if !ok {
		// Lazily allocate. Bucket starts at full capacity; we
		// consume the first token on behalf of this in-flight
		// request so a freshly observed client_id gets exactly
		// `capacity` requests before exhaustion (not capacity+1).
		// capacity < 1 → bucket cannot satisfy this request.
		bucket = &m2mBucket{
			tokens:     capacity - 1,
			capacity:   capacity,
			refillRate: refillRate,
			lastRefill: now,
		}
		l.buckets[clientID] = bucket
		return capacity >= 1
	}
	// Refill based on elapsed time. Cap at capacity so a long
	// downtime doesn't grant a huge burst.
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * bucket.refillRate
	if bucket.tokens > bucket.capacity {
		bucket.tokens = bucket.capacity
	}
	bucket.lastRefill = now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens -= 1
	return true
}
