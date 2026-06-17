// Package deprecation tracks calls to legacy endpoints that the Velox master
// keeps alive during the velox-core split. Counters are kept in memory;
// each tracked endpoint emits a one-line `[DEPRECATED]` log so operators
// can grep server.log for callers and read aggregated stats from
// `/api/_internal/deprecation_stats`. No persistence: a master restart
// resets counters; that is intentional — the sunset date is the persistent
// signal that drives removal.
package deprecation

import (
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Stat is the per-endpoint counters structure exposed via the introspection
// endpoint. Times are RFC 3339 UTC strings for JSON portability.
type Stat struct {
	Name        string `json:"name"`
	Method      string `json:"method"`
	Path        string `json:"path"`
	Successor   string `json:"successor,omitempty"`
	Hits        int64  `json:"hits"`
	Errors      int64  `json:"errors"`
	FirstHitAt  string `json:"first_hit_at,omitempty"`
	LastHitAt   string `json:"last_hit_at,omitempty"`
	LastErrorAt string `json:"last_error_at,omitempty"`
	LastClient  string `json:"last_client,omitempty"`
}

// Snapshot is the response payload of /api/_internal/deprecation_stats.
type Snapshot struct {
	RegisteredAt string `json:"registered_at"`
	SunsetAt     string `json:"sunset_at"`
	DaysToSunset int    `json:"days_to_sunset"`
	Stats        []Stat `json:"stats"`
}

// entry holds the live counters for one legacy endpoint.
// All fields are accessed under Registry.mu; nothing is read without
// holding at least the read lock.
type entry struct {
	name        string
	method      string
	path        string
	successor   string

	hits        int64
	errors      int64
	firstHitNS  int64
	lastHitNS   int64
	lastErrorNS int64
	lastClient  string
}

// Registry tracks per-endpoint deprecation counters in memory. Zero value
// is NOT usable: call New(noticedAt, sunsetAt).
type Registry struct {
	mu        sync.RWMutex
	items     []*entry
	byKey     map[string]*entry
	noticedAt time.Time
	sunsetAt  time.Time
}

// New constructs a Registry. sunsetAt must be > noticedAt.
func New(noticedAt, sunsetAt time.Time) *Registry {
	if !sunsetAt.After(noticedAt) {
		sunsetAt = noticedAt.Add(14 * 24 * time.Hour)
	}
	return &Registry{
		items:     make([]*entry, 0, 8),
		byKey:     make(map[string]*entry, 8),
		noticedAt: noticedAt,
		sunsetAt:  sunsetAt,
	}
}

// SunsetAt returns the configured sunset (UTC). Used by /api/_internal.
func (r *Registry) SunsetAt() time.Time { return r.sunsetAt }

// Register declares a legacy endpoint so subsequent Track calls know the
// successor and so the endpoint appears in snapshots even before any call.
// Subsequent calls with the same method+path are no-ops (first wins).
// successor may be empty.
func (r *Registry) Register(method, path, successor string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := method + " " + path
	if _, ok := r.byKey[key]; ok {
		return
	}
	e := &entry{
		name:      key,
		method:    method,
		path:      path,
		successor: successor,
	}
	r.items = append(r.items, e)
	r.byKey[key] = e
}

// Track returns a Gin middleware that:
//  1. records one Hit for the registered endpoint,
//  2. sets RFC 8594 Deprecation/Sunset headers on the response,
//  3. emits one [DEPRECATED] log line per call so operators can grep for
//     leftover callers during the sunset window,
//  4. increments the Errors counter when the downstream handler writes a
//     status >= 400 (best-effort; happens after c.Next()).
//
// If the endpoint was not Register()ed, returns a pass-through middleware
// so misuse is benign.
func (r *Registry) Track(method, path string) gin.HandlerFunc {
	if r == nil {
		return func(c *gin.Context) { c.Next() }
	}
	key := method + " " + path
	r.mu.RLock()
	e, ok := r.byKey[key]
	sunsetHTTPDate := r.sunsetAt.UTC().Format(http.TimeFormat)
	r.mu.RUnlock()
	if !ok {
		return func(c *gin.Context) { c.Next() }
	}

	successorLink := ""
	if e.successor != "" {
		successorLink = "<" + e.successor + ">; rel=\"successor-version\""
	}

	return func(c *gin.Context) {
		now := time.Now().UTC()
		clientIP := c.ClientIP()

		r.mu.Lock()
		e.hits++
		if e.firstHitNS == 0 {
			e.firstHitNS = now.UnixNano()
		}
		e.lastHitNS = now.UnixNano()
		e.lastClient = clientIP
		r.mu.Unlock()

		h := c.Writer.Header()
		h.Set("Deprecation", "true")
		h.Set("Sunset", sunsetHTTPDate)
		if successorLink != "" {
			h.Set("Link", successorLink)
		}

		c.Next()

		// Errors are counted AFTER the handler ran so we observe the
		// final status (handlers may write the body themselves or via
		// c.JSON / string helpers).
		if c.Writer.Status() >= http.StatusBadRequest {
			r.mu.Lock()
			e.errors++
			e.lastErrorNS = time.Now().UTC().UnixNano()
			r.mu.Unlock()
		}

		logDeprecated(method, path, clientIP, c.Writer.Status())
	}
}

// Snapshot returns a copy of the current counters, sorted by name for
// stable output. Safe to call concurrently.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make([]Stat, 0, len(r.items))
	for _, e := range r.items {
		s := Stat{
			Name:       e.name,
			Method:     e.method,
			Path:       e.path,
			Successor:  e.successor,
			Hits:       e.hits,
			Errors:     e.errors,
			LastClient: e.lastClient,
		}
		if e.firstHitNS != 0 {
			s.FirstHitAt = time.Unix(0, e.firstHitNS).UTC().Format(time.RFC3339)
		}
		if e.lastHitNS != 0 {
			s.LastHitAt = time.Unix(0, e.lastHitNS).UTC().Format(time.RFC3339)
		}
		if e.lastErrorNS != 0 {
			s.LastErrorAt = time.Unix(0, e.lastErrorNS).UTC().Format(time.RFC3339)
		}
		stats = append(stats, s)
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })

	days := int(time.Until(r.sunsetAt).Hours() / 24)
	if days < 0 {
		days = 0
	}
	return Snapshot{
		RegisteredAt: r.noticedAt.UTC().Format(time.RFC3339),
		SunsetAt:     r.sunsetAt.UTC().Format(time.RFC3339),
		DaysToSunset: days,
		Stats:        stats,
	}
}

// logDeprecated is invoked by Track after every call. Single-line,
// goes through the std log so it lands in whatever logger the master
// already routes (syslog, journal, stdout, ...).
func logDeprecated(method, path, clientIP string, status int) {
	log.Printf("[DEPRECATED] %s %s from=%s status=%d at=%s",
		method, path, clientIP, status,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
}
