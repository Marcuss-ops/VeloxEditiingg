// Package wake provides a small in-process broadcast for durable workers.
// Polling remains the recovery mechanism; wake signals only reduce latency.
package wake

import "sync"

type Hub struct {
	mu        sync.Mutex
	next      uint64
	listeners map[uint64]chan struct{}
}

func (h *Hub) Subscribe() (<-chan struct{}, func()) {
	h.mu.Lock()
	if h.listeners == nil {
		h.listeners = make(map[uint64]chan struct{})
	}
	h.next++
	id := h.next
	c := make(chan struct{}, 1)
	h.listeners[id] = c
	h.mu.Unlock()
	return c, func() { h.mu.Lock(); delete(h.listeners, id); h.mu.Unlock() }
}

func (h *Hub) Signal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.listeners {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

var Deliveries Hub
var MediaProbes Hub
