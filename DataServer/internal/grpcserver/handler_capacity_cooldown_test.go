package grpcserver

import (
	"testing"
	"time"
)

func TestWorkerSessionCapacityCooldownExpires(t *testing.T) {
	sess := &workerSession{}
	now := time.Now().UTC()

	sess.setCapacityCooldown(now.Add(capacityFullCooldown))
	if !sess.capacityCooldownActive(now) {
		t.Fatal("capacity cooldown should suppress placement before expiry")
	}
	if sess.capacityCooldownActive(now.Add(capacityFullCooldown + time.Nanosecond)) {
		t.Fatal("capacity cooldown should expire")
	}
}

func TestWorkerSessionCapacityCooldownIsSafeForNilSession(t *testing.T) {
	var sess *workerSession
	sess.setCapacityCooldown(time.Now().UTC().Add(time.Minute))
	if sess.capacityCooldownActive(time.Now().UTC()) {
		t.Fatal("nil session must not report an active cooldown")
	}
}
