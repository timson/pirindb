package main

import (
	"testing"
	"time"
)

func TestMonotonicNow(t *testing.T) {
	clock := NewHLClock(5 * time.Second)

	h1 := clock.Now()
	h2 := clock.Now()
	h3 := clock.Now()

	if h2.Compare(h1) <= 0 || h3.Compare(h2) <= 0 {
		t.Errorf("HLC is not monotonic: %v → %v → %v", h1, h2, h3)
	}
}

func TestUpdateIncreasesClock(t *testing.T) {
	clock := NewHLClock(5 * time.Second)

	local := clock.Now()

	remote := HLC{
		PhysicalTime:   local.PhysicalTime + 10,
		LogicalCounter: 0,
	}

	updated, err := clock.Update(remote)
	if err != nil {
		t.Fatalf("Unexpected error during Update: %v", err)
	}

	if updated.PhysicalTime != remote.PhysicalTime {
		t.Errorf("Expected physical time %d, got %d", remote.PhysicalTime, updated.PhysicalTime)
	}
}

func TestUpdateWithClockSkewTooBig(t *testing.T) {
	clock := NewHLClock(1 * time.Second)

	now := time.Now().UnixMilli()
	remote := HLC{
		PhysicalTime:   now + 5000, // 5 секунд вперёд
		LogicalCounter: 0,
	}

	_, err := clock.Update(remote)
	if err == nil {
		t.Error("Expected error due to clock skew, got nil")
	}
}
