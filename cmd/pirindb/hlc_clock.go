package main

import (
	"fmt"
	"golang.org/x/exp/constraints"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HLC struct {
	PhysicalTime   int64
	LogicalCounter uint32
}

type HLClock struct {
	last      HLC
	maxOffset time.Duration
	mutex     sync.Mutex
}

func NewHLClock(maxOffset time.Duration) *HLClock {
	return &HLClock{
		last: HLC{
			PhysicalTime:   time.Now().UnixMilli(),
			LogicalCounter: 0,
		},
		maxOffset: maxOffset,
	}
}

func (c *HLClock) Now() HLC {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	now := time.Now().UnixMilli()
	if now > c.last.PhysicalTime {
		c.last.PhysicalTime = now
		c.last.LogicalCounter = 0
	} else {
		c.last.LogicalCounter++
	}
	return c.last
}

func maxInt[T constraints.Ordered](args ...T) T {
	if len(args) == 0 {
		var zero T
		return zero
	}
	maxValue := args[0]
	for _, value := range args[1:] {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func (c *HLClock) Update(remote HLC) (HLC, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now().UnixMilli()
	if remote.PhysicalTime > now+c.maxOffset.Milliseconds() {
		return HLC{}, ErrMaxTimeShiftExceeded
	}
	maxPhysical := maxInt(remote.PhysicalTime, c.maxOffset.Milliseconds())
	var newLogicalCounter uint32
	switch {
	case maxPhysical == c.last.PhysicalTime && maxPhysical == remote.PhysicalTime:
		newLogicalCounter = maxInt(c.last.LogicalCounter, remote.LogicalCounter) + 1
	case maxPhysical == c.last.PhysicalTime:
		newLogicalCounter = c.last.LogicalCounter + 1
	case maxPhysical == remote.PhysicalTime:
		newLogicalCounter = remote.LogicalCounter + 1
	default:
		newLogicalCounter = 0
	}
	c.last = HLC{
		PhysicalTime:   maxPhysical,
		LogicalCounter: newLogicalCounter,
	}
	return c.last, nil
}

func (hlc HLC) Compare(other HLC) int {
	if hlc.PhysicalTime < other.PhysicalTime {
		return -1
	}
	if hlc.PhysicalTime > other.PhysicalTime {
		return 1
	}
	if hlc.LogicalCounter < other.LogicalCounter {
		return -1
	}
	if hlc.LogicalCounter > other.LogicalCounter {
		return 1
	}
	return 0
}

func (hlc HLC) String() string {
	return fmt.Sprintf("%d.%d", hlc.PhysicalTime, hlc.LogicalCounter)
}

func (hlc HLC) Serialize() string {
	return fmt.Sprintf("%d.%d", hlc.PhysicalTime, hlc.LogicalCounter)
}

func ParseHLC(s string) (HLC, error) {
	var hlc HLC
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return HLC{}, fmt.Errorf("invalid HLC string: %s", s)
	}
	physicalTime, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return HLC{}, fmt.Errorf("invalid physical time: %w", err)
	}
	logicalCounter, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return HLC{}, fmt.Errorf("invalid logical counter: %w", err)
	}
	hlc.PhysicalTime = physicalTime
	hlc.LogicalCounter = uint32(logicalCounter)
	return hlc, nil
}
