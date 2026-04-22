package snapshotters

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// syntheticChain is a BlockTimeLookup whose block timestamps are a
// deterministic function of height — 30 seconds per block starting at
// t0. Mirrors Lava's ~30s block time without any network.
type syntheticChain struct {
	t0       time.Time
	tipH     int64
	interval time.Duration
	calls    int
}

func (c *syntheticChain) BlockTime(_ context.Context, h int64) (time.Time, error) {
	c.calls++
	if h < 1 || h > c.tipH {
		return time.Time{}, fmt.Errorf("h=%d out of range [1,%d]", h, c.tipH)
	}
	return c.t0.Add(time.Duration(h-1) * c.interval), nil
}

func (c *syntheticChain) Tip(_ context.Context) (int64, error) {
	return c.tipH, nil
}

func TestFindBlockAtTime_MidRange(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ch := &syntheticChain{t0: t0, tipH: 1_000_000, interval: 30 * time.Second}

	// Target: 2025-01-17T15:00:00Z. Expected height is derived from
	// the same 30s cadence: blocks count from 1 at t0.
	target := time.Date(2025, 1, 17, 15, 0, 0, 0, time.UTC)
	h, ts, err := FindBlockAtTime(context.Background(), ch, target, 1)
	if err != nil {
		t.Fatalf("FindBlockAtTime: %v", err)
	}
	if h < 1 || h > ch.tipH {
		t.Fatalf("h = %d outside [1,%d]", h, ch.tipH)
	}
	// Sanity: the returned block's timestamp is within 30s of target.
	if ts.Before(target.Add(-30*time.Second)) || ts.After(target.Add(30*time.Second)) {
		t.Fatalf("resolved time %s not within ±30s of target %s", ts, target)
	}
	// Sanity: converges in ~log₂(1_000_000) ≈ 20 steps + 2 initial reads.
	if ch.calls > 30 {
		t.Fatalf("too many lookups: %d (want ≤ 30)", ch.calls)
	}
}

func TestFindBlockAtTime_TargetBeforeLow(t *testing.T) {
	t0 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	ch := &syntheticChain{t0: t0, tipH: 100, interval: 30 * time.Second}
	target := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) // way before t0

	h, ts, err := FindBlockAtTime(context.Background(), ch, target, 1)
	if err != nil {
		t.Fatalf("FindBlockAtTime: %v", err)
	}
	if h != 1 {
		t.Fatalf("h = %d, want 1 (low boundary)", h)
	}
	if !ts.Equal(t0) {
		t.Fatalf("ts = %s, want %s", ts, t0)
	}
}

func TestFindBlockAtTime_TargetAfterTip(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ch := &syntheticChain{t0: t0, tipH: 100, interval: 30 * time.Second}
	// Target is 1 year after tip.
	target := t0.Add(time.Duration(ch.tipH) * ch.interval).Add(365 * 24 * time.Hour)

	h, _, err := FindBlockAtTime(context.Background(), ch, target, 1)
	if err != nil {
		t.Fatalf("FindBlockAtTime: %v", err)
	}
	if h != ch.tipH {
		t.Fatalf("h = %d, want tip %d", h, ch.tipH)
	}
}

func TestFindBlockAtTime_ExactMatch(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ch := &syntheticChain{t0: t0, tipH: 100, interval: 30 * time.Second}
	// Block 50's timestamp = t0 + 49*30s.
	target := t0.Add(49 * 30 * time.Second)

	h, ts, err := FindBlockAtTime(context.Background(), ch, target, 1)
	if err != nil {
		t.Fatalf("FindBlockAtTime: %v", err)
	}
	if h != 50 || !ts.Equal(target) {
		t.Fatalf("got (h=%d, ts=%s), want (h=50, ts=%s)", h, ts, target)
	}
}

// erroringChain fails on the second BlockTime call, used to verify the
// error path doesn't eat context info.
type erroringChain struct {
	t0   time.Time
	tipH int64
	n    int
}

func (c *erroringChain) BlockTime(_ context.Context, h int64) (time.Time, error) {
	c.n++
	if c.n >= 2 {
		return time.Time{}, errors.New("simulated rpc failure")
	}
	return c.t0.Add(time.Duration(h-1) * 30 * time.Second), nil
}

func (c *erroringChain) Tip(_ context.Context) (int64, error) {
	return c.tipH, nil
}

func TestFindBlockAtTime_ErrorPropagates(t *testing.T) {
	ch := &erroringChain{t0: time.Now(), tipH: 1000}
	_, _, err := FindBlockAtTime(context.Background(), ch, time.Now().Add(1*time.Hour), 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "simulated rpc failure") {
		t.Fatalf("error doesn't carry cause: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
