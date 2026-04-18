package pipeline

import (
	"sync"
	"time"
)

// RateTracker computes a short-window blocks-per-second rate from a sliding
// ring of (timestamp, cumulative_count) samples. Safe for concurrent use.
type RateTracker struct {
	window  time.Duration
	mu      sync.Mutex
	samples []rateSample
}

type rateSample struct {
	At    time.Time
	Count int64
}

func NewRateTracker(window time.Duration) *RateTracker {
	return &RateTracker{window: window}
}

// Record appends a new cumulative count and trims samples older than the
// window. Call once per commit with the running total.
func (r *RateTracker) Record(total int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.samples = append(r.samples, rateSample{At: now, Count: total})
	cutoff := now.Add(-r.window)
	// Trim leading samples older than cutoff, but keep at least one anchor
	// so a rate can still be computed if recording rate is slow.
	keep := 0
	for i, s := range r.samples {
		if s.At.Before(cutoff) {
			keep = i
			continue
		}
		break
	}
	if keep > 0 && keep < len(r.samples) {
		r.samples = r.samples[keep:]
	}
}

// Rate returns blocks/sec over the window. Returns 0 if there are fewer
// than two samples or the window is zero-width.
func (r *RateTracker) Rate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) < 2 {
		return 0
	}
	first := r.samples[0]
	last := r.samples[len(r.samples)-1]
	dt := last.At.Sub(first.At).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(last.Count-first.Count) / dt
}
