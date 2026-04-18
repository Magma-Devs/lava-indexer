package rpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveController is an AIMD (additive-increase, multiplicative-decrease)
// concurrency governor per endpoint. It reads the live metrics and adjusts
// the budget so we push each node as hard as it can take without making
// latency or errors blow up.
//
// Heuristics (relative, NOT absolute — so it works regardless of how many
// blocks each request fetches):
//   - Warmup: do nothing until the endpoint has completed a minimum number
//     of requests. TLS handshakes + cold connection pools skew the first
//     few latencies and would otherwise trick us into collapsing.
//   - Grow (+2) when: p99 < p99Ratio × p50 AND utilization ≥ 0.7 AND
//     recent error rate < 1%. Queueing isn't hurting us and we're actually
//     using what we've been given.
//   - Shrink (×0.75) when: p99 > p99ShrinkRatio × p50 OR error rate ≥ 5%.
//   - Clamped to [Min, Max]. Tick is 3s so the controller reacts but
//     doesn't oscillate.
type AdaptiveController struct {
	Min             int
	Max             int
	P99GrowRatio    float64 // grow when p99 ≤ p50 × this
	P99ShrinkRatio  float64 // shrink when p99 ≥ p50 × this
	WarmupRequests  int64   // ignore latency readings until this many requests have completed

	current       atomic.Int64
	lastErrors    int64
	lastRequests  int64
	mu            sync.Mutex
}

func NewAdaptiveController(initial, min, max int, _ time.Duration) *AdaptiveController {
	c := &AdaptiveController{
		Min: min, Max: max,
		P99GrowRatio:   3.0,
		P99ShrinkRatio: 8.0,
		WarmupRequests: 20,
	}
	c.current.Store(int64(initial))
	return c
}

// Budget returns the current concurrency budget.
func (c *AdaptiveController) Budget() int { return int(c.current.Load()) }

// Tick re-evaluates the budget based on a fresh metrics snapshot. Safe to
// call from a single goroutine.
func (c *AdaptiveController) Tick(m EndpointMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	budget := int(c.current.Load())

	// Error rate since the last tick — this decouples from the total
	// lifetime error count so a once-flaky endpoint can recover.
	dRequests := m.TotalRequests - c.lastRequests
	dErrors := m.TotalErrors - c.lastErrors
	c.lastRequests = m.TotalRequests
	c.lastErrors = m.TotalErrors
	errRate := 0.0
	if dRequests > 0 {
		errRate = float64(dErrors) / float64(dRequests)
	}

	// Warmup: don't touch the budget until we have a meaningful sample.
	// TCP/TLS handshakes and cold worker goroutines bias the first batch
	// of latency readings high and would trip our shrink heuristic.
	if m.TotalRequests < c.WarmupRequests {
		return
	}

	// Errors always trigger a shrink — doesn't matter why the requests are
	// failing, the node is rejecting us.
	if errRate >= 0.05 {
		c.shrink(budget)
		return
	}

	// Relative p99 signal: the ratio tells us if the node is queueing
	// requests (which shows up as the long tail exploding) independent of
	// how big each request is.
	p50 := m.LatencyP50Ms
	p99 := m.LatencyP99Ms
	if p50 > 0 && p99 > p50*c.P99ShrinkRatio {
		c.shrink(budget)
		return
	}

	utilization := 0.0
	if budget > 0 {
		utilization = float64(m.InFlight) / float64(budget)
	}
	// Grow when latency tail is behaving AND we're using what we have AND
	// errors are rare.
	if p50 > 0 && p99 < p50*c.P99GrowRatio && utilization >= 0.7 && errRate < 0.01 {
		newBudget := budget + 2
		if newBudget > c.Max {
			newBudget = c.Max
		}
		c.current.Store(int64(newBudget))
	}
}

func (c *AdaptiveController) shrink(budget int) {
	nb := (budget * 3) / 4
	if nb < c.Min {
		nb = c.Min
	}
	if nb != budget {
		c.current.Store(int64(nb))
	}
}

// Acquire reserves a budget slot, blocking until one is free or ctx is done.
// Uses a simple chan-based sem so it scales with changing budget — when we
// grow, the next release widens the available slots automatically.
type EndpointSem struct {
	ctrl *AdaptiveController
	mu   sync.Mutex
	// We can't size a plain channel dynamically, so represent "available"
	// slots with a counter + cond. Cheap, correct under contention.
	available int
	cond      *sync.Cond
}

func NewEndpointSem(ctrl *AdaptiveController) *EndpointSem {
	s := &EndpointSem{ctrl: ctrl, available: ctrl.Budget()}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Resize syncs the available-counter with the controller's latest budget.
// Called every tick.
func (s *EndpointSem) Resize() {
	s.mu.Lock()
	defer s.mu.Unlock()
	newBudget := s.ctrl.Budget()
	if newBudget > s.available {
		// We grew: free up extra slots.
		s.available = newBudget
		s.cond.Broadcast()
		return
	}
	// Shrinking below available is fine — acquires will just block until
	// enough outstanding releases bring `available` back above zero.
	s.available = newBudget
}

func (s *EndpointSem) Acquire(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.available <= 0 {
		// Lock-drop-wait loop, standard cond pattern with context.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				s.cond.Broadcast()
			case <-done:
			}
		}()
		s.cond.Wait()
		close(done)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	s.available--
	return nil
}

func (s *EndpointSem) Release() {
	s.mu.Lock()
	s.available++
	s.cond.Signal()
	s.mu.Unlock()
}
