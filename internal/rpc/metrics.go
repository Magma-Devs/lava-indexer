package rpc

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// EndpointMetrics captures the live load on one RPC/REST endpoint so the UI
// can visualise throughput per node and surface when a node is at its knee.
// All read paths are safe for concurrent use.
type EndpointMetrics struct {
	InFlight       int64   // requests currently in flight
	MaxInFlight    int64   // high-water mark since startup
	TotalRequests  int64   // cumulative count (successful + failed)
	TotalErrors    int64   // cumulative failures
	RequestsPerSec float64 // rolling rate over the last minute
	LatencyP50Ms   float64 // p50 over the last N completed requests
	LatencyP95Ms   float64 // p95 over the last N completed requests
	LatencyP99Ms   float64 // p99 over the last N completed requests
	BlocksPerSec   float64 // derived (rate × batch size) — filled by caller
}

type metricsTracker struct {
	inFlight      atomic.Int64
	maxInFlight   atomic.Int64
	totalRequests atomic.Int64
	totalErrors   atomic.Int64

	rate *rollingCounter // rps over last 60s

	mu           sync.Mutex
	latencies    []time.Duration // bounded ring of recent completed-request latencies
	latenciesMax int
}

func newMetricsTracker() *metricsTracker {
	return &metricsTracker{
		rate:         newRollingCounter(60 * time.Second),
		latencies:    make([]time.Duration, 0, 512),
		latenciesMax: 512,
	}
}

// begin records the start of a request and returns a func to be deferred that
// records completion + optional error.
func (m *metricsTracker) begin() func(err error) {
	start := time.Now()
	cur := m.inFlight.Add(1)
	for {
		peak := m.maxInFlight.Load()
		if cur <= peak || m.maxInFlight.CompareAndSwap(peak, cur) {
			break
		}
	}
	return func(err error) {
		m.inFlight.Add(-1)
		m.totalRequests.Add(1)
		if err != nil {
			m.totalErrors.Add(1)
		}
		m.rate.add(1)
		dur := time.Since(start)
		m.mu.Lock()
		if len(m.latencies) >= m.latenciesMax {
			copy(m.latencies, m.latencies[1:])
			m.latencies = m.latencies[:m.latenciesMax-1]
		}
		m.latencies = append(m.latencies, dur)
		m.mu.Unlock()
	}
}

func (m *metricsTracker) snapshot() EndpointMetrics {
	m.mu.Lock()
	// Copy out for percentile math without holding the lock during sort.
	cp := make([]time.Duration, len(m.latencies))
	copy(cp, m.latencies)
	m.mu.Unlock()

	var p50, p95, p99 float64
	if len(cp) > 0 {
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		quantile := func(q int) float64 {
			idx := len(cp) * q / 100
			if idx >= len(cp) {
				idx = len(cp) - 1
			}
			return float64(cp[idx]) / float64(time.Millisecond)
		}
		p50 = quantile(50)
		p95 = quantile(95)
		p99 = quantile(99)
	}
	return EndpointMetrics{
		InFlight:       m.inFlight.Load(),
		MaxInFlight:    m.maxInFlight.Load(),
		TotalRequests:  m.totalRequests.Load(),
		TotalErrors:    m.totalErrors.Load(),
		RequestsPerSec: m.rate.rate(),
		LatencyP50Ms:   p50,
		LatencyP95Ms:   p95,
		LatencyP99Ms:   p99,
	}
}

// rollingCounter is a trimmed-window event counter used for rps.
type rollingCounter struct {
	mu     sync.Mutex
	window time.Duration
	events []time.Time
}

func newRollingCounter(w time.Duration) *rollingCounter { return &rollingCounter{window: w} }

func (r *rollingCounter) add(n int) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < n; i++ {
		r.events = append(r.events, now)
	}
	cutoff := now.Add(-r.window)
	trim := 0
	for i, t := range r.events {
		if t.Before(cutoff) {
			trim = i + 1
			continue
		}
		break
	}
	if trim > 0 {
		r.events = r.events[trim:]
	}
}

func (r *rollingCounter) rate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-r.window)
	trim := 0
	for i, t := range r.events {
		if t.Before(cutoff) {
			trim = i + 1
			continue
		}
		break
	}
	if trim > 0 {
		r.events = r.events[trim:]
	}
	if len(r.events) == 0 {
		return 0
	}
	elapsed := time.Since(r.events[0]).Seconds()
	if elapsed < 0.25 {
		elapsed = 0.25 // avoid division blow-ups on the first sample
	}
	return float64(len(r.events)) / elapsed
}
