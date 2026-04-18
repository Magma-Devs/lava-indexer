package rpc

import (
	"sync"
	"time"
)

// MetricSample is one timestamped snapshot appended to the per-endpoint
// history ring. One sample per controller tick; ~3s apart.
type MetricSample struct {
	At             time.Time `json:"at"`
	InFlight       int64     `json:"in_flight"`
	Budget         int       `json:"budget"`           // AIMD-chosen concurrency cap at this moment
	RequestsPerSec float64   `json:"requests_per_sec"`
	LatencyP50Ms   float64   `json:"latency_p50_ms"`
	LatencyP99Ms   float64   `json:"latency_p99_ms"`
	TotalErrors    int64     `json:"total_errors"`
}

type metricsHistory struct {
	mu     sync.Mutex
	buf    []MetricSample
	cap    int
}

func newMetricsHistory(capacity int) *metricsHistory {
	return &metricsHistory{cap: capacity}
}

func (h *metricsHistory) add(at time.Time, m EndpointMetrics, budget int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.buf) >= h.cap {
		copy(h.buf, h.buf[1:])
		h.buf = h.buf[:h.cap-1]
	}
	h.buf = append(h.buf, MetricSample{
		At:             at,
		InFlight:       m.InFlight,
		Budget:         budget,
		RequestsPerSec: m.RequestsPerSec,
		LatencyP50Ms:   m.LatencyP50Ms,
		LatencyP99Ms:   m.LatencyP99Ms,
		TotalErrors:    m.TotalErrors,
	})
}

func (h *metricsHistory) snapshot() []MetricSample {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]MetricSample, len(h.buf))
	copy(out, h.buf)
	return out
}
