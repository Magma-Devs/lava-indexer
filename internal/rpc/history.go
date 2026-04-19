package rpc

import (
	"sync"
	"time"
)

// MetricSample is one timestamped snapshot appended to the per-endpoint
// history ring. One sample per recorder tick; ~3s apart.
//
// The Budget field is retained (always 0) for backward compatibility with
// dashboards that may still reference it — the UI shipped with this
// change drops it from the rendering, but API consumers that haven't
// updated keep working.
type MetricSample struct {
	At             time.Time `json:"at"`
	InFlight       int64     `json:"in_flight"`
	RequestsPerSec float64   `json:"requests_per_sec"`
	LatencyP50Ms   float64   `json:"latency_p50_ms"`
	LatencyP99Ms   float64   `json:"latency_p99_ms"`
	TotalErrors    int64     `json:"total_errors"`
}

type metricsHistory struct {
	mu  sync.Mutex
	buf []MetricSample
	cap int
}

func newMetricsHistory(capacity int) *metricsHistory {
	return &metricsHistory{cap: capacity}
}

func (h *metricsHistory) add(at time.Time, m EndpointMetrics) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.buf) >= h.cap {
		copy(h.buf, h.buf[1:])
		h.buf = h.buf[:h.cap-1]
	}
	h.buf = append(h.buf, MetricSample{
		At:             at,
		InFlight:       m.InFlight,
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
