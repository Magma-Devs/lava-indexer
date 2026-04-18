package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"
)

// StatusInfo is what a Prober returns about one endpoint's coverage.
type StatusInfo struct {
	EarliestHeight int64
	EarliestTime   time.Time
	LatestHeight   int64
	LatestTime     time.Time
	// Network is the chain_id the endpoint advertises (e.g. "lava-mainnet-1").
	// Empty means the endpoint didn't report one — callers that care (e.g.
	// MultiClient.Probe) treat this distinctly from a mismatch.
	Network string
}

// Prober is optional — clients that can report their block-height window
// implement it so MultiClient can route requests away from pruning nodes
// that no longer serve historical heights, and so the UI can show dates.
type Prober interface {
	Probe(ctx context.Context) (StatusInfo, error)
}

// MultiClient wraps N underlying Clients (each pointing at a different RPC
// or REST endpoint) with round-robin load balancing and automatic failover.
// On startup callers should invoke Probe to populate per-endpoint coverage
// so historical-height requests are routed only to endpoints that serve them.
type MultiClient struct {
	endpoints []*Endpoint
	next      atomic.Int32
}

// Endpoint is one entry in the MultiClient's pool.
type Endpoint struct {
	URL    string
	Kind   string
	Client Client

	// After Probe, these describe the window this endpoint serves.
	// Zero means "unknown — do not filter by this endpoint".
	Earliest     int64
	EarliestTime time.Time
	Latest       int64
	LatestTime   time.Time

	Disabled bool
	// Reason is a short human-readable explanation for why this endpoint
	// is Disabled — e.g. "chain_id mismatch: expected=lava-mainnet-1
	// advertised=lava-testnet-2" or "probe failed: connection refused".
	// Empty when the endpoint is healthy. Surfaced on /api/status so the
	// dashboard can show why an endpoint was taken out of rotation.
	Reason string

	metrics    *metricsTracker
	ctrl       *AdaptiveController
	sem        *EndpointSem
	history    *metricsHistory
}

// Metrics returns a snapshot of live throughput/latency for this endpoint.
func (e *Endpoint) Metrics() EndpointMetrics {
	if e.metrics == nil {
		return EndpointMetrics{}
	}
	return e.metrics.snapshot()
}

// ConcurrencyBudget returns the current AIMD-controlled concurrency cap.
func (e *Endpoint) ConcurrencyBudget() int {
	if e.ctrl == nil {
		return 0
	}
	return e.ctrl.Budget()
}

// MetricHistory returns the recent snapshots for time-series rendering.
func (e *Endpoint) MetricHistory() []MetricSample {
	if e.history == nil {
		return nil
	}
	return e.history.snapshot()
}

// MultiClientOptions lets the caller tune the adaptive controller without
// editing the code. Zero values pick sensible defaults.
type MultiClientOptions struct {
	InitialConcurrency int           // default 8
	MinConcurrency     int           // default 1
	MaxConcurrency     int           // default 64
	TargetP99          time.Duration // default 500ms
	HistorySeconds     int           // default 1800 (30 min of 1-sec samples)
}

func NewMulti(eps []*Endpoint) *MultiClient {
	return NewMultiWithOptions(eps, MultiClientOptions{})
}

func NewMultiWithOptions(eps []*Endpoint, opts MultiClientOptions) *MultiClient {
	if opts.InitialConcurrency == 0 {
		opts.InitialConcurrency = 8
	}
	if opts.MinConcurrency == 0 {
		opts.MinConcurrency = 4 // never collapse below 4 — we need at least that to measure latency
	}
	if opts.MaxConcurrency == 0 {
		opts.MaxConcurrency = 64
	}
	if opts.HistorySeconds == 0 {
		opts.HistorySeconds = 1800
	}
	for _, e := range eps {
		if e.metrics == nil {
			e.metrics = newMetricsTracker()
		}
		if e.ctrl == nil {
			e.ctrl = NewAdaptiveController(opts.InitialConcurrency, opts.MinConcurrency, opts.MaxConcurrency, opts.TargetP99)
			e.sem = NewEndpointSem(e.ctrl)
		}
		if e.history == nil {
			e.history = newMetricsHistory(opts.HistorySeconds)
		}
	}
	return &MultiClient{endpoints: eps}
}

// StartController spawns a goroutine that ticks the AIMD controller for each
// endpoint every `interval` and appends a snapshot to the history ring.
// Returns when ctx is cancelled.
func (m *MultiClient) StartController(ctx context.Context, interval time.Duration) {
	if interval == 0 {
		interval = 3 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				for _, ep := range m.endpoints {
					mm := ep.Metrics()
					ep.ctrl.Tick(mm)
					ep.sem.Resize()
					ep.history.add(now, mm, ep.ctrl.Budget())
				}
			}
		}
	}()
}

// Probe queries every endpoint's status. Endpoints that fail to respond, or
// that fail chain_id validation, are flagged Disabled with a human-readable
// Reason and the process continues — a single flaky or misconfigured
// endpoint shouldn't block indexing as long as at least one healthy endpoint
// remains. The reason string is surfaced via /api/status so the dashboard
// can explain to operators why an endpoint is out of rotation.
//
// When expectedChainID is non-empty, every endpoint's advertised chain_id is
// compared against it. A mismatch — or an endpoint that didn't advertise one
// at all — disables that endpoint. We treat "didn't report" the same as a
// mismatch on purpose: without verification, the risk we're guarding against
// (indexing the wrong network into the DB) is exactly what empty Network
// leaves open. An empty expectedChainID skips validation entirely (backward
// compat) and is logged.
//
// Probe returns an error only when ZERO endpoints remain healthy — the
// existing "nothing left to work with" fail-fast.
func (m *MultiClient) Probe(ctx context.Context, expectedChainID string) error {
	if expectedChainID == "" {
		slog.Info("chain_id validation skipped (empty network.chain_id in config)")
	}
	any := false
	for _, ep := range m.endpoints {
		p, ok := ep.Client.(Prober)
		if !ok {
			continue
		}
		info, err := p.Probe(ctx)
		if err != nil {
			reason := fmt.Sprintf("probe failed: %s", err)
			slog.Warn("endpoint probe failed, will be disabled", "url", ep.URL, "err", err)
			ep.Disabled = true
			ep.Reason = reason
			continue
		}
		ep.Earliest = info.EarliestHeight
		ep.EarliestTime = info.EarliestTime
		ep.Latest = info.LatestHeight
		ep.LatestTime = info.LatestTime
		slog.Info("endpoint ready",
			"url", ep.URL, "kind", ep.Kind,
			"earliest_block", info.EarliestHeight, "latest_block", info.LatestHeight,
			"earliest_time", info.EarliestTime, "latest_time", info.LatestTime)
		if expectedChainID != "" {
			switch {
			case info.Network == "":
				reason := fmt.Sprintf("endpoint did not report chain_id (expected %s)", expectedChainID)
				slog.Warn("endpoint disabled: chain_id unverified",
					"url", ep.URL, "expected", expectedChainID)
				ep.Disabled = true
				ep.Reason = reason
				continue
			case info.Network != expectedChainID:
				reason := fmt.Sprintf("chain_id mismatch: expected=%s advertised=%s",
					expectedChainID, info.Network)
				slog.Warn("endpoint disabled: chain_id mismatch",
					"url", ep.URL, "expected", expectedChainID, "advertised", info.Network)
				ep.Disabled = true
				ep.Reason = reason
				continue
			default:
				slog.Info("endpoint chain_id verified", "url", ep.URL, "network", info.Network)
			}
		}
		any = true
	}
	if !any {
		return fmt.Errorf("no healthy endpoints")
	}
	return nil
}

// Endpoints returns a copy of the endpoint list for inspection / reporting.
func (m *MultiClient) Endpoints() []*Endpoint {
	out := make([]*Endpoint, len(m.endpoints))
	copy(out, m.endpoints)
	return out
}

// ValidateRange returns an error if NO healthy endpoint claims to cover the
// [start, end] range. When end is 0 (tip-following), only start is checked.
func (m *MultiClient) ValidateRange(start, end int64) error {
	var covering []*Endpoint
	for _, ep := range m.healthy() {
		if ep.Earliest > 0 && start < ep.Earliest {
			continue
		}
		covering = append(covering, ep)
	}
	if len(covering) == 0 {
		msg := fmt.Sprintf("no healthy endpoint serves start_height=%d. endpoint coverage:\n", start)
		for _, ep := range m.healthy() {
			msg += fmt.Sprintf("  - %s: earliest=%d latest=%d\n", ep.URL, ep.Earliest, ep.Latest)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// Tip returns the max latest_block_height across healthy endpoints. If every
// endpoint fails, the error from the last one is returned.
func (m *MultiClient) Tip(ctx context.Context) (int64, error) {
	var max int64
	var lastErr error
	for _, ep := range m.healthy() {
		h, err := ep.Client.Tip(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		if h > max {
			max = h
		}
	}
	if max == 0 {
		if lastErr == nil {
			lastErr = fmt.Errorf("no healthy endpoints")
		}
		return 0, lastErr
	}
	return max, nil
}

// FetchBlocks routes the batch to an endpoint whose coverage window covers
// the requested heights. Policy:
//
//   - Start with the eligible endpoint that has the most HEADROOM right now
//     (budget − in_flight). Under contention this packs new work onto
//     whichever node is least busy, naturally spreading load across nodes
//     when the system can handle more.
//   - On 429 (*ErrRateLimited), fail over IMMEDIATELY to the next
//     eligible endpoint — don't burn retries against a node telling us to
//     back off when another node might be fine.
//   - On any other error, also try the next endpoint. Per-endpoint HTTP
//     retry for 5xx / network errors already happens inside Client.
//   - Returns the last error if every eligible endpoint failed.
func (m *MultiClient) FetchBlocks(ctx context.Context, heights []int64) ([]*Block, error) {
	if len(heights) == 0 {
		return nil, nil
	}
	minH := heights[0]
	maxH := heights[0]
	for _, h := range heights[1:] {
		if h < minH {
			minH = h
		}
		if h > maxH {
			maxH = h
		}
	}

	eligible := make([]*Endpoint, 0, len(m.endpoints))
	for _, ep := range m.healthy() {
		if ep.Earliest > 0 && minH < ep.Earliest {
			continue
		}
		eligible = append(eligible, ep)
	}
	if len(eligible) == 0 {
		return nil, &NoEndpointCoversError{MinHeight: minH, MaxHeight: maxH}
	}

	// Wait up to ~5s for some endpoint to have budget headroom. This is the
	// AIMD controller's safety enforcement: even if a bunch of workers fire
	// at once, they don't all pile onto the same node past its capacity.
	rr := int(m.next.Add(1) - 1)
	var lastErr error
	for waited := time.Duration(0); waited < 5*time.Second; {
		// Pick endpoints with positive headroom, sorted by headroom desc.
		type cand struct {
			ep       *Endpoint
			headroom int
		}
		cands := make([]cand, 0, len(eligible))
		for _, ep := range eligible {
			headroom := ep.ConcurrencyBudget() - int(ep.Metrics().InFlight)
			cands = append(cands, cand{ep, headroom})
		}
		sort.SliceStable(cands, func(i, j int) bool {
			if cands[i].headroom != cands[j].headroom {
				return cands[i].headroom > cands[j].headroom
			}
			return (i+rr)%len(cands) < (j+rr)%len(cands)
		})

		if cands[0].headroom > 0 {
			// Try candidates in order; failover immediately on 429, retry-
			// in-place already happened inside Client for 5xx/net errors.
			for _, c := range cands {
				if c.headroom <= 0 {
					break
				}
				ep := c.ep
				done := ep.metrics.begin()
				blocks, err := ep.Client.FetchBlocks(ctx, heights)
				done(err)
				if err == nil {
					return blocks, nil
				}
				lastErr = err
				var rl *ErrRateLimited
				if errors.As(err, &rl) {
					slog.Debug("endpoint rate-limited, failing over",
						"url", ep.URL, "retry_after", rl.RetryAfter)
				} else {
					slog.Warn("fetch failed, trying next endpoint",
						"url", ep.URL, "err", err)
				}
			}
			return nil, fmt.Errorf("all %d eligible endpoints failed; last: %w", len(cands), lastErr)
		}

		// Everyone's at budget. Back off briefly and re-check. The AIMD
		// controller tick (3 s) + naturally-completing in-flight requests
		// free up slots as we wait.
		delay := 100 * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		waited += delay
	}
	return nil, fmt.Errorf("all %d eligible endpoints saturated for 5s", len(eligible))
}

// pickEndpointHint returns the endpoint with the most budget headroom right
// now, or nil if none qualify. Used for soft routing — callers still round-
// robin but can prefer a less-loaded endpoint under contention. Not wired
// in yet; keep budget observable while we tune the heuristic.
func (m *MultiClient) pickEndpointHint(eligible []*Endpoint) *Endpoint {
	if len(eligible) == 0 {
		return nil
	}
	best := eligible[0]
	for _, ep := range eligible[1:] {
		if ep.ConcurrencyBudget()-int(ep.Metrics().InFlight) >
			best.ConcurrencyBudget()-int(best.Metrics().InFlight) {
			best = ep
		}
	}
	return best
}

func (m *MultiClient) Close() {
	for _, ep := range m.endpoints {
		ep.Client.Close()
	}
}

func (m *MultiClient) healthy() []*Endpoint {
	out := make([]*Endpoint, 0, len(m.endpoints))
	for _, ep := range m.endpoints {
		if !ep.Disabled {
			out = append(out, ep)
		}
	}
	return out
}
