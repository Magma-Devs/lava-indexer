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

	// Chain-wide genesis info, resolved once via FetchGenesis. Populated
	// from the first healthy RPC endpoint's /genesis response. Not every
	// chain starts at block 1 (e.g. Lava testnet-2 starts at 340,778 due
	// to a net reset), so the dashboard / window-pct math MUST use this
	// rather than hardcoding 1. Zero until FetchGenesis succeeds.
	genesisHeight int64
	genesisTime   time.Time
}

// Genesis returns the chain's initial block height as reported by /genesis.
// Returns 1 as a safe default when FetchGenesis hasn't run / failed — the
// conservative assumption for the (common) case of chains that do start
// at 1.
func (m *MultiClient) Genesis() int64 {
	if m.genesisHeight <= 0 {
		return 1
	}
	return m.genesisHeight
}

// GenesisTime returns the chain's genesis timestamp as reported by /genesis.
// Zero value when unresolved.
func (m *MultiClient) GenesisTime() time.Time { return m.genesisTime }

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

	metrics *metricsTracker
	history *metricsHistory
}

// Metrics returns a snapshot of live throughput/latency for this endpoint.
func (e *Endpoint) Metrics() EndpointMetrics {
	if e.metrics == nil {
		return EndpointMetrics{}
	}
	return e.metrics.snapshot()
}

// MetricHistory returns the recent snapshots for time-series rendering.
func (e *Endpoint) MetricHistory() []MetricSample {
	if e.history == nil {
		return nil
	}
	return e.history.snapshot()
}

// MultiClientOptions tunes the per-endpoint metrics history and the
// metrics-recorder tick cadence.
//
// There is deliberately no per-endpoint concurrency cap here. Earlier
// iterations ran an AIMD controller that shrank per-endpoint budget on
// latency spikes and error rate; for operators running owned RPC nodes
// that they're comfortable overwhelming, the cap became throughput on
// the table (budgets pinned at Min=4 with 90+ fetch workers idle). The
// only limit now is the process-wide fetch-worker count, which is
// already resource-sized by the adaptive sizer (CPU × 8, mem/2 MiB).
// Operators running against shared public gateways should either set
// fetch_workers modestly in config, or re-introduce a per-endpoint cap
// as a config field; the routing layer picks the fastest endpoint, so
// uneven per-node throughput naturally biases load away from anything
// that's actively slowing down.
type MultiClientOptions struct {
	HistorySeconds int // default 1800 (30 min of 1-sec samples)
}

func NewMulti(eps []*Endpoint) *MultiClient {
	return NewMultiWithOptions(eps, MultiClientOptions{})
}

func NewMultiWithOptions(eps []*Endpoint, opts MultiClientOptions) *MultiClient {
	if opts.HistorySeconds == 0 {
		opts.HistorySeconds = 1800
	}
	for _, e := range eps {
		if e.metrics == nil {
			e.metrics = newMetricsTracker()
		}
		if e.history == nil {
			e.history = newMetricsHistory(opts.HistorySeconds)
		}
	}
	return &MultiClient{endpoints: eps}
}

// StartMetricsRecorder spawns a goroutine that samples per-endpoint
// metrics every `interval` and appends to the history ring. Returns
// when ctx is cancelled. This used to tick an AIMD controller as well;
// with the controller gone, it's purely an observation feed for the
// dashboard time-series.
func (m *MultiClient) StartMetricsRecorder(ctx context.Context, interval time.Duration) {
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
					ep.history.add(now, ep.Metrics())
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

// FetchGenesis resolves the chain's genesis (initial_height + genesis_time)
// from the first healthy RPC endpoint. Subsequent access goes via
// Genesis() / GenesisTime().
//
// Idempotent: does nothing once resolved. REST-only clients can't answer
// /genesis, so if every healthy endpoint is REST we log a warning and
// leave the genesis at the "assume 1" default — acceptable for chains
// that actually start at 1, wrong-but-not-catastrophic otherwise.
func (m *MultiClient) FetchGenesis(ctx context.Context) {
	if m.genesisHeight > 0 {
		return
	}
	for _, ep := range m.healthy() {
		rc, ok := ep.Client.(*RPCClient)
		if !ok {
			continue
		}
		info, err := rc.Genesis(ctx)
		if err != nil {
			slog.Warn("genesis fetch failed, trying next endpoint",
				"url", ep.URL, "err", err)
			continue
		}
		m.genesisHeight = info.InitialHeight
		m.genesisTime = info.GenesisTime
		slog.Info("chain genesis resolved",
			"url", ep.URL,
			"initial_height", info.InitialHeight,
			"genesis_time", info.GenesisTime,
			"chain_id", info.ChainID)
		return
	}
	slog.Warn("no RPC endpoint could answer /genesis; chain_genesis falls back to 1")
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
// the requested heights. This is the AUTHORITATIVE "I tried everyone" loop
// in the system — there is no outer retry wrapper, so once FetchBlocks
// returns the pipeline either bisects (multi-block batch) or dead-letters
// (size-1).
//
// Routing policy (uncapped model):
//
//   - Filter to endpoints whose coverage window includes every requested
//     height.
//   - Rank eligible endpoints by a pending-work score:
//         score = p50_latency_ms × (1 + in_flight)
//     lowest wins. This naturally routes new work to the fastest currently-
//     least-busy endpoint without any explicit concurrency cap. An endpoint
//     that's slow OR has queued-up requests gets ranked down; an endpoint
//     that recovers faster than its peers earns back traffic on its next
//     snapshot. Ties randomise via a round-robin counter for fairness when
//     metrics aren't yet populated.
//   - On *ErrRateLimited (429) or *ErrServerError (5xx), fail over
//     immediately to the next candidate. HTTP layer doesn't retry these in
//     place — sick/throttled nodes don't recover inside a 200 ms backoff.
//   - On any other error (network err that survived the HTTP layer's one
//     in-place retry, decode err), also try the next candidate.
//   - Returns the last error if every eligible endpoint failed.
//
// No saturation-wait loop: there is no per-endpoint concurrency cap, so
// "saturated" at the routing layer is no longer a real state. If an
// upstream is slow enough to make everything queue, that shows up as
// higher p50 and the score naturally biases load away from it.
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

	// Rank by pending-work score. Lower is better.
	//
	//     score = p50 × (1 + in_flight) × (1 + errRate × 3)
	//
	// The three terms together:
	//   - p50 captures how fast this endpoint has been completing requests.
	//   - (1 + in_flight) dampens piling onto an already-queued endpoint.
	//   - (1 + errRate × 3) dampens piling onto a broken endpoint. Errors
	//     typically complete fast (429 / 5xx fast-fail), so p50 stays low
	//     even when the endpoint is returning failures — without an
	//     explicit error penalty, a broken endpoint keeps winning the
	//     routing decision and self-reinforces its own broken-ness. A 0.5
	//     error rate makes the score 2.5× worse; a 1.0 rate makes it 4×
	//     worse. Moderate, not quarantining — a briefly-degraded endpoint
	//     can still earn its way back without waiting for a 500-entry
	//     latency ring to rotate out of its old slow measurements.
	type cand struct {
		ep    *Endpoint
		score float64
	}
	cands := make([]cand, 0, len(eligible))
	for _, ep := range eligible {
		m := ep.Metrics()
		p50 := m.LatencyP50Ms
		if p50 == 0 {
			p50 = 100 // unmeasured: treat as average so new endpoints get explored
		}
		errRate := 0.0
		if m.TotalRequests > 0 {
			errRate = float64(m.TotalErrors) / float64(m.TotalRequests)
		}
		cands = append(cands, cand{
			ep:    ep,
			score: p50 * float64(1+m.InFlight) * (1 + errRate*3),
		})
	}
	rr := int(m.next.Add(1) - 1)
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].score != cands[j].score {
			return cands[i].score < cands[j].score
		}
		return (i+rr)%len(cands) < (j+rr)%len(cands)
	})

	var lastErr error
	for _, c := range cands {
		ep := c.ep
		done := ep.metrics.begin()
		blocks, err := ep.Client.FetchBlocks(ctx, heights)
		done(err)
		if err == nil {
			return blocks, nil
		}
		lastErr = err
		var rl *ErrRateLimited
		var se *ErrServerError
		switch {
		case errors.As(err, &rl):
			slog.Debug("endpoint rate-limited, failing over",
				"url", ep.URL, "retry_after", rl.RetryAfter)
		case errors.As(err, &se):
			slog.Debug("endpoint server-error, failing over",
				"url", ep.URL, "status", se.Status, "retry_after", se.RetryAfter)
		default:
			slog.Warn("fetch failed, trying next endpoint",
				"url", ep.URL, "err", err)
		}
	}
	return nil, fmt.Errorf("all %d eligible endpoints failed; last: %w", len(cands), lastErr)
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
